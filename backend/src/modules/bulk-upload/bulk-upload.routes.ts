import { Router, type NextFunction, type Response } from "express";
import { randomUUID } from "crypto";
import { requireAuth } from "../../middleware/authMiddleware.js";
import { requireRole } from "../../middleware/requireRole.js";
import type { AuthenticatedRequest } from "../../middleware/authMiddleware.js";
import { db } from "../../db/mysql.js";
import type { RowDataPacket, ResultSetHeader } from "mysql2";
import { startBatchJob, getBatchJob, readBatchProgress } from "./batch-job.js";
import { buildScopeWhereClause } from "../../shared/scopeAccess.js";
import { loadRowsWithLiveStatus, reconcileStuckRows } from "./bulk-approval.service.js";
import { withDeadlockRetry } from "../../shared/deadlockRetry.js";

/**
 * A batch left in 'importing' for longer than this is assumed to be from an API that
 * died mid-import, and is released so the uploader can retry.
 */
const STALE_IMPORT_MINUTES = 15;

const router = Router();
const h = (fn: (req: AuthenticatedRequest, res: Response) => Promise<unknown>) =>
  (req: import("express").Request, res: Response, next: NextFunction) => fn(req as AuthenticatedRequest, res).catch(next);

interface UploadBatchRow extends RowDataPacket {
  id: string;
}
router.use(requireAuth);

router.get("/templates", requireRole("admin", "hr", "super_admin", "wfm", "wfm_analyst", "payroll", "payroll_hr"), h(async (_req: AuthenticatedRequest, res: Response) => {
  try {
    const [rows] = await db.execute<RowDataPacket[]>(
      "SELECT * FROM upload_template_master WHERE active_status = 1 ORDER BY upload_type_code ASC"
    );
    res.json({ success: true, data: rows });
  } catch (err: unknown) {
    // Table may not exist yet — return empty array gracefully
    if (typeof err === "object" && err !== null) {
      const code = String((err as { code?: unknown }).code ?? "");
      const message = String((err as { message?: unknown }).message ?? "");
      if (code === "ER_NO_SUCH_TABLE" || message.includes("doesn't exist")) {
        return res.json({ success: true, data: [] });
      }
    }
    throw err;
  }
}));

/**
 * Upload Batch History.
 *
 * WHAT THIS USED TO DO. `SELECT * FROM upload_batch ORDER BY created_at DESC LIMIT 50` — no scope
 * of any kind. Every role on the guard above saw every other user's uploads, from every branch,
 * including the original file name and row counts of work that was none of their business.
 *
 * WHAT IT DOES NOW. A caller always sees their own uploads, plus whatever their assignment scope
 * entitles them to, and nothing else:
 *
 *   own            uploaded_by = me. Unconditional — you can always find the file you uploaded.
 *   scope          buildScopeWhereClause(), the same helper this module's approval service already
 *                  uses. super_admin resolves to 1=1; a branch-scoped user gets their branch; a
 *                  user with no assignment scope gets 1=0 and is left with their own uploads only.
 *
 * EFFECTIVE BRANCH. upload_batch.branch_id is populated on only 32 of 65 live rows, so scoping on
 * that column alone would hide two thirds of the history from a branch head — including uploads
 * genuinely belonging to their branch. The uploader's own branch is used as the fallback, which
 * resolves for all 65, so branch scope means what a reader expects rather than what happens to
 * have been stamped.
 *
 * WHO RAISED IT. auth_user carries no name (email only), so the display name comes from the
 * employee record joined on user_id — populated for all 65 rows — falling back to the login email.
 */
router.get("/batches", requireRole("admin", "hr", "super_admin", "wfm", "wfm_analyst", "payroll", "payroll_hr"), h(async (req: AuthenticatedRequest, res: Response) => {
  const userId = req.authUser!.id;
  const scope = await buildScopeWhereClause(
    userId,
    ["admin", "hr", "wfm", "wfm_analyst", "payroll", "payroll_hr", "branch_head", "branch_admin"],
    { branchId: "COALESCE(ub.branch_id, uploader_emp.branch_id)" },
    { allowAdminBypass: true },
  );

  const where: string[] = [`(ub.uploaded_by = ? OR (${scope.sql}))`];
  const params: unknown[] = [userId, ...scope.params];

  // Filters. Each is optional and additive; an absent filter never narrows the result.
  const uploadType = String(req.query.uploadType ?? "").trim();
  if (uploadType) { where.push("ub.upload_type_code = ?"); params.push(uploadType); }

  const status = String(req.query.status ?? "").trim();
  if (status) { where.push("ub.batch_status = ?"); params.push(status); }

  const uploadedBy = String(req.query.uploadedBy ?? "").trim();
  if (uploadedBy) { where.push("ub.uploaded_by = ?"); params.push(uploadedBy); }

  // Dates are compared on the date part so an inclusive "to" does not silently drop same-day rows.
  const from = String(req.query.from ?? "").trim();
  if (/^\d{4}-\d{2}-\d{2}$/.test(from)) { where.push("DATE(ub.created_at) >= ?"); params.push(from); }
  const to = String(req.query.to ?? "").trim();
  if (/^\d{4}-\d{2}-\d{2}$/.test(to)) { where.push("DATE(ub.created_at) <= ?"); params.push(to); }

  const search = String(req.query.search ?? "").trim();
  if (search) {
    where.push("(ub.upload_batch_no LIKE ? OR ub.original_file_name LIKE ?)");
    params.push(`%${search}%`, `%${search}%`);
  }

  const limit = Math.min(200, Math.max(1, Number(req.query.limit ?? 50) || 50));

  const [rows] = await db.execute<RowDataPacket[]>(
    `SELECT ub.*,
            COALESCE(NULLIF(TRIM(uploader_emp.full_name), ''), uploader_user.email, ub.uploaded_by)
              AS uploaded_by_name,
            uploader_emp.employee_code AS uploaded_by_code,
            COALESCE(ub.branch_id, uploader_emp.branch_id) AS effective_branch_id,
            bm.branch_name AS branch_name
       FROM upload_batch ub
       LEFT JOIN employees  uploader_emp  ON uploader_emp.user_id = ub.uploaded_by
       LEFT JOIN auth_user  uploader_user ON uploader_user.id     = ub.uploaded_by
       LEFT JOIN branch_master bm ON bm.id = COALESCE(ub.branch_id, uploader_emp.branch_id)
      WHERE ${where.join(" AND ")}
      ORDER BY ub.created_at DESC
      LIMIT ${limit}`,
    params,
  );
  res.json({ success: true, data: rows });
}));

/**
 * The filter dropdown options, built from what this caller can actually see.
 *
 * Deliberately not a hardcoded list: CLAUDE.md's Form Input Rule requires option lists to come
 * from the real observed domain, and a type the user has never uploaded is noise in their filter.
 * Scoped identically to the list above, so the options can never hint at the existence of another
 * branch's uploads.
 */
router.get("/batches/filter-options", requireRole("admin", "hr", "super_admin", "wfm", "wfm_analyst", "payroll", "payroll_hr"), h(async (req: AuthenticatedRequest, res: Response) => {
  const userId = req.authUser!.id;
  const scope = await buildScopeWhereClause(
    userId,
    ["admin", "hr", "wfm", "wfm_analyst", "payroll", "payroll_hr", "branch_head", "branch_admin"],
    { branchId: "COALESCE(ub.branch_id, uploader_emp.branch_id)" },
    { allowAdminBypass: true },
  );
  const visible = `(ub.uploaded_by = ? OR (${scope.sql}))`;
  const params = [userId, ...scope.params];

  const [types] = await db.execute<RowDataPacket[]>(
    `SELECT ub.upload_type_code AS value, COUNT(*) AS n
       FROM upload_batch ub
       LEFT JOIN employees uploader_emp ON uploader_emp.user_id = ub.uploaded_by
      WHERE ${visible} GROUP BY ub.upload_type_code ORDER BY n DESC`, params);
  const [statuses] = await db.execute<RowDataPacket[]>(
    `SELECT ub.batch_status AS value, COUNT(*) AS n
       FROM upload_batch ub
       LEFT JOIN employees uploader_emp ON uploader_emp.user_id = ub.uploaded_by
      WHERE ${visible} GROUP BY ub.batch_status ORDER BY n DESC`, params);
  const [uploaders] = await db.execute<RowDataPacket[]>(
    `SELECT ub.uploaded_by AS value,
            COALESCE(NULLIF(TRIM(uploader_emp.full_name), ''), uploader_user.email, ub.uploaded_by) AS label,
            COUNT(*) AS n
       FROM upload_batch ub
       LEFT JOIN employees uploader_emp ON uploader_emp.user_id = ub.uploaded_by
       LEFT JOIN auth_user uploader_user ON uploader_user.id    = ub.uploaded_by
      WHERE ${visible} GROUP BY ub.uploaded_by, label ORDER BY n DESC`, params);

  res.json({ success: true, data: { types, statuses, uploaders } });
}));

/**
 * Each row now carries the ground truth alongside its own row_status:
 * `entity_created` — does a real record exist for this row at all — and
 * `entity_status` — that record's CURRENT status, read live from the table it
 * actually lives in (attendance_regularization / leave_request / the incentive or
 * deduction record). row_status is upload_batch_row's own bookkeeping and, before
 * this, was the only thing shown — see loadRowsWithLiveStatus's own comment for why
 * that alone was not trustworthy.
 */
router.get("/batches/:id/rows", requireRole("admin", "hr", "super_admin", "wfm", "wfm_analyst", "payroll", "payroll_hr"), h(async (req: AuthenticatedRequest, res: Response) => {
  const rows = await loadRowsWithLiveStatus(req.params.id);
  res.json({ success: true, data: rows });
}));

/**
 * DELETE /batches/:id — removes ONLY the upload log entry (upload_batch and its
 * upload_batch_row children). Per explicit user instruction, it never touches the
 * data an already-imported batch wrote into its target table (e.g. db_masmis.bb_sale)
 * — that is real business data, not log housekeeping, and stays untouched regardless
 * of the batch's status.
 *
 * A caller may delete their own upload; admin/super_admin may delete any.
 */
router.delete("/batches/:id", requireRole("admin", "hr", "super_admin", "wfm", "wfm_analyst", "payroll", "payroll_hr"), h(async (req: AuthenticatedRequest, res: Response) => {
  const { id } = req.params;
  const [rows] = await db.execute<RowDataPacket[]>(
    "SELECT id, uploaded_by FROM upload_batch WHERE id = ? LIMIT 1",
    [id],
  );
  const batch = rows[0];
  if (!batch) return res.status(404).json({ success: false, error: "Upload batch not found" });

  if (batch.uploaded_by !== req.authUser!.id) {
    const { hasAnyRole } = await import("../../shared/scopeAccess.js");
    if (!(await hasAnyRole(req.authUser!.id, "admin", "super_admin"))) {
      return res.status(403).json({ success: false, error: "You can only delete your own uploads." });
    }
  }

  await db.execute("DELETE FROM upload_batch_row WHERE upload_batch_id = ?", [id]);
  await db.execute("DELETE FROM upload_batch WHERE id = ?", [id]);
  res.json({ success: true });
}));

/**
 * On-demand healing for a batch stuck with rows that never reached a final outcome
 * (row_status still 'pending'/'valid' after the batch itself is already decided) —
 * the exact failure mode reconcileStuckRows exists to close off going forward. This
 * lets an admin repair a batch from BEFORE that fix shipped without needing direct
 * SQL access. It only ever force-resolves rows that are already stuck; it never
 * touches a row that has a real outcome.
 */
router.post("/batches/:id/reconcile", requireRole("admin", "super_admin"), h(async (req: AuthenticatedRequest, res: Response) => {
  const result = await reconcileStuckRows(req.params.id);
  res.json({ success: true, data: result });
}));

router.post("/batches", requireRole("admin", "hr", "super_admin", "wfm", "wfm_analyst", "payroll", "payroll_hr"), h(async (req: AuthenticatedRequest, res: Response) => {
  const body = req.body as {
    upload_batch_no?: string; upload_type_code: string; original_file_name?: string;
    file_path?: string; file_size_bytes?: number; total_rows: number; valid_rows: number;
    error_rows: number; batch_status?: string; error_summary?: string; metadata?: Record<string, unknown>;
  };
  if (!body.upload_type_code) {
    return res.status(400).json({ error: "upload_type_code is required" });
  }
  if (body.total_rows === undefined || body.valid_rows === undefined || body.error_rows === undefined) {
    return res.status(400).json({ error: "total_rows, valid_rows, and error_rows are required" });
  }
  const id = randomUUID();
  const batchNo = body.upload_batch_no || `BATCH-${Date.now()}`;
  // withDeadlockRetry is safe here: this is one autocommit statement (no explicit
  // transaction), and it is idempotent on retry — a lost deadlock rolls the whole INSERT
  // back (nothing partially written), and `id` was generated once above, so a retry
  // replays the exact same row rather than creating a duplicate.
  await withDeadlockRetry(() => db.execute(
    `INSERT INTO upload_batch (id, upload_batch_no, upload_type_code, original_file_name, file_path,
     file_size_bytes, total_rows, valid_rows, error_rows, batch_status, error_summary, metadata,
     uploaded_by, validated_by, validated_at)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    [id, batchNo, body.upload_type_code, body.original_file_name ?? null, body.file_path ?? null,
     body.file_size_bytes ?? null, body.total_rows, body.valid_rows, body.error_rows,
     body.batch_status ?? "pending", body.error_summary ?? null,
     body.metadata ? JSON.stringify(body.metadata) : null,
     req.authUser!.id,
     body.valid_rows > 0 ? req.authUser!.id : null,
     body.valid_rows > 0 ? new Date().toISOString().slice(0, 19).replace("T", " ") : null]
  ));
  const [rows] = await db.execute<UploadBatchRow[]>("SELECT * FROM upload_batch WHERE id = ? LIMIT 1", [id]);
  res.status(201).json({ success: true, data: rows[0] ?? null });
}));

router.post("/batches/:id/rows", requireRole("admin", "hr", "super_admin", "wfm", "wfm_analyst", "payroll", "payroll_hr"), h(async (req: AuthenticatedRequest, res: Response) => {
  const rows = req.body as Array<{
    row_no: number;
    raw_data?: Record<string, unknown> | unknown[] | string | null;
    normalized_data?: Record<string, unknown> | unknown[] | string | null;
    row_status?: string;
    error_messages?: string[] | string | null;
  }>;
  if (!Array.isArray(rows) || rows.length === 0) {
    return res.status(400).json({ error: "rows array required" });
  }
  // A single multi-row INSERT instead of one round trip per row — with a few
  // hundred rows the old per-row loop alone could take longer than the
  // frontend's 30s request timeout, which is what produced the "batch didn't
  // upload" report even though staging had actually succeeded.
  const values: unknown[] = [];
  const placeholders: string[] = [];
  for (const row of rows) {
    placeholders.push("(?, ?, ?, ?, ?, ?, ?)");
    values.push(
      randomUUID(), req.params.id, row.row_no,
      row.raw_data ? JSON.stringify(row.raw_data) : null,
      row.normalized_data ? JSON.stringify(row.normalized_data) : null,
      row.row_status ?? "pending",
      row.error_messages ? JSON.stringify(row.error_messages) : null
    );
  }
  // withDeadlockRetry is safe here for the same reason as the /batches INSERT above:
  // one autocommit statement, and every row's id was generated once above the retry, so
  // a retry replays the identical INSERT rather than double-staging rows. This is the
  // exact write that silently lost BATCH-1788948395588-R6909's 14 resubmitted rows to a
  // deadlock — the batch header had already been created with "14 valid" before this
  // statement ran, and when it lost the deadlock the rows were simply never saved, with
  // nothing left to show for it beyond a batch that claimed rows it didn't have.
  //
  // Also the exact write behind losing 6 Onfido DOC_RAW files (~137k rows) the same week —
  // that one was ER_LOCK_WAIT_TIMEOUT rather than ER_LOCK_DEADLOCK (both retried the same way
  // here), from several ~2000-row chunk uploads landing minutes apart and colliding on this
  // same table. A quick default backoff (100/200/300/400ms) is tuned for a momentary deadlock
  // between two short statements; a lock-wait-timeout on a chunk this size reflects a real,
  // possibly multi-second hold by a sibling chunk insert, so this call site gets a longer,
  // more generous backoff and more attempts than the default — the client-side timeout on
  // this endpoint (180s normal upload, 60s resubmit) has the headroom for it.
  await withDeadlockRetry(() => db.execute(
    `INSERT INTO upload_batch_row (id, upload_batch_id, row_no, raw_data, normalized_data, row_status, error_messages)
     VALUES ${placeholders.join(", ")}`,
    values
  ), { attempts: 6, delayMs: 400 });
  res.status(201).json({ success: true, count: rows.length });
}));

const KNOWN_IMPORT_RPCS = new Set([
  "import_official_email_update_batch",
  "import_pf_uan_batch",
  "import_reporting_manager_update_batch",
  "import_roster_assignment_batch",
  "import_weekoff_preference_batch",
  "import_shift_rotation_type_batch",
  "import_shift_roster_batch",
  "import_upload_batch",
  "import_process_upload_batch",
  "import_department_upload_batch",
  "import_asset_upload_batch",
  "import_branch_upload_batch",
  "import_lob_upload_batch",
  "import_designation_upload_batch",
  // Approval-gated types. These stage rows into their real domain tables in a pending
  // state; nothing applies until a Branch Head approves via /approvals/batches/:id/approve.
  "import_attendance_regularization_batch",
  "import_leave_application_batch",
  "import_incentive_bulk_batch",
  "import_deduction_bulk_batch",
  // Onfido process raw-data reports (DOC/POA volume, quality-audit, client-escalation) —
  // all seven share one generic import service; see onfido-report-configs.ts.
  "import_onfido_doc_raw_batch",
  "import_onfido_doc_quality_batch",
  "import_onfido_cre_batch",
  "import_onfido_crq_batch",
  "import_onfido_poa_raw_batch",
  "import_onfido_poa_trial_batch",
  "import_onfido_poa_quality_batch",
  "import_onfido_external_audit_batch",
  "import_onfido_doc_etm_batch",
  "import_onfido_poa_etm_batch",
  "import_onfido_task_skip_batch",
  "import_onfido_agent_daily_batch",
  // Bella Vita daily target plan - the one dataset no system emits. Sales,
  // cart leads, cancellations/RTO, call detail and inbound SLA are all NOT
  // here: db_masmis and dialer_db already hold them. See bella-report-configs.ts.
  "import_bella_target_plan_batch",
  // Floor compliance audit - a Google Form export; see compliance-audit-bulk.service.ts.
  "import_compliance_audit_batch",
  // Process-grain manual KPI feed - fills the gap left by db_masmis sales/allocation
  // tables that stopped being uploaded; see process-manual-kpi-bulk.service.ts.
  "import_process_manual_kpi_batch",
  // Per-process delivery actuals into process_delivery_actual, which the P&L already
  // reads but nothing has ever written. See process-delivery-bulk.service.ts.
  "import_process_delivery_batch",
  // Molecular Email / Reginald Men Email daily ticket actuals — the underlying
  // ticketing DB (molecular_db_email) does not exist anywhere in this project's
  // infrastructure. See email-ticket-daily-bulk.service.ts.
  "import_email_ticket_daily_batch",
  // LP WebConsole APR — dialer_db.apr_5/apr_137_235/apr_bla_bli_blu (where this
  // would otherwise land) are confirmed empty, a dead sync job. See
  // lp-apr-daily-bulk.service.ts.
  "import_lp_apr_daily_batch",
  // Clovia Email Dashboard, daily per agent — columns read verbatim from a
  // real sample ("Clovia Email Tracker Sept'26.xlsb"); no DB backing exists
  // anywhere. See clovia-email-daily-bulk.service.ts.
  "import_clovia_email_daily_batch",
  // Clovia Chat Performance, daily (Botlytics chat dump) — columns read
  // verbatim from a real sample; no DB backing exists anywhere. See
  // clovia-chat-daily-bulk.service.ts.
  "import_clovia_chat_daily_batch",
  // Clovia CRM Disposition, per ticket — columns read verbatim from a real
  // sample; no DB backing exists anywhere. See
  // clovia-crm-disposition-bulk.service.ts.
  "import_clovia_crm_disposition_batch",
  // Housing Premium's "Sale Raw" -- per its SOP, a manually-updated Google
  // Sheet with no DB backing anywhere. See
  // housing-premium-sale-raw-bulk.service.ts.
  "import_housing_premium_sale_raw_batch",
  // Housing Owner's "Sale Raw" -- per its SOP, sale data pasted "up to the
  // Discount % column" into a Google Sheet with no DB backing anywhere. See
  // housing-owner-sale-raw-bulk.service.ts.
  "import_housing_owner_sale_raw_batch",
  // LP BPO Leads (M) export, Regional/Non Regional dashboards -- columns read
  // verbatim from real samples; no DB backing exists anywhere. See
  // lp-leads-bulk.service.ts.
  "import_lp_leads_regional_batch",
  "import_lp_leads_non_regional_batch",
  // DU Digital's Agents Time details export, Korea/Thailand dashboards --
  // columns read verbatim from real samples; no DB backing exists anywhere.
  // See du-apr-daily-bulk.service.ts.
  "import_du_apr_korea_batch",
  "import_du_apr_thailand_batch",
  // LP's Mascallnet NRGN Call History export, Regional/Non Regional
  // dashboards -- columns read verbatim from real samples; no DB backing
  // exists anywhere. See lp-cdr-cr-report-bulk.service.ts. (The sibling
  // BPO CR Reports/CDR sheet's own table, lp_cdr_raw, was RETRACTED
  // 2026-09-10 -- db_masmis.CR_lp_regional/CR_lp_non_regional already
  // carry that exact data live.)
  "import_lp_cr_report_regional_batch",
  "import_lp_cr_report_non_regional_batch",
  // DU Digital's Agent ID -> MAS employee code directory, Korea/Thailand
  // dashboards -- found while auditing the same workbooks used for DU APR;
  // no DB backing exists anywhere. See du-team-mapping-bulk.service.ts.
  "import_du_team_mapping_korea_batch",
  "import_du_team_mapping_thailand_batch",
  // Housing Premium's per-agent monthly sales Target & Achievement --
  // found while auditing the same workbook used for Sale Raw; no DB
  // backing exists anywhere. See housing-premium-agent-target-bulk.service.ts.
  "import_housing_premium_agent_target_batch",
  // Housing Owner's per-agent monthly Incentive payout -- found while
  // auditing the same workbook used for Sale Raw/Call Logs; source is
  // partially corrupted (broken-formula cells), only clean cells are
  // imported. See housing-owner-incentive-bulk.service.ts.
  "import_housing_owner_incentive_batch",
  // Housing Owner's CRM lead/opportunity pipeline log (Look up Data) --
  // found while auditing the same workbook; the largest single sheet
  // found this session (398,363 rows). See
  // housing-owner-lead-pipeline-bulk.service.ts.
  "import_housing_owner_lead_pipeline_batch",
  // GNC's Agent Productivity Report -- writes into the SAME live
  // db_masmis.gnc_apr table Mydashboards already uses (confirmed real but
  // stale, last row 2026-05-30), per explicit user instruction. Reconciled
  // 2026-09-10 from an earlier new-table approach; see sql/1741 and
  // gnc-apr-masmis-bulk.service.ts.
  "import_gnc_apr_batch",
  // Bla Bli Blu's real Dial Desk complaint/query ticket export -- an HTML
  // export off the DialDesk website, no DB backing anywhere. See
  // bla-bli-blu-dd-tagging-bulk.service.ts.
  "import_bla_bli_blu_dd_tagging_batch",
  // GNC's "Date & Camp wise Overall Sale" sheet -- per explicit user
  // instruction, writes into the SAME already-live db_masmis.gnc_sale
  // table the separate My Dashboards tool (github.com/tausifansari-mcn/
  // Mydashboards) already uses, rather than a new mas_hrms table. See
  // gnc-sale-masmis-bulk.service.ts.
  "import_gnc_sale_masmis_batch",
  // Bla Bli Blu's real Auto Call Back / after-hours contact logs -- HTML
  // exports off the same DialDesk/Smartping websites as sql/1739's DD
  // Tagging file, found in the same local folder. See
  // bla-bli-blu-auto-callback-bulk.service.ts / bla-bli-blu-after-hour-
  // bulk.service.ts.
  // Reginald Men Abandoned Cart Dashboard's real Live Sales Google Form
  // export -- see sql/1752 / reginald-abandoned-cart-sales-bulk.service.ts.
  "import_reginald_abandoned_cart_sales_batch",
  "import_bla_bli_blu_auto_callback_batch",
  "import_bla_bli_blu_after_hour_batch",
  // Bla Bli Blu's real Smartping CDR export -- disposition/outcome fields
  // only (call metadata is already live in dialer_db.cdr_bla_bli_blu,
  // joined by Session Id = call_uuid). See
  // bla-bli-blu-call-disposition-bulk.service.ts.
  "import_bla_bli_blu_call_disposition_batch",
  // Bla Bli Blu's real direct Shopify order export, distinct from the
  // workbook's own curated Overall Sales Raw sheet (sql/1729). See
  // bla-bli-blu-shopify-sales-bulk.service.ts.
  "import_bla_bli_blu_shopify_sales_batch",
  // Bellavita's real "Sale" sheet -- writes into the SAME already-live
  // db_masmis.bb_sale table Mydashboards already uses. See
  // bb-sale-masmis-bulk.service.ts.
  "import_bb_sale_masmis_batch",
  "import_bb_apr_masmis_batch",
  "import_bvo_repeat_cdr_masmis_batch",
  "import_bvo_repeat_allocation_masmis_batch",
  "import_bb_cart_masmis_batch",
  "import_bb_chat_masmis_batch",
  "import_bvo_order_export_masmis_batch",
  "import_neemans_sale_raw_masmis_batch",
  "import_neemans_allocation_masmis_batch",
  "import_neemans_cart_masmis_batch",
  "import_neemans_apr_masmis_batch",
  "import_gnc_allocation_masmis_batch",
  "import_neemans_month_target_batch",
  "import_neemans_agent_details_batch",
  "import_aw_billing_batch",
  "import_aw_inbound_batch",
  "import_aw_mandate_batch",
  "import_aw_new_cdr_batch",
  "import_aw_out_batch",
  "import_owner_sale_batch",
  "import_owner_cdr_batch",
  "import_owner_agent_details_batch",
  "import_pre_sale_batch",
  "import_pre_cdr_batch",
  "import_pre_agent_details_batch",
  "import_cl_apr_batch",
  "import_cl_chat_batch",
  "import_cl_dispo_batch",
  "import_cl_email_raw_batch",
  "import_cl_feedback_batch",
  "import_cl_ib_cdr_batch",
  "import_cl_outbound_batch",
  "import_cl_quality_batch",
  "import_cl_rechurn_call_batch",
  "import_birlanu_sale_batch",
  "import_birlanu_apr_batch",
  "import_satya_allocation_batch",
  "import_satya_cdr_batch",
  "import_lp_feedback_apr_batch",
  "import_lp_feedback_cdr_batch",
  "import_lp_onboarding_apr_batch",
  "import_lp_onboarding_cdr_batch",
  "import_gnc_chat_batch",
  "import_neemans_chat_batch",
]);

// POST /batches/:id/import — dispatch import by rpc_name
router.post("/batches/:id/import", requireRole("admin", "hr", "super_admin", "wfm", "wfm_analyst", "payroll", "payroll_hr"), h(async (req: AuthenticatedRequest, res: Response) => {
  const { id } = req.params;
  const { rpc_name } = req.body as { rpc_name?: string };

  if (!rpc_name || !KNOWN_IMPORT_RPCS.has(rpc_name)) {
    return res.status(501).json({
      success: false,
      error: `Import function '${rpc_name || "unknown"}' for batch ${id} is not yet implemented in the MySQL backend.`,
    });
  }

  // A claim left behind by a crashed or restarted API would otherwise block the batch
  // forever: the claim below refuses any batch already 'importing', and nothing ever
  // cleared it. Release one that has not been touched for STALE_IMPORT_MINUTES, the
  // same treatment the approval claim already gets in bulk-approval.service.ts.
  //
  // 'validated' is where a batch sits before an import, and re-importing it is safe:
  // the importers only pick up rows still in 'valid'/'pending', so whatever the dead
  // run managed to write is not written twice.
  await db.execute(
    `UPDATE upload_batch SET batch_status = 'validated', updated_at = NOW()
      WHERE id = ? AND batch_status = 'importing'
        AND updated_at < DATE_SUB(NOW(), INTERVAL ? MINUTE)`,
    [id, STALE_IMPORT_MINUTES]
  );

  // Atomically claim the batch before running the (possibly long-running) import.
  // Without this, a client retry after a false request-timeout — the import
  // itself keeps running server-side even after the client gives up — can fire
  // a second concurrent import of the same batch. The second call finds no
  // 'valid'/'pending' rows left (the first call already flipped them), computes
  // 0 imported / 0 errors, and overwrites the first call's correct summary with
  // a misleading "imported, 0 rows" — which is exactly what happened to
  // BATCH-1787062644877. Rejecting the concurrent call instead keeps the
  // summary that the completed import actually wrote.
  const [claim] = await db.execute<ResultSetHeader>(
    `UPDATE upload_batch SET batch_status = 'importing', updated_at = NOW()
     WHERE id = ? AND batch_status NOT IN ('importing')`,
    [id]
  );
  if (claim.affectedRows === 0) {
    return res.status(409).json({
      success: false,
      error: "This batch is already being imported. Wait for it to finish, then refresh the page — do not resubmit.",
    });
  }

  // The permission checks have to run before the request is answered — a 202 must
  // mean the import is genuinely under way, not that it will fail unseen.
  try {
    await assertGatedUploader(rpc_name, req.authUser!.id);
    await assertDepartmentStructureUploader(rpc_name, req.authUser!.id);
  } catch (err) {
    await db.execute(
      `UPDATE upload_batch SET batch_status = 'validated', updated_at = NOW() WHERE id = ?`,
      [id]
    );
    throw err;
  }

  // Importing runs a domain engine per row — submitRegularization and
  // submitRequest each open a transaction — so a few hundred rows take minutes.
  // Waiting for that inside the request meant nginx closed the connection at 60s
  // and the uploader saw a 502 while the import was still running fine. Detach it
  // and let the page poll /batches/:id/import-status instead.
  const [pending] = await db.execute<RowDataPacket[]>(
    `SELECT COUNT(*) AS n FROM upload_batch_row
      WHERE upload_batch_id = ? AND row_status IN ('valid','pending')`,
    [id]
  );

  /*
   * Guard against BATCH-1788948395588-R6909's failure mode: a batch that claims valid
   * rows (from its own creation payload) but has ZERO rows actually staged in
   * upload_batch_row — at ANY status, not just 'valid'/'pending' — used to run the
   * import anyway, find nothing to do, and report a clean "imported, 0 rows" success,
   * because every importer only checks for ERRORS, never for whether it did anything
   * at all. Root cause there was the staging INSERT (POST /batches/:id/rows) losing a
   * database deadlock after the batch header already claimed "14 valid" — the two are
   * separate requests, so one can succeed while the other silently fails.
   *
   * This must NOT fire for the ordinary, legitimate case of re-importing a batch whose
   * rows already all got consumed by an earlier successful run — those rows still
   * exist, just as 'imported'/'error', which is exactly why `pending` above is 0 for
   * that case too. The only reliable way to tell "nothing left to do" apart from
   * "nothing was ever there" is whether upload_batch_row holds ANY row for this batch,
   * regardless of status — so that is checked separately, only in this already-rare
   * pending===0 branch.
   */
  if (Number((pending as RowDataPacket[])[0]?.n ?? 0) === 0) {
    const [batchRows] = await db.execute<RowDataPacket[]>(
      `SELECT valid_rows FROM upload_batch WHERE id = ? LIMIT 1`, [id]
    );
    const [stagedRows] = await db.execute<RowDataPacket[]>(
      `SELECT COUNT(*) AS n FROM upload_batch_row WHERE upload_batch_id = ?`, [id]
    );
    const validRows = Number((batchRows as RowDataPacket[])[0]?.valid_rows ?? 0);
    const stagedCount = Number((stagedRows as RowDataPacket[])[0]?.n ?? 0);
    if (validRows > 0 && stagedCount === 0) {
      const message = `This batch claims ${validRows} valid row(s), but none were ever saved to the `
        + `database — the upload's row-staging step likely failed or timed out partway through. `
        + `There is nothing here to import. Re-upload the file (or redo Edit & Resubmit) instead.`;
      await db.execute(
        `UPDATE upload_batch SET batch_status = 'validation_failed', error_summary = ?, updated_at = NOW() WHERE id = ?`,
        [message.slice(0, 1000), id]
      );
      return res.status(409).json({ success: false, error: message });
    }
  }

  startBatchJob(
    id,
    "import",
    () => dispatchImport(rpc_name, id, req.authUser!.id),
    async (err) => {
      await db.execute(
        // approval_status is cleared with it. Without that a batch that failed mid-import kept
        // whatever approval stage it had reached, so it stayed in the Branch Head's queue as
        // "pending" forever — a batch that failed can never be approved, and the queue had no
        // way to tell. This is the second half of the duplicate-pending-approval defect.
        `UPDATE upload_batch SET batch_status = 'failed', approval_status = NULL,
                error_summary = ?, updated_at = NOW() WHERE id = ?`,
        [String((err as Error)?.message ?? "Import failed").slice(0, 1000), id]
      );
    },
  );

  return res.status(202).json({
    success: true,
    processing: true,
    job: "import",
    batch_id: id,
    total_rows: Number((pending as RowDataPacket[])[0]?.n ?? 0),
    message: "Import started. Large files are processed a row at a time — the page will keep itself updated.",
  });
}));

/**
 * GET /batches/:id/import-status — where the upload page collects the import result.
 *
 * Terminal state comes from upload_batch rather than the in-process job map, so a
 * page reloaded (or an API restarted) mid-import still reports the truth.
 */
/**
 * GET /batches/active — batches owned by this user currently in 'importing' state.
 *
 * The UI calls this on page mount so a user who closed the page mid-import can pick
 * up the progress bar where they left off, rather than having to know the batch ID or
 * wait for the next refresh.
 *
 * Returns at most the last 5 in-flight batches for this user. The batch_status check
 * includes 'importing' (import in progress) and 'pending_approval' when
 * approval_status is NULL (import still claiming) to cover the edge case where the
 * claim was recorded but the job map was lost in a restart.
 */
router.get("/batches/active", requireRole("admin", "hr", "super_admin", "wfm", "wfm_analyst", "payroll", "payroll_hr"), h(async (req: AuthenticatedRequest, res: Response) => {
  const [rows] = await db.execute<RowDataPacket[]>(
    `SELECT id, upload_batch_no, upload_type_code, batch_status, approval_status,
            total_rows, valid_rows, imported_rows, error_rows, updated_at
       FROM upload_batch
      WHERE uploaded_by = ?
        AND batch_status = 'importing'
      ORDER BY updated_at DESC
      LIMIT 5`,
    [req.authUser!.id],
  );
  res.json({ success: true, data: rows });
}));

router.get("/batches/:id/import-status", requireRole("admin", "hr", "super_admin", "wfm", "wfm_analyst", "payroll", "payroll_hr"), h(async (req: AuthenticatedRequest, res: Response) => {
  const { id } = req.params;
  const [batchRows] = await db.execute<RowDataPacket[]>(
    "SELECT id, batch_status, approval_status, imported_rows, error_rows, total_rows, error_summary FROM upload_batch WHERE id = ? LIMIT 1",
    [id]
  );
  const batch = (batchRows as RowDataPacket[])[0];
  if (!batch) return res.status(404).json({ success: false, error: "Upload batch not found" });

  const job = getBatchJob(id);
  const progress = await readBatchProgress(id, "import");
  const running = batch.batch_status === "importing";
  const phase =
    running ? "running"
    : job?.phase === "failed" || batch.batch_status === "failed" ? "failed"
    : job?.phase === "done" || ["imported", "pending_approval"].includes(String(batch.batch_status)) ? "done"
    : "idle";

  return res.json({
    success: true,
    phase,
    job: "import",
    batch_status: batch.batch_status,
    approval_status: batch.approval_status,
    progress,
    error: job?.phase === "failed" ? job.error : undefined,
    message: job?.phase === "failed" ? job.error : (batch.error_summary ?? null),
    result: job?.phase === "done" ? job.result : undefined,
  });
}));

/**
 * The four approval-gated types write leave balances, attendance and payroll
 * deductions. The generic import guard above admits hr/payroll/admin as well, which is
 * right for a master-data import and wrong here: the agreed uploaders are Super Admin
 * and branch WFM only, and widening that silently would put a deduction upload in
 * reach of roles that were never meant to raise one.
 */
async function assertGatedUploader(rpc_name: string, userId: string): Promise<void> {
  const gated = new Set([
    "import_attendance_regularization_batch",
    "import_leave_application_batch",
    "import_incentive_bulk_batch",
    "import_deduction_bulk_batch",
  ]);
  if (!gated.has(rpc_name)) return;
  const { hasAnyRole } = await import("../../shared/scopeAccess.js");
  const { UPLOADER_ROLES } = await import("./bulk-approval.service.js");
  if (!(await hasAnyRole(userId, ...UPLOADER_ROLES))) {
    throw Object.assign(
      new Error("Only a Super Admin or branch WFM can upload leave, regularization, incentive or deduction batches."),
      { statusCode: 403 },
    );
  }
}

/**
 * department_master writes are super_admin-only everywhere else (requireDepartmentWrite in
 * org.routes.ts), and a spreadsheet is not an exemption.
 *
 * import_department_upload_batch INSERTs ... ON DUPLICATE KEY UPDATE dept_name = VALUES(dept_name),
 * so a row carrying an existing dept_code does not just add a department — it RENAMES one. The
 * generic import guard above admits admin/hr/wfm/wfm_analyst/payroll/payroll_hr, which would have
 * left every role locked out of the Departments UI still able to rename a department by uploading
 * a file. That is the same structure change by another door, so it takes the same gate.
 *
 * Deliberately stricter than assertGatedUploader: that one admits branch WFM alongside Super
 * Admin, which is right for leave and deduction batches and wrong for the org chart.
 */
async function assertDepartmentStructureUploader(rpc_name: string, userId: string): Promise<void> {
  if (rpc_name !== "import_department_upload_batch") return;
  const { hasAnyRole } = await import("../../shared/scopeAccess.js");
  if (!(await hasAnyRole(userId, "super_admin"))) {
    throw Object.assign(
      new Error("Only a Super Admin can create or rename departments, including by upload."),
      { statusCode: 403 },
    );
  }
}

/**
 * Run one import and return the payload the route used to send.
 *
 * It no longer writes the response itself: the import runs after the request has
 * already been answered with 202 (see the route below), so there is no response left
 * to write to by the time this finishes.
 */
async function dispatchImport(
  rpc_name: string,
  id: string,
  userId: string,
): Promise<Record<string, unknown>> {
  await assertGatedUploader(rpc_name, userId);
  await assertDepartmentStructureUploader(rpc_name, userId);

  if (rpc_name === "import_attendance_regularization_batch") {
    const { importRegularizationBatch } = await import(
      "./attendance-regularization-bulk.service.js"
    );
    const data = await importRegularizationBatch(id, userId);
    return { success: true, requires_approval: true, data };
  }

  if (rpc_name === "import_leave_application_batch") {
    const { importLeaveBatch } = await import("./leave-application-bulk.service.js");
    const data = await importLeaveBatch(id, userId);
    return { success: true, requires_approval: true, data };
  }

  if (rpc_name === "import_incentive_bulk_batch") {
    const { importIncentiveBatch } = await import("./incentive-bulk.service.js");
    const data = await importIncentiveBatch(id, userId);
    return { success: true, requires_approval: true, data };
  }

  if (rpc_name === "import_deduction_bulk_batch") {
    const { importDeductionBatch } = await import("./deduction-bulk.service.js");
    const data = await importDeductionBatch(id, userId);
    return { success: true, requires_approval: true, data };
  }

  if (rpc_name === "import_official_email_update_batch") {
    const { importOfficialEmailBatch } = await import(
      "../it-provisioning/it-provisioning.bulk.service.js"
    );
    const data = await importOfficialEmailBatch(id, userId);
    return { success: true, data };
  }

  if (rpc_name === "import_pf_uan_batch") {
    const { importPfUanBatch } = await import(
      "../bulk-upload/pf-uan-bulk.service.js"
    );
    const data = await importPfUanBatch(id, userId);
    return { success: true, data };
  }

  if (rpc_name === "import_reporting_manager_update_batch") {
    const { importReportingManagerBatch } = await import(
      "../bulk-upload/reporting-manager-bulk.service.js"
    );
    const data = await importReportingManagerBatch(id, userId);
    return { success: true, data };
  }

  if (rpc_name === "import_roster_assignment_batch") {
    const { importRosterAssignmentBatch } = await import(
      "../bulk-upload/roster-assignment-bulk.service.js"
    );
    const data = await importRosterAssignmentBatch(id, userId);
    return { success: true, data };
  }

  if (rpc_name === "import_weekoff_preference_batch") {
    const { importWeekOffPreferenceBatch } = await import(
      "../bulk-upload/weekoff-preference-bulk.service.js"
    );
    const data = await importWeekOffPreferenceBatch(id, userId);
    return { success: true, data };
  }

  if (rpc_name === "import_shift_rotation_type_batch") {
    const { importShiftRotationTypeBatch } = await import(
      "../bulk-upload/shift-rotation-type-bulk.service.js"
    );
    const data = await importShiftRotationTypeBatch(id, userId);
    return { success: true, data };
  }

  if (rpc_name === "import_shift_roster_batch") {
    const { importShiftRosterBatch } = await import(
      "../bulk-upload/shift-roster-bulk.service.js"
    );
    const data = await importShiftRosterBatch(id, userId);
    return { success: true, data };
  }

  if (rpc_name === "import_upload_batch") {
    const { importEmployeeMasterBatch } = await import(
      "../bulk-upload/employee-master-bulk.service.js"
    );
    const data = await importEmployeeMasterBatch(id, userId);
    return { success: true, data };
  }

  if (rpc_name === "import_process_upload_batch") {
    const { importProcessMasterBatch } = await import(
      "../bulk-upload/process-master-bulk.service.js"
    );
    const data = await importProcessMasterBatch(id, userId);
    return { success: true, data };
  }

  if (rpc_name === "import_department_upload_batch") {
    const { importDepartmentMasterBatch } = await import(
      "../bulk-upload/department-master-bulk.service.js"
    );
    const data = await importDepartmentMasterBatch(id, userId);
    return { success: true, data };
  }

  if (rpc_name === "import_asset_upload_batch") {
    const { importAssetMasterBatch } = await import(
      "../bulk-upload/asset-master-bulk.service.js"
    );
    const data = await importAssetMasterBatch(id, userId);
    return { success: true, data };
  }

  if (rpc_name === "import_branch_upload_batch") {
    const { importBranchMasterBatch } = await import(
      "../bulk-upload/branch-master-bulk.service.js"
    );
    const data = await importBranchMasterBatch(id, userId);
    return { success: true, data };
  }

  if (rpc_name === "import_lob_upload_batch") {
    const { importLobMasterBatch } = await import(
      "../bulk-upload/lob-master-bulk.service.js"
    );
    const data = await importLobMasterBatch(id, userId);
    return { success: true, data };
  }

  if (rpc_name === "import_designation_upload_batch") {
    const { importDesignationMasterBatch } = await import(
      "../bulk-upload/designation-master-bulk.service.js"
    );
    const data = await importDesignationMasterBatch(id, userId);
    return { success: true, data };
  }

  if (rpc_name.startsWith("import_onfido_")) {
    const { importOnfidoRawBatch, findOnfidoConfig } = await import(
      "../bulk-upload/onfido-raw-bulk.service.js"
    );
    const config = findOnfidoConfig(rpc_name);
    if (!config) {
      throw new Error(`No Onfido report config registered for rpc_name '${rpc_name}'.`);
    }
    const data = await importOnfidoRawBatch(config, id, userId);
    return { success: true, data };
  }

  if (rpc_name === "import_process_delivery_batch") {
    const { importProcessDeliveryBatch } = await import(
      "../bulk-upload/process-delivery-bulk.service.js"
    );
    const data = await importProcessDeliveryBatch(id, userId);
    return { success: true, data };
  }

  if (rpc_name === "import_email_ticket_daily_batch") {
    const { importEmailTicketDailyBatch } = await import(
      "../bulk-upload/email-ticket-daily-bulk.service.js"
    );
    const data = await importEmailTicketDailyBatch(id, userId);
    return { success: true, data };
  }

  if (rpc_name === "import_lp_apr_daily_batch") {
    const { importLpAprDailyBatch } = await import(
      "../bulk-upload/lp-apr-daily-bulk.service.js"
    );
    const data = await importLpAprDailyBatch(id, userId);
    return { success: true, data };
  }

  if (rpc_name === "import_clovia_email_daily_batch") {
    const { importCloviaEmailDailyBatch } = await import(
      "../bulk-upload/clovia-email-daily-bulk.service.js"
    );
    const data = await importCloviaEmailDailyBatch(id, userId);
    return { success: true, data };
  }

  if (rpc_name === "import_clovia_chat_daily_batch") {
    const { importCloviaChatDailyBatch } = await import(
      "../bulk-upload/clovia-chat-daily-bulk.service.js"
    );
    const data = await importCloviaChatDailyBatch(id, userId);
    return { success: true, data };
  }

  if (rpc_name === "import_clovia_crm_disposition_batch") {
    const { importCloviaCrmDispositionBatch } = await import(
      "../bulk-upload/clovia-crm-disposition-bulk.service.js"
    );
    const data = await importCloviaCrmDispositionBatch(id, userId);
    return { success: true, data };
  }

  if (rpc_name === "import_housing_premium_sale_raw_batch") {
    const { importHousingPremiumSaleRawBatch } = await import(
      "../bulk-upload/housing-premium-sale-raw-bulk.service.js"
    );
    const data = await importHousingPremiumSaleRawBatch(id, userId);
    return { success: true, data };
  }

  if (rpc_name === "import_housing_owner_sale_raw_batch") {
    const { importHousingOwnerSaleRawBatch } = await import(
      "../bulk-upload/housing-owner-sale-raw-bulk.service.js"
    );
    const data = await importHousingOwnerSaleRawBatch(id, userId);
    return { success: true, data };
  }

  if (rpc_name === "import_lp_leads_regional_batch") {
    const { importLpLeadsRegionalBatch } = await import(
      "../bulk-upload/lp-leads-bulk.service.js"
    );
    const data = await importLpLeadsRegionalBatch(id, userId);
    return { success: true, data };
  }

  if (rpc_name === "import_lp_leads_non_regional_batch") {
    const { importLpLeadsNonRegionalBatch } = await import(
      "../bulk-upload/lp-leads-bulk.service.js"
    );
    const data = await importLpLeadsNonRegionalBatch(id, userId);
    return { success: true, data };
  }

  if (rpc_name === "import_du_apr_korea_batch") {
    const { importDuAprKoreaBatch } = await import(
      "../bulk-upload/du-apr-daily-bulk.service.js"
    );
    const data = await importDuAprKoreaBatch(id, userId);
    return { success: true, data };
  }

  if (rpc_name === "import_du_apr_thailand_batch") {
    const { importDuAprThailandBatch } = await import(
      "../bulk-upload/du-apr-daily-bulk.service.js"
    );
    const data = await importDuAprThailandBatch(id, userId);
    return { success: true, data };
  }

  if (rpc_name === "import_gnc_apr_batch") {
    // Reconciled 2026-09-10: redirected from the retired gnc_apr_daily_actual
    // (mas_hrms) to db_masmis.gnc_apr directly, per explicit user instruction
    // to use the same table My Dashboards already writes into. See sql/1741.
    const { importGncAprMasmisBatch } = await import(
      "../bulk-upload/gnc-apr-masmis-bulk.service.js"
    );
    const data = await importGncAprMasmisBatch(id, userId);
    return { success: true, data };
  }

  if (rpc_name === "import_lp_cr_report_regional_batch") {
    const { importLpCrReportRegionalBatch } = await import(
      "../bulk-upload/lp-cdr-cr-report-bulk.service.js"
    );
    const data = await importLpCrReportRegionalBatch(id, userId);
    return { success: true, data };
  }

  if (rpc_name === "import_lp_cr_report_non_regional_batch") {
    const { importLpCrReportNonRegionalBatch } = await import(
      "../bulk-upload/lp-cdr-cr-report-bulk.service.js"
    );
    const data = await importLpCrReportNonRegionalBatch(id, userId);
    return { success: true, data };
  }

  if (rpc_name === "import_du_team_mapping_korea_batch") {
    const { importDuTeamMappingKoreaBatch } = await import(
      "../bulk-upload/du-team-mapping-bulk.service.js"
    );
    const data = await importDuTeamMappingKoreaBatch(id, userId);
    return { success: true, data };
  }

  if (rpc_name === "import_du_team_mapping_thailand_batch") {
    const { importDuTeamMappingThailandBatch } = await import(
      "../bulk-upload/du-team-mapping-bulk.service.js"
    );
    const data = await importDuTeamMappingThailandBatch(id, userId);
    return { success: true, data };
  }

  if (rpc_name === "import_housing_premium_agent_target_batch") {
    const { importHousingPremiumAgentTargetBatch } = await import(
      "../bulk-upload/housing-premium-agent-target-bulk.service.js"
    );
    const data = await importHousingPremiumAgentTargetBatch(id, userId);
    return { success: true, data };
  }

  if (rpc_name === "import_housing_owner_incentive_batch") {
    const { importHousingOwnerIncentiveBatch } = await import(
      "../bulk-upload/housing-owner-incentive-bulk.service.js"
    );
    const data = await importHousingOwnerIncentiveBatch(id, userId);
    return { success: true, data };
  }

  if (rpc_name === "import_housing_owner_lead_pipeline_batch") {
    const { importHousingOwnerLeadPipelineBatch } = await import(
      "../bulk-upload/housing-owner-lead-pipeline-bulk.service.js"
    );
    const data = await importHousingOwnerLeadPipelineBatch(id, userId);
    return { success: true, data };
  }

  if (rpc_name === "import_bla_bli_blu_dd_tagging_batch") {
    const { importBlaBliBluDdTaggingBatch } = await import(
      "../bulk-upload/bla-bli-blu-dd-tagging-bulk.service.js"
    );
    const data = await importBlaBliBluDdTaggingBatch(id, userId);
    return { success: true, data };
  }

  if (rpc_name === "import_gnc_sale_masmis_batch") {
    const { importGncSaleMasmisBatch } = await import(
      "../bulk-upload/gnc-sale-masmis-bulk.service.js"
    );
    const data = await importGncSaleMasmisBatch(id, userId);
    return { success: true, data };
  }

  if (rpc_name === "import_reginald_abandoned_cart_sales_batch") {
    const { importReginaldAbandonedCartSalesBatch } = await import(
      "../bulk-upload/reginald-abandoned-cart-sales-bulk.service.js"
    );
    const data = await importReginaldAbandonedCartSalesBatch(id, userId);
    return { success: true, data };
  }

  if (rpc_name === "import_bla_bli_blu_auto_callback_batch") {
    const { importBlaBliBluAutoCallbackBatch } = await import(
      "../bulk-upload/bla-bli-blu-auto-callback-bulk.service.js"
    );
    const data = await importBlaBliBluAutoCallbackBatch(id, userId);
    return { success: true, data };
  }

  if (rpc_name === "import_bla_bli_blu_after_hour_batch") {
    const { importBlaBliBluAfterHourBatch } = await import(
      "../bulk-upload/bla-bli-blu-after-hour-bulk.service.js"
    );
    const data = await importBlaBliBluAfterHourBatch(id, userId);
    return { success: true, data };
  }

  if (rpc_name === "import_bla_bli_blu_call_disposition_batch") {
    const { importBlaBliBluCallDispositionBatch } = await import(
      "../bulk-upload/bla-bli-blu-call-disposition-bulk.service.js"
    );
    const data = await importBlaBliBluCallDispositionBatch(id, userId);
    return { success: true, data };
  }

  if (rpc_name === "import_bla_bli_blu_shopify_sales_batch") {
    const { importBlaBliBluShopifySalesBatch } = await import(
      "../bulk-upload/bla-bli-blu-shopify-sales-bulk.service.js"
    );
    const data = await importBlaBliBluShopifySalesBatch(id, userId);
    return { success: true, data };
  }

  if (rpc_name === "import_bb_sale_masmis_batch") {
    const { importBbSaleMasmisBatch } = await import(
      "../bulk-upload/bb-sale-masmis-bulk.service.js"
    );
    const data = await importBbSaleMasmisBatch(id, userId);
    return { success: true, data };
  }

  if (rpc_name === "import_bb_apr_masmis_batch") {
    const { importBbAprMasmisBatch } = await import(
      "../bulk-upload/bb-apr-masmis-bulk.service.js"
    );
    const data = await importBbAprMasmisBatch(id, userId);
    return { success: true, data };
  }

  if (rpc_name === "import_bvo_repeat_cdr_masmis_batch") {
    const { importBvoRepeatCdrMasmisBatch } = await import(
      "../bulk-upload/bvo-repeat-cdr-masmis-bulk.service.js"
    );
    const data = await importBvoRepeatCdrMasmisBatch(id, userId);
    return { success: true, data };
  }

  if (rpc_name === "import_bvo_repeat_allocation_masmis_batch") {
    const { importBvoRepeatAllocationMasmisBatch } = await import(
      "../bulk-upload/bvo-repeat-allocation-masmis-bulk.service.js"
    );
    const data = await importBvoRepeatAllocationMasmisBatch(id, userId);
    return { success: true, data };
  }

  if (rpc_name === "import_bb_cart_masmis_batch") {
    const { importBbCartMasmisBatch } = await import(
      "../bulk-upload/bb-cart-masmis-bulk.service.js"
    );
    const data = await importBbCartMasmisBatch(id, userId);
    return { success: true, data };
  }

  if (rpc_name === "import_bb_chat_masmis_batch") {
    const { importBbChatMasmisBatch } = await import(
      "../bulk-upload/bb-chat-masmis-bulk.service.js"
    );
    const data = await importBbChatMasmisBatch(id, userId);
    return { success: true, data };
  }

  if (rpc_name === "import_bvo_order_export_masmis_batch") {
    const { importBvoOrderExportMasmisBatch } = await import(
      "../bulk-upload/bvo-order-export-masmis-bulk.service.js"
    );
    const data = await importBvoOrderExportMasmisBatch(id, userId);
    return { success: true, data };
  }

  if (rpc_name === "import_neemans_sale_raw_masmis_batch") {
    const { importNeemansSaleRawMasmisBatch } = await import(
      "../bulk-upload/neemans-sale-raw-masmis-bulk.service.js"
    );
    const data = await importNeemansSaleRawMasmisBatch(id, userId);
    return { success: true, data };
  }

  if (rpc_name === "import_neemans_allocation_masmis_batch") {
    const { importNeemansAllocationMasmisBatch } = await import(
      "../bulk-upload/neemans-allocation-masmis-bulk.service.js"
    );
    const data = await importNeemansAllocationMasmisBatch(id, userId);
    return { success: true, data };
  }

  if (rpc_name === "import_neemans_cart_masmis_batch") {
    const { importNeemansCartMasmisBatch } = await import(
      "../bulk-upload/neemans-cart-masmis-bulk.service.js"
    );
    const data = await importNeemansCartMasmisBatch(id, userId);
    return { success: true, data };
  }

  if (rpc_name === "import_neemans_apr_masmis_batch") {
    const { importNeemansAprMasmisBatch } = await import(
      "../bulk-upload/neemans-apr-masmis-bulk.service.js"
    );
    const data = await importNeemansAprMasmisBatch(id, userId);
    return { success: true, data };
  }

  if (rpc_name === "import_gnc_allocation_masmis_batch") {
    const { importGncAllocationMasmisBatch } = await import(
      "../bulk-upload/gnc-allocation-masmis-bulk.service.js"
    );
    const data = await importGncAllocationMasmisBatch(id, userId);
    return { success: true, data };
  }

  if (rpc_name === "import_neemans_month_target_batch") {
    const { importNeemansMonthTargetBatch } = await import(
      "../bulk-upload/neemans-month-target-bulk.service.js"
    );
    const data = await importNeemansMonthTargetBatch(id, userId);
    return { success: true, data };
  }

  if (rpc_name === "import_neemans_agent_details_batch") {
    const { importNeemansAgentDetailsBatch } = await import(
      "../bulk-upload/neemans-agent-details-bulk.service.js"
    );
    const data = await importNeemansAgentDetailsBatch(id, userId);
    return { success: true, data };
  }

  if (rpc_name === "import_aw_billing_batch") {
    const { importAwBillingBatch } = await import("../bulk-upload/aw-billing-bulk.service.js");
    const data = await importAwBillingBatch(id, userId);
    return { success: true, data };
  }

  if (rpc_name === "import_aw_inbound_batch") {
    const { importAwInboundBatch } = await import("../bulk-upload/aw-inbound-bulk.service.js");
    const data = await importAwInboundBatch(id, userId);
    return { success: true, data };
  }

  if (rpc_name === "import_aw_mandate_batch") {
    const { importAwMandateBatch } = await import("../bulk-upload/aw-mandate-bulk.service.js");
    const data = await importAwMandateBatch(id, userId);
    return { success: true, data };
  }

  if (rpc_name === "import_aw_new_cdr_batch") {
    const { importAwNewCdrBatch } = await import("../bulk-upload/aw-new-cdr-bulk.service.js");
    const data = await importAwNewCdrBatch(id, userId);
    return { success: true, data };
  }

  if (rpc_name === "import_aw_out_batch") {
    const { importAwOutBatch } = await import("../bulk-upload/aw-out-bulk.service.js");
    const data = await importAwOutBatch(id, userId);
    return { success: true, data };
  }

  if (rpc_name === "import_owner_sale_batch") {
    const { importOwnerSaleBatch } = await import("../bulk-upload/owner-sale-bulk.service.js");
    const data = await importOwnerSaleBatch(id, userId);
    return { success: true, data };
  }

  if (rpc_name === "import_owner_cdr_batch") {
    const { importOwnerCdrBatch } = await import("../bulk-upload/owner-cdr-bulk.service.js");
    const data = await importOwnerCdrBatch(id, userId);
    return { success: true, data };
  }

  if (rpc_name === "import_owner_agent_details_batch") {
    const { importOwnerAgentDetailsBatch } = await import("../bulk-upload/owner-agent-details-bulk.service.js");
    const data = await importOwnerAgentDetailsBatch(id, userId);
    return { success: true, data };
  }

  if (rpc_name === "import_pre_sale_batch") {
    const { importPreSaleBatch } = await import("../bulk-upload/pre-sale-bulk.service.js");
    const data = await importPreSaleBatch(id, userId);
    return { success: true, data };
  }

  if (rpc_name === "import_pre_cdr_batch") {
    const { importPreCdrBatch } = await import("../bulk-upload/pre-cdr-bulk.service.js");
    const data = await importPreCdrBatch(id, userId);
    return { success: true, data };
  }

  if (rpc_name === "import_pre_agent_details_batch") {
    const { importPreAgentDetailsBatch } = await import("../bulk-upload/pre-agent-details-bulk.service.js");
    const data = await importPreAgentDetailsBatch(id, userId);
    return { success: true, data };
  }

  if (rpc_name === "import_cl_apr_batch") {
    const { importClAprBatch } = await import("../bulk-upload/cl-apr-bulk.service.js");
    const data = await importClAprBatch(id, userId);
    return { success: true, data };
  }

  if (rpc_name === "import_cl_chat_batch") {
    const { importClChatBatch } = await import("../bulk-upload/cl-chat-bulk.service.js");
    const data = await importClChatBatch(id, userId);
    return { success: true, data };
  }

  if (rpc_name === "import_cl_dispo_batch") {
    const { importClDispoBatch } = await import("../bulk-upload/cl-dispo-bulk.service.js");
    const data = await importClDispoBatch(id, userId);
    return { success: true, data };
  }

  if (rpc_name === "import_cl_email_raw_batch") {
    const { importClEmailRawBatch } = await import("../bulk-upload/cl-email-raw-bulk.service.js");
    const data = await importClEmailRawBatch(id, userId);
    return { success: true, data };
  }

  if (rpc_name === "import_cl_feedback_batch") {
    const { importClFeedbackBatch } = await import("../bulk-upload/cl-feedback-bulk.service.js");
    const data = await importClFeedbackBatch(id, userId);
    return { success: true, data };
  }

  if (rpc_name === "import_cl_ib_cdr_batch") {
    const { importClIbCdrBatch } = await import("../bulk-upload/cl-ib-cdr-bulk.service.js");
    const data = await importClIbCdrBatch(id, userId);
    return { success: true, data };
  }

  if (rpc_name === "import_cl_outbound_batch") {
    const { importClOutboundBatch } = await import("../bulk-upload/cl-outbound-bulk.service.js");
    const data = await importClOutboundBatch(id, userId);
    return { success: true, data };
  }

  if (rpc_name === "import_cl_quality_batch") {
    const { importClQualityBatch } = await import("../bulk-upload/cl-quality-bulk.service.js");
    const data = await importClQualityBatch(id, userId);
    return { success: true, data };
  }

  if (rpc_name === "import_cl_rechurn_call_batch") {
    const { importClRechurnCallBatch } = await import("../bulk-upload/cl-rechurn-call-bulk.service.js");
    const data = await importClRechurnCallBatch(id, userId);
    return { success: true, data };
  }

  if (rpc_name === "import_birlanu_sale_batch") {
    const { importBirlanuSaleBatch } = await import("../bulk-upload/birlanu-sale-bulk.service.js");
    const data = await importBirlanuSaleBatch(id, userId);
    return { success: true, data };
  }

  if (rpc_name === "import_birlanu_apr_batch") {
    const { importBirlanuAprBatch } = await import("../bulk-upload/birlanu-apr-bulk.service.js");
    const data = await importBirlanuAprBatch(id, userId);
    return { success: true, data };
  }

  if (rpc_name === "import_satya_allocation_batch") {
    const { importSatyaAllocationBatch } = await import("../bulk-upload/satya-allocation-bulk.service.js");
    const data = await importSatyaAllocationBatch(id, userId);
    return { success: true, data };
  }

  if (rpc_name === "import_satya_cdr_batch") {
    const { importSatyaCdrBatch } = await import("../bulk-upload/satya-cdr-bulk.service.js");
    const data = await importSatyaCdrBatch(id, userId);
    return { success: true, data };
  }

  if (rpc_name === "import_lp_feedback_apr_batch") {
    const { importLpFeedbackAprBatch } = await import("../bulk-upload/lp-feedback-apr-bulk.service.js");
    const data = await importLpFeedbackAprBatch(id, userId);
    return { success: true, data };
  }

  if (rpc_name === "import_lp_feedback_cdr_batch") {
    const { importLpFeedbackCdrBatch } = await import("../bulk-upload/lp-feedback-cdr-bulk.service.js");
    const data = await importLpFeedbackCdrBatch(id, userId);
    return { success: true, data };
  }

  if (rpc_name === "import_lp_onboarding_apr_batch") {
    const { importLpOnboardingAprBatch } = await import("../bulk-upload/lp-onboarding-apr-bulk.service.js");
    const data = await importLpOnboardingAprBatch(id, userId);
    return { success: true, data };
  }

  if (rpc_name === "import_lp_onboarding_cdr_batch") {
    const { importLpOnboardingCdrBatch } = await import("../bulk-upload/lp-onboarding-cdr-bulk.service.js");
    const data = await importLpOnboardingCdrBatch(id, userId);
    return { success: true, data };
  }

  if (rpc_name === "import_gnc_chat_batch") {
    const { importGncChatBatch } = await import("../bulk-upload/gnc-chat-bulk.service.js");
    const data = await importGncChatBatch(id, userId);
    return { success: true, data };
  }

  if (rpc_name === "import_neemans_chat_batch") {
    const { importNeemansChatBatch } = await import("../bulk-upload/neemans-chat-bulk.service.js");
    const data = await importNeemansChatBatch(id, userId);
    return { success: true, data };
  }

  if (rpc_name === "import_compliance_audit_batch") {
    const { importComplianceAuditBatch } = await import(
      "../bulk-upload/compliance-audit-bulk.service.js"
    );
    const data = await importComplianceAuditBatch(id, userId);
    return { success: true, data };
  }

  if (rpc_name === "import_process_manual_kpi_batch") {
    const { importProcessManualKpiBatch } = await import(
      "../bulk-upload/process-manual-kpi-bulk.service.js"
    );
    const data = await importProcessManualKpiBatch(id, userId);
    return { success: true, data };
  }

  if (rpc_name.startsWith("import_bella_")) {
    const { importBellaRawBatch, findBellaConfig } = await import(
      "../bulk-upload/bella-raw-bulk.service.js"
    );
    const config = findBellaConfig(rpc_name);
    if (!config) {
      throw new Error(`No Bella Vita report config registered for rpc_name '${rpc_name}'.`);
    }
    const data = await importBellaRawBatch(config, id, userId);
    return { success: true, data };
  }

  // Unreachable in practice — rpc_name is checked against KNOWN_IMPORT_RPCS
  // before this function is ever called — kept as a safety net so the caller's
  // try/catch still resets batch_status off 'importing' if it is ever hit.
  throw new Error(`Import function '${rpc_name}' for batch ${id} is not yet implemented in the MySQL backend.`);
}

export { router as bulkUploadRouter };
