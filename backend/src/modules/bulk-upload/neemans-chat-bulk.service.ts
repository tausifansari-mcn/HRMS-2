import { RowDataPacket } from "mysql2";
import { db } from "../../db/mysql.js";

/**
 * neemans_chat -- writes into db_masmis.neemans_chat (sql/1776). Source:
 * Neamans Chat.xlsx (Kwikengage-style chat/DM ticket export, Neeman's WA +
 * Instagram DM inboxes). Columns confirmed directly against the real
 * file, not guessed.
 */

function normalizeKey(k: string): string {
  return k.toLowerCase().replace(/[^a-z0-9]/g, "");
}
function getByColumn(data: Record<string, unknown>, ...columnNames: string[]): string {
  const normalized: Record<string, unknown> = {};
  for (const k of Object.keys(data)) normalized[normalizeKey(k)] = data[k];
  for (const col of columnNames) {
    const v = normalized[normalizeKey(col)];
    if (v !== undefined && v !== null && String(v).trim() !== "") return String(v).trim();
  }
  return "";
}
function n(data: Record<string, unknown>, ...columnNames: string[]): string | null {
  const v = getByColumn(data, ...columnNames);
  return v || null;
}

interface BatchRow extends RowDataPacket {
  id: string;
  row_no: number;
  normalized_data: string | Record<string, unknown>;
}

export async function importNeemansChatBatch(
  batchId: string,
  importedByUserId: string,
): Promise<{ importedRows: number; errorRows: number; errors: string[] }> {
  const [batchRows] = await db.execute<BatchRow[]>(
    `SELECT id, row_no, normalized_data FROM upload_batch_row
      WHERE upload_batch_id = ? AND row_status IN ('valid','pending')
      ORDER BY row_no`,
    [batchId],
  );
  if (batchRows.length === 0) return { importedRows: 0, errorRows: 0, errors: [] };

  const errors: string[] = [];
  const errorUpdates: Array<{ rowId: string; message: string }> = [];
  let importedRows = 0;
  let errorRows = 0;

  const uploadedByInt = /^\d+$/.test(importedByUserId) ? Number(importedByUserId) : null;

  for (const row of batchRows) {
    const data =
      typeof row.normalized_data === "string"
        ? JSON.parse(row.normalized_data)
        : ((row.normalized_data ?? {}) as Record<string, unknown>);

    const requiredVal = getByColumn(data, "Ticket ID");
    if (!requiredVal) {
      const msg = `Row ${row.row_no}: "Ticket ID" is required`;
      errors.push(msg); errorUpdates.push({ rowId: row.id, message: msg }); errorRows++; continue;
    }

    try {
      await db.execute(
        `INSERT INTO db_masmis.neemans_chat
           (ticket_id, inbox_id, inbox_name, identifier, ticket_status, agent_name, email, phone_number, instagram_username, created_at_src, assigned_at, agent_frt_at, frt, resolution_time_at, resolution_time, avg_wait_time, avg_handled_time, csat_rating, agent_message_blocks, is_resolved, is_outside_working_hours, frt_in_sec, resolution_time_in_min, frt_tat, resolution_tat, report_date, time_slot, emp_id, lob, week, hour_val, uploaded_by, upload_batch_id)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        [
          requiredVal,
          n(data, "Inbox ID"),
          n(data, "Inbox Name"),
          n(data, "Identifier"),
          n(data, "Ticket Status"),
          n(data, "Agent Name"),
          n(data, "Email"),
          n(data, "Phone Number"),
          n(data, "Instagram Username"),
          n(data, "Created At"),
          n(data, "Assigned At"),
          n(data, "Agent FRT At"),
          n(data, "FRT"),
          n(data, "Resolution Time At"),
          n(data, "Resolution Time"),
          n(data, "Average Wait Time"),
          n(data, "Average Handled Time"),
          n(data, "CSAT Rating"),
          n(data, "Agent Message Blocks"),
          n(data, "Is Resolved?"),
          n(data, "Is Outside Working Hours"),
          n(data, "FRT (IN Sec)"),
          n(data, "Resolution Time (In Min)"),
          n(data, "FRT TAT"),
          n(data, "Resolution TAT"),
          n(data, "Date"),
          n(data, "Time Slot"),
          n(data, "EMP ID"),
          n(data, "LOB"),
          n(data, "Week"),
          n(data, "Hour"),
          uploadedByInt, batchId,
        ] as never[],
      );
      importedRows++;
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err);
      errors.push(`Row ${row.row_no}: ${msg}`);
      errorUpdates.push({ rowId: row.id, message: msg.slice(0, 500) });
      errorRows++;
    }
  }

  if (importedRows > 0) {
    await db.execute(
      `INSERT INTO db_masmis.upload_log (batch_id, table_name, file_name, row_count, uploaded_by)
       VALUES (?, 'neemans_chat', ?, ?, NULL)`,
      [batchId, `HRMS2 upload by ${importedByUserId}`, importedRows],
    );
  }

  if (errorUpdates.length) {
    const cases = errorUpdates.map(() => "WHEN ? THEN CAST(? AS JSON)").join(" ");
    const ids = errorUpdates.map((u) => u.rowId);
    await db.execute(
      `UPDATE upload_batch_row SET row_status = 'error', error_messages = CASE id ${cases} END
        WHERE id IN (${ids.map(() => "?").join(",")})`,
      [...errorUpdates.flatMap((u) => [u.rowId, JSON.stringify([u.message])]), ...ids],
    );
  }

  const finalStatus =
    errorRows === 0 ? "imported" : importedRows === 0 ? "validation_failed" : "imported_with_errors";
  await db.execute(
    `UPDATE upload_batch SET batch_status = ?, imported_rows = ?, error_rows = ? WHERE id = ?`,
    [finalStatus, importedRows, errorRows, batchId],
  );

  return { importedRows, errorRows, errors };
}
