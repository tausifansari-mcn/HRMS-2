import { useState } from "react";
import { DashboardLayout } from "@/components/layout/DashboardLayout";
import { BellavitaMasmisUploader } from "@/components/process-performance/BellavitaMasmisUploader";
import { ProjectDetailView } from "@/pages/NativeInboundDashboard";
import {
  Activity, ChevronLeft, ChevronRight, LayoutDashboard, Upload,
  ShoppingBag, MessageSquare, ShoppingCart, Target, Users,
  Receipt, PhoneIncoming, PhoneOutgoing, ClipboardList,
  Mail, Star, ShieldCheck, Repeat, RotateCcw,
} from "lucide-react";

/**
 * dalmia/dubangladesh/viega/exicom are spelled exactly as the backend's
 * inbound.service.ts PROJECTS[].key (dubangladesh has no underscore) --
 * company is passed straight through as projectKey with no separate
 * mapping table, same as bellavita/gnc/clovia/neemans already are.
 */
type CompanyKey = "bellavita" | "gnc" | "neemans" | "appreciate_health" | "housing_owner" | "housing_premium" | "clovia" | "birlanu" | "satya_retail" | "lp_feedback" | "lp_onboarding" | "dalmia" | "dubangladesh" | "viega" | "exicom";
type SectionKey = "dashboards" | "uploader";

const COMPANIES: Array<{ key: CompanyKey; label: string }> = [
  { key: "bellavita", label: "Bellavita" },
  { key: "gnc", label: "GNC" },
  { key: "neemans", label: "Neemans" },
  { key: "appreciate_health", label: "Appreciate Health" },
  { key: "housing_owner", label: "Housing Owner" },
  { key: "housing_premium", label: "Housing Premium" },
  { key: "clovia", label: "Clovia" },
  { key: "birlanu", label: "Birlanu" },
  { key: "satya_retail", label: "Satya Retail" },
  { key: "lp_feedback", label: "LP Feedback" },
  { key: "lp_onboarding", label: "LP Onboarding" },
  { key: "dalmia", label: "Dalmia" },
  { key: "dubangladesh", label: "DU Bangladesh" },
  { key: "viega", label: "Viega" },
  { key: "exicom", label: "Exicom" },
];

/**
 * Named dashboard entries per company. "inbound" entries render the exact
 * same live dialer_db view as /call-master/inbound/:projectKey (via the
 * shared ProjectDetailView component) -- projectKey there already uses the
 * same lowercase keys as this page's CompanyKey (bellavita/gnc/clovia/
 * neemans), confirmed against backend/src/modules/call-master/
 * inbound.service.ts, so `company` is passed straight through with no
 * separate mapping. "stub" entries are the pre-existing "nothing built
 * yet" placeholders (Neemans' Sale/Allocation cards) -- unchanged.
 */
const DASHBOARDS_BY_COMPANY: Partial<Record<CompanyKey, Array<{ key: string; label: string; description: string; kind: "inbound" | "stub" }>>> = {
  bellavita: [
    { key: "inbound", label: "Inbound", description: "Live call performance — AL%, SL%, ACHT, Repeat%", kind: "inbound" },
  ],
  gnc: [
    { key: "inbound", label: "Inbound", description: "Live call performance — AL%, SL%, ACHT, Repeat%", kind: "inbound" },
  ],
  clovia: [
    { key: "inbound", label: "Inbound", description: "Live call performance — AL%, SL%, ACHT, Repeat%", kind: "inbound" },
  ],
  neemans: [
    { key: "sale", label: "Sale Dashboard", description: "Coming soon", kind: "stub" },
    { key: "allocation", label: "Allocation Dashboard", description: "Coming soon", kind: "stub" },
    { key: "inbound", label: "Inbound", description: "Live call performance — AL%, SL%, ACHT, Repeat%, FCR%", kind: "inbound" },
  ],
  dalmia: [
    { key: "inbound", label: "Inbound", description: "Live call performance — AL%, SL%, ACHT, Repeat%", kind: "inbound" },
  ],
  dubangladesh: [
    { key: "inbound", label: "Inbound", description: "Live call performance — AL%, SL%, ACHT, Repeat%", kind: "inbound" },
  ],
  viega: [
    { key: "inbound", label: "Inbound", description: "Live call performance — AL%, SL%, ACHT, Repeat%", kind: "inbound" },
  ],
  exicom: [
    { key: "inbound", label: "Inbound", description: "Live call performance — AL%, SL%, ACHT, Repeat%", kind: "inbound" },
  ],
};

const SECTIONS: Array<{ key: SectionKey; label: string; description: string }> = [
  { key: "dashboards", label: "Dashboards", description: "Coming soon" },
  { key: "uploader", label: "Uploader", description: "Bulk data uploaders" },
];

/**
 * Bellavita's 4 live uploaders only — the other 3 db_masmis upload types this process
 * already has (Repeat CDR, Repeat Allocation, Shopify Order Export) are deliberately
 * left out here per explicit scope; they remain reachable from the main Bulk Upload
 * Hub's template dropdown.
 */
const BELLAVITA_UPLOADERS = [
  { code: "BB_SALE_MASMIS", label: "Sale Data", description: "Upload Bellavita sale data", icon: ShoppingBag },
  { code: "BB_APR_MASMIS",  label: "APR Data",  description: "Upload Bellavita APR data",  icon: Activity },
  { code: "BB_CHAT_MASMIS", label: "Chat Data", description: "Upload Bellavita chat data", icon: MessageSquare },
  { code: "BB_CART_MASMIS", label: "Cart Data", description: "Upload Bellavita cart data", icon: ShoppingCart },
];

/** GNC's 3 live db_masmis uploaders (gnc_sale, gnc_apr, gnc_allocation). */
const GNC_UPLOADERS = [
  { code: "GNC_SALE_MASMIS",       label: "Sale Data",       description: "Upload GNC sale data",       icon: ShoppingBag },
  { code: "GNC_APR",               label: "APR Data",        description: "Upload GNC APR data",        icon: Activity },
  { code: "GNC_ALLOCATION_MASMIS", label: "Allocation Data", description: "Upload GNC allocation data", icon: ShoppingCart },
  { code: "GNC_CHAT_MASMIS",       label: "Chat Data",       description: "Upload GNC chat/CS ticket data", icon: MessageSquare },
];

/** Neemans' 5 live db_masmis uploaders. Cart (neemans_cart, 0 rows, never
 * used) is deliberately left out here per explicit scope, same as
 * Bellavita's own left-out extras above; still reachable from the main
 * Bulk Upload Hub. */
const NEEMANS_UPLOADERS = [
  { code: "NEEMANS_SALE_RAW_MASMIS",       label: "Sale Raw",       description: "Upload Neemans sale raw data",     icon: ShoppingBag },
  { code: "NEEMANS_ALLOCATION_MASMIS",     label: "Allocation",     description: "Upload Neemans allocation data",   icon: ShoppingCart },
  { code: "NEEMANS_APR_MASMIS",            label: "APR",            description: "Upload Neemans APR data",          icon: Activity },
  { code: "NEEMANS_MONTH_TARGET_MASMIS",   label: "Target",         description: "Upload Neemans monthly target",    icon: Target },
  { code: "NEEMANS_AGENT_DETAILS_MASMIS",  label: "Agent Details",  description: "Upload Neemans agent roster",      icon: Users },
  { code: "NEEMANS_CHAT_MASMIS",           label: "Chat Data",      description: "Upload Neemans chat/DM ticket data", icon: MessageSquare },
];

/** Appreciate Health's 5 live db_masmis uploaders. No real Excel export has
 * been seen for any of them yet (unlike GNC/Neemans, no sample file or
 * screenshot) -- header matching is normalized on both this component's own
 * pre-check and each aw-*-bulk.service.ts backend importer, so real-world
 * spelling/case/spacing differences shouldn't block an upload the way they
 * did for GNC/Neemans before those were fixed. See aw-mandate-bulk.service.ts. */
const APPRECIATE_HEALTH_UPLOADERS = [
  { code: "AW_BILLING_MASMIS", label: "Billing",  description: "Upload Appreciate Health billing data",  icon: Receipt },
  { code: "AW_INBOUND_MASMIS", label: "Inbound",   description: "Upload Appreciate Health inbound CDR",   icon: PhoneIncoming },
  { code: "AW_MANDATE_MASMIS", label: "Mandate",   description: "Upload Appreciate Health billing mandate", icon: ClipboardList },
  { code: "AW_NEW_CDR_MASMIS", label: "New CDR",   description: "Upload Appreciate Health new CDR data",   icon: Activity },
  { code: "AW_OUT_MASMIS",     label: "Outbound",  description: "Upload Appreciate Health outbound data",  icon: PhoneOutgoing },
];

/** Housing Owner's 3 uploaders, writing into brand-new db_masmis tables
 * (owner_sale/Owner_cdr/owner_agent_details, sql/1766) -- deliberately
 * separate from the existing, more sophisticated Housing Dashboards
 * feature and existing CR_housing_owner CDR table, per explicit user
 * confirmation. NOTE: the target tables could not be created by this app's
 * DB user (no CREATE privilege on db_masmis) -- uploads will fail until
 * someone with sufficient privilege runs sql/1766. */
const HOUSING_OWNER_UPLOADERS = [
  { code: "OWNER_SALE_MASMIS",          label: "Owner Sale",          description: "Upload Housing Owner sale data",   icon: ShoppingBag },
  { code: "OWNER_CDR_MASMIS",           label: "Owner CDR",           description: "Upload Housing Owner CDR data",     icon: PhoneIncoming },
  { code: "OWNER_AGENT_DETAILS_MASMIS", label: "Owner Agent Details", description: "Upload Housing Owner agent roster", icon: Users },
];

/** Housing Premium's 3 uploaders, writing into brand-new db_masmis tables
 * (pre_sale/Pre_cdr/pre_agent_details, sql/1766) -- same caveat as Housing
 * Owner above: tables not yet created, blocked on DB CREATE privilege. */
const HOUSING_PREMIUM_UPLOADERS = [
  { code: "PRE_SALE_MASMIS",          label: "Premium Sale",          description: "Upload Housing Premium sale data",   icon: ShoppingBag },
  { code: "PRE_CDR_MASMIS",           label: "Premium CDR",           description: "Upload Housing Premium CDR data",     icon: PhoneIncoming },
  { code: "PRE_AGENT_DETAILS_MASMIS", label: "Premium Agent Details", description: "Upload Housing Premium agent roster", icon: Users },
];

/** Clovia's 9 uploaders, writing into brand-new db_masmis tables (sql/1768).
 * 6 of these 9 (Chat/Disposition/Email Raw/Feedback/Quality/Rechurn Call)
 * already have a different, working Clovia upload feature in this codebase
 * (mas_hrms-backed) -- kept deliberately separate per explicit user
 * confirmation, same as Housing Owner/Premium. APR/Inbound CDR/Outbound
 * have no existing Clovia equivalent. Tables not yet created (user is
 * running sql/1768 themselves) -- same status as Housing Owner/Premium
 * before their tables existed. */
const CLOVIA_UPLOADERS = [
  { code: "CL_APR_MASMIS",           label: "APR",           description: "Upload Clovia APR data",             icon: Activity },
  { code: "CL_CHAT_MASMIS",          label: "Chat",          description: "Upload Clovia chat data",            icon: MessageSquare },
  { code: "CL_DISPO_MASMIS",         label: "Disposition",   description: "Upload Clovia CRM disposition data", icon: ClipboardList },
  { code: "CL_EMAIL_RAW_MASMIS",     label: "Email Raw",     description: "Upload Clovia email raw data",       icon: Mail },
  { code: "CL_FEEDBACK_MASMIS",      label: "Feedback",      description: "Upload Clovia feedback data",        icon: Star },
  { code: "CL_IB_CDR_MASMIS",        label: "Inbound CDR",   description: "Upload Clovia inbound CDR data",     icon: PhoneIncoming },
  { code: "CL_OUTBOUND_MASMIS",      label: "Outbound",      description: "Upload Clovia outbound call data",   icon: PhoneOutgoing },
  { code: "CL_QUALITY_MASMIS",       label: "Quality",       description: "Upload Clovia quality audit data",   icon: ShieldCheck },
  { code: "CL_RECHURN_CALL_MASMIS",  label: "Rechurn Call",  description: "Upload Clovia rechurn call data",    icon: Repeat },
];

/** Birlanu's 2 uploaders, writing into brand-new db_masmis tables
 * (birlanu_sale/birlanu_apr, sql/1770). Same status as Clovia before its
 * tables existed -- CREATE TABLE SQL is ready, user runs it themselves. */
const BIRLANU_UPLOADERS = [
  { code: "BIRLANU_SALE_MASMIS", label: "Sale",  description: "Upload Birlanu sale/lead data", icon: ShoppingCart },
  { code: "BIRLANU_APR_MASMIS",  label: "APR",   description: "Upload Birlanu agent APR data", icon: Activity },
];

/** Satya Retail's 2 uploaders, writing into brand-new db_masmis tables
 * (satya_allocation/satya_cdr, sql/1770). Same status as above. */
const SATYA_RETAIL_UPLOADERS = [
  { code: "SATYA_ALLOCATION_MASMIS", label: "Allocation", description: "Upload Satya Retail beat/shop allocation data", icon: Target },
  { code: "SATYA_CDR_MASMIS",        label: "CDR",         description: "Upload Satya Retail call detail records",      icon: PhoneOutgoing },
];

/** LP Feedback's 2 uploaders, writing into brand-new db_masmis tables
 * (lp_feedback_apr/lp_feedback_cdr, sql/1772). Same status as Clovia
 * before its tables existed -- CREATE TABLE SQL is ready, user runs it
 * themselves. */
const LP_FEEDBACK_UPLOADERS = [
  { code: "LP_FEEDBACK_APR_MASMIS", label: "APR", description: "Upload LP Feedback agent APR data",       icon: Activity },
  { code: "LP_FEEDBACK_CDR_MASMIS", label: "CDR", description: "Upload LP Feedback call detail records",  icon: PhoneOutgoing },
];

/** LP Onboarding's 2 uploaders, writing into brand-new db_masmis tables
 * (lp_onboarding_apr/lp_onboarding_cdr, sql/1772). Same status as above. */
const LP_ONBOARDING_UPLOADERS = [
  { code: "LP_ONBOARDING_APR_MASMIS", label: "APR", description: "Upload LP Onboarding agent APR data",      icon: Activity },
  { code: "LP_ONBOARDING_CDR_MASMIS", label: "CDR", description: "Upload LP Onboarding call detail records", icon: PhoneOutgoing },
];

function BoxGrid({ children }: { children: React.ReactNode }) {
  return <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4">{children}</div>;
}

function Box({
  icon: Icon, label, description, onClick,
}: { icon: React.ComponentType<{ className?: string }>; label: string; description: string; onClick: () => void }) {
  return (
    <button
      type="button"
      onClick={onClick}
      className="flex items-center justify-between rounded-xl border border-slate-200 bg-white p-5 text-left shadow-sm transition hover:border-indigo-300 hover:shadow-md"
    >
      <div className="flex items-center gap-3">
        <span className="flex h-10 w-10 items-center justify-center rounded-full bg-slate-900 text-white">
          <Icon className="h-4.5 w-4.5" />
        </span>
        <div>
          <div className="text-sm font-bold text-slate-900">{label}</div>
          <div className="text-xs text-slate-500">{description}</div>
        </div>
      </div>
      <ChevronRight className="h-4 w-4 text-slate-300" />
    </button>
  );
}

function Breadcrumb({ parts, onBack }: { parts: string[]; onBack: () => void }) {
  return (
    <div className="flex items-center gap-2 text-sm font-medium text-slate-500">
      <button type="button" onClick={onBack} className="flex items-center gap-1 hover:text-slate-900">
        <ChevronLeft className="h-4 w-4" />
      </button>
      <span>{parts.join(" / ")}</span>
    </div>
  );
}

const todayStr = () => new Date().toISOString().slice(0, 10);
const sevenDaysAgoStr = () => {
  const d = new Date();
  d.setDate(d.getDate() - 6);
  return d.toISOString().slice(0, 10);
};

/**
 * Date-range wrapper around NativeInboundDashboard's ProjectDetailView —
 * that component takes from/to as props rather than owning its own range,
 * so this tab supplies the same default (last 7 days) and controls its
 * own date pickers, reusing the live summary/trend/hourly view as-is.
 */
function InboundDashboardTab({ projectKey }: { projectKey: string }) {
  const [from, setFrom] = useState(sevenDaysAgoStr());
  const [to, setTo] = useState(todayStr());

  return (
    <div className="space-y-4">
      <div className="flex flex-wrap items-center gap-2">
        <input
          type="date"
          value={from}
          onChange={(e) => setFrom(e.target.value)}
          className="rounded-lg border border-slate-200 bg-white px-3 py-1.5 text-sm text-slate-700 shadow-sm"
        />
        <span className="text-sm text-slate-400">—</span>
        <input
          type="date"
          value={to}
          onChange={(e) => setTo(e.target.value)}
          className="rounded-lg border border-slate-200 bg-white px-3 py-1.5 text-sm text-slate-700 shadow-sm"
        />
        <button
          type="button"
          onClick={() => { setFrom(sevenDaysAgoStr()); setTo(todayStr()); }}
          className="flex items-center gap-1.5 rounded-lg bg-slate-100 px-3 py-1.5 text-sm text-slate-600 hover:bg-slate-200"
        >
          <RotateCcw className="h-3.5 w-3.5" /> Reset
        </button>
      </div>
      <ProjectDetailView projectKey={projectKey} from={from} to={to} />
    </div>
  );
}

export default function ProcessPerformanceV2Page() {
  const [company, setCompany] = useState<CompanyKey | null>(null);
  const [section, setSection] = useState<SectionKey | null>(null);
  const [selectedUploader, setSelectedUploader] = useState<{ code: string; label: string } | null>(null);
  const [selectedDashboard, setSelectedDashboard] = useState<{ key: string; label: string; kind: "inbound" | "stub" } | null>(null);

  const companyLabel = COMPANIES.find((c) => c.key === company)?.label ?? "";
  const sectionLabel = SECTIONS.find((s) => s.key === section)?.label ?? "";

  const reset = () => { setCompany(null); setSection(null); setSelectedUploader(null); setSelectedDashboard(null); };
  const backToCompany = () => { setSection(null); setSelectedUploader(null); setSelectedDashboard(null); };
  const backToUploaderGrid = () => setSelectedUploader(null);
  const backToDashboardGrid = () => setSelectedDashboard(null);

  return (
    <DashboardLayout>
      <div className="p-4 sm:p-6 space-y-4">
        <div className="flex items-center gap-2.5">
          <span className="flex h-9 w-9 items-center justify-center rounded-xl bg-indigo-50 text-indigo-600">
            <Activity className="h-4.5 w-4.5" />
          </span>
          <div>
            <h1 className="text-lg font-bold text-slate-900">Process Performance V2</h1>
            <p className="text-xs text-slate-500">
              {company ? (section ? `${companyLabel} / ${sectionLabel}` : companyLabel) : "Select a process"}
            </p>
          </div>
        </div>

        {/* Level 1: company picker */}
        {!company && (
          <BoxGrid>
            {COMPANIES.map((c) => (
              <Box
                key={c.key}
                icon={ShoppingBag}
                label={c.label}
                description="Open process"
                onClick={() => setCompany(c.key)}
              />
            ))}
          </BoxGrid>
        )}

        {/* Level 2: section picker (Dashboards / Uploader) */}
        {company && !section && (
          <div className="space-y-4">
            <Breadcrumb parts={[companyLabel]} onBack={reset} />
            <BoxGrid>
              {SECTIONS.map((s) => (
                <Box
                  key={s.key}
                  icon={s.key === "dashboards" ? LayoutDashboard : Upload}
                  label={s.label}
                  description={s.description}
                  onClick={() => setSection(s.key)}
                />
              ))}
            </BoxGrid>
          </div>
        )}

        {/* Level 3: Dashboards — blank for now (single stub for every company without named sub-dashboards) */}
        {(company === "appreciate_health" || company === "housing_owner" || company === "housing_premium" || company === "birlanu" || company === "satya_retail" || company === "lp_feedback" || company === "lp_onboarding") && section === "dashboards" && (
          <div className="space-y-4">
            <Breadcrumb parts={[companyLabel, "Dashboards"]} onBack={backToCompany} />
            <div className="flex items-center justify-center rounded-xl border border-dashed border-slate-200 bg-white p-16 text-sm text-slate-400">
              Nothing here yet
            </div>
          </div>
        )}

        {/* Level 3: Dashboards — companies with named dashboard cards (Bellavita/GNC/
            Clovia/Neemans' Inbound, plus Neemans' pre-existing Sale/Allocation stubs) */}
        {company && DASHBOARDS_BY_COMPANY[company] && section === "dashboards" && !selectedDashboard && (
          <div className="space-y-4">
            <Breadcrumb parts={[companyLabel, "Dashboards"]} onBack={backToCompany} />
            <BoxGrid>
              {DASHBOARDS_BY_COMPANY[company]!.map((d) => (
                <Box
                  key={d.key}
                  icon={d.kind === "inbound" ? PhoneIncoming : LayoutDashboard}
                  label={d.label}
                  description={d.description}
                  onClick={() => setSelectedDashboard({ key: d.key, label: d.label, kind: d.kind })}
                />
              ))}
            </BoxGrid>
          </div>
        )}

        {company && DASHBOARDS_BY_COMPANY[company] && section === "dashboards" && selectedDashboard && (
          <div className="space-y-4">
            <Breadcrumb parts={[companyLabel, "Dashboards", selectedDashboard.label]} onBack={backToDashboardGrid} />
            {selectedDashboard.kind === "inbound" ? (
              <InboundDashboardTab projectKey={company} />
            ) : (
              <div className="flex items-center justify-center rounded-xl border border-dashed border-slate-200 bg-white p-16 text-sm text-slate-400">
                Nothing here yet
              </div>
            )}
          </div>
        )}

        {/* Level 3: Uploader — pick a data type, then upload right here */}
        {company === "bellavita" && section === "uploader" && !selectedUploader && (
          <div className="space-y-4">
            <Breadcrumb parts={[companyLabel, "Data Uploader"]} onBack={backToCompany} />
            <BoxGrid>
              {BELLAVITA_UPLOADERS.map((u) => (
                <Box
                  key={u.code}
                  icon={u.icon}
                  label={u.label}
                  description={u.description}
                  onClick={() => setSelectedUploader({ code: u.code, label: u.label })}
                />
              ))}
            </BoxGrid>
          </div>
        )}

        {company === "bellavita" && section === "uploader" && selectedUploader && (
          <div className="space-y-4">
            <Breadcrumb parts={[companyLabel, "Data Uploader", selectedUploader.label]} onBack={backToUploaderGrid} />
            <BellavitaMasmisUploader templateCode={selectedUploader.code} label={selectedUploader.label} />
          </div>
        )}

        {company === "gnc" && section === "uploader" && !selectedUploader && (
          <div className="space-y-4">
            <Breadcrumb parts={[companyLabel, "Data Uploader"]} onBack={backToCompany} />
            <BoxGrid>
              {GNC_UPLOADERS.map((u) => (
                <Box
                  key={u.code}
                  icon={u.icon}
                  label={u.label}
                  description={u.description}
                  onClick={() => setSelectedUploader({ code: u.code, label: u.label })}
                />
              ))}
            </BoxGrid>
          </div>
        )}

        {company === "gnc" && section === "uploader" && selectedUploader && (
          <div className="space-y-4">
            <Breadcrumb parts={[companyLabel, "Data Uploader", selectedUploader.label]} onBack={backToUploaderGrid} />
            <BellavitaMasmisUploader templateCode={selectedUploader.code} label={selectedUploader.label} />
          </div>
        )}

        {company === "neemans" && section === "uploader" && !selectedUploader && (
          <div className="space-y-4">
            <Breadcrumb parts={[companyLabel, "Data Uploader"]} onBack={backToCompany} />
            <BoxGrid>
              {NEEMANS_UPLOADERS.map((u) => (
                <Box
                  key={u.code}
                  icon={u.icon}
                  label={u.label}
                  description={u.description}
                  onClick={() => setSelectedUploader({ code: u.code, label: u.label })}
                />
              ))}
            </BoxGrid>
          </div>
        )}

        {company === "neemans" && section === "uploader" && selectedUploader && (
          <div className="space-y-4">
            <Breadcrumb parts={[companyLabel, "Data Uploader", selectedUploader.label]} onBack={backToUploaderGrid} />
            <BellavitaMasmisUploader templateCode={selectedUploader.code} label={selectedUploader.label} />
          </div>
        )}

        {company === "appreciate_health" && section === "uploader" && !selectedUploader && (
          <div className="space-y-4">
            <Breadcrumb parts={[companyLabel, "Data Uploader"]} onBack={backToCompany} />
            <BoxGrid>
              {APPRECIATE_HEALTH_UPLOADERS.map((u) => (
                <Box
                  key={u.code}
                  icon={u.icon}
                  label={u.label}
                  description={u.description}
                  onClick={() => setSelectedUploader({ code: u.code, label: u.label })}
                />
              ))}
            </BoxGrid>
          </div>
        )}

        {company === "appreciate_health" && section === "uploader" && selectedUploader && (
          <div className="space-y-4">
            <Breadcrumb parts={[companyLabel, "Data Uploader", selectedUploader.label]} onBack={backToUploaderGrid} />
            <BellavitaMasmisUploader templateCode={selectedUploader.code} label={selectedUploader.label} />
          </div>
        )}

        {company === "housing_owner" && section === "uploader" && !selectedUploader && (
          <div className="space-y-4">
            <Breadcrumb parts={[companyLabel, "Data Uploader"]} onBack={backToCompany} />
            <BoxGrid>
              {HOUSING_OWNER_UPLOADERS.map((u) => (
                <Box
                  key={u.code}
                  icon={u.icon}
                  label={u.label}
                  description={u.description}
                  onClick={() => setSelectedUploader({ code: u.code, label: u.label })}
                />
              ))}
            </BoxGrid>
          </div>
        )}

        {company === "housing_owner" && section === "uploader" && selectedUploader && (
          <div className="space-y-4">
            <Breadcrumb parts={[companyLabel, "Data Uploader", selectedUploader.label]} onBack={backToUploaderGrid} />
            <BellavitaMasmisUploader templateCode={selectedUploader.code} label={selectedUploader.label} />
          </div>
        )}

        {company === "housing_premium" && section === "uploader" && !selectedUploader && (
          <div className="space-y-4">
            <Breadcrumb parts={[companyLabel, "Data Uploader"]} onBack={backToCompany} />
            <BoxGrid>
              {HOUSING_PREMIUM_UPLOADERS.map((u) => (
                <Box
                  key={u.code}
                  icon={u.icon}
                  label={u.label}
                  description={u.description}
                  onClick={() => setSelectedUploader({ code: u.code, label: u.label })}
                />
              ))}
            </BoxGrid>
          </div>
        )}

        {company === "housing_premium" && section === "uploader" && selectedUploader && (
          <div className="space-y-4">
            <Breadcrumb parts={[companyLabel, "Data Uploader", selectedUploader.label]} onBack={backToUploaderGrid} />
            <BellavitaMasmisUploader templateCode={selectedUploader.code} label={selectedUploader.label} />
          </div>
        )}

        {company === "clovia" && section === "uploader" && !selectedUploader && (
          <div className="space-y-4">
            <Breadcrumb parts={[companyLabel, "Data Uploader"]} onBack={backToCompany} />
            <BoxGrid>
              {CLOVIA_UPLOADERS.map((u) => (
                <Box
                  key={u.code}
                  icon={u.icon}
                  label={u.label}
                  description={u.description}
                  onClick={() => setSelectedUploader({ code: u.code, label: u.label })}
                />
              ))}
            </BoxGrid>
          </div>
        )}

        {company === "clovia" && section === "uploader" && selectedUploader && (
          <div className="space-y-4">
            <Breadcrumb parts={[companyLabel, "Data Uploader", selectedUploader.label]} onBack={backToUploaderGrid} />
            <BellavitaMasmisUploader templateCode={selectedUploader.code} label={selectedUploader.label} />
          </div>
        )}

        {company === "birlanu" && section === "uploader" && !selectedUploader && (
          <div className="space-y-4">
            <Breadcrumb parts={[companyLabel, "Data Uploader"]} onBack={backToCompany} />
            <BoxGrid>
              {BIRLANU_UPLOADERS.map((u) => (
                <Box
                  key={u.code}
                  icon={u.icon}
                  label={u.label}
                  description={u.description}
                  onClick={() => setSelectedUploader({ code: u.code, label: u.label })}
                />
              ))}
            </BoxGrid>
          </div>
        )}

        {company === "birlanu" && section === "uploader" && selectedUploader && (
          <div className="space-y-4">
            <Breadcrumb parts={[companyLabel, "Data Uploader", selectedUploader.label]} onBack={backToUploaderGrid} />
            <BellavitaMasmisUploader templateCode={selectedUploader.code} label={selectedUploader.label} />
          </div>
        )}

        {company === "satya_retail" && section === "uploader" && !selectedUploader && (
          <div className="space-y-4">
            <Breadcrumb parts={[companyLabel, "Data Uploader"]} onBack={backToCompany} />
            <BoxGrid>
              {SATYA_RETAIL_UPLOADERS.map((u) => (
                <Box
                  key={u.code}
                  icon={u.icon}
                  label={u.label}
                  description={u.description}
                  onClick={() => setSelectedUploader({ code: u.code, label: u.label })}
                />
              ))}
            </BoxGrid>
          </div>
        )}

        {company === "satya_retail" && section === "uploader" && selectedUploader && (
          <div className="space-y-4">
            <Breadcrumb parts={[companyLabel, "Data Uploader", selectedUploader.label]} onBack={backToUploaderGrid} />
            <BellavitaMasmisUploader templateCode={selectedUploader.code} label={selectedUploader.label} />
          </div>
        )}

        {company === "lp_feedback" && section === "uploader" && !selectedUploader && (
          <div className="space-y-4">
            <Breadcrumb parts={[companyLabel, "Data Uploader"]} onBack={backToCompany} />
            <BoxGrid>
              {LP_FEEDBACK_UPLOADERS.map((u) => (
                <Box
                  key={u.code}
                  icon={u.icon}
                  label={u.label}
                  description={u.description}
                  onClick={() => setSelectedUploader({ code: u.code, label: u.label })}
                />
              ))}
            </BoxGrid>
          </div>
        )}

        {company === "lp_feedback" && section === "uploader" && selectedUploader && (
          <div className="space-y-4">
            <Breadcrumb parts={[companyLabel, "Data Uploader", selectedUploader.label]} onBack={backToUploaderGrid} />
            <BellavitaMasmisUploader templateCode={selectedUploader.code} label={selectedUploader.label} />
          </div>
        )}

        {company === "lp_onboarding" && section === "uploader" && !selectedUploader && (
          <div className="space-y-4">
            <Breadcrumb parts={[companyLabel, "Data Uploader"]} onBack={backToCompany} />
            <BoxGrid>
              {LP_ONBOARDING_UPLOADERS.map((u) => (
                <Box
                  key={u.code}
                  icon={u.icon}
                  label={u.label}
                  description={u.description}
                  onClick={() => setSelectedUploader({ code: u.code, label: u.label })}
                />
              ))}
            </BoxGrid>
          </div>
        )}

        {company === "lp_onboarding" && section === "uploader" && selectedUploader && (
          <div className="space-y-4">
            <Breadcrumb parts={[companyLabel, "Data Uploader", selectedUploader.label]} onBack={backToUploaderGrid} />
            <BellavitaMasmisUploader templateCode={selectedUploader.code} label={selectedUploader.label} />
          </div>
        )}

        {/* Dalmia/DU Bangladesh/Viega/Exicom only have an Inbound dashboard so far
            (no uploader requested) — stub, same "nothing here yet" state the
            Dashboards section uses elsewhere, so Uploader is never a dead end. */}
        {(company === "dalmia" || company === "dubangladesh" || company === "viega" || company === "exicom") && section === "uploader" && (
          <div className="space-y-4">
            <Breadcrumb parts={[companyLabel, "Data Uploader"]} onBack={backToCompany} />
            <div className="flex items-center justify-center rounded-xl border border-dashed border-slate-200 bg-white p-16 text-sm text-slate-400">
              Nothing here yet
            </div>
          </div>
        )}
      </div>
    </DashboardLayout>
  );
}
