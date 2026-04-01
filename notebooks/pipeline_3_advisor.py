# Databricks notebook source
# =============================================================================
# pipeline_3_advisor.py — AI Schema Advisor (Self-Healing, P1-Triggered)
# =============================================================================
#
# TRIGGER:
#   Pipeline 1 calls this notebook via dbutils.notebook.run() when it detects
#   CRITICAL schema drift (missing column, PHI type change).  WARNING drift is
#   logged to ADLS and can also trigger this notebook via Databricks Job or
#   manually for advisory review.
#
# TWO MODES:
#
#   AUTONOMOUS (default — used when P1 calls this via dbutils.notebook.run()):
#     • table_name + drift_event JSON are passed as notebook arguments by P1.
#     • P3 calls GPT-4o, validates DDL, executes it, updates master_schema.json.
#     • No human interaction. Exits with JSON payload P1 reads in its
#       trigger_pipeline_3() function.
#
#   INTERACTIVE (manual run for Warning drift or post-incident review):
#     • Operator sets table_name widget, optionally pastes drift JSON.
#     • P3 shows rich HTML recommendation card.
#     • Operator sets approval_decision widget → re-runs from Cell 9.
#
# WHY dbutils.notebook.exit() ALONE IS NOT ENOUGH:
#   When called via dbutils.notebook.run(), exit() signals the *caller*
#   but does NOT stop subsequent cells from executing in the child context.
#   Every cell after a potential exit path MUST check _p3_early_exit.
#
# EXIT PAYLOAD (always JSON):
#   {"status": "fix_applied"|"fix_declined"|"no_fix_needed"|"error",
#    "table": str, "severity": str, "sql_fix": str, "reasoning": str,
#    "ddl_executed": bool, "schema_updated": bool}
#
# PARAMETERS (widget / Job task param):
#   table_name    — short Synthea table name (e.g. "patients")
#   drift_event   — JSON string of the drift report dict from detect_drift()
#   run_mode      — "autonomous" (default, P1-triggered) | "interactive" (manual)
#
# DEPENDS ON: utils/00_config, utils/04_metadata_manager, utils/03_openai_client
# =============================================================================

# COMMAND ----------

# MAGIC %run ./utils/00_config

# COMMAND ----------

# MAGIC %run ./utils/04_metadata_manager

# COMMAND ----------

# MAGIC %run ./utils/03_openai_client

# COMMAND ----------

# =============================================================================
# CELL 1 — Imports
# =============================================================================

import json
import re
import traceback
from datetime import datetime

# COMMAND ----------

# =============================================================================
# CELL 2 — Widget Definitions
# =============================================================================
# In AUTONOMOUS mode (called by P1 via dbutils.notebook.run()):
#   • table_name and drift_event are passed as notebook arguments.
#   • run_mode defaults to "autonomous".
#   • approval_decision is ignored (auto-approved after DDL validation).
#
# In INTERACTIVE mode (manual run):
#   • Operator sets table_name. drift_event can be left empty → loads latest
#     drift log from ADLS for that table automatically.
#   • Operator reads Cell 8 HTML card, then sets approval_decision and
#     re-runs from Cell 9 onward.

dbutils.widgets.text(
    "table_name",
    "",
    "Table name (e.g. patients)",
)
dbutils.widgets.text(
    "drift_event",
    "",
    "Drift event JSON (from P1). Leave empty to load latest ADLS drift log.",
)
dbutils.widgets.dropdown(
    "run_mode",
    "autonomous",
    ["autonomous", "interactive"],
    "Run mode: autonomous (P1-triggered) or interactive (manual review)",
)
dbutils.widgets.dropdown(
    "approval_decision",
    "pending",
    ["pending", "approve", "decline"],
    "INTERACTIVE ONLY: set to 'approve' or 'decline', then re-run from Cell 9",
)

# COMMAND ----------

# =============================================================================
# CELL 3 — Read Parameters & Initialise
# =============================================================================

TABLE_NAME = dbutils.widgets.get("table_name").strip().lower()
DRIFT_JSON = dbutils.widgets.get("drift_event").strip()
RUN_MODE   = dbutils.widgets.get("run_mode").strip().lower()
APPROVAL   = dbutils.widgets.get("approval_decision").strip().lower()

# --- State flags ------------------------------------------------------------
# _p3_early_exit: set to True on ANY path that calls dbutils.notebook.exit().
# Every subsequent cell must check this flag first so notebook.run() child
# execution does not crash on None sentinels.
# ---------------------------------------------------------------------------
_p3_early_exit = False

# Sentinel values — overwritten in cells that produce them
DRIFT_REPORT      = None
AI_RECOMMENDATION = None
SQL_FIX           = ""
NEW_JSON          = {}
AI_SEVERITY       = "UNKNOWN"
AI_REASONING      = ""
DDL_EXECUTED      = False
SCHEMA_UPDATED    = False
SHOULD_APPLY      = False

if not TABLE_NAME:
    _p3_early_exit = True
    dbutils.notebook.exit(json.dumps({
        "status":         "error",
        "table":          "",
        "severity":       "NONE",
        "sql_fix":        "",
        "ddl_executed":   False,
        "schema_updated": False,
        "reasoning":      "table_name widget is empty. Pass a table name.",
    }))

if not _p3_early_exit:
    init_catalog()
    print(f"{'─'*65}")
    print(f"  Pipeline 3 — AI Schema Advisor")
    print(f"  Started:   {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Table:     {TABLE_NAME}")
    print(f"  Run mode:  {RUN_MODE.upper()}")
    print(f"{'─'*65}")

# COMMAND ----------

# =============================================================================
# CELL 4 — Load Drift Event
# =============================================================================
# Resolution order:
#   1. Inline JSON string passed by P1 via drift_event widget/notebook arg.
#   2. Most recent timestamped drift log in ADLS /logs/drift/<table>_drift_*.json

def load_drift_event(table_name: str, raw_json: str) -> dict:
    """
    Load and validate the drift report.

    P1 passes it as a JSON string (via build_drift_event_payload → json.dumps).
    Interactive runs can leave it empty → load from ADLS drift log.
    """
    # ── Source 1: Inline JSON from P1 ────────────────────────────────────
    if raw_json:
        try:
            drift = json.loads(raw_json)
        except json.JSONDecodeError as e:
            raise ValueError(f"drift_event widget contains invalid JSON: {e}") from e

        # Verify the drift event is for the right table
        if drift.get("table") and drift.get("table") != table_name:
            raise ValueError(
                f"Drift event is for table '{drift.get('table')}', "
                f"not '{table_name}'. Check the table_name widget."
            )
        log.info(f"[P3] Drift event loaded from inline parameter ({len(raw_json):,} chars)")
        return drift

    # ── Source 2: Latest ADLS drift log ──────────────────────────────────
    drift_dir = f"{LOGS_PATH}/drift"
    prefix    = f"{table_name}_drift_"
    try:
        files = dbutils.fs.ls(drift_dir)
        matches = sorted(
            [f for f in files if f.name.startswith(prefix) and f.name.endswith(".json")],
            key=lambda f: f.name,
            reverse=True,   # most recent first — name format: TABLE_drift_YYYYMMDD_HHMMSS.json
        )
        if not matches:
            raise FileNotFoundError(
                f"No drift log for '{table_name}' in {drift_dir}. "
                f"Run Pipeline 1 to generate one or paste drift JSON in the widget."
            )
        raw    = dbutils.fs.head(matches[0].path, 524_288)   # 512 KB cap
        payload = json.loads(raw)
        drift   = payload.get("drift", payload)  # log format: {"timestamp":..., "drift":{...}}
        log.info(f"[P3] Drift event loaded from ADLS: {matches[0].path}")
        return drift
    except FileNotFoundError:
        raise
    except Exception as e:
        raise ValueError(
            f"Could not load drift event for '{table_name}': {e}. "
            f"Provide drift_event JSON in the widget or trigger via P1."
        ) from e


if not _p3_early_exit:
    try:
        DRIFT_REPORT = load_drift_event(TABLE_NAME, DRIFT_JSON)
        print(f"\n  Drift report loaded:")
        print(f"    has_drift:   {DRIFT_REPORT.get('has_drift', False)}")
        print(f"    severity:    {DRIFT_REPORT.get('severity', 'NONE')}")
        print(f"    new_cols:    {len(DRIFT_REPORT.get('new_columns', []))}")
        print(f"    missing:     {len(DRIFT_REPORT.get('missing_columns', []))}")
        print(f"    type_chg:    {len(DRIFT_REPORT.get('type_changes', []))}")
    except Exception as ve:
        log.error(f"[P3] Drift event load failed: {ve}")
        _p3_early_exit = True
        dbutils.notebook.exit(json.dumps({
            "status":         "error",
            "table":          TABLE_NAME,
            "severity":       "NONE",
            "sql_fix":        "",
            "ddl_executed":   False,
            "schema_updated": False,
            "reasoning":      str(ve),
        }))

# COMMAND ----------

# =============================================================================
# CELL 5 — Early Exit: No Drift
# =============================================================================
# P1 should only trigger P3 on CRITICAL/WARNING drift, but guard anyway.

if not _p3_early_exit and DRIFT_REPORT is not None:
    if not DRIFT_REPORT.get("has_drift", False):
        msg = f"No drift detected for '{TABLE_NAME}'. No action required."
        log.info(f"[P3] {msg}")
        print(f"  ✅ {msg}")
        _p3_early_exit = True
        dbutils.notebook.exit(json.dumps({
            "status":         "no_fix_needed",
            "table":          TABLE_NAME,
            "severity":       "NONE",
            "sql_fix":        "",
            "ddl_executed":   False,
            "schema_updated": False,
            "reasoning":      msg,
        }))

# COMMAND ----------

# =============================================================================
# CELL 6 — Call AI Advisor (GPT-4o)
# =============================================================================

if not _p3_early_exit and DRIFT_REPORT is not None:
    print(f"\n  Calling AI Advisor (GPT-4o) …")
    print(f"  Drift severity: {DRIFT_REPORT.get('severity', 'UNKNOWN')}")
    try:
        AI_RECOMMENDATION = get_advisor_recommendation(DRIFT_REPORT, TABLE_NAME)
        print(f"  AI response received:")
        print(f"    SEVERITY:  {AI_RECOMMENDATION['SEVERITY']}")
        print(f"    SQL_FIX:   {AI_RECOMMENDATION['SQL_FIX'][:120] if AI_RECOMMENDATION['SQL_FIX'] else '(none — advisory only)'}")
        print(f"    NEW_JSON:  {list(AI_RECOMMENDATION['NEW_JSON'].keys())}")
        print(f"    REASONING: {AI_RECOMMENDATION['REASONING'][:150]}…")
    except Exception as e:
        err = f"AI Advisor call failed: {e}"
        log.error(f"[P3] {err}")
        log.error(traceback.format_exc())
        _p3_early_exit = True
        dbutils.notebook.exit(json.dumps({
            "status":         "error",
            "table":          TABLE_NAME,
            "severity":       DRIFT_REPORT.get("severity", "UNKNOWN"),
            "sql_fix":        "",
            "ddl_executed":   False,
            "schema_updated": False,
            "reasoning":      err,
        }))

# COMMAND ----------

# =============================================================================
# CELL 7 — Validate AI-Generated DDL
# =============================================================================
# The advisor may only emit ADD COLUMNS DDL.  Never DROP, TRUNCATE, etc.
# An empty SQL_FIX is valid — means advisory-only (e.g. missing column where
# data cannot be restored without reingestion).

_ADVISOR_DDL_ALLOWED_AUTONOMOUS = re.compile(
    r"^\s*ALTER\s+TABLE\s+[\w.`]+\s+ADD\s+COLUMNS?\s*\(",
    re.IGNORECASE,
)
# Block destructive row-level or access operations, and DROP TABLE entirely.
# We explicitly allow DROP COLUMN, ALTER COLUMN, REPLACE TABLE in interactive mode.
_ADVISOR_DDL_BLOCKED_ALL = re.compile(
    r"\b(DELETE|UPDATE|INSERT|TRUNCATE|DROP\s+TABLE|CREATE\s+TABLE|GRANT|REVOKE|MERGE)\b",
    re.IGNORECASE,
)


def validate_advisor_ddl(sql_fix: str, table_name: str, run_mode: str) -> tuple:
    """
    Validate that the AI-generated DDL is safe to execute based on run_mode.

    Rules:
      1. Empty SQL_FIX is valid — advisory only (e.g. missing columns).
      2. In AUTONOMOUS mode: Must start with ALTER TABLE ... ADD COLUMNS (
      3. In INTERACTIVE mode: Allows other schema modifications like DROP COLUMN,
         ALTER COLUMN, but blocks DROP TABLE, DELETE, UPDATE, etc.
      4. Must reference the correct table name (partial or full).
      5. Must not contain subquery keywords (SELECT / FROM / WHERE).

    Returns:
        (is_valid: bool, reason: str)
    """
    if not sql_fix or not sql_fix.strip():
        return True, "SQL_FIX is empty — advisory only, no DDL to execute"

    sql        = sql_fix.strip()
    full_table = get_full_table_name(table_name)

    table_ref_pattern = re.compile(
        re.escape(table_name) + r"|" + re.escape(full_table),
        re.IGNORECASE,
    )
    if not table_ref_pattern.search(sql):
        return False, (
            f"DDL does not reference '{table_name}' or '{full_table}'. "
            f"Possible wrong-table DDL: {sql[:120]}"
        )

    # Mode-specific validation
    if run_mode == "autonomous":
        if not _ADVISOR_DDL_ALLOWED_AUTONOMOUS.match(sql):
            return False, (
                f"AUTONOMOUS MODE: DDL must start with 'ALTER TABLE <table> ADD COLUMNS ('. "
                f"Destructive/complex schema changes require manual review in interactive mode. Got: {sql[:120]}"
            )
        # Verify no blocked terms that slip through
        if _ADVISOR_DDL_BLOCKED_ALL.search(sql):
            return False, f"Blocked keyword found in SQL_FIX"
    else:
        # Interactive mode: more permissive format
        if not sql.upper().startswith("ALTER TABLE") and not sql.upper().startswith("CREATE OR REPLACE TABLE"):
            return False, (
                "INTERACTIVE MODE: Only 'ALTER TABLE' or 'CREATE OR REPLACE TABLE' "
                "commands are permitted."
            )
        
        blocked_match = _ADVISOR_DDL_BLOCKED_ALL.search(sql)
        if blocked_match:
            return False, f"Blocked DDL keyword '{blocked_match.group()}' found in SQL_FIX"

    if re.search(r"\bSELECT\b|\bFROM\b|\bWHERE\b", sql, re.IGNORECASE):
        return False, "SQL_FIX contains subquery keywords (SELECT/FROM/WHERE) — rejected"

    return True, f"DDL validation passed ({run_mode} mode)"


if not _p3_early_exit and AI_RECOMMENDATION is not None:
    SQL_FIX      = AI_RECOMMENDATION["SQL_FIX"].strip()
    NEW_JSON     = AI_RECOMMENDATION["NEW_JSON"]
    AI_SEVERITY  = AI_RECOMMENDATION["SEVERITY"]
    AI_REASONING = AI_RECOMMENDATION["REASONING"]

    is_valid_ddl, validation_msg = validate_advisor_ddl(SQL_FIX, TABLE_NAME, RUN_MODE)

    print(f"\n  DDL Validation: {'✅ PASSED' if is_valid_ddl else '❌ FAILED'}")
    print(f"    Reason: {validation_msg}")

    if not is_valid_ddl:
        log.error(f"[P3] AI-generated DDL failed validation: {validation_msg}")
        log.error(f"[P3] Rejected DDL: {SQL_FIX}")
        _p3_early_exit = True
        dbutils.notebook.exit(json.dumps({
            "status":         "error",
            "table":          TABLE_NAME,
            "severity":       AI_SEVERITY,
            "sql_fix":        SQL_FIX,
            "ddl_executed":   False,
            "schema_updated": False,
            "reasoning":      f"DDL validation failed: {validation_msg}",
        }))

# COMMAND ----------

# =============================================================================
# CELL 8 — Display Rich AI Recommendation
# =============================================================================
# In AUTONOMOUS mode: output goes to the detached notebook's stdout (log only).
# In INTERACTIVE mode: displayHTML() renders the card for the operator to read.

def render_recommendation_html(table: str, drift: dict, rec: dict) -> str:
    """Build an HTML summary card of the AI recommendation for Databricks UI."""
    severity_color = {
        "CRITICAL": "#c0392b", "WARNING": "#e67e22", "INFO": "#27ae60"
    }.get(rec["SEVERITY"], "#7f8c8d")

    drift_severity_color = {
        "CRITICAL": "#c0392b", "WARNING": "#e67e22", "NONE": "#27ae60"
    }.get(drift.get("severity", "NONE"), "#7f8c8d")

    def _rows(items, key_map):
        return "".join(
            "<tr>" + "".join(f"<td style='padding:4px 8px'>{row.get(k,'')}</td>" for k in key_map) + "</tr>"
            for row in items
        ) or "<tr><td colspan='3' style='padding:4px 8px;color:#888'>None</td></tr>"

    new_cols_rows = _rows(
        [{"col": c["column"], "type": c.get("incoming_type","?"), "phi": "⚠️ Possible PHI"} for c in drift.get("new_columns", [])],
        ["col", "type", "phi"]
    )
    missing_cols_rows = _rows(
        [{"col": c["column"], "type": c.get("expected_type","?"), "phi": "⛔ PHI" if c.get("is_phi") else "⚠️"} for c in drift.get("missing_columns", [])],
        ["col", "type", "phi"]
    )
    type_chg_rows = _rows(
        [{"col": c["column"], "type": f"{c.get('expected_type','?')} → {c.get('incoming_type','?')}", "phi": "⛔ PHI" if c.get("is_phi") else "⚠️"} for c in drift.get("type_changes", [])],
        ["col", "type", "phi"]
    )

    sql_code = rec["SQL_FIX"] if rec["SQL_FIX"] else "(no DDL — advisory only)"
    new_json_str = json.dumps(rec["NEW_JSON"], indent=2) if rec["NEW_JSON"] else "(none)"

    return f"""
<div style="font-family:monospace;max-width:960px;background:#1a1a2e;color:#e0e0e0;
            padding:24px;border-radius:10px;border:1px solid #444">
  <h2 style="color:#f0c040;margin:0 0 12px">🔬 AI Schema Advisor — {table.upper()}</h2>
  <p style="margin:0 0 16px">
    <span style="background:{drift_severity_color};color:#fff;padding:3px 10px;border-radius:4px">
      DRIFT: {drift.get('severity','NONE')}
    </span>
    &nbsp;
    <span style="background:{severity_color};color:#fff;padding:3px 10px;border-radius:4px">
      AI SEVERITY: {rec['SEVERITY']}
    </span>
  </p>

  <h3 style="color:#a0d0ff;margin:12px 0 6px">Schema Changes Detected</h3>
  <table style="width:100%;border-collapse:collapse;background:#111;border-radius:4px">
    <tr style="background:#2a2a4a;color:#a0d0ff">
      <th style="padding:6px 8px;text-align:left">Column</th>
      <th style="padding:6px 8px;text-align:left">Type</th>
      <th style="padding:6px 8px;text-align:left">PHI Risk</th>
    </tr>
    {new_cols_rows}
    {missing_cols_rows}
    {type_chg_rows}
  </table>

  <h3 style="color:#a0d0ff;margin:16px 0 6px">AI Reasoning</h3>
  <p style="background:#111;padding:10px;border-radius:4px;color:#d0e0d0;white-space:pre-wrap">{rec['REASONING']}</p>

  <h3 style="color:#a0d0ff;margin:16px 0 6px">Proposed DDL Fix</h3>
  <pre style="background:#111;padding:10px;border-radius:4px;color:#f0c040;overflow-x:auto">{sql_code}</pre>

  <h3 style="color:#a0d0ff;margin:16px 0 6px">master_schema.json Updates (NEW_JSON)</h3>
  <pre style="background:#111;padding:10px;border-radius:4px;color:#90ee90;overflow-x:auto">{new_json_str}</pre>

  <div style="margin-top:20px;padding:12px;background:#2a1a1a;border-radius:4px;border-left:3px solid #c0392b">
    <strong style="color:#f0c040">⚠️ INTERACTIVE MODE — ACTION REQUIRED</strong><br/>
    1. Review the recommendation above carefully (check PHI flags).<br/>
    2. Set <code style="background:#333;padding:2px 4px">approval_decision</code> widget to
       <strong>approve</strong> or <strong>decline</strong>.<br/>
    3. Re-run from <strong>Cell 9</strong> onward (Run ▶ from this cell).
  </div>
</div>"""


if not _p3_early_exit and AI_RECOMMENDATION is not None:
    print(f"\n{'='*65}")
    print(f"  AI RECOMMENDATION — {TABLE_NAME.upper()}")
    print(f"{'='*65}")
    print(f"  AI Severity:  {AI_SEVERITY}")
    print(f"  SQL Fix:      {SQL_FIX[:120] if SQL_FIX else '(none — advisory only)'}")
    print(f"  Schema cols:  {list(NEW_JSON.keys())}")
    print(f"\n  Reasoning (excerpt):\n  {AI_REASONING[:300]}{'…' if len(AI_REASONING) > 300 else ''}")
    print(f"{'='*65}")

    if RUN_MODE == "interactive":
        displayHTML(render_recommendation_html(TABLE_NAME, DRIFT_REPORT, AI_RECOMMENDATION))
        print("\n  ⏸️  INTERACTIVE MODE — set approval_decision widget and re-run from Cell 9.")

# COMMAND ----------

# =============================================================================
# CELL 9 — Human Approval Gate / Autonomous Decision
# =============================================================================
# AUTONOMOUS:   Always approved (DDL already validated in Cell 7).
# INTERACTIVE:  Human must set approval_decision widget to "approve" or "decline".
#               If still "pending", we stop — do NOT apply anything.

if not _p3_early_exit and AI_RECOMMENDATION is not None:
    if RUN_MODE == "autonomous":
        SHOULD_APPLY = True
        print(f"\n  Mode: AUTONOMOUS — applying AI fix (DDL validated ✅)")
    else:
        # Interactive: respect human decision
        if APPROVAL == "pending":
            msg = (
                "Approval is still 'pending'. Set the approval_decision widget to "
                "'approve' or 'decline' and re-run from Cell 9."
            )
            print(f"\n  ⏸️  {msg}")
            _p3_early_exit = True
            dbutils.notebook.exit(json.dumps({
                "status":         "error",
                "table":          TABLE_NAME,
                "severity":       AI_SEVERITY,
                "sql_fix":        SQL_FIX,
                "ddl_executed":   False,
                "schema_updated": False,
                "reasoning":      msg,
            }))
        elif APPROVAL == "approve":
            SHOULD_APPLY = True
            print(f"\n  ✅ Human approved the AI fix for '{TABLE_NAME}'.")
        else:
            SHOULD_APPLY = False
            print(f"\n  ❌ Human declined the AI fix for '{TABLE_NAME}'.")

    if not _p3_early_exit:
        print(f"  Decision: {'APPLY FIX ✅' if SHOULD_APPLY else 'DECLINE ❌'}")

# COMMAND ----------

# =============================================================================
# CELL 10 — Execute DDL Fix (if approved and SQL_FIX is non-empty)
# =============================================================================
# For MISSING COLUMNS (subtractive drift):
#   SQL_FIX will be empty — the AI cannot restore lost data with DDL.
#   The schema fix still runs (Cell 11) to note the missing column as removed.
#   The operator or a full P1 re-run is required to rehydrate data.

if not _p3_early_exit and SHOULD_APPLY and SQL_FIX:
    full_table_name = get_full_table_name(TABLE_NAME)
    print(f"\n  Executing DDL on {full_table_name}:")
    print(f"  {SQL_FIX}\n")

    try:
        spark.sql(SQL_FIX)
        DDL_EXECUTED = True
        log.info(f"[P3] DDL executed successfully: {SQL_FIX}")
        print(f"  ✅ DDL executed successfully")

        # Verify columns were actually added
        new_col_names = list(NEW_JSON.keys())
        if new_col_names:
            desc        = spark.sql(f"DESCRIBE TABLE {full_table_name}")
            actual_cols = {row["col_name"].lower() for row in desc.collect()}
            for cn in new_col_names:
                if cn.lower() in actual_cols:
                    print(f"  ✅ Column verified in Delta table: {cn}")
                else:
                    print(f"  ⚠️  Column NOT found after DDL — check manually: {cn}")

    except Exception as e:
        err_str = str(e)
        if "already exists" in err_str.lower():
            # Idempotent — column was already added (e.g. re-run after partial failure)
            log.warning(f"[P3] Column already exists — DDL is idempotent: {e}")
            print(f"  ⚠️  Column already exists (schema is already correct — treating as success)")
            DDL_EXECUTED = True
        else:
            log.error(f"[P3] DDL execution failed: {e}")
            log.error(traceback.format_exc())
            print(f"  ❌ DDL failed: {e}")
            _p3_early_exit = True
            dbutils.notebook.exit(json.dumps({
                "status":         "error",
                "table":          TABLE_NAME,
                "severity":       AI_SEVERITY,
                "sql_fix":        SQL_FIX,
                "ddl_executed":   False,
                "schema_updated": False,
                "reasoning":      f"DDL execution failed: {err_str}",
            }))

elif not _p3_early_exit and SHOULD_APPLY and not SQL_FIX:
    # Advisory only (e.g. missing column — AI recommends reingestion, no DDL)
    print(f"\n  ℹ️  No DDL to execute — AI advisory only.")
    print(f"  Will update master_schema.json metadata only (Cell 11).")

# COMMAND ----------

# =============================================================================
# CELL 11 — Update master_schema.json
# =============================================================================
# Always runs if SHOULD_APPLY is True (even if no DDL was executed).
# NEW_JSON entries add/update column definitions in the schema.
# For missing columns — the AI may return an EMPTY NEW_JSON (no update needed)
# or a NEW_JSON with the column removed (if the schema should reflect reality).

if not _p3_early_exit and SHOULD_APPLY:
    if NEW_JSON:
        try:
            update_table_columns(TABLE_NAME, NEW_JSON)
            invalidate_cache()
            SCHEMA_UPDATED = True
            print(f"\n  ✅ master_schema.json updated — {len(NEW_JSON)} column definition(s):")
            for col_name, col_def in NEW_JSON.items():
                phi_flag = "⛔ PHI" if col_def.get("phi") else "✅ non-PHI"
                print(
                    f"     {col_name}: {col_def.get('type','?')} | "
                    f"{phi_flag} | masking={col_def.get('masking','none')}"
                )
        except Exception as e:
            log.error(f"[P3] master_schema.json update failed: {e}")
            print(f"\n  ❌ Schema JSON update failed: {e}")
            print(f"      DDL may already be committed. Manually patch master_schema.json:")
            print(json.dumps(NEW_JSON, indent=4))
            # Do NOT exit with error here — DDL is committed, partial state is worse
    else:
        print(f"\n  ℹ️  NEW_JSON is empty — no schema metadata changes to apply.")
        print(f"      For missing columns, re-run Pipeline 1 after CSV is fixed.")

# COMMAND ----------

# =============================================================================
# CELL 12 — Log Advisory Event to ADLS
# =============================================================================

def log_advisor_event(outcome: dict) -> None:
    """
    Persist the full advisor event — inputs, AI output, and outcome — to
    ADLS /logs/advisor/<table>_advisor_<timestamp>.json for audit trail.
    """
    if DRIFT_REPORT is None or AI_RECOMMENDATION is None:
        log.warning("[P3] Skipping advisor log — DRIFT_REPORT or AI_RECOMMENDATION is None")
        return

    ts       = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = f"{LOGS_PATH}/advisor/{TABLE_NAME}_advisor_{ts}.json"
    payload  = {
        "timestamp":      datetime.now().isoformat(),
        "table":          TABLE_NAME,
        "run_mode":       RUN_MODE,
        "drift_input":    DRIFT_REPORT,
        "ai_severity":    AI_RECOMMENDATION.get("SEVERITY", "UNKNOWN"),
        "sql_fix":        AI_RECOMMENDATION.get("SQL_FIX", ""),
        "new_json":       AI_RECOMMENDATION.get("NEW_JSON", {}),
        "reasoning":      AI_RECOMMENDATION.get("REASONING", ""),
        "ddl_executed":   outcome.get("ddl_executed", False),
        "schema_updated": outcome.get("schema_updated", False),
        "outcome_status": outcome.get("status", "unknown"),
    }
    try:
        dbutils.fs.put(log_path, json.dumps(payload, indent=2), overwrite=True)
        log.info(f"[P3] Advisory event logged: {log_path}")
        print(f"\n  📋 Advisory event logged: {log_path}")
    except Exception as e:
        log.warning(f"[P3] Advisory log write failed (non-critical): {e}")


if not _p3_early_exit:
    outcome_status = "fix_applied" if SHOULD_APPLY else "fix_declined"
    log_advisor_event({
        "ddl_executed":   DDL_EXECUTED,
        "schema_updated": SCHEMA_UPDATED,
        "status":         outcome_status,
    })

# COMMAND ----------

# =============================================================================
# CELL 13 — Final Summary & Notebook Exit
# =============================================================================
# Determine final status and build the exit payload that Pipeline 1 reads
# in its trigger_pipeline_3() function.

if not _p3_early_exit:
    if not SHOULD_APPLY:
        final_status = "fix_declined"
    elif DDL_EXECUTED or SCHEMA_UPDATED:
        final_status = "fix_applied"
    elif not SQL_FIX and not NEW_JSON:
        final_status = "no_fix_needed"
    else:
        final_status = "fix_applied"   # advisory-only — reasoning noted, schema possibly updated

    print(f"\n{'='*65}")
    print(f"  Pipeline 3 — AI Advisor COMPLETE")
    print(f"  Completed:      {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Table:          {TABLE_NAME}")
    print(f"  Drift severity: {DRIFT_REPORT.get('severity', 'UNKNOWN') if DRIFT_REPORT else 'UNKNOWN'}")
    print(f"  AI severity:    {AI_SEVERITY}")
    print(f"  DDL executed:   {DDL_EXECUTED}")
    print(f"  Schema updated: {SCHEMA_UPDATED}")
    print(f"  Final status:   {final_status.upper()}")
    print(f"{'='*65}")

    if final_status == "fix_applied":
        print(f"\n  ✅ Fix applied. Pipeline 1 can now re-run '{TABLE_NAME}'.")
    elif final_status == "fix_declined":
        print(f"\n  ❌ Fix declined. No changes made. Investigate drift manually.")
    elif final_status == "no_fix_needed":
        print(f"\n  ℹ️  No DDL required. Schema metadata updated where applicable.")

    # ── Exit payload read by P1's trigger_pipeline_3() ────────────────────
    exit_payload = json.dumps({
        "status":         final_status,
        "table":          TABLE_NAME,
        "severity":       AI_SEVERITY,
        "sql_fix":        SQL_FIX,
        "ddl_executed":   DDL_EXECUTED,
        "schema_updated": SCHEMA_UPDATED,
        "reasoning":      AI_REASONING,
    })

    dbutils.notebook.exit(exit_payload)
