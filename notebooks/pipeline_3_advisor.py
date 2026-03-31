# Databricks notebook source
# =============================================================================
# pipeline_3_advisor.py — AI Schema Advisor (Self-Healing)
# =============================================================================
# Receives a drift event from Pipeline 1 (or a human operator), calls GPT-4o
# to generate a remediation plan, and then either:
#   • AUTONOMOUS mode (called by P1 via dbutils.notebook.run):
#     Validates AI fix → executes DDL → updates master_schema.json → exits.
#     No human interaction required; P1 gets the result in its exit payload.
#
#   • INTERACTIVE mode (manual run in Databricks Repos UI):
#     Displays rich AI recommendation → operator reads & sets approval widget
#     → re-runs from the approval cell → fix applied / declined.
#
# WHY TWO MODES?
#   dbutils.notebook.run() executes a notebook in a separate, hidden context.
#   There is no Databricks UI for the operator to interact with widgets during
#   that run.  So autonomous mode skips the widget approval step, trusting the
#   DDL validation and PHI safety checks we perform in code.
#   Interactive mode is for human review of WARNING-level drift events that P1
#   already proceeded past (using mergeSchema fallback) but want proper fixes.
#
# Exit payload (always JSON for dbutils.notebook.exit()):
#   {"status": "fix_applied"|"fix_declined"|"no_fix_needed"|"error",
#    "table": str, "severity": str, "sql_fix": str, "reasoning": str}
#
# Parameters (widgets / Job task params):
#   table_name    — short table name (e.g. "patients")
#   drift_event   — JSON string of the drift report from detect_drift()
#   run_mode      — "autonomous" (default, from P1) or "interactive" (manual)
#
# Depends on: utils/00_config, utils/04_metadata_manager, utils/03_openai_client
# =============================================================================

# COMMAND ----------

# MAGIC %run ./utils/00_config

# COMMAND ----------

# MAGIC %run ./utils/04_metadata_manager

# COMMAND ----------

# MAGIC %run ./utils/03_openai_client

# COMMAND ----------

# =============================================================================
# CELL 1 — Imports (all at module level)
# =============================================================================

import json
import re
import traceback
from datetime import datetime

# COMMAND ----------

# =============================================================================
# CELL 2 — Widget Definitions
# =============================================================================
# In AUTONOMOUS mode (called from Pipeline 1 via dbutils.notebook.run):
#   - table_name and drift_event are passed as arguments => widgets get those values
#   - run_mode defaults to "autonomous"
#   - approval_decision widget is defined but irrelevant (auto-approved in code)
#
# In INTERACTIVE mode (manual run from Databricks Repos UI):
#   - Operator sets table_name and pastes drift JSON (or leaves drift_event empty
#     to load from the most recent ADLS drift log for that table)
#   - Operator reads the AI recommendation displayed in Cell 8
#   - Operator sets approval_decision to "approve" or "decline"
#   - Operator re-runs from Cell 9 onward

dbutils.widgets.text(
    "table_name",
    "",
    "Table name (e.g. patients)",
)
dbutils.widgets.text(
    "drift_event",
    "",
    "Drift event JSON (from P1) — leave empty to load latest from ADLS log",
)
dbutils.widgets.dropdown(
    "run_mode",
    "autonomous",
    ["autonomous", "interactive"],
    "Run mode: autonomous (from P1) or interactive (manual)",
)
dbutils.widgets.dropdown(
    "approval_decision",
    "pending",
    ["pending", "approve", "decline"],
    "INTERACTIVE: Set to 'approve' or 'decline' then re-run from Cell 9",
)

# COMMAND ----------

# =============================================================================
# CELL 3 — Read Parameters & Initialise
# =============================================================================

TABLE_NAME    = dbutils.widgets.get("table_name").strip().lower()
DRIFT_JSON    = dbutils.widgets.get("drift_event").strip()
RUN_MODE      = dbutils.widgets.get("run_mode").strip().lower()
APPROVAL      = dbutils.widgets.get("approval_decision").strip().lower()

if not TABLE_NAME:
    dbutils.notebook.exit(json.dumps({
        "status":    "error",
        "table":     "",
        "severity":  "NONE",
        "sql_fix":   "",
        "reasoning": "table_name widget is empty. Pass a table name.",
    }))

init_catalog()

print(f"{'─'*65}")
print(f"  Pipeline 3 — AI Schema Advisor")
print(f"  Started:    {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"  Table:      {TABLE_NAME}")
print(f"  Run mode:   {RUN_MODE.upper()}")
print(f"{'─'*65}")

# COMMAND ----------

# =============================================================================
# CELL 4 — Load Drift Event
# =============================================================================

def load_drift_event(table_name: str, raw_json: str) -> dict:
    """
    Return the drift report dict. Resolution order:
      1. Inline JSON string (passed by Pipeline 1 via widget / notebook argument)
      2. Most recent drift log file in ADLS /logs/drift/<table>_drift_*.json

    Raises ValueError if neither source can produce a valid drift report.
    """
    # ── Source 1: Inline JSON ──────────────────────────────────────────────
    if raw_json:
        try:
            drift = json.loads(raw_json)
            if drift.get("table") != table_name:
                raise ValueError(
                    f"Drift event is for table '{drift.get('table')}', "
                    f"not '{table_name}'. Check table_name widget."
                )
            log.info(f"Drift event loaded from inline parameter ({len(raw_json):,} chars)")
            return drift
        except json.JSONDecodeError as e:
            raise ValueError(f"drift_event widget contains invalid JSON: {e}") from e

    # ── Source 2: Latest ADLS drift log ───────────────────────────────────
    drift_dir = f"{LOGS_PATH}/drift"
    prefix    = f"{table_name}_drift_"
    try:
        files = dbutils.fs.ls(drift_dir)
        matches = sorted(
            [f for f in files if f.name.startswith(prefix) and f.name.endswith(".json")],
            key=lambda f: f.name,
            reverse=True,   # most recent first (name is timestamped: TABLE_drift_YYYYMMDD_HHMMSS.json)
        )
        if matches:
            raw = dbutils.fs.head(matches[0].path, 524_288)   # 512 KB cap
            payload = json.loads(raw)
            drift = payload.get("drift", payload)   # log has {"timestamp":..., "drift":{...}}
            log.info(f"Drift event loaded from ADLS: {matches[0].path}")
            return drift
        else:
            raise FileNotFoundError(f"No drift log for '{table_name}' in {drift_dir}")
    except Exception as e:
        raise ValueError(
            f"Could not load drift event for '{table_name}': {e}. "
            f"Provide drift_event JSON in the widget or upload a CSV that triggers P1."
        ) from e


# Sentinel: ensures DRIFT_REPORT is always defined even if dbutils.notebook.exit()
# is called inside the try/except (Databricks continues executing subsequent cells
# in a Run All — exit only propagates to the caller, not to the current session).
DRIFT_REPORT   = None
_p3_early_exit = False

try:
    DRIFT_REPORT = load_drift_event(TABLE_NAME, DRIFT_JSON)
    print(f"\n  Drift report loaded:")
    print(f"    has_drift:  {DRIFT_REPORT.get('has_drift', False)}")
    print(f"    severity:   {DRIFT_REPORT.get('severity', 'NONE')}")
    print(f"    new_cols:   {len(DRIFT_REPORT.get('new_columns', []))}")
    print(f"    missing:    {len(DRIFT_REPORT.get('missing_columns', []))}")
    print(f"    type chg:   {len(DRIFT_REPORT.get('type_changes', []))}")
except ValueError as ve:
    log.error(f"[P3] Drift event load failed: {ve}")
    _p3_early_exit = True
    dbutils.notebook.exit(json.dumps({
        "status":    "error",
        "table":     TABLE_NAME,
        "severity":  "NONE",
        "sql_fix":   "",
        "reasoning": str(ve),
    }))

# COMMAND ----------

# =============================================================================
# CELL 5 — Early Exit: No Drift
# =============================================================================
# If P1 mistakenly triggered P3 for a table with no drift, exit cleanly.
# The _p3_early_exit guard prevents NameError on DRIFT_REPORT if Cell 4 failed.

if not _p3_early_exit and DRIFT_REPORT is not None and not DRIFT_REPORT.get("has_drift", False):
    msg = f"No drift detected for '{TABLE_NAME}'. No action required."
    log.info(f"[P3] {msg}")
    print(f"  ✅ {msg}")
    _p3_early_exit = True
    dbutils.notebook.exit(json.dumps({
        "status":    "no_fix_needed",
        "table":     TABLE_NAME,
        "severity":  "NONE",
        "sql_fix":   "",
        "reasoning": msg,
    }))

# COMMAND ----------

# =============================================================================
# CELL 6 — Call AI Advisor
# =============================================================================

AI_RECOMMENDATION = None   # sentinel — prevents NameError in later cells if AI call fails

if not _p3_early_exit:
    print(f"\n  Calling AI Advisor (GPT-4o) …")
    try:
        AI_RECOMMENDATION = get_advisor_recommendation(DRIFT_REPORT, TABLE_NAME)
        print(f"  AI response received:")
        print(f"    SEVERITY:  {AI_RECOMMENDATION['SEVERITY']}")
        print(f"    SQL_FIX:   {AI_RECOMMENDATION['SQL_FIX'][:100] if AI_RECOMMENDATION['SQL_FIX'] else '(none)'}")
        print(f"    NEW_JSON:  {list(AI_RECOMMENDATION['NEW_JSON'].keys())}")
        print(f"    REASONING: {AI_RECOMMENDATION['REASONING'][:120]}…")
    except Exception as e:
        err = f"AI Advisor call failed: {e}\n{traceback.format_exc()}"
        log.error(f"[P3] {err}")
        _p3_early_exit = True
        dbutils.notebook.exit(json.dumps({
            "status":    "error",
            "table":     TABLE_NAME,
            "severity":  DRIFT_REPORT.get("severity", "UNKNOWN") if DRIFT_REPORT else "UNKNOWN",
            "sql_fix":   "",
            "reasoning": str(e),
        }))

# COMMAND ----------

# =============================================================================
# CELL 7 — Validate AI-Generated DDL
# =============================================================================
# The advisor can only issue ADD COLUMNS DDL — never DROP, TRUNCATE, etc.
# We apply strict pattern validation BEFORE any execution.

# Allowed DDL patterns for P3 (more permissive than Pipeline 2 guardrail)
_ADVISOR_DDL_ALLOWED = re.compile(
    r"^\s*ALTER\s+TABLE\s+[\w.`]+\s+ADD\s+COLUMNS?\s*\(",
    re.IGNORECASE,
)
_ADVISOR_DDL_BLOCKED = re.compile(
    r"\b(DROP|DELETE|UPDATE|INSERT|TRUNCATE|CREATE\s+TABLE|GRANT|REVOKE|MERGE|REPLACE)\b",
    re.IGNORECASE,
)

def validate_advisor_ddl(sql_fix: str, table_name: str) -> tuple:
    """
    Validate that the AI-generated DDL is safe to execute.

    Rules:
      1. SQL_FIX must be non-empty to trigger validation (empty = advisory only).
      2. Must start with ALTER TABLE ... ADD COLUMNS (
      3. Must reference the correct fully-qualified table name.
      4. Must contain no blocked destructive keywords.
      5. Must not have subqueries (no SELECT, FROM, WHERE in the fix).

    Returns:
        (is_valid: bool, reason: str)
    """
    if not sql_fix or not sql_fix.strip():
        return True, "SQL_FIX is empty — advisory only, no DDL to execute"

    sql = sql_fix.strip()
    full_table = get_full_table_name(table_name)

    # Rule 1: Must start with ALTER TABLE ... ADD COLUMNS
    if not _ADVISOR_DDL_ALLOWED.match(sql):
        return False, (
            f"DDL does not match required pattern "
            f"'ALTER TABLE <table> ADD COLUMNS (...)'. Got: {sql[:120]}"
        )

    # Rule 2: Table reference must include the correct table name
    # Allow both backtick (project_5.delta_tables.`patients`) and plain forms
    table_ref_pattern = re.compile(
        re.escape(table_name) + r"|" + re.escape(full_table),
        re.IGNORECASE,
    )
    if not table_ref_pattern.search(sql):
        return False, (
            f"DDL references a different table. Expected '{table_name}' or "
            f"'{full_table}' in: {sql[:120]}"
        )

    # Rule 3: No blocked destructive keywords
    blocked_match = _ADVISOR_DDL_BLOCKED.search(sql)
    if blocked_match:
        return False, f"Blocked keyword '{blocked_match.group()}' found in DDL"

    # Rule 4: No subqueries
    if re.search(r"\bSELECT\b|\bFROM\b|\bWHERE\b", sql, re.IGNORECASE):
        return False, "DDL contains subquery keywords (SELECT/FROM/WHERE) — rejected"

    return True, "DDL validation passed"


# These variables are used by later cells — initialise safely with sentinels
SQL_FIX      = ""
NEW_JSON     = {}
AI_SEVERITY  = "UNKNOWN"
AI_REASONING = ""

if not _p3_early_exit and AI_RECOMMENDATION is not None:
    SQL_FIX      = AI_RECOMMENDATION["SQL_FIX"].strip()
    NEW_JSON     = AI_RECOMMENDATION["NEW_JSON"]
    AI_SEVERITY  = AI_RECOMMENDATION["SEVERITY"]
    AI_REASONING = AI_RECOMMENDATION["REASONING"]

    is_valid_ddl, validation_msg = validate_advisor_ddl(SQL_FIX, TABLE_NAME)

    print(f"\n  DDL Validation: {'✅ PASSED' if is_valid_ddl else '❌ FAILED'}")
    print(f"    Reason: {validation_msg}")

    if not is_valid_ddl:
        log.error(f"[P3] AI-generated DDL failed validation: {validation_msg}")
        log.error(f"[P3] Rejected DDL: {SQL_FIX}")
        _p3_early_exit = True
        dbutils.notebook.exit(json.dumps({
            "status":    "error",
            "table":     TABLE_NAME,
            "severity":  AI_SEVERITY,
            "sql_fix":   SQL_FIX,
            "reasoning": f"DDL validation failed: {validation_msg}",
        }))

# COMMAND ----------

# =============================================================================
# CELL 8 — Display Rich AI Recommendation (Interactive mode)
# =============================================================================
# In AUTONOMOUS mode this cell still displays to logs — but there is no human
# watching, so the output goes to the detached notebook's stdout.
# In INTERACTIVE mode this is the key "read before approving" step.

def render_recommendation_html(table: str, drift: dict, rec: dict) -> str:
    """Build an HTML card summarising the AI recommendation for the operator."""
    severity_color = {"CRITICAL": "#c0392b", "WARNING": "#e67e22", "INFO": "#27ae60"}.get(
        rec["SEVERITY"], "#7f8c8d"
    )
    drift_severity_color = {"CRITICAL": "#c0392b", "WARNING": "#e67e22", "NONE": "#27ae60"}.get(
        drift.get("severity", "NONE"), "#7f8c8d"
    )

    new_cols_rows = "".join(
        f"<tr><td style='padding:4px 8px'>{c['column']}</td>"
        f"<td style='padding:4px 8px'>{c.get('incoming_type','?')}</td>"
        f"<td style='padding:4px 8px;color:#e67e22'>⚠️ Could be PHI</td></tr>"
        for c in drift.get("new_columns", [])
    )
    missing_cols_rows = "".join(
        f"<tr><td style='padding:4px 8px'>{c['column']}</td>"
        f"<td style='padding:4px 8px'>{c.get('expected_type','?')}</td>"
        f"<td style='padding:4px 8px;color:#c0392b'>{'⛔ PHI' if c.get('is_phi') else '⚠️'}</td></tr>"
        for c in drift.get("missing_columns", [])
    )
    type_change_rows = "".join(
        f"<tr><td style='padding:4px 8px'>{c['column']}</td>"
        f"<td style='padding:4px 8px'>{c.get('expected_type','?')} → {c.get('incoming_type','?')}</td>"
        f"<td style='padding:4px 8px;color:#{'c0392b' if c.get('is_phi') else 'e67e22'}'>{'⛔ PHI' if c.get('is_phi') else '⚠️'}</td></tr>"
        for c in drift.get("type_changes", [])
    )
    drift_table = (new_cols_rows or missing_cols_rows or type_change_rows) or \
        "<tr><td colspan='3' style='padding:4px 8px;color:#888'>No items</td></tr>"

    new_json_pre = json.dumps(rec["NEW_JSON"], indent=2) if rec["NEW_JSON"] else "(none)"
    sql_fix_code = rec["SQL_FIX"] if rec["SQL_FIX"] else "(no DDL — advisory only)"

    return f"""
<div style="font-family:monospace;max-width:900px;background:#1a1a2e;color:#e0e0e0;
            padding:20px;border-radius:8px;border:1px solid #444;">
  <h2 style="color:#f0c040;margin:0 0 12px">🔬 AI Schema Advisor — {table.upper()}</h2>
  <p style="margin:0 0 16px">
    <span style="background:{drift_severity_color};color:#fff;padding:3px 10px;border-radius:4px;font-size:13px">
      DRIFT: {drift.get('severity','NONE')}
    </span>
    &nbsp;
    <span style="background:{severity_color};color:#fff;padding:3px 10px;border-radius:4px;font-size:13px">
      AI ASSESSMENT: {rec['SEVERITY']}
    </span>
  </p>

  <h3 style="color:#a0d0ff;margin:12px 0 6px">Schema Changes Detected</h3>
  <table style="width:100%;border-collapse:collapse;background:#111;border-radius:4px">
    <tr style="background:#2a2a4a;color:#a0d0ff">
      <th style="padding:6px 8px;text-align:left">Column</th>
      <th style="padding:6px 8px;text-align:left">Type Change</th>
      <th style="padding:6px 8px;text-align:left">PHI Risk</th>
    </tr>
    {drift_table}
  </table>

  <h3 style="color:#a0d0ff;margin:16px 0 6px">AI Reasoning</h3>
  <p style="background:#111;padding:10px;border-radius:4px;color:#d0e0d0;white-space:pre-wrap">{rec['REASONING']}</p>

  <h3 style="color:#a0d0ff;margin:16px 0 6px">Proposed DDL Fix</h3>
  <pre style="background:#111;padding:10px;border-radius:4px;color:#f0c040;overflow-x:auto">{sql_fix_code}</pre>

  <h3 style="color:#a0d0ff;margin:16px 0 6px">master_schema.json Updates (NEW_JSON)</h3>
  <pre style="background:#111;padding:10px;border-radius:4px;color:#90ee90;overflow-x:auto">{new_json_pre}</pre>

  <div style="margin-top:20px;padding:12px;background:#2a1a1a;border-radius:4px;border-left:3px solid #c0392b">
    <strong style="color:#f0c040">⚠️  INTERACTIVE MODE ACTION REQUIRED</strong><br/>
    1. Review the recommendation above carefully.<br/>
    2. Set the <code style="background:#333;padding:2px 4px">approval_decision</code> widget to
       <strong>approve</strong> or <strong>decline</strong>.<br/>
    3. Re-run from <strong>Cell 9</strong> onward to apply or reject the fix.
  </div>
</div>"""


print(f"\n{'='*65}")
print(f"  AI RECOMMENDATION — {TABLE_NAME.upper()}")
print(f"{'='*65}")
print(f"  AI Severity:  {AI_SEVERITY}")
print(f"  SQL Fix:      {SQL_FIX[:120] if SQL_FIX else '(none — advisory only)'}")
print(f"  New columns to register in master_schema.json: {list(NEW_JSON.keys())}")
print(f"\n  Reasoning:\n  {AI_REASONING}")
print(f"{'='*65}")

if RUN_MODE == "interactive":
    displayHTML(render_recommendation_html(TABLE_NAME, DRIFT_REPORT, AI_RECOMMENDATION))
    print("\n  ⏸️  INTERACTIVE MODE: Set 'approval_decision' widget and re-run from Cell 9.")

# COMMAND ----------

# =============================================================================
# CELL 9 — Human Approval Gate / Autonomous Decision
# =============================================================================
# INTERACTIVE:  Human sets approval_decision widget before running this cell.
# AUTONOMOUS:   Decision is always "approve" if DDL passed validation.
#               The DDL validation in Cell 7 is our quality gate for autonomous mode.

if RUN_MODE == "autonomous":
    SHOULD_APPLY = True
    print(f"\n  Mode: AUTONOMOUS — applying AI fix (DDL already validated).")
else:
    # Interactive: respect the human's widget choice
    SHOULD_APPLY = (APPROVAL == "approve")
    if APPROVAL == "pending":
        # Human hasn't set the widget yet — stop here, don't apply anything
        msg = (
            "Approval is still 'pending'. Set the approval_decision widget to "
            "'approve' or 'decline' and re-run from Cell 9."
        )
        print(f"\n  ⏸️  {msg}")
        dbutils.notebook.exit(json.dumps({
            "status":    "error",
            "table":     TABLE_NAME,
            "severity":  AI_SEVERITY,
            "sql_fix":   SQL_FIX,
            "reasoning": msg,
        }))
    elif not SHOULD_APPLY:
        print(f"\n  ❌ Human declined the AI fix for '{TABLE_NAME}'.")

print(f"\n  Decision: {'APPLY FIX ✅' if SHOULD_APPLY else 'DECLINE ❌'}")

# COMMAND ----------

# =============================================================================
# CELL 10 — Execute DDL Fix (if approved and SQL_FIX is non-empty)
# =============================================================================

DDL_EXECUTED = False

if SHOULD_APPLY and SQL_FIX:
    full_table_name = get_full_table_name(TABLE_NAME)
    print(f"\n  Executing DDL on {full_table_name}:")
    print(f"  {SQL_FIX}\n")

    try:
        spark.sql(SQL_FIX)
        DDL_EXECUTED = True
        log.info(f"[P3] DDL executed successfully: {SQL_FIX}")
        print(f"  ✅ DDL executed successfully")

        # Verify: DESCRIBE the table to confirm the column(s) were added
        new_col_names = list(NEW_JSON.keys())
        if new_col_names:
            desc = spark.sql(f"DESCRIBE TABLE {full_table_name}")
            actual_cols = {row["col_name"].lower() for row in desc.collect()}
            for col_name in new_col_names:
                if col_name.lower() in actual_cols:
                    print(f"  ✅ Column verified in table: {col_name}")
                else:
                    print(f"  ⚠️  Column NOT found after DDL: {col_name} — check DDL manually")

    except Exception as e:
        err_str = str(e)
        # Handle idempotency: "already exists" is not a real failure
        if "already exists" in err_str.lower():
            log.warning(f"[P3] Column already exists — DDL was idempotent: {e}")
            print(f"  ⚠️  Column already exists (DDL is idempotent — schema is already correct)")
            DDL_EXECUTED = True   # treat as success; master_schema.json still needs updating
        else:
            log.error(f"[P3] DDL execution failed: {e}")
            log.error(traceback.format_exc())
            print(f"  ❌ DDL failed: {e}")
            dbutils.notebook.exit(json.dumps({
                "status":    "error",
                "table":     TABLE_NAME,
                "severity":  AI_SEVERITY,
                "sql_fix":   SQL_FIX,
                "reasoning": f"DDL execution failed: {err_str}",
            }))

elif SHOULD_APPLY and not SQL_FIX:
    # Advisory only — AI determined no DDL is needed (e.g. missing column WHERE data is gone)
    print(f"\n  ℹ️  No DDL required — advisory recommendation only.")
    print(f"  Updating master_schema.json with NEW_JSON entries if any …")

# COMMAND ----------

# =============================================================================
# CELL 11 — Update master_schema.json
# =============================================================================
# Regardless of whether DDL was executed, if the human approved (or we're in
# autonomous mode) we update master_schema.json with the AI's NEW_JSON entries.
# This keeps the schema metadata in sync with the actual table structure.

SCHEMA_UPDATED = False

if SHOULD_APPLY:
    if NEW_JSON:
        try:
            update_table_columns(TABLE_NAME, NEW_JSON)
            invalidate_cache()
            SCHEMA_UPDATED = True
            print(f"\n  ✅ master_schema.json updated with {len(NEW_JSON)} column definition(s):")
            for col_name, col_def in NEW_JSON.items():
                phi_flag = "⛔ PHI" if col_def.get("phi") else "✅ non-PHI"
                print(f"     {col_name}: {col_def.get('type','?')} | {phi_flag} | "
                      f"masking={col_def.get('masking','none')}")
        except Exception as e:
            log.error(f"[P3] master_schema.json update failed: {e}")
            print(f"\n  ❌ Schema JSON update failed: {e}")
            print(f"      DDL was already executed. Manually add these columns to master_schema.json:")
            print(json.dumps(NEW_JSON, indent=4))
            # Do not exit with error — DDL is already committed to Delta, partial state is worse
    else:
        print(f"\n  ℹ️  NEW_JSON is empty — no schema metadata changes needed.")

# COMMAND ----------

# =============================================================================
# CELL 12 — Log Advisory Event to ADLS
# =============================================================================

def log_advisor_event(table_name: str, drift: dict, rec: dict, outcome: dict) -> None:
    """Persist the full advisor event — inputs, AI output, and outcome — to ADLS /logs/advisor/."""
    ts       = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = f"{LOGS_PATH}/advisor/{table_name}_advisor_{ts}.json"
    payload  = {
        "timestamp":      datetime.now().isoformat(),
        "table":          table_name,
        "run_mode":       RUN_MODE,
        "drift_input":    drift,
        "ai_severity":    rec["SEVERITY"],
        "sql_fix":        rec["SQL_FIX"],
        "new_json":       rec["NEW_JSON"],
        "reasoning":      rec["REASONING"],
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


outcome = {
    "ddl_executed":   DDL_EXECUTED,
    "schema_updated": SCHEMA_UPDATED,
    "status":         "fix_applied" if SHOULD_APPLY else "fix_declined",
}
log_advisor_event(TABLE_NAME, DRIFT_REPORT, AI_RECOMMENDATION, outcome)

# COMMAND ----------

# =============================================================================
# CELL 13 — Final Summary & Notebook Exit
# =============================================================================

if not SHOULD_APPLY:
    final_status = "fix_declined"
elif DDL_EXECUTED or SCHEMA_UPDATED:
    final_status = "fix_applied"
elif not SQL_FIX and not NEW_JSON:
    final_status = "no_fix_needed"
else:
    final_status = "fix_applied"   # approved + advisory-only (no DDL but reasoning noted)

print(f"\n{'='*65}")
print(f"  Pipeline 3 — AI Advisor COMPLETE")
print(f"  Completed:      {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"  Table:          {TABLE_NAME}")
print(f"  AI severity:    {AI_SEVERITY}")
print(f"  DDL executed:   {DDL_EXECUTED}")
print(f"  Schema updated: {SCHEMA_UPDATED}")
print(f"  Final status:   {final_status.upper()}")
print(f"{'='*65}")

if final_status == "fix_applied":
    print(f"\n  ✅ Fix applied. Pipeline 1 can now re-run ingestion for '{TABLE_NAME}'.")
elif final_status == "fix_declined":
    print(f"\n  ❌ Fix declined. No changes made. Review drift manually.")
elif final_status == "no_fix_needed":
    print(f"\n  ℹ️  No DDL needed. Schema metadata updated where applicable.")

# ── Exit payload — consumed by Pipeline 1's trigger_pipeline_3() ─────────────
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
