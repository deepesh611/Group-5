# Databricks notebook source
# =============================================================================
# pipeline_3_advisor.py — AI Schema Advisor (Self-Healing, P1-Triggered)
# =============================================================================
#
# ARCHITECTURE NOTES — WHY THIS IS STRUCTURED THIS WAY:
#
# Previous P3 used 13 cells with shared notebook-level state variables
# (_p3_early_exit, DRIFT_REPORT, SQL_FIX, etc.). Any unguarded exception
# in any cell left the notebook in an undefined state. The Catch-Clear-Exit
# pattern required perfect discipline across every cell — one missed guard
# caused the JVM to raise WorkflowException in P1, which was
# indistinguishable from a clean error exit.
#
# NEW DESIGN — two execution cells only:
#   Cell 1-3:  %run utils + imports + widgets (unchanged, safe)
#   Cell 4:    ONE function `run_advisor()` that handles EVERYTHING internally.
#              All logic is inside try/except. The function always returns a
#              result dict — it never raises. No shared state. No early exits
#              mid-execution. The function is the unit of failure, not the cell.
#   Cell 5:    Single exit cell. Reads the result dict. Calls
#              dbutils.notebook.exit() exactly once, always, unconditionally.
#              This is the ONLY place exit() is called in the entire notebook.
#
# This eliminates the WorkflowException problem at the root: dbutils.notebook
# .exit() is never called from inside an except block, and it is always called
# (so Databricks never sees an incomplete notebook execution).
#
# EXIT PAYLOAD CONTRACT (P1 reads this in trigger_pipeline_3()):
#   status:          "fix_applied" | "advisory" | "fix_declined" |
#                    "no_fix_needed" | "error"
#   table:           str
#   severity:        "INFO" | "WARNING" | "CRITICAL" | "UNKNOWN"
#   sql_fix:         str (empty if no DDL)
#   ddl_executed:    bool
#   schema_updated:  bool
#   reasoning:       str
#   missing_columns: list[str]  (populated for subtractive drift)
#   action:          str        ("merge_schema_fallback" for subtractive drift)
#
# PARAMETERS (widget / Job task param):
#   table_name    — short Synthea table name (e.g. "patients")
#   drift_event   — JSON string of drift report from detect_drift()
#   run_mode      — "autonomous" (P1-triggered) | "interactive" (manual)
#   approval_decision — "pending"|"approve"|"decline" (interactive mode only)
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
# CELL 1 — Imports & Widget Definitions
# =============================================================================

import json
import re
import traceback
from datetime import datetime

dbutils.widgets.text(    "table_name",        "",             "Table name (e.g. patients)")
dbutils.widgets.text(    "drift_event",        "",             "Drift event JSON from P1")
dbutils.widgets.dropdown("run_mode",           "autonomous",   ["autonomous", "interactive"], "Run mode")
dbutils.widgets.dropdown("approval_decision",  "pending",      ["pending", "approve", "decline"], "Approval (interactive only)")

# COMMAND ----------

# =============================================================================
# CELL 2 — DDL Validation (pure function, no side effects)
# =============================================================================

_AUTONOMOUS_DDL_PATTERN = re.compile(
    r"^\s*ALTER\s+TABLE\s+[\w.`]+\s+ADD\s+COLUMNS?\s*\(",
    re.IGNORECASE,
)
_BLOCKED_KEYWORDS = re.compile(
    r"\b(DELETE|UPDATE|INSERT|TRUNCATE|DROP\s+TABLE|CREATE\s+TABLE|GRANT|REVOKE|MERGE)\b",
    re.IGNORECASE,
)

def validate_ddl(sql_fix: str, table_name: str, run_mode: str) -> tuple:
    """
    Returns (is_valid: bool, reason: str).
    Empty sql_fix is always valid — means advisory only.
    """
    if not sql_fix or not sql_fix.strip():
        return True, "empty — advisory only, no DDL to execute"

    sql        = sql_fix.strip()
    full_table = get_full_table_name(table_name)

    # Must reference the correct table
    if not re.search(
        re.escape(table_name) + r"|" + re.escape(full_table),
        sql, re.IGNORECASE
    ):
        return False, f"DDL does not reference '{table_name}' or '{full_table}'"

    # Block dangerous keywords regardless of mode
    m = _BLOCKED_KEYWORDS.search(sql)
    if m:
        return False, f"Blocked keyword '{m.group()}' in SQL_FIX"

    # Block subqueries
    if re.search(r"\bSELECT\b|\bFROM\b|\bWHERE\b", sql, re.IGNORECASE):
        return False, "SQL_FIX contains subquery keywords — rejected"

    # Autonomous mode: only ADD COLUMNS is permitted
    if run_mode == "autonomous" and not _AUTONOMOUS_DDL_PATTERN.match(sql):
        return False, (
            "AUTONOMOUS: only 'ALTER TABLE <t> ADD COLUMNS (...)' is allowed. "
            f"Got: {sql[:100]}"
        )

    # Interactive mode: only ALTER TABLE or CREATE OR REPLACE TABLE
    if run_mode == "interactive":
        if not re.match(r"^\s*(ALTER\s+TABLE|CREATE\s+OR\s+REPLACE\s+TABLE)", sql, re.IGNORECASE):
            return False, "INTERACTIVE: only ALTER TABLE or CREATE OR REPLACE TABLE permitted"

    return True, "passed"

# COMMAND ----------

# =============================================================================
# CELL 3 — HTML Recommendation Card (interactive mode only)
# =============================================================================

def render_recommendation_html(table: str, drift: dict, rec: dict) -> str:
    severity_color = {"CRITICAL": "#c0392b", "WARNING": "#e67e22", "INFO": "#27ae60"}.get(
        rec.get("SEVERITY", "INFO"), "#7f8c8d"
    )
    sql_code     = rec.get("SQL_FIX") or "(no DDL — advisory only)"
    new_json_str = json.dumps(rec.get("NEW_JSON", {}), indent=2) or "(none)"
    reasoning    = rec.get("REASONING", "")

    missing_rows = "".join(
        f"<tr><td style='padding:4px 8px'>{c['column']}</td>"
        f"<td style='padding:4px 8px'>{c.get('expected_type','?')}</td>"
        f"<td style='padding:4px 8px'>{'⛔ PHI' if c.get('is_phi') else '⚠️'}</td></tr>"
        for c in drift.get("missing_columns", [])
    ) or "<tr><td colspan=3 style='padding:4px 8px;color:#888'>None</td></tr>"

    new_rows = "".join(
        f"<tr><td style='padding:4px 8px'>{c['column']}</td>"
        f"<td style='padding:4px 8px'>{c.get('incoming_type','?')}</td>"
        f"<td style='padding:4px 8px'>⚠️ Possible PHI</td></tr>"
        for c in drift.get("new_columns", [])
    ) or "<tr><td colspan=3 style='padding:4px 8px;color:#888'>None</td></tr>"

    return f"""
<div style="font-family:monospace;max-width:960px;background:#1a1a2e;color:#e0e0e0;padding:24px;border-radius:10px">
  <h2 style="color:#f0c040;margin:0 0 12px">🔬 AI Schema Advisor — {table.upper()}</h2>
  <span style="background:{severity_color};color:#fff;padding:3px 10px;border-radius:4px">AI: {rec.get('SEVERITY','?')}</span>
  <h3 style="color:#a0d0ff;margin:16px 0 6px">Missing Columns</h3>
  <table style="width:100%;border-collapse:collapse;background:#111">
    <tr style="background:#2a2a4a;color:#a0d0ff"><th style="padding:6px 8px;text-align:left">Column</th><th>Type</th><th>PHI</th></tr>
    {missing_rows}
  </table>
  <h3 style="color:#a0d0ff;margin:16px 0 6px">New Columns</h3>
  <table style="width:100%;border-collapse:collapse;background:#111">
    <tr style="background:#2a2a4a;color:#a0d0ff"><th style="padding:6px 8px;text-align:left">Column</th><th>Type</th><th>PHI Risk</th></tr>
    {new_rows}
  </table>
  <h3 style="color:#a0d0ff;margin:16px 0 6px">AI Reasoning</h3>
  <p style="background:#111;padding:10px;border-radius:4px;white-space:pre-wrap">{reasoning}</p>
  <h3 style="color:#a0d0ff;margin:16px 0 6px">Proposed DDL</h3>
  <pre style="background:#111;padding:10px;color:#f0c040">{sql_code}</pre>
  <h3 style="color:#a0d0ff;margin:16px 0 6px">master_schema.json Updates</h3>
  <pre style="background:#111;padding:10px;color:#90ee90">{new_json_str}</pre>
  <div style="margin-top:20px;padding:12px;background:#2a1a1a;border-radius:4px;border-left:3px solid #c0392b">
    <strong style="color:#f0c040">⚠️ ACTION REQUIRED</strong><br/>
    Set <code>approval_decision</code> widget to <strong>approve</strong> or <strong>decline</strong>,
    then re-run from Cell 4.
  </div>
</div>"""

# COMMAND ----------

# =============================================================================
# CELL 4 — Main Advisor Logic (single function, always returns, never raises)
# =============================================================================
#
# WHY ONE FUNCTION:
#   dbutils.notebook.exit() must be called exactly once, at the very end,
#   unconditionally. The only safe way to guarantee this in a multi-step
#   process is to run all steps inside a single function that catches every
#   possible exception and returns a result dict. The exit cell then reads
#   that dict and calls exit() once.
#
#   This eliminates the WorkflowException / NotebookExecutionException problem
#   entirely: the notebook never "crashes" — it always reaches Cell 5 and
#   calls exit() cleanly with a status of either success or error.

def _load_drift(table_name: str, raw_json: str) -> dict:
    """Load and validate the drift event. Raises on failure."""
    if raw_json:
        drift = json.loads(raw_json)
        if drift.get("table") and drift.get("table") != table_name:
            raise ValueError(
                f"Drift event is for '{drift.get('table')}', not '{table_name}'"
            )
        return drift

    # Load from ADLS drift log (interactive mode)
    drift_dir = f"{LOGS_PATH}/drift"
    prefix    = f"{table_name}_drift_"
    files     = dbutils.fs.ls(drift_dir)
    matches   = sorted(
        [f for f in files if f.name.startswith(prefix) and f.name.endswith(".json")],
        key=lambda f: f.name, reverse=True,
    )
    if not matches:
        raise FileNotFoundError(
            f"No drift log for '{table_name}' in {drift_dir}. "
            "Trigger via P1 or paste drift JSON in the widget."
        )
    raw     = dbutils.fs.head(matches[0].path, 524_288)
    payload = json.loads(raw)
    return payload.get("drift", payload)


def _flag_missing_columns_in_schema(table_name: str, missing_cols: list, reasoning: str) -> bool:
    """
    Stamp each missing column in master_schema.json with source_status=
    'missing_from_upstream'. Returns True on success, False on failure.
    Does NOT raise — caller handles the bool.
    """
    try:
        schema     = load_master_schema(force_reload=True)
        table_cols = schema.get("tables", {}).get(table_name, {}).get("columns", {})
        flagged    = []
        for missing in missing_cols:
            col_name = missing["column"]
            if col_name in table_cols:
                table_cols[col_name]["source_status"]       = "missing_from_upstream"
                table_cols[col_name]["missing_detected_at"] = datetime.now().isoformat()
                table_cols[col_name]["missing_ai_reasoning"] = reasoning[:300]
                flagged.append(col_name)
            else:
                log.warning(
                    "[P3] Missing column '%s' not in master_schema.json for '%s' — skip flag",
                    col_name, table_name,
                )
        if flagged:
            schema["tables"][table_name]["columns"] = table_cols
            save_master_schema(schema)
            invalidate_cache()
            log.info("[P3] Flagged missing columns in master_schema.json: %s", flagged)
        return True
    except Exception as e:
        log.error("[P3] Failed to flag missing columns: %s", e)
        return False


def _merge_new_columns_in_schema(table_name: str, new_json: dict) -> bool:
    """
    Merge AI-provided column definitions into master_schema.json.
    Returns True on success, False on failure. Does NOT raise.
    """
    try:
        update_table_columns(table_name, new_json)
        invalidate_cache()
        log.info("[P3] Merged %d new column(s) into master_schema.json", len(new_json))
        return True
    except Exception as e:
        log.error("[P3] Failed to merge new columns into schema: %s", e)
        return False


def _log_advisor_event(table_name: str, run_mode: str, drift: dict,
                        rec: dict, outcome: dict) -> None:
    """Write advisor event to ADLS. Non-fatal — never raises."""
    try:
        ts       = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_path = f"{LOGS_PATH}/advisor/{table_name}_advisor_{ts}.json"
        payload  = {
            "timestamp":      datetime.now().isoformat(),
            "table":          table_name,
            "run_mode":       run_mode,
            "drift_input":    drift,
            "ai_severity":    rec.get("SEVERITY", "UNKNOWN"),
            "sql_fix":        rec.get("SQL_FIX", ""),
            "new_json":       rec.get("NEW_JSON", {}),
            "reasoning":      rec.get("REASONING", ""),
            "ddl_executed":   outcome.get("ddl_executed", False),
            "schema_updated": outcome.get("schema_updated", False),
            "outcome_status": outcome.get("status", "unknown"),
        }
        dbutils.fs.put(log_path, json.dumps(payload, indent=2), overwrite=True)
        log.info("[P3] Advisor event logged: %s", log_path)
    except Exception as e:
        log.warning("[P3] Advisor log write failed (non-critical): %s", e)


def _error_result(table_name: str, severity: str, sql_fix: str, reason: str) -> dict:
    """Convenience builder for error exit payloads."""
    return {
        "status":          "error",
        "table":           table_name,
        "severity":        severity,
        "sql_fix":         sql_fix,
        "ddl_executed":    False,
        "schema_updated":  False,
        "reasoning":       reason[:500] + ("..." if len(reason) > 500 else ""),
        "missing_columns": [],
        "action":          "",
    }


def run_advisor(table_name: str, drift_json: str, run_mode: str, approval: str) -> dict:
    """
    Main advisor logic. ALWAYS returns a result dict, NEVER raises.

    Every failure path returns an error dict so Cell 5 can always call
    dbutils.notebook.exit() cleanly regardless of what happened here.
    """
    init_catalog()
    print(f"{'─'*65}")
    print(f"  Pipeline 3 — AI Schema Advisor")
    print(f"  Started:  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Table:    {table_name}")
    print(f"  Mode:     {run_mode.upper()}")
    print(f"{'─'*65}")

    # ── Guard: table_name required ────────────────────────────────────────
    if not table_name:
        return _error_result("", "NONE", "", "table_name widget is empty.")

    # ── Step 1: Load drift event ──────────────────────────────────────────
    print("\n  [1/5] Loading drift event …")
    try:
        drift = _load_drift(table_name, drift_json)
    except Exception as e:
        reason = f"Drift event load failed: {e}"
        log.error("[P3] %s", reason)
        return _error_result(table_name, "NONE", "", reason)

    print(f"      has_drift:       {drift.get('has_drift', False)}")
    print(f"      severity:        {drift.get('severity', 'NONE')}")
    print(f"      new_columns:     {len(drift.get('new_columns', []))}")
    print(f"      missing_columns: {len(drift.get('missing_columns', []))}")
    print(f"      type_changes:    {len(drift.get('type_changes', []))}")

    # ── Step 2: Early exit if no drift ───────────────────────────────────
    if not drift.get("has_drift", False):
        msg = f"No drift in '{table_name}' — no action required."
        log.info("[P3] %s", msg)
        print(f"\n  ✅ {msg}")
        return {
            "status": "no_fix_needed", "table": table_name, "severity": "NONE",
            "sql_fix": "", "ddl_executed": False, "schema_updated": False,
            "reasoning": msg, "missing_columns": [], "action": "",
        }

    # ── Step 3: Call AI Advisor ───────────────────────────────────────────
    print("\n  [2/5] Calling AI Advisor (GPT-4o) …")
    try:
        rec = get_advisor_recommendation(drift, table_name)
    except Exception as e:
        reason = f"AI Advisor call failed: {e}\n{traceback.format_exc()}"
        log.error("[P3] %s", reason)
        return _error_result(table_name, drift.get("severity", "UNKNOWN"), "", reason)

    sql_fix    = rec.get("SQL_FIX", "")
    new_json   = rec.get("NEW_JSON", {})
    severity   = rec.get("SEVERITY", "WARNING")
    reasoning  = rec.get("REASONING", "")
    missing    = drift.get("missing_columns", [])

    print(f"      AI severity:  {severity}")
    print(f"      SQL_FIX:      {sql_fix[:100] if sql_fix else '(none — advisory)'}")
    print(f"      NEW_JSON cols: {list(new_json.keys())}")
    print(f"      Reasoning:    {reasoning[:150]}…")

    # ── Step 4: Validate DDL ──────────────────────────────────────────────
    print("\n  [3/5] Validating DDL …")
    is_valid, validation_msg = validate_ddl(sql_fix, table_name, run_mode)
    print(f"      {'✅ PASSED' if is_valid else '❌ FAILED'}: {validation_msg}")

    if not is_valid:
        reason = f"DDL validation failed: {validation_msg}"
        log.error("[P3] %s | Rejected DDL: %s", reason, sql_fix)
        result = _error_result(table_name, severity, sql_fix, reason)
        _log_advisor_event(table_name, run_mode, drift, rec, result)
        return result

    # ── Step 5: Interactive mode — show card, check approval ─────────────
    if run_mode == "interactive":
        try:
            displayHTML(render_recommendation_html(table_name, drift, rec))
        except Exception as e:
            print(f"\n  ⚠️ Could not render HTML card: {e}")
            print(f"      SQL_FIX:  {sql_fix or '(none)'}")
            print(f"      NEW_JSON: {json.dumps(new_json, indent=2)}")
            print(f"      Reasoning: {reasoning}")

        if approval == "pending":
            msg = "Approval pending. Set approval_decision and re-run from Cell 4."
            print(f"\n  ⏸️  {msg}")
            result = _error_result(table_name, severity, sql_fix, msg)
            result["status"] = "error"   # keep P1 from proceeding
            return result

        if approval == "decline":
            msg = f"Human declined AI fix for '{table_name}'."
            log.info("[P3] %s", msg)
            print(f"\n  ❌ {msg}")
            result = {
                "status": "fix_declined", "table": table_name, "severity": severity,
                "sql_fix": sql_fix, "ddl_executed": False, "schema_updated": False,
                "reasoning": msg, "missing_columns": [m["column"] for m in missing],
                "action": "",
            }
            _log_advisor_event(table_name, run_mode, drift, rec, result)
            return result

        # approval == "approve" — fall through to execution

    # ── Step 6: Determine action type ────────────────────────────────────
    # Subtractive drift: column missing from CSV. Delta DDL cannot restore
    # data. The correct action is to flag the schema and tell P1 to use
    # mergeSchema fallback so the column stays in the Delta table with NULLs.
    is_subtractive = bool(missing) and not sql_fix

    # ── Step 7: Execute DDL (if any) ─────────────────────────────────────
    print("\n  [4/5] Executing DDL …")
    ddl_executed   = False
    schema_updated = False

    if sql_fix:
        full_table = get_full_table_name(table_name)
        print(f"      Running: {sql_fix}")
        try:
            spark.sql(sql_fix)
            ddl_executed = True
            log.info("[P3] DDL executed: %s", sql_fix)
            print("      ✅ DDL executed successfully")

            # Verify columns were added
            for col_name in new_json:
                desc     = spark.sql(f"DESCRIBE TABLE {full_table}")
                act_cols = {r["col_name"].lower() for r in desc.collect()}
                icon     = "✅" if col_name.lower() in act_cols else "⚠️ "
                print(f"      {icon} Column '{col_name}' {'found' if col_name.lower() in act_cols else 'NOT found'} in Delta table")

        except Exception as e:
            err_str = str(e)
            if "already exists" in err_str.lower():
                # Idempotent — column was already there, treat as success
                ddl_executed = True
                log.warning("[P3] Column already exists (idempotent): %s", err_str)
                print("      ⚠️  Column already exists — treating as success (idempotent)")
            else:
                reason = f"DDL execution failed: {err_str}"
                log.error("[P3] %s", reason)
                print(f"      ❌ {reason}")
                result = _error_result(table_name, severity, sql_fix, reason)
                _log_advisor_event(table_name, run_mode, drift, rec, result)
                return result
    else:
        if is_subtractive:
            print("      ℹ️  Subtractive drift — no DDL possible. Will flag schema only.")
        else:
            print("      ℹ️  No DDL to execute — advisory only.")

    # ── Step 8: Update master_schema.json ────────────────────────────────
    print("\n  [5/5] Updating master_schema.json …")

    if new_json:
        schema_updated = _merge_new_columns_in_schema(table_name, new_json)
        if schema_updated:
            print(f"      ✅ Merged {len(new_json)} column definition(s):")
            for col_name, col_def in new_json.items():
                phi_flag = "⛔ PHI" if col_def.get("phi") else "✅ non-PHI"
                print(f"         {col_name}: {col_def.get('type','?')} | {phi_flag}")
        else:
            print("      ❌ Schema update failed — see logs. DDL may already be committed.")
            print(f"         Manual patch needed: {json.dumps(new_json, indent=4)}")

    if is_subtractive:
        flag_ok = _flag_missing_columns_in_schema(table_name, missing, reasoning)
        if flag_ok:
            schema_updated = True
            print(f"      ✅ Flagged {len(missing)} missing column(s) as 'missing_from_upstream':")
            for m in missing:
                phi_flag = "⛔ PHI" if m.get("is_phi") else "non-PHI"
                print(f"         {m['column']} ({phi_flag})")
        else:
            print("      ⚠️  Could not flag missing columns — see logs. Non-fatal.")

    if not new_json and not is_subtractive:
        print("      ℹ️  No schema metadata changes required.")

    # ── Step 9: Determine final status ───────────────────────────────────
    if is_subtractive:
        final_status = "advisory"
        action       = "merge_schema_fallback"
        print(f"\n  ℹ️  Subtractive drift — advisory status.")
        print(f"      P1 will use mergeSchema fallback for '{table_name}'.")
        print(f"      Missing column stays in Delta with NULL for new rows.")
        print(f"      Investigate upstream CSV source.")
    elif ddl_executed or schema_updated:
        final_status = "fix_applied"
        action       = ""
        print(f"\n  ✅ Fix applied — P1 can re-run '{table_name}'.")
    elif not sql_fix and not new_json:
        final_status = "no_fix_needed"
        action       = ""
        print(f"\n  ℹ️  No action required — drift was compatible.")
    else:
        final_status = "advisory"
        action       = ""
        print(f"\n  ℹ️  Advisory only — schema metadata updated.")

    result = {
        "status":          final_status,
        "table":           table_name,
        "severity":        severity,
        "sql_fix":         sql_fix,
        "ddl_executed":    ddl_executed,
        "schema_updated":  schema_updated,
        "reasoning":       reasoning,
        "missing_columns": [m["column"] for m in missing],
        "action":          action,
    }

    _log_advisor_event(table_name, run_mode, drift, rec, result)

    print(f"\n{'='*65}")
    print(f"  Pipeline 3 — COMPLETE")
    print(f"  Completed:      {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Table:          {table_name}")
    print(f"  AI severity:    {severity}")
    print(f"  DDL executed:   {ddl_executed}")
    print(f"  Schema updated: {schema_updated}")
    print(f"  Final status:   {final_status.upper()}")
    print(f"{'='*65}")

    return result


# ── Execute ───────────────────────────────────────────────────────────────────
TABLE_NAME = dbutils.widgets.get("table_name").strip().lower()
DRIFT_JSON = dbutils.widgets.get("drift_event").strip()
RUN_MODE   = dbutils.widgets.get("run_mode").strip().lower()
APPROVAL   = dbutils.widgets.get("approval_decision").strip().lower()

_ADVISOR_RESULT = run_advisor(TABLE_NAME, DRIFT_JSON, RUN_MODE, APPROVAL)

# COMMAND ----------

# =============================================================================
# CELL 5 — Exit (called exactly once, unconditionally, never inside except)
# =============================================================================
# This is the ONLY place dbutils.notebook.exit() is called in this notebook.
# _ADVISOR_RESULT is always a dict at this point — run_advisor() never raises.

dbutils.notebook.exit(json.dumps(_ADVISOR_RESULT))