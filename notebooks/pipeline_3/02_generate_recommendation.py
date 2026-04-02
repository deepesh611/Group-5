# Databricks notebook source
# =============================================================================
# Pipeline 3 / Task 02 — Generate AI Recommendation
# =============================================================================
# Responsibilities:
#   1. Load intake artifact.
#   2. Call AI Advisor only when drift exists.
#   3. Persist recommendation artifact.
#
# This task does NOT validate or execute the recommendation.

# COMMAND ----------

# MAGIC %run ../utils/00_config

# COMMAND ----------

# MAGIC %run ../utils/04_metadata_manager

# COMMAND ----------

# MAGIC %run ../utils/03_openai_client

# COMMAND ----------

# MAGIC %run ../utils/05_advisor_state_manager

# COMMAND ----------

# MAGIC %run ../utils/06_advisor_policy

# COMMAND ----------

dbutils.widgets.text("run_id", "")
dbutils.widgets.text("table_name", "")

run_id = dbutils.widgets.get("run_id")
table_name = dbutils.widgets.get("table_name")

dbutils.jobs.taskValues.set("recommendation_ready", True)

# COMMAND ----------

# DBTITLE 1,Generate recommendation and set task values
import json


def _safe_set(key, value):
    """Set a job task value, silently skip if not in a job context."""
    try:
        dbutils.jobs.taskValues.set(key=key, value=value)
    except Exception:
        pass


def _print_recommendation(rec: dict, table: str, status: str):
    """Print the AI Advisor's recommendation prominently for demo visibility."""
    print(f"\n{'═'*70}")
    print(f"  🤖  AI ADVISOR RECOMMENDATION — {table.upper()}")
    print(f"{'═'*70}")
    print(f"  Status:    {status}")
    print(f"  Severity:  {rec.get('SEVERITY', 'N/A')}")
    print(f"{'─'*70}")
    print(f"  📝 REASONING:")
    for line in (rec.get('REASONING', '') or '').split('. '):
        if line.strip():
            print(f"     {line.strip()}.")
    print(f"{'─'*70}")
    sql_fix = rec.get('SQL_FIX', '') or ''
    if sql_fix:
        print(f"  🔧 AI-GENERATED SQL_FIX:")
        for line in sql_fix.strip().split('\n'):
            print(f"     {line}")
    else:
        print(f"  🔧 SQL_FIX:  (none — no DDL needed for this drift type)")
    print(f"{'─'*70}")
    new_json = rec.get('NEW_JSON', {}) or {}
    if new_json:
        print(f"  📋 NEW_JSON (column definitions):")
        print(f"     {json.dumps(new_json, indent=6)}")
    else:
        print(f"  📋 NEW_JSON:  (empty)")
    print(f"{'═'*70}\n")


dbutils.widgets.text("table_name", "", "Table name")
dbutils.widgets.text("run_id", "", "Advisor run id")

TABLE_NAME = dbutils.widgets.get("table_name").strip().lower()
RUN_ID = dbutils.widgets.get("run_id").strip()

_result = None

try:
    intake = read_advisor_artifact(RUN_ID, "01_intake", TABLE_NAME)
    drift = intake["drift"]

    if not drift.get("has_drift", False):
        rec = {"SQL_FIX": "", "NEW_JSON": {}, "SEVERITY": "INFO", "REASONING": "No drift detected."}
        status = "skipped"
    else:
        print(f"\n🧠 Calling AI Advisor (OpenAI {OPENAI_MODEL}) for table '{TABLE_NAME}'...")
        print(f"   Drift kind: {intake.get('drift_kind', 'unknown')}")
        print(f"   New columns: {[c['column'] for c in drift.get('new_columns', [])]}")
        print(f"   Missing columns: {[c['column'] for c in drift.get('missing_columns', [])]}")
        print(f"   Type changes: {[c['column'] for c in drift.get('type_changes', [])]}")
        rec = get_advisor_recommendation(drift, TABLE_NAME)
        status = "ok"

    _print_recommendation(rec, TABLE_NAME, status)

    artifact_path = write_advisor_artifact(
        RUN_ID,
        "02_recommendation",
        {
            "table": TABLE_NAME,
            "recommendation": rec,
            "status": status,
        },
        TABLE_NAME,
    )

    _result = {
        "status": status,
        "table": TABLE_NAME,
        "run_id": RUN_ID,
        "artifact_path": artifact_path,
        "advisor_severity": rec.get("SEVERITY", "UNKNOWN"),
        "sql_fix": rec.get("SQL_FIX", ""),
        "reasoning": rec.get("REASONING", ""),
        "new_json_columns": list((rec.get("NEW_JSON") or {}).keys()),
        "drift_kind": intake.get("drift_kind", "unknown"),
        "new_columns": [c["column"] for c in drift.get("new_columns", [])],
        "missing_columns": [c["column"] for c in drift.get("missing_columns", [])],
        "type_changes": [c["column"] for c in drift.get("type_changes", [])],
    }

    # Set task values DIRECTLY — NOT inside a function that calls notebook.exit()
    _safe_set("recommendation_status", status)
    _safe_set("advisor_severity", rec.get("SEVERITY", "UNKNOWN"))

    print(f"\u2705 Recommendation generated: severity={rec.get('SEVERITY', 'UNKNOWN')}, sql_fix={'present' if rec.get('SQL_FIX') else 'empty'}")

except Exception as e:
    write_advisor_artifact(
        RUN_ID or "unknown",
        "02_recommendation",
        {"table": TABLE_NAME, "status": "error", "reason": str(e)},
        TABLE_NAME,
    )
    _result = {"status": "error", "table": TABLE_NAME, "run_id": RUN_ID, "reason": str(e)}

    _safe_set("recommendation_status", "error")
    _safe_set("advisor_severity", "UNKNOWN")

    print(f"\u274c Generation error: {e}")

# COMMAND ----------

# DBTITLE 1,Notebook exit (separate cell)
# Physically separated from try/except to prevent exit exception from being caught
dbutils.notebook.exit(json.dumps(_result))
