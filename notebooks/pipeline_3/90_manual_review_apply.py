# Databricks notebook source
# =============================================================================
# Pipeline 3 / Manual Utility — Review + Apply
# =============================================================================
# Use this notebook ONLY when 06_finalize returns:
#   status = error
#   action = manual_review_required
#
# Responsibilities:
#   1. Load prior recommendation + validation artifacts.
#   2. Let a human approve/decline, or override SQL/NEW_JSON.
#   3. Re-validate in interactive mode.
#   4. Apply DDL + metadata if approved.
#
# This is intentionally separate from the automated workflow.

# COMMAND ----------
# MAGIC %run ../utils/00_config
# COMMAND ----------
# MAGIC %run ../utils/04_metadata_manager
# COMMAND ----------
# MAGIC %run ../utils/05_advisor_state_manager
# COMMAND ----------
# MAGIC %run ../utils/06_advisor_policy

# COMMAND ----------
import json


dbutils.widgets.text("table_name", "", "Table name")
dbutils.widgets.text("run_id", "", "Advisor run id")
dbutils.widgets.dropdown("approval_decision", "decline", ["approve", "decline"], "Approval")
dbutils.widgets.text("override_sql_fix", "", "Override SQL_FIX (optional)")
dbutils.widgets.text("override_new_json", "", "Override NEW_JSON JSON (optional)")

TABLE_NAME = dbutils.widgets.get("table_name").strip().lower()
RUN_ID = dbutils.widgets.get("run_id").strip()
DECISION = dbutils.widgets.get("approval_decision").strip().lower()
OVERRIDE_SQL = dbutils.widgets.get("override_sql_fix").strip()
OVERRIDE_NEW_JSON = dbutils.widgets.get("override_new_json").strip()

try:
    intake = read_advisor_artifact(RUN_ID, "01_intake", TABLE_NAME)
    rec_artifact = read_advisor_artifact(RUN_ID, "02_recommendation", TABLE_NAME)
    drift = intake["drift"]
    rec = rec_artifact.get("recommendation", {})

    if OVERRIDE_SQL:
        rec["SQL_FIX"] = OVERRIDE_SQL
    if OVERRIDE_NEW_JSON:
        rec["NEW_JSON"] = json.loads(OVERRIDE_NEW_JSON)

    validation = validate_recommendation(
        drift=drift,
        rec=rec,
        table_name=TABLE_NAME,
        full_table_name=get_full_table_name(TABLE_NAME),
        run_mode="interactive",
    )

    if DECISION != "approve":
        result = {
            "status": "fix_declined",
            "table": TABLE_NAME,
            "run_id": RUN_ID,
            "severity": rec.get("SEVERITY", drift.get("severity", "UNKNOWN")),
            "sql_fix": rec.get("SQL_FIX", ""),
            "ddl_executed": False,
            "schema_updated": False,
            "reasoning": "Human declined manual review apply.",
            "missing_columns": [m["column"] for m in drift.get("missing_columns", [])],
            "action": "",
        }
        write_advisor_artifact(RUN_ID, "90_manual_review", result, TABLE_NAME)
        dbutils.notebook.exit(json.dumps(result))

    if not validation["validation_ok"]:
        raise ValueError(f"Manual plan failed validation: {validation}")

    ddl_executed = False
    schema_updated = False
    if rec.get("SQL_FIX"):
        spark.sql(rec["SQL_FIX"])
        ddl_executed = True
    if rec.get("NEW_JSON"):
        update_table_columns(TABLE_NAME, rec["NEW_JSON"])
        schema_updated = True

    result = {
        "status": "fix_applied" if (ddl_executed or schema_updated) else "advisory",
        "table": TABLE_NAME,
        "run_id": RUN_ID,
        "severity": rec.get("SEVERITY", drift.get("severity", "UNKNOWN")),
        "sql_fix": rec.get("SQL_FIX", ""),
        "ddl_executed": ddl_executed,
        "schema_updated": schema_updated,
        "reasoning": rec.get("REASONING", ""),
        "missing_columns": [m["column"] for m in drift.get("missing_columns", [])],
        "action": "",
    }
    write_advisor_artifact(RUN_ID, "90_manual_review", result, TABLE_NAME)
    dbutils.notebook.exit(json.dumps(result))
except Exception as e:
    dbutils.notebook.exit(json.dumps({
        "status": "error",
        "table": TABLE_NAME,
        "run_id": RUN_ID,
        "severity": "UNKNOWN",
        "sql_fix": "",
        "ddl_executed": False,
        "schema_updated": False,
        "reasoning": str(e),
        "missing_columns": [],
        "action": "manual_review_required",
    }))
