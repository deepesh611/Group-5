# Databricks notebook source
# =============================================================================
# Pipeline 3 / Task 06 — Finalize Result
# =============================================================================
# Responsibilities:
#   1. Read all prior artifacts.
#   2. Build the canonical Pipeline 3 exit payload.
#   3. Persist final summary artifact.
#   4. Exit exactly once.
#
# Output contract (backward compatible with Pipeline 1 intent):
#   status: fix_applied | advisory | no_fix_needed | error
#   action: merge_schema_fallback | manual_review_required | ""

# COMMAND ----------
# MAGIC %run ../utils/00_config
# COMMAND ----------
# MAGIC %run ../utils/05_advisor_state_manager
# COMMAND ----------
# MAGIC %run ../utils/06_advisor_policy

# COMMAND ----------
import json


dbutils.widgets.text("table_name", "", "Table name")
dbutils.widgets.text("run_id", "", "Advisor run id")

TABLE_NAME = dbutils.widgets.get("table_name").strip().lower()
RUN_ID = dbutils.widgets.get("run_id").strip()

try:
    intake = read_advisor_artifact(RUN_ID, "01_intake", TABLE_NAME)
    recommendation = read_advisor_artifact(RUN_ID, "02_recommendation", TABLE_NAME) if artifact_exists(RUN_ID, "02_recommendation", TABLE_NAME) else {}
    validation_artifact = read_advisor_artifact(RUN_ID, "03_validation", TABLE_NAME) if artifact_exists(RUN_ID, "03_validation", TABLE_NAME) else {}
    execution_artifact = read_advisor_artifact(RUN_ID, "04_execution", TABLE_NAME) if artifact_exists(RUN_ID, "04_execution", TABLE_NAME) else {"ddl_executed": False}
    metadata_artifact = read_advisor_artifact(RUN_ID, "05_metadata", TABLE_NAME) if artifact_exists(RUN_ID, "05_metadata", TABLE_NAME) else {"schema_updated": False}

    drift = intake.get("drift", {})
    rec = recommendation.get("recommendation", {})
    validation = validation_artifact.get("validation", {})
    strategy = validation.get("execution_strategy", Strategy.MANUAL_REVIEW)
    ddl_executed = bool(execution_artifact.get("ddl_executed", False))
    schema_updated = bool(metadata_artifact.get("schema_updated", False))
    severity = rec.get("SEVERITY", drift.get("severity", "UNKNOWN"))
    reasoning = rec.get("REASONING", "") or metadata_artifact.get("reason", "") or execution_artifact.get("reason", "")

    action = ""
    if strategy == Strategy.NO_ACTION:
        status = "no_fix_needed"
    elif strategy == Strategy.AUTO_APPLY_DDL and ddl_executed and schema_updated:
        status = "fix_applied"
    elif strategy == Strategy.METADATA_ONLY:
        status = "advisory"
        action = "merge_schema_fallback"
    elif strategy == Strategy.ADVISORY_ONLY:
        status = "advisory"
    else:
        status = "error"
        action = "manual_review_required"

    final_payload = {
        "status": status,
        "table": TABLE_NAME,
        "run_id": RUN_ID,
        "severity": severity,
        "sql_fix": rec.get("SQL_FIX", ""),
        "ddl_executed": ddl_executed,
        "schema_updated": schema_updated,
        "reasoning": reasoning,
        "missing_columns": [m["column"] for m in drift.get("missing_columns", [])],
        "action": action,
        "execution_strategy": strategy,
        "drift_kind": validation.get("drift_kind", intake.get("drift_kind", "unknown")),
        "artifacts": {
            "intake": get_advisor_artifact_path(RUN_ID, "01_intake", TABLE_NAME),
            "recommendation": get_advisor_artifact_path(RUN_ID, "02_recommendation", TABLE_NAME),
            "validation": get_advisor_artifact_path(RUN_ID, "03_validation", TABLE_NAME),
            "execution": get_advisor_artifact_path(RUN_ID, "04_execution", TABLE_NAME),
            "metadata": get_advisor_artifact_path(RUN_ID, "05_metadata", TABLE_NAME),
        },
    }

    write_advisor_artifact(RUN_ID, "06_final", final_payload, TABLE_NAME)
    dbutils.notebook.exit(json.dumps(final_payload))
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
        "execution_strategy": Strategy.MANUAL_REVIEW,
    }))
