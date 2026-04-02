# Databricks notebook source
# =============================================================================
# Pipeline 3 / Task 03 — Validate Execution Plan
# =============================================================================
# Responsibilities:
#   1. Load intake + recommendation artifacts.
#   2. Run deterministic validation and strategy selection.
#   3. Persist validation artifact.
#
# This task decides WHAT MAY HAPPEN next.
# It does NOT change Delta or metadata.

# COMMAND ----------

# MAGIC %run ../utils/00_config

# COMMAND ----------

# MAGIC %run ../utils/05_advisor_state_manager

# COMMAND ----------

# MAGIC %run ../utils/06_advisor_policy

# COMMAND ----------

# DBTITLE 1,Validate and set task values
import json

dbutils.widgets.text("table_name", "", "Table name")
dbutils.widgets.text("run_id", "", "Advisor run id")
dbutils.widgets.dropdown("run_mode", "autonomous", ["autonomous", "manual"], "Run mode")

TABLE_NAME = dbutils.widgets.get("table_name").strip().lower()
RUN_ID = dbutils.widgets.get("run_id").strip()
RUN_MODE = dbutils.widgets.get("run_mode").strip().lower() or "autonomous"

def _safe_set(key, value):
    """Set a job task value, silently skip if not in a job context."""
    try:
        dbutils.jobs.taskValues.set(key=key, value=value)
    except Exception:
        pass

_result = None

try:
    intake = read_advisor_artifact(RUN_ID, "01_intake", TABLE_NAME)
    rec_artifact = read_advisor_artifact(RUN_ID, "02_recommendation", TABLE_NAME)

    drift = intake["drift"]
    rec = rec_artifact.get("recommendation", {})

    validation = validate_recommendation(
        drift=drift,
        rec=rec,
        table_name=TABLE_NAME,
        full_table_name=get_full_table_name(TABLE_NAME),
        run_mode=RUN_MODE,
    )

    artifact_path = write_advisor_artifact(
        RUN_ID,
        "03_validation",
        {
            "table": TABLE_NAME,
            "validation": validation,
            "recommendation": rec,
        },
        TABLE_NAME,
    )

    _result = {
        "status": "ok",
        "table": TABLE_NAME,
        "run_id": RUN_ID,
        "execution_strategy": validation["execution_strategy"],
        "validation_ok": validation["validation_ok"],
        "artifact_path": artifact_path,
        "drift_kind": validation.get("drift_kind", "unknown"),
        "ddl_columns": validation.get("ddl_columns", []),
        "json_columns": validation.get("json_columns", []),
        "sql_validation_ok": validation.get("sql_validation", {}).get("ok", False),
        "json_validation_ok": validation.get("json_validation", {}).get("ok", False),
        "cross_validation_ok": validation.get("cross_validation", {}).get("ok", False),
    }

    # Set task values DIRECTLY — must not be inside a function that calls notebook.exit()
    _safe_set("validation_status", "ok")
    _safe_set("execution_strategy", validation["execution_strategy"])
    _safe_set("validation_ok", str(bool(validation["validation_ok"])).lower())
    _safe_set("should_apply_ddl", str(validation["execution_strategy"] == "AUTO_APPLY_DDL").lower())
    _safe_set("manual_review_required", str(validation["execution_strategy"] == "MANUAL_REVIEW").lower())

    print(f"\u2705 Validation complete: strategy={validation['execution_strategy']}, should_apply_ddl={validation['execution_strategy'] == 'AUTO_APPLY_DDL'}")

except Exception as e:
    write_advisor_artifact(
        RUN_ID or "unknown",
        "03_validation",
        {"table": TABLE_NAME, "status": "error", "reason": str(e)},
        TABLE_NAME,
    )
    _result = {"status": "error", "table": TABLE_NAME, "run_id": RUN_ID, "reason": str(e)}

    _safe_set("validation_status", "error")
    _safe_set("execution_strategy", Strategy.MANUAL_REVIEW)
    _safe_set("validation_ok", "false")
    _safe_set("should_apply_ddl", "false")
    _safe_set("manual_review_required", "true")

    print(f"\u274c Validation error: {e}")

# COMMAND ----------

# DBTITLE 1,Notebook exit (separate cell)
# Physically separated from try/except to prevent exit exception from being caught
dbutils.notebook.exit(json.dumps(_result))
