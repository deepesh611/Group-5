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
import json


def _set_task_value(key: str, value):
    try:
        dbutils.jobs.taskValues.set(key=key, value=value)
    except Exception:
        pass


def _exit(result: dict, **task_values):
    for key, value in task_values.items():
        _set_task_value(key, value)
    dbutils.notebook.exit(json.dumps(result))


dbutils.widgets.text("table_name", "", "Table name")
dbutils.widgets.text("run_id", "", "Advisor run id")
dbutils.widgets.dropdown("run_mode", "autonomous", ["autonomous", "manual"], "Run mode")

TABLE_NAME = dbutils.widgets.get("table_name").strip().lower()
RUN_ID = dbutils.widgets.get("run_id").strip()
RUN_MODE = dbutils.widgets.get("run_mode").strip().lower() or "autonomous"

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

    result = {
        "status": "ok",
        "table": TABLE_NAME,
        "run_id": RUN_ID,
        "execution_strategy": validation["execution_strategy"],
        "validation_ok": validation["validation_ok"],
        "artifact_path": artifact_path,
    }
    _exit(
        result,
        validation_status="ok",
        execution_strategy=validation["execution_strategy"],
        validation_ok=str(bool(validation["validation_ok"])).lower(),
    )
except Exception as e:
    write_advisor_artifact(
        RUN_ID or "unknown",
        "03_validation",
        {"table": TABLE_NAME, "status": "error", "reason": str(e)},
        TABLE_NAME,
    )
    _exit(
        {"status": "error", "table": TABLE_NAME, "run_id": RUN_ID, "reason": str(e)},
        validation_status="error",
        execution_strategy=Strategy.MANUAL_REVIEW,
        validation_ok="false",
    )
