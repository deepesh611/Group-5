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

TABLE_NAME = dbutils.widgets.get("table_name").strip().lower()
RUN_ID = dbutils.widgets.get("run_id").strip()

try:
    intake = read_advisor_artifact(RUN_ID, "01_intake", TABLE_NAME)
    drift = intake["drift"]

    if not drift.get("has_drift", False):
        rec = {"SQL_FIX": "", "NEW_JSON": {}, "SEVERITY": "INFO", "REASONING": "No drift detected."}
        status = "skipped"
    else:
        rec = get_advisor_recommendation(drift, TABLE_NAME)
        status = "ok"

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

    _exit(
        {
            "status": status,
            "table": TABLE_NAME,
            "run_id": RUN_ID,
            "artifact_path": artifact_path,
            "advisor_severity": rec.get("SEVERITY", "UNKNOWN"),
        },
        recommendation_status=status,
        advisor_severity=rec.get("SEVERITY", "UNKNOWN"),
    )
except Exception as e:
    write_advisor_artifact(
        RUN_ID or "unknown",
        "02_recommendation",
        {"table": TABLE_NAME, "status": "error", "reason": str(e)},
        TABLE_NAME,
    )
    _exit(
        {"status": "error", "table": TABLE_NAME, "run_id": RUN_ID, "reason": str(e)},
        recommendation_status="error",
        advisor_severity="UNKNOWN",
    )
