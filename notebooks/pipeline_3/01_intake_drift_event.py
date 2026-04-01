# Databricks notebook source
# =============================================================================
# Pipeline 3 / Task 01 — Intake Drift Event
# =============================================================================
# Responsibilities:
#   1. Validate parameters.
#   2. Load drift JSON from widget or latest drift log.
#   3. Create a durable advisor run_id and persist intake artifact.
#   4. Publish small task values for downstream tasks.
#
# This task does NOT call the LLM and does NOT modify Delta or metadata.

# COMMAND ----------

dbutils.widgets.text("table_name", "")
dbutils.widgets.text("drift_event", "{}")
dbutils.widgets.text("trigger_source", "manual")
dbutils.widgets.text("run_mode", "autonomous")

table_name = dbutils.widgets.get("table_name")
drift_event = dbutils.widgets.get("drift_event")
trigger_source = dbutils.widgets.get("trigger_source")
run_mode = dbutils.widgets.get("run_mode")

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
from datetime import datetime


def _set_task_value(key: str, value):
    try:
        dbutils.jobs.taskValues.set(key=key, value=value)
    except Exception:
        pass


def _exit(payload: dict):
    for key, value in payload.get("task_values", {}).items():
        _set_task_value(key, value)
    dbutils.notebook.exit(json.dumps(payload["result"]))


def _load_drift(table_name: str, raw_json: str) -> dict:
    if raw_json:
        drift = json.loads(raw_json)
        if drift.get("table") and drift.get("table") != table_name:
            raise ValueError(f"drift_event table '{drift.get('table')}' != widget table '{table_name}'")
        return drift

    drift_dir = f"{LOGS_PATH}/drift"
    prefix = f"{table_name}_drift_"
    files = dbutils.fs.ls(drift_dir)
    matches = sorted(
        [f for f in files if f.name.startswith(prefix) and f.name.endswith(".json")],
        key=lambda f: f.name,
        reverse=True,
    )
    if not matches:
        raise FileNotFoundError(f"No drift log found for '{table_name}' in {drift_dir}")
    raw = dbutils.fs.head(matches[0].path, 1_048_576)
    payload = json.loads(raw)
    return payload.get("drift", payload)


dbutils.widgets.text("table_name", "", "Table name")
dbutils.widgets.text("drift_event", "", "Drift JSON")
dbutils.widgets.text("trigger_source", "pipeline1", "Trigger source")
dbutils.widgets.dropdown("run_mode", "autonomous", ["autonomous", "manual"], "Run mode")

TABLE_NAME = dbutils.widgets.get("table_name").strip().lower()
DRIFT_JSON = dbutils.widgets.get("drift_event").strip()
TRIGGER_SOURCE = dbutils.widgets.get("trigger_source").strip() or "pipeline1"
RUN_MODE = dbutils.widgets.get("run_mode").strip().lower() or "autonomous"

init_catalog()

try:
    if not TABLE_NAME:
        raise ValueError("table_name is required")

    drift = _load_drift(TABLE_NAME, DRIFT_JSON)
    run_id = new_advisor_run_id(TABLE_NAME)
    run_dir = get_advisor_run_dir(run_id, TABLE_NAME)
    kind = drift_kind(drift)

    intake_payload = {
        "run_id": run_id,
        "run_dir": run_dir,
        "table": TABLE_NAME,
        "trigger_source": TRIGGER_SOURCE,
        "run_mode": RUN_MODE,
        "started_at": datetime.utcnow().isoformat() + "Z",
        "drift": drift,
        "drift_kind": kind,
    }
    artifact_path = write_advisor_artifact(run_id, "01_intake", intake_payload, TABLE_NAME)

    result = {
        "status": "ok",
        "table": TABLE_NAME,
        "run_id": run_id,
        "drift_kind": kind,
        "has_drift": drift.get("has_drift", False),
        "artifact_path": artifact_path,
    }
    _exit({
        "result": result,
        "task_values": {
            "run_id": run_id,
            "run_dir": run_dir,
            "table_name": TABLE_NAME,
            "has_drift": str(bool(drift.get("has_drift", False))).lower(),
            "drift_kind": kind,
            "intake_status": "ok",
        },
    })
except Exception as e:
    result = {
        "status": "error",
        "table": TABLE_NAME,
        "run_id": "",
        "reason": str(e),
    }
    _exit({
        "result": result,
        "task_values": {
            "run_id": "",
            "run_dir": "",
            "table_name": TABLE_NAME,
            "has_drift": "false",
            "drift_kind": "error",
            "intake_status": "error",
        },
    })

# COMMAND ----------

# after you compute them:
dbutils.jobs.taskValues.set("run_id", run_id)
dbutils.jobs.taskValues.set("table_name", table_name)
dbutils.jobs.taskValues.set("has_drift", has_drift)
