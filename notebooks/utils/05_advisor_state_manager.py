# 05_advisor_state_manager.py — Pipeline 3 run-state persistence helpers
#
# Purpose:
#   Store each Pipeline 3 step's inputs/outputs as JSON artifacts in ADLS so the
#   workflow state is durable, inspectable, and resumable.
#
# Depends on:
#   00_config.py

import json
import re
from datetime import datetime

ADVISOR_RUNS_PATH = f"{LOGS_PATH}/advisor_runs"


def _safe_token(value: str) -> str:
    value = str(value or "").strip().lower()
    return re.sub(r"[^a-z0-9_\-]+", "_", value).strip("_") or "unknown"


def new_advisor_run_id(table_name: str) -> str:
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S_%f")
    return f"{_safe_token(table_name)}_{ts}"


def get_advisor_run_dir(run_id: str, table_name: str = "") -> str:
    table_part = _safe_token(table_name) if table_name else "unknown"
    return f"{ADVISOR_RUNS_PATH}/{table_part}/{run_id}"


def get_advisor_artifact_path(run_id: str, step_name: str, table_name: str = "") -> str:
    return f"{get_advisor_run_dir(run_id, table_name)}/{step_name}.json"


def write_advisor_artifact(run_id: str, step_name: str, payload: dict, table_name: str = "") -> str:
    path = get_advisor_artifact_path(run_id, step_name, table_name)
    enriched = {
        "artifact_written_at": datetime.utcnow().isoformat() + "Z",
        **payload,
    }
    dbutils.fs.put(path, json.dumps(enriched, indent=2, ensure_ascii=False), overwrite=True)
    log.info("[P3] Wrote artifact: %s", path)
    return path


def read_advisor_artifact(run_id: str, step_name: str, table_name: str = "") -> dict:
    path = get_advisor_artifact_path(run_id, step_name, table_name)
    raw = dbutils.fs.head(path, 5_242_880)
    return json.loads(raw)


def artifact_exists(run_id: str, step_name: str, table_name: str = "") -> bool:
    path = get_advisor_artifact_path(run_id, step_name, table_name)
    try:
        dbutils.fs.head(path, 1024)
        return True
    except Exception:
        return False
