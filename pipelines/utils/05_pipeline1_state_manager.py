# Databricks notebook source
# =============================================================================
# 05_pipeline1_state_manager — durable state for split Pipeline 1
# =============================================================================

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

import json
import time
from datetime import datetime

# COMMAND ----------

def _p1_runs_base() -> str:
    return f"{LOGS_PATH.rstrip('/')}/pipeline1"

def make_run_id(prefix: str = "p1") -> str:
    return f"{prefix}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{int(time.time()*1000)}"

def run_dir(run_id: str) -> str:
    return f"{_p1_runs_base()}/{run_id}"

def _write_text(path: str, text: str) -> None:
    parent = path.rsplit("/", 1)[0]
    dbutils.fs.mkdirs(parent)
    dbutils.fs.put(path, text, overwrite=True)

def _read_text(path: str) -> str:
    return dbutils.fs.head(path, 20_000_000)

def write_json(path: str, payload: dict) -> None:
    _write_text(path, json.dumps(payload, indent=2))

def read_json(path: str) -> dict:
    return json.loads(_read_text(path))

def preflight_manifest_path(run_id: str) -> str:
    return f"{run_dir(run_id)}/01_preflight_manifest.json"

def write_results_path(run_id: str) -> str:
    return f"{run_dir(run_id)}/02_write_results.json"

def final_summary_path(run_id: str) -> str:
    return f"{run_dir(run_id)}/03_final_summary.json"

def write_preflight_manifest(run_id: str, payload: dict) -> str:
    path = preflight_manifest_path(run_id)
    write_json(path, payload)
    return path

def read_preflight_manifest(run_id: str) -> dict:
    return read_json(preflight_manifest_path(run_id))

def write_write_results(run_id: str, payload: dict) -> str:
    path = write_results_path(run_id)
    write_json(path, payload)
    return path

def read_write_results(run_id: str) -> dict:
    return read_json(write_results_path(run_id))

def write_final_summary(run_id: str, payload: dict) -> str:
    path = final_summary_path(run_id)
    write_json(path, payload)
    return path