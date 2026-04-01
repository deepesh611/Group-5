# Databricks notebook source
# =============================================================================
# 01_preflight_ingestion — Split Pipeline 1 preflight + manifest builder
# =============================================================================

# COMMAND ----------

# MAGIC %run ../utils/00_config

# COMMAND ----------

# MAGIC %run ../utils/04_metadata_manager

# COMMAND ----------

# MAGIC %run ../utils/01_schema_utils

# COMMAND ----------

# MAGIC %run ../utils/05_pipeline1_state_manager

# COMMAND ----------

import json
import traceback
from datetime import datetime
from pyspark.sql.functions import col

# COMMAND ----------

dbutils.widgets.text("table_filter", "all", "Tables to ingest (comma-separated or 'all')")
dbutils.widgets.dropdown("dry_run", "false", ["false", "true"], "Dry run")
dbutils.widgets.dropdown("on_critical_drift", "halt", ["halt", "fallback"], "On CRITICAL drift")

TABLE_FILTER = dbutils.widgets.get("table_filter").strip()
DRY_RUN = dbutils.widgets.get("dry_run").strip().lower() == "true"
ON_CRITICAL_DRIFT = dbutils.widgets.get("on_critical_drift").strip().lower()

# COMMAND ----------

init_catalog()

if TABLE_FILTER.lower() == "all":
    TABLES_TO_PROCESS = list(SYNTHEA_TABLES)
else:
    TABLES_TO_PROCESS = [t.strip().lower() for t in TABLE_FILTER.split(",") if t.strip()]

RUN_ID = make_run_id("p1")
KNOWN_TABLES = set(get_all_table_names())
CSV_FILES = {f.name.replace(".csv", "").lower(): f for f in list_raw_csvs()}

print(f"{'─'*70}")
print("Pipeline 1 — PRE-FLIGHT")
print(f"Run ID:            {RUN_ID}")
print(f"Started:           {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"Tables:            {TABLES_TO_PROCESS}")
print(f"Dry run:           {DRY_RUN}")
print(f"On critical drift: {ON_CRITICAL_DRIFT.upper()}")
print(f"{'─'*70}")

# COMMAND ----------

def set_task_value_safe(key: str, value):
    try:
        dbutils.jobs.taskValues.set(key=key, value=value)
    except Exception:
        pass  # allows interactive notebook testing

# COMMAND ----------

entries = []
ready_count = 0
blocked_count = 0
failed_precheck_count = 0

has_critical_drift = False
handoff_table_name = ""
handoff_drift_event = ""

for table_name in TABLES_TO_PROCESS:
    entry = {
        "table": table_name,
        "status": "FAILED_PRECHECK",
        "source_path": "",
        "source_row_count": 0,
        "columns": [],
        "drift": {
            "has_drift": False,
            "severity": "NONE",
            "new_columns": [],
            "missing_columns": [],
            "type_changes": [],
        },
        "notes": [],
        "error": "",
    }

    print(f"\n{'='*70}")
    print(f"PRE-FLIGHT: {table_name.upper()}")
    print(f"{'='*70}")

    try:
        if table_name not in CSV_FILES:
            msg = f"CSV '{table_name}.csv' not found in {RAW_PATH}"
            entry["status"] = "SKIPPED"
            entry["error"] = msg
            entries.append(entry)
            print(f"⏭️  {msg}")
            continue

        csv_file = CSV_FILES[table_name]
        entry["source_path"] = csv_file.path

        if table_name in KNOWN_TABLES:
            df = spark.read.csv(
                csv_file.path,
                header=True,
                inferSchema=False,
                multiLine=False,
            )
        else:
            df = spark.read.csv(
                csv_file.path,
                header=True,
                inferSchema=True,
                samplingRatio=0.25,
            )
            entry["notes"].append("Table not in master_schema.json; drift check skipped")

        source_row_count = df.count()
        entry["source_row_count"] = int(source_row_count)

        df = normalize_column_names(df)
        entry["columns"] = list(df.columns)

        if table_name in KNOWN_TABLES:
            df = cast_date_columns(df, table_name)
            drift = detect_drift(table_name, df)
        else:
            drift = {
                "table": table_name,
                "has_drift": False,
                "severity": "NONE",
                "new_columns": [],
                "missing_columns": [],
                "type_changes": [],
            }

        entry["drift"] = drift
        print(generate_drift_summary(drift))

        if drift["has_drift"]:
            log_drift_event(table_name, drift)

        if drift["severity"] == "CRITICAL" and ON_CRITICAL_DRIFT == "halt":
            entry["status"] = "BLOCKED_CRITICAL_DRIFT"
            entry["notes"].append("Blocked in preflight; handoff required to Pipeline 3")

            if not has_critical_drift:
                has_critical_drift = True
                handoff_table_name = table_name
                handoff_drift_event = build_drift_event_payload(table_name, drift)

            blocked_count += 1

        else:
            entry["status"] = "READY_TO_WRITE"
            if drift["severity"] == "CRITICAL" and ON_CRITICAL_DRIFT == "fallback":
                entry["notes"].append("CRITICAL drift allowed to continue due to fallback mode")
            ready_count += 1

    except Exception as exc:
        entry["status"] = "FAILED_PRECHECK"
        entry["error"] = str(exc)
        failed_precheck_count += 1
        log.error(f"[{table_name}] preflight failed: {exc}")
        log.error(traceback.format_exc())
        print(f"❌ Preflight failed for {table_name}: {exc}")

    finally:
        try:
            spark.catalog.clearCache()
        except Exception:
            pass

    entries.append(entry)

# COMMAND ----------

manifest = {
    "run_id": RUN_ID,
    "created_at": datetime.utcnow().isoformat() + "Z",
    "table_filter": TABLE_FILTER,
    "dry_run": DRY_RUN,
    "on_critical_drift": ON_CRITICAL_DRIFT,
    "entries": entries,
    "summary": {
        "ready_count": ready_count,
        "blocked_count": blocked_count,
        "failed_precheck_count": failed_precheck_count,
    },
    "handoff": {
        "has_critical_drift": has_critical_drift,
        "table_name": handoff_table_name,
        "drift_event": handoff_drift_event,
    },
}

manifest_path = write_preflight_manifest(RUN_ID, manifest)

set_task_value_safe("run_id", RUN_ID)
set_task_value_safe("dry_run", str(DRY_RUN).lower())
set_task_value_safe("has_critical_drift", str(has_critical_drift).lower())
set_task_value_safe("handoff_table_name", handoff_table_name)
set_task_value_safe("handoff_drift_event", handoff_drift_event)
set_task_value_safe("manifest_path", manifest_path)
set_task_value_safe("ready_count", str(ready_count))
set_task_value_safe("blocked_count", str(blocked_count))
set_task_value_safe("failed_precheck_count", str(failed_precheck_count))

exit_payload = {
    "status": "critical_drift_handoff" if has_critical_drift else "ready",
    "run_id": RUN_ID,
    "manifest_path": manifest_path,
    "summary": manifest["summary"],
    "handoff": manifest["handoff"],
    "dry_run": DRY_RUN,
}

print(f"\nManifest written to: {manifest_path}")
dbutils.notebook.exit(json.dumps(exit_payload))