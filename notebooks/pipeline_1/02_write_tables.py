# Databricks notebook source
# =============================================================================
# 02_write_tables — Split Pipeline 1 Delta writer
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
import time
import traceback
from datetime import datetime

# COMMAND ----------

dbutils.widgets.text("run_id", "", "Preflight run ID")
dbutils.widgets.dropdown("dry_run", "false", ["false", "true"], "Dry run")
dbutils.widgets.dropdown("write_blocked", "false", ["false", "true"], "Write blocked tables (after P3)")

RUN_ID = dbutils.widgets.get("run_id").strip()
DRY_RUN = dbutils.widgets.get("dry_run").strip().lower() == "true"
WRITE_BLOCKED = dbutils.widgets.get("write_blocked").strip().lower() == "true"

if not RUN_ID:
    raise ValueError("Missing required parameter: run_id")

init_catalog()

print(f"{'\u2500'*70}")
print("Pipeline 1 \u2014 WRITE TABLES")
print(f"Run ID:        {RUN_ID}")
print(f"Dry run:       {DRY_RUN}")
print(f"Write blocked: {WRITE_BLOCKED}")
print(f"Started:       {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"{'\u2500'*70}")

# COMMAND ----------

def write_delta_table(df, table_name: str) -> int:
    full_name = get_full_table_name(table_name)
    tmp_view = f"_p1_write_{table_name}_{int(time.time())}"

    df.createOrReplaceTempView(tmp_view)
    try:
        spark.sql(f"""
            CREATE OR REPLACE TABLE {full_name}
            USING DELTA
            AS SELECT * FROM {tmp_view}
        """)
    finally:
        try:
            spark.catalog.dropTempView(tmp_view)
        except Exception:
            pass

    return spark.sql(f"SELECT COUNT(*) AS n FROM {full_name}").collect()[0]["n"]

# COMMAND ----------

manifest = read_preflight_manifest(RUN_ID)
results = {
    "run_id": RUN_ID,
    "created_at": datetime.utcnow().isoformat() + "Z",
    "dry_run": DRY_RUN,
    "writes": [],
}

for entry in manifest["entries"]:
    table_name = entry["table"]
    result = {
        "table": table_name,
        "status": "SKIPPED",
        "row_count": 0,
        "columns": len(entry.get("columns", [])),
        "source_path": entry.get("source_path", ""),
        "notes": [],
        "error": "",
    }

    should_write = (entry["status"] == "READY_TO_WRITE")
    if not should_write and WRITE_BLOCKED and entry["status"] == "BLOCKED_CRITICAL_DRIFT":
        should_write = True
        result["notes"].append("Writing after P3 drift resolution (write_blocked=true)")
    if not should_write:
        result["notes"].append(f"Skipped because preflight status={entry['status']}")
        results["writes"].append(result)
        continue

    try:
        print(f"\n{'='*70}")
        print(f"WRITE: {table_name.upper()}")
        print(f"{'='*70}")

        source_path = entry["source_path"]
        if table_name in set(get_all_table_names()):
            df = spark.read.csv(source_path, header=True, inferSchema=False, multiLine=False)
            df = normalize_column_names(df)
            df = cast_date_columns(df, table_name)
        else:
            df = spark.read.csv(source_path, header=True, inferSchema=True, samplingRatio=0.25)
            df = normalize_column_names(df)

        source_row_count = df.count()

        if DRY_RUN:
            result["status"] = "DRY_RUN"
            result["row_count"] = int(source_row_count)
            result["notes"].append("Dry run \u2014 no write performed")
        else:
            final_count = write_delta_table(df, table_name)
            if int(final_count) != int(source_row_count):
                raise RuntimeError(
                    f"Row count mismatch after write: source={source_row_count}, delta={final_count}"
                )
            result["status"] = "OK"
            result["row_count"] = int(final_count)

    except Exception as exc:
        result["status"] = "FAILED"
        result["error"] = str(exc)
        log.error(f"[{table_name}] write failed: {exc}")
        log.error(traceback.format_exc())

    finally:
        try:
            spark.catalog.clearCache()
        except Exception:
            pass

    results["writes"].append(result)

# COMMAND ----------

results_path = write_write_results(RUN_ID, results)

ok_count = sum(1 for x in results["writes"] if x["status"] == "OK")
failed_count = sum(1 for x in results["writes"] if x["status"] == "FAILED")
skipped_count = sum(1 for x in results["writes"] if x["status"] in ("SKIPPED", "DRY_RUN"))

try:
    dbutils.jobs.taskValues.set("write_results_path", results_path)
    dbutils.jobs.taskValues.set("write_ok_count", str(ok_count))
    dbutils.jobs.taskValues.set("write_failed_count", str(failed_count))
    dbutils.jobs.taskValues.set("write_skipped_count", str(skipped_count))
except Exception:
    pass

exit_payload = {
    "status": "ok" if failed_count == 0 else "partial_failure",
    "run_id": RUN_ID,
    "results_path": results_path,
    "counts": {
        "ok": ok_count,
        "failed": failed_count,
        "skipped": skipped_count,
    },
}

print(f"\nWrite results written to: {results_path}")
dbutils.notebook.exit(json.dumps(exit_payload))
