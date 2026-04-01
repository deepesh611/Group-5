# Databricks notebook source
# =============================================================================
# 03_finalize_ingestion — Split Pipeline 1 metadata + audit finalizer
# =============================================================================

# COMMAND ----------

# MAGIC %run ../utils/00_config

# COMMAND ----------

# MAGIC %run ../utils/04_metadata_manager

# COMMAND ----------

# MAGIC %run ../utils/05_pipeline1_state_manager

# COMMAND ----------

import json
from datetime import datetime

# COMMAND ----------

dbutils.widgets.text("run_id", "", "Pipeline 1 run ID")
dbutils.widgets.dropdown("dry_run", "false", ["false", "true"], "Dry run")

RUN_ID = dbutils.widgets.get("run_id").strip()
DRY_RUN = dbutils.widgets.get("dry_run").strip().lower() == "true"

if not RUN_ID:
    raise ValueError("Missing required parameter: run_id")

init_catalog()

print(f"{'─'*70}")
print("Pipeline 1 — FINALIZE")
print(f"Run ID:    {RUN_ID}")
print(f"Dry run:   {DRY_RUN}")
print(f"Started:   {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"{'─'*70}")

# COMMAND ----------

manifest = read_preflight_manifest(RUN_ID)
write_results = read_write_results(RUN_ID)

manifest_by_table = {e["table"]: e for e in manifest["entries"]}
final_reports = []

for item in write_results["writes"]:
    table_name = item["table"]
    pre = manifest_by_table.get(table_name, {})

    report = {
        "table": table_name,
        "status": item["status"],
        "row_count": item.get("row_count", 0),
        "columns": item.get("columns", 0),
        "drift_severity": pre.get("drift", {}).get("severity", "NONE"),
        "error": item.get("error", ""),
        "notes": list(pre.get("notes", [])) + list(item.get("notes", [])),
    }

    if item["status"] == "OK" and not DRY_RUN:
        update_row_count(table_name, int(item["row_count"]))
        log_ingestion_run(table_name, {
            "source_path": item.get("source_path", ""),
            "row_count": int(item["row_count"]),
            "columns": item.get("columns", 0),
            "drift_severity": report["drift_severity"],
            "notes": report["notes"],
        })

    final_reports.append(report)

# Also surface blocked critical drift entries that were never sent to write step
for entry in manifest["entries"]:
    if entry["status"] == "BLOCKED_CRITICAL_DRIFT":
        final_reports.append({
            "table": entry["table"],
            "status": "BLOCKED_CRITICAL_DRIFT",
            "row_count": 0,
            "columns": len(entry.get("columns", [])),
            "drift_severity": entry.get("drift", {}).get("severity", "CRITICAL"),
            "error": "",
            "notes": entry.get("notes", []),
        })

# COMMAND ----------

counts = {}
for r in final_reports:
    counts[r["status"]] = counts.get(r["status"], 0) + 1

final_summary = {
    "run_id": RUN_ID,
    "created_at": datetime.utcnow().isoformat() + "Z",
    "dry_run": DRY_RUN,
    "counts": counts,
    "handoff": manifest.get("handoff", {}),
    "reports": final_reports,
}

summary_path = write_final_summary(RUN_ID, final_summary)

exit_payload = {
    "status": "critical_drift_handoff" if manifest.get("handoff", {}).get("has_critical_drift") else "ok",
    "run_id": RUN_ID,
    "summary_path": summary_path,
    "counts": counts,
    "handoff": manifest.get("handoff", {}),
}

print(f"\nFinal summary written to: {summary_path}")
dbutils.notebook.exit(json.dumps(exit_payload))