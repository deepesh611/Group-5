# Databricks notebook source
# =============================================================================
# Pipeline 3 / Task 05 — Update Metadata
# =============================================================================
# Responsibilities:
#   1. Merge NEW_JSON for successful additive fixes.
#   2. Flag missing columns for subtractive drift.
#   3. Persist metadata-update artifact.
#
# This task owns master_schema.json mutation.

# COMMAND ----------

# MAGIC %run ../utils/00_config

# COMMAND ----------

# MAGIC %run ../utils/04_metadata_manager

# COMMAND ----------

# MAGIC %run ../utils/05_advisor_state_manager

# COMMAND ----------

# MAGIC %run ../utils/06_advisor_policy

# COMMAND ----------

# DBTITLE 1,Helpers
import json
from datetime import datetime

def _safe_set(key, value):
    """Set a job task value, silently skip if not in a job context."""
    try:
        dbutils.jobs.taskValues.set(key=key, value=value)
    except Exception:
        pass

def _flag_missing_columns_in_schema(table_name: str, missing_cols: list, reasoning: str) -> bool:
    schema = load_master_schema(force_reload=True)
    table_cols = schema.get("tables", {}).get(table_name, {}).get("columns", {})
    changed = []
    for missing in missing_cols:
        col_name = missing["column"]
        if col_name in table_cols:
            table_cols[col_name]["source_status"] = "missing_from_upstream"
            table_cols[col_name]["missing_detected_at"] = datetime.utcnow().isoformat() + "Z"
            table_cols[col_name]["missing_ai_reasoning"] = str(reasoning)[:300]
            changed.append(col_name)
    if changed:
        schema["tables"][table_name]["columns"] = table_cols
        save_master_schema(schema)
        invalidate_cache()
    return True

# COMMAND ----------

dbutils.widgets.text("table_name", "", "Table name")
dbutils.widgets.text("run_id", "", "Advisor run id")

TABLE_NAME = dbutils.widgets.get("table_name").strip().lower()
RUN_ID = dbutils.widgets.get("run_id").strip()

# COMMAND ----------

# DBTITLE 1,Update metadata and set task values
_result = None

try:
    intake = read_advisor_artifact(RUN_ID, "01_intake", TABLE_NAME)
    validation_artifact = read_advisor_artifact(RUN_ID, "03_validation", TABLE_NAME)

    # Load execution artifact — prefer 04_execution, fall back to 90_manual_review
    execution_artifact = {"status": "skipped", "ddl_executed": False}
    if artifact_exists(RUN_ID, "04_execution", TABLE_NAME):
        execution_artifact = read_advisor_artifact(RUN_ID, "04_execution", TABLE_NAME)
    if not execution_artifact.get("ddl_executed") and artifact_exists(RUN_ID, "90_manual_review", TABLE_NAME):
        manual_artifact = read_advisor_artifact(RUN_ID, "90_manual_review", TABLE_NAME)
        if manual_artifact.get("ddl_executed"):
            execution_artifact = manual_artifact

    drift = intake["drift"]
    rec = validation_artifact.get("recommendation", {})
    validation = validation_artifact.get("validation", {})
    strategy = validation.get("execution_strategy", Strategy.MANUAL_REVIEW)

    schema_updated = False
    status = "skipped"
    reason = "No metadata action required"
    columns_merged = []
    columns_flagged = []

    print(f"\n{'═'*70}")
    print(f"  📁 METADATA UPDATE — {TABLE_NAME.upper()}")
    print(f"{'═'*70}")
    print(f"  Strategy:     {strategy}")
    print(f"  DDL executed: {execution_artifact.get('ddl_executed', False)}")
    print(f"{'─'*70}")

    if strategy == Strategy.AUTO_APPLY_DDL and execution_artifact.get("ddl_executed"):
        new_json = rec.get("NEW_JSON", {}) or {}
        if new_json:
            print(f"  🔄 Merging {len(new_json)} AI-defined column(s) into master_schema.json:")
            for col_name, col_def in new_json.items():
                phi_flag = " ⛔ PHI" if col_def.get("phi") else ""
                print(f"     + {col_name} ({col_def.get('type', '?')}){phi_flag}")
                print(f"       desc: {col_def.get('description', '')}")
            update_table_columns(TABLE_NAME, new_json)
            invalidate_cache()
            schema_updated = True
            columns_merged = list(new_json.keys())
            status = "ok"
            reason = f"Merged {len(new_json)} new column(s) into master_schema.json"
            print(f"  ✅ master_schema.json updated successfully!")

    elif strategy == Strategy.METADATA_ONLY:
        missing = drift.get('missing_columns', [])
        print(f"  📝 Flagging {len(missing)} missing column(s) in master_schema.json:")
        for m in missing:
            print(f"     − {m['column']} ({m.get('expected_type', '?')})")
        _flag_missing_columns_in_schema(TABLE_NAME, missing, rec.get("REASONING", ""))
        schema_updated = True
        columns_flagged = [m["column"] for m in missing]
        status = "ok"
        reason = f"Flagged {len(missing)} missing column(s) in master_schema.json"
        print(f"  ✅ Missing columns flagged with source_status=missing_from_upstream")

    elif strategy == Strategy.ADVISORY_ONLY:
        status = "ok"
        reason = "Advisory-only drift — no schema mutation required"
        print(f"  💡 Advisory-only — no changes to master_schema.json")

    elif strategy == Strategy.MANUAL_REVIEW:
        mr_artifact = None
        if artifact_exists(RUN_ID, "90_manual_review", TABLE_NAME):
            mr_artifact = read_advisor_artifact(RUN_ID, "90_manual_review", TABLE_NAME)

        if mr_artifact and mr_artifact.get("schema_updated"):
            schema_updated = True
            new_json = rec.get("NEW_JSON", {}) or {}
            columns_merged = list(new_json.keys())
            status = "ok"
            reason = f"Schema already updated by manual review (DDL executed: {mr_artifact.get('ddl_executed', False)})"
            print(f"  🔄 Manual review already updated master_schema.json")
            print(f"  ✅ Acknowledging manual review schema update")

        elif mr_artifact and mr_artifact.get("ddl_executed") and not mr_artifact.get("schema_updated"):
            new_json = rec.get("NEW_JSON", {}) or {}
            if new_json:
                print(f"  🔄 Manual review applied DDL but schema not yet updated.")
                update_table_columns(TABLE_NAME, new_json)
                invalidate_cache()
                schema_updated = True
                columns_merged = list(new_json.keys())
                status = "ok"
                reason = f"Merged {len(new_json)} column(s) after manual review DDL"
                print(f"  ✅ master_schema.json updated successfully!")
            else:
                status = "ok"
                reason = "Manual review applied DDL; no NEW_JSON to merge"
                print(f"  ✅ DDL applied via manual review; no additional schema changes needed")
        else:
            print(f"  ⏳ Manual review pending or declined — no metadata action")
            status = "skipped"
            reason = "Manual review not yet approved or no artifact found"
    else:
        print(f"  ⏭️  No metadata action for strategy: {strategy}")

    print(f"{'═'*70}\n")

    artifact_path = write_advisor_artifact(
        RUN_ID,
        "05_metadata",
        {
            "table": TABLE_NAME,
            "status": status,
            "schema_updated": schema_updated,
            "reason": reason,
            "strategy": strategy,
            "columns_merged": columns_merged,
            "columns_flagged": columns_flagged,
        },
        TABLE_NAME,
    )

    _result = {
        "status": status,
        "table": TABLE_NAME,
        "run_id": RUN_ID,
        "schema_updated": schema_updated,
        "strategy": strategy,
        "columns_merged": columns_merged,
        "columns_flagged": columns_flagged,
        "reason": reason,
        "artifact_path": artifact_path,
    }

    _safe_set("metadata_status", status)
    _safe_set("schema_updated", str(bool(schema_updated)).lower())

    print(f"✅ Task values set: schema_updated={schema_updated}, status={status}")

except Exception as e:
    write_advisor_artifact(
        RUN_ID or "unknown",
        "05_metadata",
        {"table": TABLE_NAME, "status": "error", "schema_updated": False, "reason": str(e)},
        TABLE_NAME,
    )
    _result = {"status": "error", "table": TABLE_NAME, "run_id": RUN_ID, "schema_updated": False, "reason": str(e)}

    _safe_set("metadata_status", "error")
    _safe_set("schema_updated", "false")

    print(f"❌ Metadata error: {e}")

# COMMAND ----------

# DBTITLE 1,Notebook exit (separate cell)
# Physically separated from try/except to prevent exit exception from being caught
dbutils.notebook.exit(json.dumps(_result))
