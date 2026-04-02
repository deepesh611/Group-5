# Databricks notebook source
# =============================================================================
# Pipeline 3 / Task 06 — Finalize Result
# =============================================================================
# Responsibilities:
#   1. Read all prior artifacts.
#   2. Build the canonical Pipeline 3 exit payload.
#   3. Persist final summary artifact.
#   4. Exit exactly once.
#
# Output contract (backward compatible with Pipeline 1 intent):
#   status: fix_applied | advisory | no_fix_needed | error
#   action: merge_schema_fallback | manual_review_required | ""

# COMMAND ----------

# MAGIC %run ../utils/00_config

# COMMAND ----------

# MAGIC %run ../utils/05_advisor_state_manager

# COMMAND ----------

# MAGIC %run ../utils/06_advisor_policy

# COMMAND ----------

import json

# COMMAND ----------

dbutils.widgets.text("table_name", "", "Table name")
dbutils.widgets.text("run_id", "", "Advisor run id")

TABLE_NAME = dbutils.widgets.get("table_name").strip().lower()
RUN_ID = dbutils.widgets.get("run_id").strip()

# COMMAND ----------

# DBTITLE 1,Build final summary
_result = None

try:
    intake = read_advisor_artifact(RUN_ID, "01_intake", TABLE_NAME)
    recommendation = (
        read_advisor_artifact(RUN_ID, "02_recommendation", TABLE_NAME)
        if artifact_exists(RUN_ID, "02_recommendation", TABLE_NAME)
        else {}
    )
    validation_artifact = (
        read_advisor_artifact(RUN_ID, "03_validation", TABLE_NAME)
        if artifact_exists(RUN_ID, "03_validation", TABLE_NAME)
        else {}
    )
    execution_artifact = (
        read_advisor_artifact(RUN_ID, "04_execution", TABLE_NAME)
        if artifact_exists(RUN_ID, "04_execution", TABLE_NAME)
        else {"ddl_executed": False}
    )
    metadata_artifact = (
        read_advisor_artifact(RUN_ID, "05_metadata", TABLE_NAME)
        if artifact_exists(RUN_ID, "05_metadata", TABLE_NAME)
        else {"schema_updated": False}
    )
    manual_review_artifact = (
        read_advisor_artifact(RUN_ID, "90_manual_review", TABLE_NAME)
        if artifact_exists(RUN_ID, "90_manual_review", TABLE_NAME)
        else {}
    )

    drift = intake.get("drift", {})
    rec = recommendation.get("recommendation", {})
    validation = validation_artifact.get("validation", {})
    strategy = validation.get("execution_strategy", Strategy.MANUAL_REVIEW)

    ddl_executed = bool(execution_artifact.get("ddl_executed", False))
    if not ddl_executed and manual_review_artifact:
        ddl_executed = bool(manual_review_artifact.get("ddl_executed", False))

    schema_updated = bool(metadata_artifact.get("schema_updated", False))
    if not schema_updated and manual_review_artifact:
        schema_updated = bool(manual_review_artifact.get("schema_updated", False))

    severity = rec.get("SEVERITY", drift.get("severity", "UNKNOWN"))
    reasoning = (
        rec.get("REASONING", "")
        or metadata_artifact.get("reason", "")
        or execution_artifact.get("reason", "")
        or manual_review_artifact.get("reasoning", "")
    )

    action = ""
    if strategy == Strategy.NO_ACTION:
        status = "no_fix_needed"
    elif strategy == Strategy.AUTO_APPLY_DDL and ddl_executed and schema_updated:
        status = "fix_applied"
    elif strategy == Strategy.MANUAL_REVIEW and ddl_executed and schema_updated:
        status = "fix_applied"
    elif strategy == Strategy.MANUAL_REVIEW and manual_review_artifact.get("status") == "fix_applied":
        status = "fix_applied"
    elif strategy == Strategy.METADATA_ONLY:
        status = "advisory"
        action = "merge_schema_fallback"
    elif strategy == Strategy.ADVISORY_ONLY:
        status = "advisory"
    elif strategy == Strategy.MANUAL_REVIEW and not manual_review_artifact:
        status = "error"
        action = "manual_review_required"
    else:
        status = "error"
        action = "manual_review_required"

    fix_path = ""
    if manual_review_artifact and manual_review_artifact.get("ddl_executed"):
        fix_path = "manual_review"
    elif execution_artifact.get("ddl_executed"):
        fix_path = "auto_apply"

    # ─── Rich summary for demo visibility ───
    status_icons = {
        "fix_applied": "✅", "advisory": "📝", "no_fix_needed": "✅", "error": "❌",
    }
    icon = status_icons.get(status, "❓")
    print(f"\n{'═'*70}")
    print(f"  {icon}  PIPELINE 3 — AI ADVISOR FINAL SUMMARY")
    print(f"{'═'*70}")
    print(f"  Table:              {TABLE_NAME}")
    print(f"  Run ID:             {RUN_ID}")
    print(f"  Final Status:       {status.upper()}")
    print(f"  Execution Strategy: {strategy}")
    print(f"  Severity:           {severity}")
    if fix_path:
        print(f"  Fix Path:           {fix_path}")
    print(f"{'─'*70}")
    print(f"  🤖 AI ADVISOR DECISION CHAIN:")
    print(f"     1️⃣  Intake:        drift_kind = {validation.get('drift_kind', intake.get('drift_kind', '?'))}")
    print(f"     2️⃣  AI Generated:  SQL_FIX = {'(present)' if rec.get('SQL_FIX') else '(empty)'}")
    print(f"                        NEW_JSON = {len(rec.get('NEW_JSON', {}) or {})} column(s)")
    print(f"     3️⃣  Validated:     strategy = {strategy}, validation_ok = {validation.get('validation_ok', '?')}")
    print(f"     4️⃣  DDL Applied:   {ddl_executed}")
    if rec.get('SQL_FIX'):
        print(f"                        {rec['SQL_FIX']}")
    if fix_path == "manual_review":
        print(f"                        (applied via manual review approval)")
    print(f"     5️⃣  Schema Updated: {schema_updated}")
    if metadata_artifact.get('reason'):
        print(f"                        {metadata_artifact['reason']}")
    print(f"{'─'*70}")
    print(f"  💬 REASONING: {reasoning}")
    if action:
        print(f"  ⚠️  ACTION: {action}")
    print(f"{'═'*70}\n")

    _result = {
        "status": status,
        "table": TABLE_NAME,
        "run_id": RUN_ID,
        "severity": severity,
        "sql_fix": rec.get("SQL_FIX", ""),
        "ddl_executed": ddl_executed,
        "schema_updated": schema_updated,
        "reasoning": reasoning,
        "missing_columns": [m["column"] for m in drift.get("missing_columns", [])],
        "action": action,
        "execution_strategy": strategy,
        "fix_path": fix_path,
        "drift_kind": validation.get("drift_kind", intake.get("drift_kind", "unknown")),
        "artifacts": {
            "intake": get_advisor_artifact_path(RUN_ID, "01_intake", TABLE_NAME),
            "recommendation": get_advisor_artifact_path(RUN_ID, "02_recommendation", TABLE_NAME),
            "validation": get_advisor_artifact_path(RUN_ID, "03_validation", TABLE_NAME),
            "execution": get_advisor_artifact_path(RUN_ID, "04_execution", TABLE_NAME),
            "metadata": get_advisor_artifact_path(RUN_ID, "05_metadata", TABLE_NAME),
            "manual_review": get_advisor_artifact_path(RUN_ID, "90_manual_review", TABLE_NAME) if manual_review_artifact else None,
        },
    }

    write_advisor_artifact(RUN_ID, "06_final", _result, TABLE_NAME)

except Exception as e:
    _result = {
        "status": "error",
        "table": TABLE_NAME,
        "run_id": RUN_ID,
        "severity": "UNKNOWN",
        "sql_fix": "",
        "ddl_executed": False,
        "schema_updated": False,
        "reasoning": str(e),
        "missing_columns": [],
        "action": "manual_review_required",
        "execution_strategy": Strategy.MANUAL_REVIEW,
    }
    print(f"❌ Finalize error: {e}")

# COMMAND ----------

# DBTITLE 1,Notebook exit (separate cell)
# Physically separated from try/except to prevent exit exception from being caught
dbutils.notebook.exit(json.dumps(_result))
