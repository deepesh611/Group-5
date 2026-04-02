# Databricks notebook source
# =============================================================================
# Pipeline 3 / Task 04 — Apply DDL
# =============================================================================
# Responsibilities:
#   1. Load validation + recommendation.
#   2. Execute DDL only for AUTO_APPLY_DDL strategy.
#   3. Persist execution artifact.
#
# All non-additive strategies become safe no-ops here.

# COMMAND ----------

# MAGIC %run ../utils/00_config

# COMMAND ----------

# MAGIC %run ../utils/05_advisor_state_manager

# COMMAND ----------

# MAGIC %run ../utils/06_advisor_policy

# COMMAND ----------

# DBTITLE 1,Helper
import json

def _safe_set(key, value):
    """Set a job task value, silently skip if not in a job context."""
    try:
        dbutils.jobs.taskValues.set(key=key, value=value)
    except Exception:
        pass

# COMMAND ----------

dbutils.widgets.text("table_name", "", "Table name")
dbutils.widgets.text("run_id", "", "Advisor run id")

TABLE_NAME = dbutils.widgets.get("table_name").strip().lower()
RUN_ID = dbutils.widgets.get("run_id").strip()

# COMMAND ----------

# DBTITLE 1,Execute DDL and set task values
_result = None

try:
    validation_artifact = read_advisor_artifact(RUN_ID, "03_validation", TABLE_NAME)
    rec = validation_artifact.get("recommendation", {})
    validation = validation_artifact.get("validation", {})
    strategy = validation.get("execution_strategy", Strategy.MANUAL_REVIEW)

    ddl_executed = False
    sql_fix = rec.get("SQL_FIX", "") or ""
    status = "skipped"
    reason = "DDL not applicable for this strategy"

    print(f"\n{'═'*70}")
    print(f"  ⚙️  DDL EXECUTION — {TABLE_NAME.upper()}")
    print(f"{'═'*70}")
    print(f"  Strategy:  {strategy}")
    print(f"  Drift kind: {validation.get('drift_kind', 'unknown')}")
    print(f"  Validation OK: {validation.get('validation_ok', False)}")
    print(f"{'─'*70}")

    if strategy == Strategy.AUTO_APPLY_DDL:
        full_table = get_full_table_name(TABLE_NAME)
        if not spark.catalog.tableExists(full_table):
            ddl_executed = False
            status = "ok"
            reason = "Table does not exist yet — DDL skipped, P1 will create it"
            print(f"  ⏭️  Table {full_table} does not exist — skipping ALTER TABLE (P1 will create)")
        else:
            print(f"  🚀 EXECUTING AI-GENERATED DDL:")
            for line in sql_fix.strip().split('\n'):
                print(f"     {line}")
            print(f"{'─'*70}")
            try:
                spark.sql(sql_fix)
                ddl_executed = True
                status = "ok"
                reason = "DDL executed successfully"
                print(f"  ✅ DDL executed successfully!")
            except Exception as e:
                if "already exists" in str(e).lower():
                    ddl_executed = True
                    status = "ok"
                    reason = "DDL treated as idempotent success — column already exists"
                    print(f"  ✅ Column already exists (idempotent success)")
                else:
                    status = "error"
                    reason = f"DDL execution failed: {e}"
                    print(f"  ❌ DDL execution FAILED: {e}")
    else:
        print(f"  ⏭️  Skipped — strategy is {strategy}, not AUTO_APPLY_DDL")
        if sql_fix:
            print(f"  (AI did generate SQL_FIX, but it won't be applied:)")
            print(f"     {sql_fix}")

    print(f"{'═'*70}\n")

    artifact_path = write_advisor_artifact(
        RUN_ID,
        "04_execution",
        {
            "table": TABLE_NAME,
            "status": status,
            "ddl_executed": ddl_executed,
            "sql_fix": sql_fix,
            "reason": reason,
        },
        TABLE_NAME,
    )

    _result = {
        "status": status,
        "table": TABLE_NAME,
        "run_id": RUN_ID,
        "ddl_executed": ddl_executed,
        "sql_fix": sql_fix,
        "strategy": strategy,
        "drift_kind": validation.get("drift_kind", "unknown"),
        "reason": reason,
        "artifact_path": artifact_path,
    }

    _safe_set("ddl_status", status)
    _safe_set("ddl_executed", str(bool(ddl_executed)).lower())

    print(f"✅ Task values set: ddl_executed={ddl_executed}, status={status}")

except Exception as e:
    write_advisor_artifact(
        RUN_ID or "unknown",
        "04_execution",
        {"table": TABLE_NAME, "status": "error", "ddl_executed": False, "reason": str(e)},
        TABLE_NAME,
    )
    _result = {"status": "error", "table": TABLE_NAME, "run_id": RUN_ID, "ddl_executed": False, "reason": str(e)}

    _safe_set("ddl_status", "error")
    _safe_set("ddl_executed", "false")

    print(f"❌ Execution error: {e}")

# COMMAND ----------

# DBTITLE 1,Notebook exit (separate cell)
# Physically separated from try/except to prevent exit exception from being caught
dbutils.notebook.exit(json.dumps(_result))
