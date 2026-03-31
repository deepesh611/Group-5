# Databricks notebook source
# =============================================================================
# pipeline_1_ingestion.py — Self-Healing Ingestion Pipeline
# =============================================================================
# Pipeline 1: Synthea CSVs (ADLS /raw/) → Pre-flight schema check
#             → Cast + normalize → Write Delta tables → Validate → Log
#
# Triggers Pipeline 3 (AI Advisor) for CRITICAL schema drift (synchronous,
# waits for human approval). WARNING drift is logged and proceeds with
# mergeSchema fallback; P3 can be run manually for advisory review.
#
# Run via: Databricks Jobs (scheduled daily) or manually cell-by-cell.
# Parameters (via Databricks widgets or Databricks Job task params):
#   table_filter      — comma-separated table names to process, or "all"
#   dry_run           — "true" to detect drift without writing Delta tables
#   on_critical_drift — "halt" (default) or "fallback" on CRITICAL drift
#
# Depends on: utils/00_config, utils/01_schema_utils, utils/04_metadata_manager
# =============================================================================

# COMMAND ----------

# MAGIC %run ./utils/00_config

# COMMAND ----------

# MAGIC %run ./utils/04_metadata_manager

# COMMAND ----------

# MAGIC %run ./utils/01_schema_utils

# COMMAND ----------

# =============================================================================
# CELL 1 — Imports
# =============================================================================
# All imports at module level — avoids re-import overhead inside the main loop
# and ensures 'traceback' is available in the except handler.

import json
import traceback
from datetime import datetime

from pyspark.sql.functions import col, count, when, isnan

# COMMAND ----------

# =============================================================================
# CELL 2 — Widget Parameters
# =============================================================================
# Widgets create interactive UI controls at the top of the notebook.
# When run as a Databricks Job, pass these as task parameters instead.
# Re-running the notebook reuses existing widget values (Databricks behavior).

dbutils.widgets.text(
    "table_filter",
    "all",
    "Tables to ingest (comma-separated or 'all')",
)
dbutils.widgets.dropdown(
    "dry_run",
    "false",
    ["false", "true"],
    "Dry run: detect drift only, no Delta write",
)
dbutils.widgets.dropdown(
    "on_critical_drift",
    "halt",
    ["halt", "fallback"],
    "On CRITICAL drift: halt table or use mergeSchema fallback",
)

# COMMAND ----------

# =============================================================================
# CELL 3 — Read Parameters & Initialise
# =============================================================================

TABLE_FILTER      = dbutils.widgets.get("table_filter").strip()
DRY_RUN           = dbutils.widgets.get("dry_run").strip().lower() == "true"
ON_CRITICAL_DRIFT = dbutils.widgets.get("on_critical_drift").strip().lower()

# Set catalog context (USE CATALOG project_5; USE SCHEMA delta_tables)
init_catalog()

# Resolve which tables to process
if TABLE_FILTER.lower() == "all":
    TABLES_TO_PROCESS = list(SYNTHEA_TABLES)   # copy list to avoid mutating constant
else:
    TABLES_TO_PROCESS = [t.strip().lower() for t in TABLE_FILTER.split(",") if t.strip()]

print(f"{'─'*65}")
print(f"  Pipeline 1 — Self-Healing Ingestion")
print(f"  Started:           {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"  Tables:            {TABLES_TO_PROCESS}")
print(f"  Dry run:           {DRY_RUN}")
print(f"  On critical drift: {ON_CRITICAL_DRIFT.upper()}")
print(f"  Raw CSV source:    {RAW_PATH}")
print(f"  Target catalog:    {FULL_SCHEMA}")
print(f"{'─'*65}")

if DRY_RUN:
    print("\n⚠️  DRY RUN MODE — No Delta tables will be written.\n")

# COMMAND ----------

# =============================================================================
# CELL 4 — Helper: Build Schema-Aware Spark SchemaType
# =============================================================================
# Using inferSchema=True + samplingRatio can produce inconsistent types when
# a column has rare values in the unsampled portion (e.g. a numeric column with
# one stray empty string in the last 90% would be inferred as StringType).
# This helper builds an explicit StructType from master_schema.json so that
# Spark reads the CSV with the declared types — no sampling instability.

from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, IntegerType, DoubleType,
    BooleanType, DateType, TimestampType,
)

_TYPE_MAP = {
    "string":    StringType(),
    "long":      LongType(),
    "int":       IntegerType(),
    "integer":   IntegerType(),
    "double":    DoubleType(),
    "float":     DoubleType(),
    "boolean":   BooleanType(),
    "date":      DateType(),
    "timestamp": TimestampType(),
}

def build_spark_schema_from_master(table_name: str):
    """
    Build a Spark StructType from master_schema.json column definitions.
    All columns default to StringType for safe CSV reading; explicit types
    for known numeric/date columns are applied in cast_date_columns() after load.

    We read ALL CSV columns as StringType initially and then cast — this is the
    safest pattern for CSVs where a single bad value can corrupt type inference.

    Returns None if the table is not in master_schema.json (fall back to inferSchema).
    """
    try:
        cols = get_table_columns(table_name)
        if not cols:
            return None
        fields = [StructField(c, StringType(), nullable=True) for c in cols.keys()]
        return StructType(fields)
    except Exception:
        return None


# COMMAND ----------

# =============================================================================
# CELL 5 — Helper: Compute Null Stats
# =============================================================================

def compute_null_stats(df) -> dict:
    """
    Compute null percentage per column after casting (double columns may have NaN).
    Returns {column_name: null_pct_float}.
    Uses a single .collect() over all columns for efficiency.
    """
    exprs = []
    dtype_map = dict(df.dtypes)
    for c in df.columns:
        if dtype_map.get(c) in ("float", "double"):
            expr = (
                count(when(col(c).isNull() | isnan(col(c)), c)) / count("*") * 100
            ).alias(c)
        else:
            expr = (
                count(when(col(c).isNull(), c)) / count("*") * 100
            ).alias(c)
        exprs.append(expr)

    row = df.select(exprs).collect()[0]
    return {c: round(float(row[c]), 2) for c in df.columns}

# COMMAND ----------

# =============================================================================
# CELL 6 — Helper: Trigger Pipeline 3 (AI Advisor — CRITICAL drift only)
# =============================================================================

def trigger_pipeline_3(table_name: str, drift: dict) -> dict:
    """
    Call Pipeline 3 synchronously via dbutils.notebook.run().
    IMPORTANT: This is a BLOCKING call — ingestion pauses while P3 runs.
    Use only for CRITICAL drift where human approval is required before proceeding.

    Timeout: 1800 seconds (30 min) — allows time for the human approval step.
    P3's exit payload is {"status": "fix_applied"|"fix_declined"|"error", ...}.
    """
    drift_payload = build_drift_event_payload(table_name, drift)

    try:
        log.info(f"Triggering Pipeline 3 (blocking) for '{table_name}'...")
        result_str = dbutils.notebook.run(
            "./pipeline_3_advisor",
            timeout_seconds=1800,
            arguments={
                "table_name":  table_name,
                "drift_event": drift_payload,
            },
        )
        result = json.loads(result_str) if result_str else {}
        log.info(f"Pipeline 3 returned for '{table_name}': {result}")
        return result
    except Exception as e:
        log.error(f"Pipeline 3 call failed for '{table_name}': {e}")
        return {"status": "error", "message": str(e)}

# COMMAND ----------

# =============================================================================
# CELL 7 — Helper: Write Delta Table (Unity Catalog)
# =============================================================================

def write_delta_table(df, table_name: str, merge_schema: bool = False) -> int:
    """
    Write a DataFrame to the Unity Catalog Delta table using saveAsTable().
    saveAsTable() is the correct API for Unity Catalog — it creates the table
    on first write and manages its location under the catalog/schema.

    mergeSchema=true allows the Delta table schema to evolve on overwrite
    (e.g. if the new DataFrame has an extra column that wasn't in the previous
    table version). Without it, Databricks will reject schema mismatches.

    Returns the final post-write row count from the Delta table for validation.
    """
    full_name = get_full_table_name(table_name)  # project_5.delta_tables.<table>

    (
        df.write
        .format("delta")
        .mode(DEFAULT_WRITE_MODE)
        .option("mergeSchema", str(merge_schema).lower())
        .saveAsTable(full_name)
    )

    # Post-write validation: query the Delta table directly
    final_count = spark.sql(f"SELECT COUNT(*) AS n FROM {full_name}").collect()[0]["n"]
    return final_count

# COMMAND ----------

# =============================================================================
# CELL 8 — Helper: Print Per-Table Run Report
# =============================================================================

def print_table_report(report: dict) -> None:
    """Print a single-line status summary for one table."""
    status_icon = {
        "ok":       "✅",
        "skipped":  "⏭️ ",
        "fallback": "⚠️ ",
        "failed":   "❌",
    }.get(report["status"], "❓")

    # Safe formatting: row_count is always int (initialized to 0)
    row_count_str = f"{report['row_count']:>10,}" if isinstance(report["row_count"], int) else f"{'n/a':>10}"
    col_count_str = f"{report['columns']:>3}" if isinstance(report["columns"], int) else "n/a"

    print(
        f"\n  {status_icon} {report['table']:20s} "
        f"rows={row_count_str}  "
        f"cols={col_count_str}  "
        f"drift={report.get('drift_severity', 'NONE'):>8}  "
        f"status={report['status'].upper()}"
    )
    if report.get("error"):
        print(f"      Error: {report['error']}")
    if report.get("notes"):
        for note in report["notes"]:
            print(f"      Note:  {note}")

# COMMAND ----------

# =============================================================================
# CELL 9 — Main Ingestion Loop
# =============================================================================

run_reports   = []
all_csv_files = {f.name.replace(".csv", "").lower(): f for f in list_raw_csvs()}

print(f"\nFound {len(all_csv_files)} CSV file(s) in {RAW_PATH}:")
for name in sorted(all_csv_files.keys()):
    print(f"  • {name}.csv")
print()

# ─── Pre-load master schema table list (cached) ──────────────────────────────
known_tables = get_all_table_names()

for table_name in TABLES_TO_PROCESS:

    report = {
        "table":          table_name,
        "status":         "failed",      # default — overwritten on success
        "row_count":      0,
        "columns":        0,
        "drift_severity": "NONE",
        "error":          "",
        "notes":          [],
    }

    print(f"\n{'='*65}")
    print(f"  Processing: {table_name.upper()}")
    print(f"{'='*65}")

    # ── Guard: CSV must exist in ADLS ─────────────────────────────────────
    if table_name not in all_csv_files:
        msg = f"CSV '{table_name}.csv' not found in {RAW_PATH} — skipping."
        log.warning(f"[{table_name}] {msg}")
        report["status"] = "skipped"
        report["error"]  = msg
        run_reports.append(report)
        print(f"  ⏭️  {msg}")
        continue

    csv_file = all_csv_files[table_name]

    try:
        # ── STEP 1: Load CSV with schema-aware reading ────────────────────
        print(f"\n  [1/6] Reading CSV: {csv_file.path}")

        # Use explicit schema (all-StringType) if the table is in master_schema.
        # This avoids samplingRatio type-inference instability on large files.
        # cast_date_columns() will convert to correct types after load.
        explicit_schema = build_spark_schema_from_master(table_name) \
            if table_name in known_tables else None

        if explicit_schema is not None:
            df = spark.read.csv(
                csv_file.path,
                header=True,
                schema=explicit_schema,
            )
            print(f"      Mode: explicit schema ({len(explicit_schema)} fields) from master_schema.json")
        else:
            # Unknown table or schema not in master_schema.json — infer types
            df = spark.read.csv(
                csv_file.path,
                header=True,
                inferSchema=True,
                samplingRatio=0.25,   # 25% sample for better type accuracy
            )
            print(f"      Mode: inferred schema (table not in master_schema.json)")

        source_row_count    = df.count()
        report["row_count"] = source_row_count
        report["columns"]   = len(df.columns)
        print(f"      Rows: {source_row_count:,}  |  Columns: {len(df.columns)}")

        # ── STEP 2: Normalize column names ───────────────────────────────
        print(f"\n  [2/6] Normalizing column names …")
        df = normalize_column_names(df)
        print(f"      Columns: {df.columns}")

        # ── STEP 3: Pre-flight schema drift check ────────────────────────
        print(f"\n  [3/6] Pre-flight schema drift check …")

        if table_name not in known_tables:
            msg = (
                f"Table '{table_name}' not in master_schema.json. "
                f"Drift check skipped — table will be written with CSV schema."
            )
            log.warning(f"[{table_name}] {msg}")
            report["notes"].append(msg)
            drift = {
                "table": table_name, "has_drift": False, "severity": "NONE",
                "new_columns": [], "missing_columns": [], "type_changes": [],
            }
        else:
            # When reading with explicit all-StringType schema, cast first
            # so that drift detection sees the actual target types (not all "string").
            df = cast_date_columns(df, table_name)
            drift = detect_drift(table_name, df)
            print(generate_drift_summary(drift))
            report["drift_severity"] = drift["severity"]

        # ── Handle drift ─────────────────────────────────────────────────
        use_merge_schema = False

        if drift["has_drift"]:
            log_drift_event(table_name, drift)  # always audit-log drift events

            if drift["severity"] == "CRITICAL":
                if ON_CRITICAL_DRIFT == "fallback":
                    msg = (
                        f"CRITICAL drift on '{table_name}' — proceeding with "
                        f"mergeSchema fallback as requested. Run Pipeline 3 separately."
                    )
                    log.warning(f"[{table_name}] {msg}")
                    report["notes"].append(msg)
                    use_merge_schema = True

                else:  # ON_CRITICAL_DRIFT == "halt" (default)
                    print(f"\n  🚨 CRITICAL drift — triggering AI Advisor (Pipeline 3) …")
                    p3_result = trigger_pipeline_3(table_name, drift)

                    if p3_result.get("status") == "fix_applied":
                        msg = (
                            f"Pipeline 3 fix applied for '{table_name}'. "
                            f"Re-run ingestion for this table to pick up the fixed schema."
                        )
                        print(f"  ✅ {msg}")
                        report["notes"].append(msg)
                        invalidate_cache()          # force reload of updated master_schema.json
                        report["status"] = "skipped"
                        report["error"]  = "CRITICAL drift — P3 fix applied; re-run required"
                        run_reports.append(report)
                        print_table_report(report)
                        continue

                    elif p3_result.get("status") == "fix_declined":
                        msg = f"Human declined AI fix for '{table_name}'. Table skipped."
                        log.warning(f"[{table_name}] {msg}")
                        report["status"] = "skipped"
                        report["error"]  = msg
                        run_reports.append(report)
                        print_table_report(report)
                        continue

                    else:
                        # P3 error or timeout — fall back rather than blocking whole run
                        msg = (
                            f"Pipeline 3 could not resolve drift for '{table_name}' "
                            f"({p3_result.get('status', 'unknown')}). "
                            f"Using mergeSchema fallback."
                        )
                        log.warning(f"[{table_name}] {msg}")
                        report["notes"].append(msg)
                        use_merge_schema = True

            elif drift["severity"] == "WARNING":
                # WARNING: proceed with mergeSchema; log for manual P3 review.
                # We do NOT call P3 synchronously here because P3 requires human
                # approval — blocking the full ingestion run for a WARNING would
                # mean all remaining tables wait for someone to click Approve.
                # Instead: drift is logged to ADLS /logs/drift/, which P3 can be
                # pointed at in a separate manual or scheduled run.
                msg = (
                    f"WARNING drift on '{table_name}' — new/changed non-critical columns. "
                    f"Drift logged to ADLS. Run Pipeline 3 manually for review."
                )
                log.warning(f"[{table_name}] {msg}")
                report["notes"].append(msg)
                use_merge_schema = MERGE_SCHEMA_FALLBACK

            elif drift["severity"] == "INFO":
                # INFO: trivial compatible type widening — no action needed
                msg = f"INFO drift on '{table_name}' — compatible type widening, proceeding."
                report["notes"].append(msg)
                log.info(f"[{table_name}] {msg}")

        # ── STEP 4: Cast types (if not already done in STEP 3) ───────────
        print(f"\n  [4/6] Casting column types …")
        if table_name in known_tables and not drift["has_drift"]:
            # If we already cast in STEP 3 (for known tables without drift),
            # don't cast again. Only cast here for the "no drift" fast path
            # where we skipped the early cast.
            # Note: if build_spark_schema_from_master returned None (unknown table),
            # cast_date_columns was not called yet — call it now.
            if explicit_schema is None:
                df = cast_date_columns(df, table_name)
        elif table_name not in known_tables and explicit_schema is None:
            pass  # unknown table, no schema to cast against
        print(f"      Types finalised")

        # ── STEP 5: Write Delta table ─────────────────────────────────────
        full_table_name = get_full_table_name(table_name)

        if DRY_RUN:
            print(f"\n  [5/6] DRY RUN — would write to {full_table_name} ({source_row_count:,} rows)")
            report["status"] = "skipped"
            report["notes"].append("Dry run — no write performed")
            run_reports.append(report)
            print_table_report(report)
            continue

        print(f"\n  [5/6] Writing Delta table → {full_table_name}")
        print(f"      Mode: overwrite | mergeSchema={str(use_merge_schema).lower()}")

        final_count = write_delta_table(df, table_name, merge_schema=use_merge_schema)

        # Row count sanity check
        if final_count != source_row_count:
            msg = (
                f"Row count mismatch after write: "
                f"source={source_row_count:,} vs delta={final_count:,}. "
                f"This may indicate a Delta write failure or race condition."
            )
            log.error(f"[{table_name}] {msg}")
            report["error"] = msg
            # report is appended in the finally → post-finally append block
            raise RuntimeError(msg)   # bubble up so except handler catches it

        print(f"      ✓ {final_count:,} rows written and verified")

        # ── STEP 6: Update metadata & log run ────────────────────────────
        print(f"\n  [6/6] Updating metadata …")
        update_row_count(table_name, final_count)

        null_stats = compute_null_stats(df)
        high_null  = {c: pct for c, pct in null_stats.items() if pct > 50}
        if high_null:
            note = f"Columns with >50% nulls (expected for Synthea): {high_null}"
            report["notes"].append(note)
            print(f"      ℹ️  {note}")

        log_ingestion_run(table_name, {
            "source_path":    csv_file.path,
            "row_count":      final_count,
            "columns":        len(df.columns),
            "drift_severity": report["drift_severity"],
            "merge_schema":   use_merge_schema,
            "null_stats":     null_stats,
            "notes":          report["notes"],
        })

        report["status"] = "fallback" if use_merge_schema else "ok"
        print(f"\n  {'⚠️ ' if use_merge_schema else '✅'} "
              f"'{table_name}' complete — {final_count:,} rows")

    except Exception as exc:
        report["error"] = str(exc)
        log.error(f"[{table_name}] FAILED: {exc}")
        log.error(traceback.format_exc())
        print(f"\n  ❌ '{table_name}' FAILED: {exc}")

    finally:
        # Release Spark's block manager memory between tables.
        # This is important for large tables like observations (5.3M rows).
        spark.catalog.clearCache()
        try:
            df.unpersist()
        except Exception:
            pass   # df may not have been created if STEP 1 failed

    run_reports.append(report)
    print_table_report(report)

# COMMAND ----------

# =============================================================================
# CELL 10 — Pipeline Summary Report
# =============================================================================

print(f"\n{'='*65}")
print(f"  PIPELINE 1 — INGESTION SUMMARY")
print(f"  Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"{'='*65}")

# Deduplicate reports: tables that had an early 'continue' are already in
# run_reports; we must not double-count them in the summary loop.
seen_tables = set()
deduped_reports = []
for r in run_reports:
    if r["table"] not in seen_tables:
        seen_tables.add(r["table"])
        deduped_reports.append(r)

counts = {"ok": 0, "skipped": 0, "fallback": 0, "failed": 0}
for r in deduped_reports:
    status = r["status"]
    counts[status] = counts.get(status, 0) + 1
    print_table_report(r)

print(f"\n  {'─'*55}")
print(f"  ✅ OK: {counts['ok']}   "
      f"⚠️  Fallback: {counts['fallback']}   "
      f"⏭️  Skipped: {counts['skipped']}   "
      f"❌ Failed: {counts['failed']}")
print(f"  {'─'*55}")

# Exit value for Job chaining or dbutils.notebook.run() callers
exit_payload = json.dumps({
    "status":  "ok" if counts.get("failed", 0) == 0 else "partial_failure",
    "reports": deduped_reports,
    "counts":  counts,
    "dry_run": DRY_RUN,
})
dbutils.notebook.exit(exit_payload)
