# Databricks notebook source
# =============================================================================
# pipeline_1_ingestion.py — Self-Healing Ingestion Pipeline
# =============================================================================
# Pipeline 1: Synthea CSVs (ADLS /raw/)  →  Pre-flight schema check
#             →  Normalize + Cast  →  Write Delta tables  →  Validate  →  Log
#
# Triggers Pipeline 3 (AI Advisor) for CRITICAL schema drift (synchronous,
# waits for human approval). WARNING drift is logged and proceeds; P3 can be
# run separately for advisory review.
#
# Run via: Databricks Jobs (scheduled daily) or manually cell-by-cell.
#
# Parameters (Databricks widgets or Job task params):
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

import json
import time
import traceback
from datetime import datetime

from pyspark.sql.functions import col, count, when, isnan, expr

# COMMAND ----------

# =============================================================================
# CELL 2 — Widget Parameters
# =============================================================================
# Widgets create interactive UI controls at the top of the notebook.
# When run as a Databricks Job, pass these as task parameters instead.

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
    "On CRITICAL drift: halt table or use overwriteSchema fallback",
)

# COMMAND ----------

# =============================================================================
# CELL 3 — Read Parameters & Initialise
# =============================================================================

TABLE_FILTER      = dbutils.widgets.get("table_filter").strip()
DRY_RUN           = dbutils.widgets.get("dry_run").strip().lower() == "true"
ON_CRITICAL_DRIFT = dbutils.widgets.get("on_critical_drift").strip().lower()

# Set catalog context (USE CATALOG project5; USE SCHEMA delta_tables)
init_catalog()

# Resolve which tables to process
if TABLE_FILTER.lower() == "all":
    TABLES_TO_PROCESS = list(SYNTHEA_TABLES)
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
# CELL 4 — CSV Reading Strategy (Documentation)
# =============================================================================
# We ALWAYS read CSVs with inferSchema=False for known tables.
#
# WHY NOT use an explicit StructType schema built from master_schema.json?
#   When spark.read.csv() is given an explicit schema, Spark ONLY materialises
#   columns listed in the schema. Extra CSV columns (drift columns) are SILENTLY
#   DROPPED before drift detection ever runs — so the pipeline reports "no drift"
#   even when new columns exist in the CSV. This is incorrect.
#
# inferSchema=False reads ALL columns from the CSV header as StringType.
# cast_date_columns() then converts known columns to their declared types.
# Unknown / new columns remain StringType → correctly detected as drift.
#
# inferSchema=True is only used for genuinely unknown tables (not in
# master_schema.json) because there we have no declared types to cast to.

# COMMAND ----------

# =============================================================================
# CELL 5 — Helper: Count bad-cast rows (date/type quality audit)
# =============================================================================

def count_null_after_cast(df_before, df_after, col_name: str) -> int:
    """
    Count rows where casting produced NULL from a non-null source value.
    This surfaces rows where try_to_date / try_to_timestamp returned NULL
    because the source value was malformed (e.g. concatenated birthdate+id).

    Uses a single Spark action on df_after; df_before is used only for schema
    info to decide whether the col existed before casting.
    """
    try:
        # Rows where the CAST result is NULL but we know the source was a string.
        # For simplicity, count NULLs in the cast output col — most date cols in
        # Synthea have very low null rates, so any NULL is suspicious.
        null_count = df_after.filter(col(col_name).isNull()).count()
        return null_count
    except Exception:
        return -1  # couldn't compute — non-fatal


def compute_null_stats(df) -> dict:
    """
    Compute null percentage per column in a single Spark action.
    Returns {column_name: null_pct_float}.
    Handles double/float NaN as well as NULL.
    """
    exprs = []
    dtype_map = dict(df.dtypes)
    for c in df.columns:
        if dtype_map.get(c) in ("float", "double"):
            expr_agg = (
                count(when(col(c).isNull() | isnan(col(c)), c)) / count("*") * 100
            ).alias(c)
        else:
            expr_agg = (
                count(when(col(c).isNull(), c)) / count("*") * 100
            ).alias(c)
        exprs.append(expr_agg)

    row = df.select(exprs).collect()[0]
    return {c: round(float(row[c]), 2) for c in df.columns}

# COMMAND ----------

# =============================================================================
# CELL 6 — Helper: Trigger Pipeline 3 (AI Advisor — CRITICAL drift only)
# =============================================================================

def trigger_pipeline_3(table_name: str, drift: dict) -> dict:
    """
    Call Pipeline 3 synchronously via dbutils.notebook.run().
    BLOCKING call — ingestion pauses while P3 runs and awaits human approval.
    Timeout: 1800 s (30 min). P3 exits with JSON payload.
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
                "run_mode":    "autonomous",
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

def write_delta_table(df, table_name: str) -> int:
    """
    Write a DataFrame to the Unity Catalog Delta table using:
        CREATE OR REPLACE TABLE <catalog.schema.table> USING DELTA
        AS SELECT * FROM <tmp_view>

    WHY NOT saveAsTable(mode='overwrite')?
    ──────────────────────────────────────────────────────────────────────────
    saveAsTable() reconciles the incoming DataFrame schema against the existing
    Delta _delta_log on ADLS.  DROP TABLE in Unity Catalog removes the catalog
    entry but DOES NOT always purge the _delta_log and Parquet files from the
    managed ADLS path.  When saveAsTable() finds stale files, Delta reads the
    old (potentially corrupt) schema and tries to CAST the new DataFrame's
    columns to match it.  This causes:
      •  CAST_INVALID_INPUT     — e.g. UUID being cast to DATE
      •  CANNOT_PARSE_TIMESTAMP — e.g. "1987-03-02<uuid>" in a date column

    CREATE OR REPLACE TABLE (CORT) ... AS SELECT is immune because:
      •  It is a DDL statement — Delta does NOT read the existing _delta_log
         to determine what to cast; it derives schema from the SELECT result
      •  It atomically drops + recreates the table from the SELECT's schema
      •  Unity Catalog guarantees the managed ADLS path is cleaned on REPLACE
      •  No schema reconciliation, no casting errors, no stale-log interaction

    Returns the final post-write row count (from the Delta table, not the
    input DataFrame) for validation.
    """
    full_name = get_full_table_name(table_name)

    # Timestamped view name prevents collisions if tables run in parallel
    tmp_view = f"_p1_staging_{table_name}_{int(time.time())}"

    df.createOrReplaceTempView(tmp_view)
    try:
        spark.sql(f"""
            CREATE OR REPLACE TABLE {full_name}
            USING DELTA
            AS SELECT * FROM {tmp_view}
        """)
        log.info(f"[{table_name}] CREATE OR REPLACE TABLE complete → {full_name}")
    finally:
        # Always clean up — even if the SQL fails
        try:
            spark.catalog.dropTempView(tmp_view)
        except Exception:
            pass

    # Post-write validation: query the Delta table directly (not the in-memory DF)
    final_count = spark.sql(f"SELECT COUNT(*) AS n FROM {full_name}").collect()[0]["n"]
    return final_count

# COMMAND ----------

# =============================================================================
# CELL 8 — Helper: Print Per-Table Run Report
# =============================================================================

def print_table_report(report: dict) -> None:
    """Print a single-line status summary for one table."""
    status_icon = {
        "ok":          "✅",
        "skipped":     "⏭️ ",
        "fallback":    "⚠️ ",
        "overwritten": "⚠️ ",
        "failed":      "❌",
    }.get(report["status"], "❓")

    row_count_str = (
        f"{report['row_count']:>10,}"
        if isinstance(report["row_count"], int)
        else f"{'n/a':>10}"
    )
    col_count_str = (
        f"{report['columns']:>3}"
        if isinstance(report["columns"], int)
        else "n/a"
    )

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

# Pre-load master schema table list (cached between iterations)
known_tables = get_all_table_names()

for table_name in TABLES_TO_PROCESS:

    report = {
        "table":          table_name,
        "status":         "failed",   # default; overwritten on success
        "row_count":      0,
        "columns":        0,
        "drift_severity": "NONE",
        "error":          "",
        "notes":          [],
    }

    print(f"\n{'='*65}")
    print(f"  Processing: {table_name.upper()}")
    print(f"{'='*65}")

    # Guard: CSV must exist in ADLS
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
        # ── STEP 1: Read CSV ─────────────────────────────────────────────
        print(f"\n  [1/6] Reading CSV: {csv_file.path}")

        if table_name in known_tables:
            # KNOWN TABLE — read all columns as StringType.
            # This preserves drift columns (new CSV fields not yet in
            # master_schema.json) so detect_drift() can see them.
            # cast_date_columns() converts known columns to their declared
            # types in STEP 3.
            df = spark.read.csv(
                csv_file.path,
                header=True,
                inferSchema=False,   # ALL columns → StringType, nothing dropped
                multiLine=False,     # Synthea CSVs are single-line-per-row
            )
            print(f"      Mode: all-string read ({len(df.columns)} cols from CSV header)")
        else:
            # UNKNOWN TABLE — infer types because we have no declared schema
            # to cast against. These tables will have no drift check.
            df = spark.read.csv(
                csv_file.path,
                header=True,
                inferSchema=True,
                samplingRatio=0.25,
            )
            print(f"      Mode: inferred schema (table not in master_schema.json)")

        source_row_count    = df.count()
        report["row_count"] = source_row_count
        report["columns"]   = len(df.columns)
        print(f"      Rows: {source_row_count:,}  |  Columns: {len(df.columns)}")

        # ── STEP 2: Normalize column names ────────────────────────────────
        print(f"\n  [2/6] Normalizing column names …")
        df = normalize_column_names(df)
        print(f"      Columns: {df.columns}")

        # ── STEP 3: Cast known columns + pre-flight drift check ──────────
        print(f"\n  [3/6] Type casting & schema drift check …")

        if table_name not in known_tables:
            # Unknown table: no schema to cast against, skip drift check
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
            # Cast known columns to declared types using safe try_to_date /
            # try_to_timestamp — malformed values become NULL, never crash.
            # Unknown columns remain StringType for drift detection.
            df = cast_date_columns(df, table_name)

            # Audit: warn about any date/numeric columns that became NULL
            # after casting (indicates malformed values in the source CSV).
            # This is purely informational — the pipeline continues.
            expected_cols = get_expected_columns(table_name)
            castable_types = {"date", "timestamp", "double", "long", "int", "integer", "boolean"}
            for c_name, c_meta in expected_cols.items():
                if c_meta["type"] in castable_types and c_name in df.columns:
                    null_pct = df.filter(col(c_name).isNull()).count() / max(source_row_count, 1) * 100
                    if null_pct > 0.1:  # more than 0.1% nulls in a cast column → warn
                        warn_msg = (
                            f"Column '{c_name}' ({c_meta['type']}): "
                            f"{null_pct:.1f}% rows are NULL after casting. "
                            f"Source CSV may contain malformed values in this column."
                        )
                        log.warning(f"[{table_name}] {warn_msg}")
                        report["notes"].append(warn_msg)
                        print(f"      ⚠️  {warn_msg}")

            drift = detect_drift(table_name, df)
            print(generate_drift_summary(drift))
            report["drift_severity"] = drift["severity"]

        # ── Handle drift ─────────────────────────────────────────────────
        skip_table = False

        if drift["has_drift"]:
            log_drift_event(table_name, drift)

            if drift["severity"] == "CRITICAL":
                if ON_CRITICAL_DRIFT == "fallback":
                    msg = (
                        f"CRITICAL drift on '{table_name}' — "
                        f"proceeding without schema enforcement. "
                        f"Run Pipeline 3 separately to fix master_schema.json."
                    )
                    log.warning(f"[{table_name}] {msg}")
                    report["notes"].append(msg)
                    report["drift_severity"] = "CRITICAL"

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
                        invalidate_cache()
                        report["status"] = "skipped"
                        report["error"]  = "CRITICAL drift — P3 fix applied; re-run required"
                        run_reports.append(report)
                        print_table_report(report)
                        skip_table = True

                    elif p3_result.get("status") == "fix_declined":
                        msg = f"Human declined AI fix for '{table_name}'. Table skipped."
                        log.warning(f"[{table_name}] {msg}")
                        report["status"] = "skipped"
                        report["error"]  = msg
                        run_reports.append(report)
                        print_table_report(report)
                        skip_table = True

                    else:
                        # P3 error or timeout — warn and continue (don't block all tables)
                        msg = (
                            f"Pipeline 3 could not resolve drift for '{table_name}' "
                            f"({p3_result.get('status', 'unknown')}). "
                            f"Proceeding with current schema."
                        )
                        log.warning(f"[{table_name}] {msg}")
                        report["notes"].append(msg)

            elif drift["severity"] == "WARNING":
                # WARNING: new columns detected — log for P3 review.
                # We do NOT block the full run for a WARNING; drift is logged
                # to ADLS /logs/drift/ for P3 to process separately.
                msg = (
                    f"WARNING drift on '{table_name}' — new/changed columns. "
                    f"Drift logged to ADLS. Run Pipeline 3 manually for review."
                )
                log.warning(f"[{table_name}] {msg}")
                report["notes"].append(msg)

            elif drift["severity"] == "INFO":
                msg = f"INFO drift on '{table_name}' — compatible type widening, proceeding."
                report["notes"].append(msg)
                log.info(f"[{table_name}] {msg}")

        if skip_table:
            continue

        # ── STEP 4: Log cast summary ──────────────────────────────────────
        print(f"\n  [4/6] Cast summary …")
        dtype_map = dict(df.dtypes)
        cast_cols = {
            c: dtype_map[c]
            for c in df.columns
            if dtype_map[c] not in ("string",)
        }
        if cast_cols:
            for c, t in cast_cols.items():
                print(f"      {c}: string → {t}")
        else:
            print(f"      All columns remain StringType (no type casting applied)")

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
        print(f"      Strategy: CREATE OR REPLACE TABLE (schema always fresh)")

        final_count = write_delta_table(df, table_name)

        # Row count sanity check: CORT preserves every row, including those
        # that became NULL after date casting. If counts differ, it indicates
        # a genuine write problem (I/O error, cluster issue, etc.).
        if final_count != source_row_count:
            msg = (
                f"Row count mismatch after write: "
                f"source={source_row_count:,} vs delta={final_count:,}. "
                f"This indicates a write failure or race condition — not a cast issue "
                f"(NULLs from try_to_date preserve the row, not drop it)."
            )
            log.error(f"[{table_name}] {msg}")
            raise RuntimeError(msg)

        print(f"      ✓ {final_count:,} rows written and verified")

        # ── STEP 6: Update metadata & log run ─────────────────────────────
        print(f"\n  [6/6] Updating metadata …")
        update_row_count(table_name, final_count)

        null_stats = compute_null_stats(df)
        high_null  = {c: pct for c, pct in null_stats.items() if pct > 50}
        if high_null:
            note = f"Columns with >50% nulls (expected for optional Synthea fields): {high_null}"
            report["notes"].append(note)
            print(f"      ℹ️  {note}")

        log_ingestion_run(table_name, {
            "source_path":    csv_file.path,
            "row_count":      final_count,
            "columns":        len(df.columns),
            "drift_severity": report["drift_severity"],
            "null_stats":     null_stats,
            "notes":          report["notes"],
        })

        report["status"] = "ok"
        print(f"\n  ✅ '{table_name}' complete — {final_count:,} rows")

    except Exception as exc:
        report["error"] = str(exc)
        log.error(f"[{table_name}] FAILED: {exc}")
        log.error(traceback.format_exc())
        print(f"\n  ❌ '{table_name}' FAILED: {exc}")

    finally:
        # Release Spark block manager memory between tables.
        # Critical for large tables like observations (millions of rows).
        spark.catalog.clearCache()
        try:
            df.unpersist()
        except Exception:
            pass

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

# Deduplicate: tables halted early (continue) are already in run_reports
seen_tables  = set()
deduped_reports = []
for r in run_reports:
    if r["table"] not in seen_tables:
        seen_tables.add(r["table"])
        deduped_reports.append(r)

counts = {"ok": 0, "skipped": 0, "fallback": 0, "overwritten": 0, "failed": 0}
for r in deduped_reports:
    counts[r["status"]] = counts.get(r["status"], 0) + 1
    print_table_report(r)

print(f"\n  {'─'*55}")
print(
    f"  ✅ OK: {counts['ok']}   "
    f"⚠️  Fallback/Overwritten: {counts['fallback'] + counts['overwritten']}   "
    f"⏭️  Skipped: {counts['skipped']}   "
    f"❌ Failed: {counts['failed']}"
)
print(f"  {'─'*55}")

# Exit value for Job chaining or dbutils.notebook.run() callers
exit_payload = json.dumps({
    "status":  "ok" if counts.get("failed", 0) == 0 else "partial_failure",
    "reports": deduped_reports,
    "counts":  counts,
    "dry_run": DRY_RUN,
})
dbutils.notebook.exit(exit_payload)
