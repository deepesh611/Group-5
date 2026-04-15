import os
import json
import glob
import csv as csv_module

from datetime import datetime
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

from pyspark.sql.functions import (
    col, to_timestamp, sha2, concat_ws, lit,    
    year, count, when, isnan
)


# ---------------------------------------------------------------------------
# PHI Masking Configuration
# ---------------------------------------------------------------------------
#
# Strategy per column type:
#
#   HASH   — deterministic SHA-256 hash (salted). Used for UUID-like ID columns
#             so that joins across tables still work after masking.
#             e.g.  PATIENT in encounters == PATIENT in conditions (after hashing)
#
#   REDACT — replaced with NULL. Direct identifiers with no analytical value.
#             e.g.  SSN, DRIVERS, PASSPORT, FIRST, LAST, ADDRESS
#
#   YEAR   — date generalized to year-only integer (keeps age-band analysis).
#             e.g.  BIRTHDATE "1982-03-14" → 1982
#
#   KEEP   — not PHI; used for clinical/demographic analysis as-is.
#             e.g.  CODE, DESCRIPTION, RACE, GENDER, MARITAL
#
# Column names are matched AFTER lowercasing (clean_column_names runs first).

# Salt makes hashes non-reversible even if the original UUIDs are known.
# Change this value per deployment; store it securely (e.g. env var / secret manager).
HASH_SALT = os.environ.get("PHI_HASH_SALT", "synthea_default_salt_change_me")

# Columns to hash (lowercase). These are patient/encounter UUIDs used as join keys.
HASH_COLS = {"patient", "id", "encounter"}

# Column fragments identifying UUIDs/IDs (not to be treated as timestamps)
_UUID_KEYWORDS = ("id", "uuid") 

# Columns to redact (lowercase). Direct identifiers — no analytical use.
REDACT_COLS = {"ssn", "drivers", "passport", "first", "last", "maiden", "address"}

# Columns to generalize to year only (lowercase).
YEAR_COLS = {"birthdate", "deathdate"}

# Everything else (start, stop, date, code, description, race, gender, etc.) is kept as-is.


# ---------------------------------------------------------------------------
# Spark session
# ---------------------------------------------------------------------------

def create_spark_session():
    builder = (
        SparkSession.builder
        .appName("Synthea-CSV-to-Delta")
        .master("local[*]")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")

        # ✅ REQUIRED for Delta
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

        # ✅ Driver memory — bump if you hit OOM on large Synthea exports
        .config("spark.driver.memory", "4g")
        .config("spark.driver.maxResultSize", "2g")

        # ✅ Adaptive query execution — lets Spark optimise joins/shuffles at runtime
        .config("spark.sql.adaptive.enabled", "true")

        # ✅ Robustness: Disable ANSI mode so malformed date casts return NULL instead of crashing
        .config("spark.sql.ansi.enabled", "false")
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    return spark


# ---------------------------------------------------------------------------
# Cleaning helpers
# ---------------------------------------------------------------------------

_TIMESTAMP_KEYWORDS = ("start", "stop")   # date cols handled separately (YEAR_COLS)
# Using refined keywords for UUID/ID detection


def clean_column_names(df):
    """Lowercase column names and replace special characters with underscores."""
    for c in df.columns:
        new_name = (
            c.strip()
            .lower()
            .replace(" ", "_")
            .replace("-", "_")
            .replace(".", "_")
        )
        if new_name != c:
            df = df.withColumnRenamed(c, new_name)
    return df


def convert_dates(df):
    """
    Cast timestamp columns (start/stop/date).
    Skips YEAR_COLS — those are handled separately in mask_phi.
    Skips UUID columns.
    """
    skip = YEAR_COLS | REDACT_COLS
    for c in df.columns:
        is_ts = any(kw == c for kw in _TIMESTAMP_KEYWORDS) or (
            "date" in c and c not in skip
        )
        is_uuid = any(kw in c for kw in _UUID_KEYWORDS)
        if is_ts and not is_uuid:
            df = df.withColumn(c, to_timestamp(col(c)))
    return df


# ---------------------------------------------------------------------------
# PHI Masking
# ---------------------------------------------------------------------------

def mask_phi(df):
    """
    Apply PHI masking rules to a DataFrame.

    - HASH_COLS   → salted SHA-256 hex string (preserves join-ability across tables)
    - REDACT_COLS → NULL              (direct identifiers, no analytical value)
    - YEAR_COLS   → integer year only (preserves age-band analysis)
    - everything else → unchanged
    """
    masked_cols = []

    for c in df.columns:
        if c in HASH_COLS:
            # Deterministic hash: SHA-256(salt | value)
            # Nulls hash to a fixed sentinel so joins don't break on null keys.
            hashed = sha2(concat_ws("|", lit(HASH_SALT), col(c).cast("string")), 256)
            masked_cols.append(hashed.alias(c))

        elif c in REDACT_COLS:
            masked_cols.append(lit(None).cast("string").alias(c))

        elif c in YEAR_COLS:
            # Keep year as integer for age-band queries; drop month/day
            masked_cols.append(year(to_timestamp(col(c))).alias(c))

        else:
            masked_cols.append(col(c))

    return df.select(masked_cols)


# ---------------------------------------------------------------------------
# Data quality
# ---------------------------------------------------------------------------

def compute_null_stats(df):
    """Return {column_name: null_pct} for every column."""
    null_exprs = []
    for c in df.columns:
        dtype = dict(df.dtypes)[c]
        if dtype in ("float", "double"):
            expr = (count(when(col(c).isNull() | isnan(col(c)), c)) / count("*") * 100).alias(c)
        else:
            expr = (count(when(col(c).isNull(), c)) / count("*") * 100).alias(c)
        null_exprs.append(expr)
    row = df.select(null_exprs).collect()[0]
    return {c: round(float(row[c]), 2) for c in df.columns}


# ---------------------------------------------------------------------------
# Schema export (LLM SQL context)
# ---------------------------------------------------------------------------

def build_schema_entry(table_name, df, delta_path, null_stats, row_count):
    """
    Build a schema dict for LLM consumption.

    Paste the contents of the exported schema.json into your LLM system prompt:

        The following Delta tables are available. Use exact table and column names.
        <schema>
        { ...schema.json contents... }
        </schema>
        Write SQL to answer: <user question>
    """
    type_map = dict(df.dtypes)
    columns = [
        {
            "name": c,
            "type": type_map[c],
            "nullable": True,
            "null_pct": null_stats.get(c, 0.0),
            "phi_masked": c in (HASH_COLS | REDACT_COLS | YEAR_COLS),
        }
        for c in df.columns
    ]
    return {
        "table": table_name,
        "delta_path": delta_path,
        "row_count": row_count,
        "columns": columns,
    }


# ---------------------------------------------------------------------------
# Core per-table workflow
# ---------------------------------------------------------------------------

def process_single_csv(spark, csv_path, delta_base_dir):
    """
    Full pipeline for one CSV file.

    Steps:
      1. Load CSV
      2. Clean column names + cast timestamps
      3. Mask PHI
      4. Write Delta table  (NO rows removed — full row count preserved)
      5. Register in Spark catalog
      6. Validate + null stats

    Returns (report, df, null_stats) on success, or report dict on failure.
    """
    filename = os.path.basename(csv_path)
    table_name = filename.replace(".csv", "").lower()
    delta_path = os.path.join(delta_base_dir, table_name)

    report = {
        "table": table_name,
        "source_csv": csv_path,
        "status": "failed",
        "row_count": 0,
        "columns": 0,
        "phi_cols_masked": [],
        "error": "",
    }

    print(f"\n{'='*60}")
    print(f"Processing: {table_name}")
    print(f"{'='*60}")

    # ------------------------------------------------------------------
    # Step 1 – Load
    # ------------------------------------------------------------------
    print("[1/6] Loading CSV ...")
    try:
        # samplingRatio=0.1 limits schema inference to 10% of rows — avoids
        # reading the full file just to decide column types.
        df = spark.read.csv(csv_path, header=True, inferSchema=True, samplingRatio=0.1)
    except Exception as exc:
        report["error"] = str(exc)
        print(f"      FAILED: {exc}")
        return report

    row_count = df.count()
    report["row_count"] = row_count
    print(f"      Rows: {row_count:,}  |  Columns: {len(df.columns)}")

    # ------------------------------------------------------------------
    # Step 2 – Clean column names + cast timestamps
    # ------------------------------------------------------------------
    print("[2/6] Cleaning column names and casting timestamps ...")
    df = clean_column_names(df)
    df = convert_dates(df)

    # ------------------------------------------------------------------
    # Step 3 – Mask PHI
    # ------------------------------------------------------------------
    print("[3/6] Masking PHI ...")
    phi_in_table = {
        "hashed":   sorted(HASH_COLS   & set(df.columns)),
        "redacted": sorted(REDACT_COLS & set(df.columns)),
        "year_only": sorted(YEAR_COLS  & set(df.columns)),
    }
    for strategy, cols in phi_in_table.items():
        if cols:
            print(f"      {strategy}: {cols}")

    report["phi_cols_masked"] = phi_in_table
    df = mask_phi(df)

    # ------------------------------------------------------------------
    # Step 4 – Write Delta  (no rows removed)
    # ------------------------------------------------------------------
    print(f"[4/6] Writing Delta table → {delta_path} ...")
    try:
        df.write.format("delta").mode("overwrite").save(delta_path)
    except Exception as exc:
        report["error"] = str(exc)
        print(f"      FAILED: {exc}")
        return report

    # ------------------------------------------------------------------
    # Step 5 – Register in Spark catalog
    # ------------------------------------------------------------------
    print(f"[5/6] Registering '{table_name}' in Spark catalog ...")
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    spark.sql(
        f"CREATE TABLE {table_name} USING DELTA LOCATION '{os.path.abspath(delta_path)}'"
    )

    # ------------------------------------------------------------------
    # Step 6 – Validate
    # ------------------------------------------------------------------
    print("[6/6] Validating ...")
    null_stats = compute_null_stats(df)
    high_null = {c: pct for c, pct in null_stats.items() if pct > 50}
    if high_null:
        print(f"      ⚠  High-null columns (>50%): {high_null}")

    final_count = spark.sql(f"SELECT COUNT(*) FROM {table_name}").collect()[0][0]
    assert final_count == row_count, (
        f"Row count mismatch! source={row_count}, delta={final_count}"
    )
    print(f"      ✓ Row count verified: {final_count:,}")

    print("      Sample (5 rows):")
    spark.sql(f"SELECT * FROM {table_name} LIMIT 5").show(truncate=False)

    report["status"] = "ok"
    report["columns"] = len(df.columns)
    report["null_stats"] = null_stats

    # Free cached RDDs / broadcast vars before moving to the next table
    spark.catalog.clearCache()

    return report, df, null_stats


# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------

def save_schema_json(schema_entries, output_path):
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(schema_entries, f, indent=2)
    print(f"\nSchema written → {output_path}")


def save_run_report(reports, output_path):
    if not reports:
        return
    fieldnames = ["table", "status", "row_count", "columns", "phi_cols_masked", "source_csv", "error"]
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv_module.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(reports)
    print(f"Run report written → {output_path}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    csv_dir = "data/csv"
    delta_dir = "delta_tables"
    schema_path = os.path.join(delta_dir, "schema.json")
    report_path = os.path.join(
        delta_dir,
        f"run_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
    )

    if not os.path.exists(csv_dir):
        print(f"Error: CSV directory '{csv_dir}' does not exist.")
        return

    os.makedirs(delta_dir, exist_ok=True)

    csv_files = sorted(glob.glob(os.path.join(csv_dir, "*.csv")))
    if not csv_files:
        print(f"No CSV files found in {csv_dir}.")
        return

    print(f"Found {len(csv_files)} CSV file(s).")
    print("Initialising Spark Session ...")
    spark = create_spark_session()

    all_reports = []
    schema_entries = []

    for csv_file in csv_files:
        result = process_single_csv(spark, csv_file, delta_dir)

        # Success → tuple(report, df, null_stats); failure → plain report dict
        if isinstance(result, tuple):
            report, df, null_stats = result
        else:
            report = result
            df, null_stats = None, {}

        all_reports.append(report)

        if report["status"] == "ok":
            table_name = report["table"]
            delta_path = os.path.join(delta_dir, table_name)
            entry = build_schema_entry(
                table_name=table_name,
                df=df,
                delta_path=delta_path,
                null_stats=null_stats,
                row_count=report["row_count"],
            )
            schema_entries.append(entry)

    save_schema_json(schema_entries, schema_path)
    save_run_report(all_reports, report_path)

    ok = sum(1 for r in all_reports if r["status"] == "ok")
    failed = len(all_reports) - ok
    print(f"\n{'='*60}")
    print(f"Done. {ok} succeeded, {failed} failed.")
    print(
        f"PHI salt: {'(from PHI_HASH_SALT env var)' if 'PHI_HASH_SALT' in os.environ else '(default — set PHI_HASH_SALT env var in production!)'}"
    )
    print(f"{'='*60}")

    spark.stop()


if __name__ == "__main__":
    main()