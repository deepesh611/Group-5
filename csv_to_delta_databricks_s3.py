# ================================
# CONFIG
# ================================
bucket = "s3a://deep-group5-project-bucket"

BASE_PATH = f"{bucket}/data/csv/"
CATALOG = "`project-5`"
SCHEMA = "delta_tables"

# Set context
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

# ================================
# IMPORTS
# ================================
from pyspark.sql.functions import col, to_timestamp, count, when, isnan, expr

# ================================
# HELPERS
# ================================
def clean_column_names(df):
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
    for c in df.columns:
        if "date" in c or c in ["start", "stop"]:
            df = df.withColumn(c, expr(f"try_cast(`{c}` as timestamp)"))
    return df


def compute_null_stats(df):
    exprs = []
    for c in df.columns:
        dtype = dict(df.dtypes)[c]
        if dtype in ("float", "double"):
            expr = (count(when(col(c).isNull() | isnan(col(c)), c)) / count("*") * 100).alias(c)
        else:
            expr = (count(when(col(c).isNull(), c)) / count("*") * 100).alias(c)
        exprs.append(expr)

    row = df.select(exprs).collect()[0]
    return {c: round(float(row[c]), 2) for c in df.columns}


# ================================
# MAIN PIPELINE
# ================================
files = dbutils.fs.ls(BASE_PATH)

for file in files:
    if not file.name.endswith(".csv"):
        continue

    table_name = file.name.replace(".csv", "").lower()
    file_path = BASE_PATH + file.name

    print(f"\n{'='*50}")
    print(f"Processing: {table_name}")
    print(f"{'='*50}")

    # 1. Load CSV
    df = spark.read.csv(
        file_path,
        header=True,
        inferSchema=True
    )

    row_count = df.count()
    print(f"Rows: {row_count}, Columns: {len(df.columns)}")

    # 2. Clean + convert
    df = clean_column_names(df)
    df = convert_dates(df)

    # 3. Write to Delta (NO MASKING)
    full_table = f"{CATALOG}.{SCHEMA}.{table_name}"

    df.write \
      .format("delta") \
      .mode("overwrite") \
      .saveAsTable(full_table)

    # 4. Validate
    final_count = spark.sql(
        f"SELECT COUNT(*) FROM {full_table}"
    ).collect()[0][0]

    print(f"✓ Written: {final_count} rows")

    # 5. Null stats (optional)
    nulls = compute_null_stats(df)
    high_null = {k: v for k, v in nulls.items() if v > 50}

    if high_null:
        print(f"⚠ High null columns: {high_null}")


print("\n✅ ALL FILES PROCESSED")