# Databricks notebook source
# DBTITLE 1,Install required packages
# MAGIC %pip install langchain-openai openai tiktoken -q

# COMMAND ----------

# 00_config.py — Central Configuration
# Single source of truth for all constants, paths, and helper functions.
# Every pipeline and utility notebook runs this first via: %run ./utils/00_config
#
# STORAGE APPROACH: Azure Databricks (Unity Catalog) with External Location.
# No dbutils.fs.mount() — access ADLS Gen2 directly via abfss:// URIs.
# The Access Connector for Azure Databricks (Managed Identity) grants access
# at the External Location level — no storage keys needed in code.

# COMMAND ----------

# ─── Unity Catalog
CATALOG     = "project5"
SCHEMA      = "delta_tables"
FULL_SCHEMA = f"{CATALOG}.{SCHEMA}"

# ─── ADLS Gen2 External Location (abfss:// — no mounting)
# Fill these in to match your Azure storage account and container.
# These are used only for file I/O (master_schema.json, logs).
# Delta tables are referenced via Unity Catalog (CATALOG.SCHEMA.table) only.
ADLS_ACCOUNT_NAME = "project5data"           # Your storage account name
ADLS_CONTAINER    = "data"              # Your container name
ADLS_BASE         = f"abfss://{ADLS_CONTAINER}@{ADLS_ACCOUNT_NAME}.dfs.core.windows.net"

RAW_PATH           = f"{ADLS_BASE}/raw"
METADATA_PATH      = f"{ADLS_BASE}/metadata"
LOGS_PATH          = f"{ADLS_BASE}/logs"
MASTER_SCHEMA_FILE = f"{METADATA_PATH}/master_schema.json"

# ─── Secret Scope
SECRET_SCOPE      = "llm-scope"
OPENAI_SECRET_KEY = "openai-key"

# ─── OpenAI
OPENAI_MODEL        = "gpt-4o"
OPENAI_TEMPERATURE  = 0.0
OPENAI_MAX_TOKENS   = 4096
MAX_RETRIES         = 3
RETRY_DELAY_SECONDS = 2     # base delay — doubles on each retry (exponential backoff)

# ─── Synthea Tables (canonical ingestion order — patients first as FK anchor)
SYNTHEA_TABLES = [
    "patients",
    "encounters",
    "conditions",
    "medications",
    "procedures",
    "allergies",
    "immunizations",
    "observations",
    "careplans",
]

# ─── Role-Based Access
VALID_ROLES  = {"doctor", "analyst", "sysadmin"}
DEFAULT_ROLE = "analyst"    # most restrictive by default

# ─── SQL Safety Guardrails
BLOCKED_SQL_PATTERNS = [
    "DROP", "DELETE", "UPDATE", "INSERT", "ALTER", "TRUNCATE",
    "CREATE", "GRANT", "REVOKE", "MERGE",
]
MAX_QUERY_ROWS = 10_000     # hard cap on SELECT results returned to users

# ─── Delta Write Options
MERGE_SCHEMA_FALLBACK = True    # use mergeSchema if AI fix fails
DEFAULT_WRITE_MODE    = "overwrite"

# ─── Schema Cache TTL
SCHEMA_CACHE_TTL_SECONDS = 300  # 5 minutes — avoids repeated ADLS I/O per session

# ─── Doctor PHI Targeted-Query Threshold
DOCTOR_TARGETED_ROW_THRESHOLD = 10  # ≤ N rows → treat as individual patient query

# COMMAND ----------

import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
)
log = logging.getLogger("project5")

# COMMAND ----------

# ─── Helper Functions

def get_full_table_name(table: str) -> str:
    """Return fully qualified Unity Catalog table name: project_5.delta_tables.<table>"""
    return f"{CATALOG}.{SCHEMA}.{table}"


def init_catalog():
    """
    Set the active catalog and schema for this Spark session.
    Call once at the top of each pipeline notebook.
    """
    spark.sql(f"USE CATALOG {CATALOG}")
    spark.sql(f"USE SCHEMA {SCHEMA}")
    log.info(f"Catalog context set to {FULL_SCHEMA}")


def get_secret(key: str) -> str:
    """Retrieve a secret from the Databricks secret scope (llm-scope)."""
    return dbutils.secrets.get(scope=SECRET_SCOPE, key=key)


def table_exists(table_name: str) -> bool:
    """Check if a Delta table exists in the Unity Catalog."""
    full_name = get_full_table_name(table_name)
    try:
        spark.sql(f"DESCRIBE TABLE {full_name}")
        return True
    except Exception:
        return False


def list_raw_csvs() -> list:
    """
    List all CSV FileInfo objects in the ADLS raw/ External Location.
    Returns a list of dbutils FileInfo items (use .path and .name).
    """
    try:
        files = dbutils.fs.ls(RAW_PATH)
        return [f for f in files if f.name.lower().endswith(".csv")]
    except Exception as e:
        log.error(f"Cannot list {RAW_PATH}: {e}")
        return []


def adls_file_exists(path: str) -> bool:
    """Check if a file exists at an abfss:// path."""
    try:
        dbutils.fs.ls(path)
        return True
    except Exception:
        return False


def ensure_adls_dirs():
    """
    Verify that the required External Location sub-paths are reachable.
    Called once during cluster init or first pipeline run.
    Checks all paths that pipelines read from or write to.
    """
    paths_to_check = [
        RAW_PATH,                       # Pipeline 1: CSV source
        METADATA_PATH,                  # All pipelines: master_schema.json
        LOGS_PATH,                      # All pipelines: log root
        f"{LOGS_PATH}/ingestion",       # Pipeline 1: run audit logs
        f"{LOGS_PATH}/drift",           # Pipeline 1 writes, Pipeline 3 reads
        f"{LOGS_PATH}/advisor",         # Pipeline 3: advisor event logs
    ]
    for path in paths_to_check:
        try:
            dbutils.fs.ls(path)
            log.info(f"✅ Reachable: {path}")
        except Exception:
            log.warning(
                f"⚠️  Cannot reach {path} — verify External Location and "
                "Access Connector IAM permissions (Storage Blob Data Contributor)."
            )
