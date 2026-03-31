# Databricks notebook source
# 04_metadata_manager.py — ADLS Read/Write for master_schema.json
#
# Manages the single source of truth (master_schema.json) stored in ADLS
# via an abfss:// External Location — no mount point required.
# Provides in-memory caching, timestamped backup-before-write,
# section-level accessors, and helpers used by Pipeline 3 (AI Advisor).
#
# Depends on: 00_config
#
# dbutils.fs.head() and dbutils.fs.put() both work natively on abfss:// URIs
# when the cluster has access via the Access Connector (Managed Identity).

# COMMAND ----------

import json
from datetime import datetime

# ─── In-Memory Cache
_schema_cache    = None
_cache_timestamp = None

# COMMAND ----------

# ─── Core Read / Write

def load_master_schema(force_reload: bool = False) -> dict:
    """
    Load master_schema.json from ADLS (abfss:// external location).
    Caches in memory for SCHEMA_CACHE_TTL_SECONDS to avoid repeated I/O.

    Parameters:
        force_reload: If True, bypass the cache and re-read from ADLS.
    """
    global _schema_cache, _cache_timestamp

    now = datetime.now()
    if (
        not force_reload
        and _schema_cache is not None
        and _cache_timestamp is not None
    ):
        elapsed = (now - _cache_timestamp).total_seconds()
        if elapsed < SCHEMA_CACHE_TTL_SECONDS:
            return _schema_cache

    try:
        # dbutils.fs.head reads the file as a UTF-8 string from abfss://
        raw = dbutils.fs.head(MASTER_SCHEMA_FILE, 2_097_152)  # 2 MB cap
        _schema_cache    = json.loads(raw)
        _cache_timestamp = now
        log.info(f"master_schema.json loaded from ADLS ({len(raw):,} bytes)")
        return _schema_cache
    except Exception as e:
        log.error(f"Failed to load master_schema.json from {MASTER_SCHEMA_FILE}: {e}")
        raise RuntimeError(
            f"Cannot load master_schema.json — verify External Location access "
            f"and that the file exists at: {MASTER_SCHEMA_FILE}"
        ) from e


def save_master_schema(schema_data: dict, create_backup: bool = True) -> None:
    """
    Write master_schema.json back to ADLS.
    Creates a timestamped backup of the previous version before overwriting.

    Parameters:
        schema_data:    The full schema dict to write.
        create_backup:  Set False for minor updates (e.g. row count refresh).
    """
    global _schema_cache, _cache_timestamp

    # ── Backup current version ────────────────────────────────────────────
    if create_backup:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = f"{METADATA_PATH}/backups/master_schema_{ts}.json"
        try:
            current_raw = dbutils.fs.head(MASTER_SCHEMA_FILE, 2_097_152)
            dbutils.fs.put(backup_path, current_raw, overwrite=True)
            log.info(f"Backup created at: {backup_path}")
        except Exception:
            log.warning("No existing master_schema.json to backup (first write?)")

    # ── Stamp last_updated ────────────────────────────────────────────────
    if "_metadata" in schema_data:
        schema_data["_metadata"]["last_updated"] = datetime.now().strftime("%Y-%m-%d")

    # ── Write to ADLS ─────────────────────────────────────────────────────
    json_str = json.dumps(schema_data, indent=2, ensure_ascii=False)
    dbutils.fs.put(MASTER_SCHEMA_FILE, json_str, overwrite=True)

    # Update cache
    _schema_cache    = schema_data
    _cache_timestamp = datetime.now()
    log.info(f"master_schema.json saved to ADLS ({len(json_str):,} bytes)")


def invalidate_cache() -> None:
    """Force next load_master_schema() call to re-read from ADLS."""
    global _schema_cache, _cache_timestamp
    _schema_cache    = None
    _cache_timestamp = None
    log.info("Schema cache invalidated — next call will re-read from ADLS")

# COMMAND ----------

# ─── Section-Level Accessors

def get_table_metadata(table_name: str) -> dict:
    """Return the full metadata block for a single table from master_schema.json."""
    schema = load_master_schema()
    tables = schema.get("tables", {})
    if table_name not in tables:
        raise ValueError(
            f"Table '{table_name}' not found in master_schema.json. "
            f"Available tables: {sorted(tables.keys())}"
        )
    return tables[table_name]


def get_all_table_names() -> list:
    """Return all table names defined in master_schema.json."""
    schema = load_master_schema()
    return list(schema.get("tables", {}).keys())


def get_table_columns(table_name: str) -> dict:
    """Return {col_name: col_definition} for a table."""
    return get_table_metadata(table_name).get("columns", {})


def get_few_shot_examples() -> list:
    """Return the few-shot SQL examples list used by Pipeline 2."""
    schema = load_master_schema()
    return schema.get("few_shot_examples", [])


def get_join_paths() -> list:
    """Return the canonical join path definitions used by the SQL prompt builder."""
    schema = load_master_schema()
    return schema.get("relationships", {}).get("paths", [])


def get_phi_masking_rules() -> dict:
    """Return the phi_masking_rules block (role configs + strategy definitions)."""
    schema = load_master_schema()
    return schema.get("phi_masking_rules", {})


def get_drift_handling_config() -> dict:
    """Return the drift_handling block (response format instructions for AI Advisor)."""
    schema = load_master_schema()
    return schema.get("drift_handling", {})

# COMMAND ----------

# ─── Schema Update Helpers (used by Pipeline 3 — AI Advisor)

def update_table_columns(table_name: str, column_updates: dict) -> None:
    """
    Merge new or updated column definitions into a table's columns block.
    Creates a backup before writing. Invalidates the in-memory cache.

    Parameters:
        table_name:      e.g. "patients"
        column_updates:  {col_name: {type, phi, phi_type, masking, description}}
    """
    schema = load_master_schema(force_reload=True)

    if table_name not in schema.get("tables", {}):
        raise ValueError(f"Table '{table_name}' not found in master_schema.json")

    for col_name, col_def in column_updates.items():
        schema["tables"][table_name]["columns"][col_name] = col_def

    save_master_schema(schema)
    log.info(
        f"Updated {len(column_updates)} column(s) in '{table_name}': "
        f"{sorted(column_updates.keys())}"
    )


def add_table_to_schema(table_name: str, table_def: dict) -> None:
    """
    Add a new table definition to master_schema.json.
    Used when a previously unseen CSV arrives in the raw/ directory.
    """
    schema = load_master_schema(force_reload=True)

    if table_name in schema.get("tables", {}):
        log.warning(f"Table '{table_name}' already exists in schema — overwriting definition")

    schema["tables"][table_name] = table_def
    save_master_schema(schema)
    log.info(f"Added new table '{table_name}' to master_schema.json")


def update_row_count(table_name: str, row_count: int) -> None:
    """Update the approximate row count for a table after a successful ingestion."""
    schema = load_master_schema(force_reload=True)
    if table_name in schema.get("tables", {}):
        schema["tables"][table_name]["row_count_approx"] = row_count
        save_master_schema(schema, create_backup=False)
        log.info(f"Row count updated: {table_name} = {row_count:,}")


def log_drift_event(table_name: str, drift_report: dict) -> None:
    """
    Persist a drift event to ADLS /logs/drift/ for auditing.
    One JSON file per event, timestamped, never overwritten.
    """
    ts       = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = f"{LOGS_PATH}/drift/{table_name}_drift_{ts}.json"
    payload  = {
        "timestamp":      datetime.now().isoformat(),
        "table":          table_name,
        "adls_log_path":  log_path,
        "drift":          drift_report,
    }
    dbutils.fs.put(log_path, json.dumps(payload, indent=2), overwrite=True)
    log.info(f"Drift event logged: {log_path}")


def log_ingestion_run(table_name: str, run_stats: dict) -> None:
    """
    Persist a pipeline run summary to ADLS /logs/ingestion/ for auditing.
    Useful for tracking row counts and drift status over time.
    """
    ts       = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = f"{LOGS_PATH}/ingestion/{table_name}_run_{ts}.json"
    payload  = {
        "timestamp": datetime.now().isoformat(),
        "table":     table_name,
        **run_stats,
    }
    dbutils.fs.put(log_path, json.dumps(payload, indent=2), overwrite=True)
    log.info(f"Ingestion run logged: {log_path}")
