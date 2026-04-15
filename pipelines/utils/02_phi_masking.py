# Databricks notebook source
# 02_phi_masking.py — Post-Query PHI Masking by Role
#
# Applies ACCESS-LEVEL (not storage-level) masking to Spark DataFrames that
# are query results from Pipeline 2.  Data is stored RAW in Delta tables.
# Masking is applied AFTER SQL execution, based on the requesting user's role.
#
# Masking strategies (defined in master_schema.json phi_masking_rules):
#   REDACT         → replace with '***REDACTED***'
#   FPE_UUID       → deterministic, format-preserving UUID pseudonymisation
#                    (SHA-256 based — joins remain valid after masking)
#   FPE_DATE_SHIFT → shift date by a patient-specific ±365-day offset
#                    (temporal relationships between dates preserved per patient)
#   GENERALIZE     → reduce geographic granularity (city → state)
#   TRUNCATE_3DIG  → keep first 3 characters (ZIP codes: 02134 → 021)
#   ROUND_2DP      → round to 2 decimal places (lat/lon coordinates)
#
# Roles:
#   doctor    → TARGETED_ONLY: DIRECT_IDENTIFIERs visible for ≤10-row results
#   analyst   → NONE: all DIRECT_IDENTIFIER + QUASI_IDENTIFIER masked
#   sysadmin  → NONE: same as analyst (structural access, no clinical PHI)
#
# Depends on: 00_config, 04_metadata_manager

# COMMAND ----------

import hashlib
from datetime import timedelta

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, lit, udf, when, substring, length,
    round as spark_round,
)
from pyspark.sql.types import StringType, TimestampType, DateType

# COMMAND ----------

# ─── PHI Column Lookup (Built from master_schema.json)

def build_phi_lookup() -> dict:
    """
    Scan ALL tables in master_schema.json and build a flat {col_name: info} map
    for every column flagged phi=true.

    Many columns appear in multiple tables (e.g. 'patient', 'encounter', 'start').
    When a column name appears in multiple tables we merge the table list.
    The phi_type and masking strategy are table-consistent for shared columns.
    """
    schema = load_master_schema()
    lookup = {}

    for table_name, table_data in schema.get("tables", {}).items():
        for col_name, col_def in table_data.get("columns", {}).items():
            if not col_def.get("phi", False):
                continue
            if col_name in lookup:
                lookup[col_name]["tables"].append(table_name)
            else:
                lookup[col_name] = {
                    "phi_type":  col_def.get("phi_type"),
                    "masking":   col_def.get("masking"),
                    "data_type": col_def.get("type", "string"),
                    "tables":    [table_name],
                }
    return lookup


def get_role_masking_config(role: str) -> dict:
    """
    Return the masking config for a given role from master_schema.json.
    Falls back to DEFAULT_ROLE (analyst) if the role is unrecognized.
    """
    rules = get_phi_masking_rules()
    roles = rules.get("roles", {})
    if role not in roles:
        log.warning(f"Unknown role '{role}' — defaulting to '{DEFAULT_ROLE}'")
        role = DEFAULT_ROLE
    return roles[role]

# COMMAND ----------

# ─── FPE Key Management

_fpe_key_cache = None

def _get_fpe_key() -> str:
    """
    Load FPE pseudonymisation key from Databricks secret scope.
    Falls back to a deterministic dev key with a clear warning.

    Production: add secret 'fpe-encryption-key' to your llm-scope.
    """
    global _fpe_key_cache
    if _fpe_key_cache is not None:
        return _fpe_key_cache
    try:
        _fpe_key_cache = get_secret("fpe-encryption-key")
        log.info("FPE key loaded from secret scope")
    except Exception:
        _fpe_key_cache = hashlib.sha256(b"project5_dev_fpe_key_not_for_prod").hexdigest()[:32]
        log.warning(
            "⚠️  FPE key 'fpe-encryption-key' not found in '%s' scope. "
            "Using deterministic dev fallback — NOT safe for production PHI data. "
            "Add the secret before deploying.", SECRET_SCOPE
        )
    return _fpe_key_cache

# COMMAND ----------

# ─── Python-Level Masking Primitives

def _fpe_encrypt_uuid(uuid_val: str) -> str:
    """
    Deterministic UUID pseudonymisation.
    SHA-256(key‖uuid) → first 32 hex chars, formatted as a valid UUID v4 shape.
    Same input always produces the same output → joins work after masking.
    """
    if uuid_val is None:
        return None
    key = _get_fpe_key()
    h = hashlib.sha256(f"{key}:{uuid_val}".encode()).hexdigest()
    return f"{h[0:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:32]}"


def _patient_date_offset_days(patient_id: str) -> int:
    """
    Derive a deterministic day offset in [−365, +365] from a patient UUID.
    The same patient always gets the same shift, so relative dates (e.g.
    encounter start → discharge) remain consistent after masking.
    """
    key = _get_fpe_key()
    h = hashlib.sha256(f"{key}:dateshift:{patient_id}".encode()).hexdigest()
    return (int(h[:4], 16) % 731) - 365    # 731 values → [−365, +365]


def _generalize_location(value: str) -> str:
    """
    Reduce geographic granularity: 'Springfield, MA, US' → 'MA'.
    If only one part (e.g. a bare city name) returns 'GENERALIZED'.
    """
    if value is None:
        return None
    parts = [p.strip() for p in str(value).split(",")]
    return parts[-2] if len(parts) >= 2 else "GENERALIZED"

# COMMAND ----------

# ─── Spark UDFs (registered once at %run time)

_udf_fpe_uuid   = udf(_fpe_encrypt_uuid, StringType())
_udf_generalize = udf(_generalize_location, StringType())


def _make_date_shift_udf(return_type):
    """
    Factory for date-shift UDFs that accept (date_val, patient_id_val).
    Two variants: one for DateType, one for TimestampType.
    """
    def _shift(date_val, patient_id):
        if date_val is None or patient_id is None:
            return None
        try:
            offset = _patient_date_offset_days(str(patient_id))
            return date_val + timedelta(days=offset)
        except Exception:
            return None
    return udf(_shift, return_type)


_udf_date_shift_date = _make_date_shift_udf(DateType())
_udf_date_shift_ts   = _make_date_shift_udf(TimestampType())

# COMMAND ----------

# ─── Single-Column Strategy Dispatcher

def _apply_strategy(
    df: DataFrame,
    col_name: str,
    strategy: str,
    data_type: str = "string",
    patient_col: str = None,
) -> DataFrame:
    """Apply one masking strategy to one column. Returns the modified DataFrame."""

    if strategy == "REDACT":
        return df.withColumn(col_name, lit("***REDACTED***"))

    if strategy == "FPE_UUID":
        return df.withColumn(col_name, _udf_fpe_uuid(col(col_name).cast("string")))

    if strategy == "FPE_DATE_SHIFT":
        if patient_col and patient_col in df.columns:
            shift_udf = (
                _udf_date_shift_date if data_type == "date" else _udf_date_shift_ts
            )
            return df.withColumn(
                col_name, shift_udf(col(col_name), col(patient_col).cast("string"))
            )
        # No patient column in result → cannot compute deterministic shift → redact
        log.warning(
            f"FPE_DATE_SHIFT on '{col_name}': patient column '{patient_col}' not "
            f"in result — redacting instead."
        )
        return df.withColumn(col_name, lit("***DATE_REDACTED***"))

    if strategy == "GENERALIZE":
        return df.withColumn(col_name, _udf_generalize(col(col_name).cast("string")))

    if strategy == "TRUNCATE_3DIG":
        return df.withColumn(
            col_name,
            when(
                col(col_name).isNotNull() & (length(col(col_name).cast("string")) >= 3),
                substring(col(col_name).cast("string"), 1, 3),
            ).otherwise(lit(None).cast("string")),
        )

    if strategy == "ROUND_2DP":
        return df.withColumn(
            col_name, spark_round(col(col_name).cast("double"), 2)
        )

    log.warning(
        f"Unknown masking strategy '{strategy}' for '{col_name}' — defaulting to REDACT"
    )
    return df.withColumn(col_name, lit("***REDACTED***"))

# COMMAND ----------

# ─── Main Entry Point

def apply_phi_masking(
    df: DataFrame,
    role: str,
    patient_col: str = "patient",
) -> DataFrame:
    """
    Apply role-based PHI masking to a query result DataFrame.

    Parameters
    ----------
    df          : Spark DataFrame returned by spark.sql() in Pipeline 2.
    role        : One of 'doctor', 'analyst', 'sysadmin'.
    patient_col : Column containing patient FK (needed for FPE_DATE_SHIFT).
                  Defaults to 'patient' (standard Synthea FK column name).

    Returns
    -------
    DataFrame with PHI columns masked per the role's policy.
    """
    if role not in VALID_ROLES:
        log.warning(f"Invalid role '{role}' — defaulting to '{DEFAULT_ROLE}'")
        role = DEFAULT_ROLE

    role_config    = get_role_masking_config(role)
    masked_phi_set = set(role_config.get("masked_phi_types", []))
    phi_access     = role_config.get("phi_access", "NONE")
    phi_lookup     = build_phi_lookup()

    # ── Doctor targeted-query exemption ──────────────────────────────────
    # Use .limit(THRESHOLD + 1).count() instead of .count() — avoids a full
    # scan of potentially millions of rows just to decide masking policy.
    is_targeted = False
    if phi_access == "TARGETED_ONLY":
        probe_count = df.limit(DOCTOR_TARGETED_ROW_THRESHOLD + 1).count()
        is_targeted = probe_count <= DOCTOR_TARGETED_ROW_THRESHOLD
        log.info(
            f"Doctor query probe: {probe_count} row(s) ≤ {DOCTOR_TARGETED_ROW_THRESHOLD} "
            f"→ targeted={'YES' if is_targeted else 'NO'}"
        )

    result_df = df

    for col_name in df.columns:
        if col_name not in phi_lookup:
            continue

        info     = phi_lookup[col_name]
        phi_type = info["phi_type"]
        strategy = info["masking"]

        if strategy is None:
            continue  # PHI flagged but no masking strategy defined — skip safely

        if phi_type not in masked_phi_set:
            continue  # this phi_type is allowed for this role

        if is_targeted and phi_type == "DIRECT_IDENTIFIER":
            continue  # doctor exemption: show direct IDs in targeted queries

        result_df = _apply_strategy(
            result_df,
            col_name,
            strategy,
            data_type=info.get("data_type", "string"),
            patient_col=patient_col if patient_col in result_df.columns else None,
        )

    return result_df

# COMMAND ----------

# ─── Audit / Transparency Helpers

def get_masking_summary(df: DataFrame, role: str) -> list:
    """
    Return a list of dicts describing what masking will be applied to each
    PHI column in the result, given the user's role.  Shown before results.
    """
    phi_lookup  = build_phi_lookup()
    role_config = get_role_masking_config(role)
    masked_set  = set(role_config.get("masked_phi_types", []))

    summary = []
    for col_name in df.columns:
        if col_name not in phi_lookup:
            continue
        info     = phi_lookup[col_name]
        phi_type = info["phi_type"]
        will_mask = phi_type in masked_set and info["masking"] is not None
        summary.append({
            "column":   col_name,
            "phi_type": phi_type,
            "strategy": info["masking"],
            "action":   "MASKED" if will_mask else "VISIBLE (allowed for role)",
        })
    return summary


def print_masking_report(df: DataFrame, role: str) -> None:
    """Pretty-print masking summary to notebook cell output."""
    summary = get_masking_summary(df, role)
    if not summary:
        print(f"ℹ️  No PHI columns in query result — nothing to mask for role '{role}'.")
        return

    print(f"\n🔒 PHI Masking Report — Role: {role.upper()}")
    print("─" * 65)
    for s in summary:
        icon = "🚫" if s["action"] == "MASKED" else "✅"
        print(f"  {icon} {s['column']:25s} {s['phi_type']:25s} → {s['action']}")
    print("─" * 65)
