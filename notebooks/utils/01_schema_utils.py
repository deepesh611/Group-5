# Databricks notebook source
# 01_schema_utils.py — Schema Drift Detection & Comparison
#
# Compares incoming CSV DataFrame schemas against master_schema.json to detect
# additive, subtractive, and type-change drift.  Returns structured drift
# reports that Pipeline 1 acts on (halt / warn / trigger Pipeline 3).
# Also provides column normalization and type casting used during ingestion.
#
# Depends on: 00_config, 04_metadata_manager

# COMMAND ----------

import json
from collections import OrderedDict

# ─── Spark↔Schema Type Compatibility Map
# Maps master_schema.json type names → the set of Spark simpleString() types
# that are considered compatible (no drift action needed).

_SCHEMA_TO_SPARK_COMPAT = {
    "string":    {"string"},
    "date":      {"date", "string"},           # CSV dates often inferred as string
    "timestamp": {"timestamp", "string"},      # same for timestamps
    "double":    {"double", "float", "decimal(38,18)", "decimal"},
    "long":      {"bigint", "long", "int"},
    "int":       {"int", "integer", "bigint", "long"},
    "boolean":   {"boolean", "string"},
}

def _norm_type(t: str) -> str:
    """Lower-case and normalize Spark type string aliases."""
    t = t.strip().lower()
    aliases = {"integer": "int", "bigint": "long", "float": "double"}
    return aliases.get(t, t)


def types_compatible(expected: str, incoming: str) -> bool:
    """
    Return True if the incoming Spark-inferred type is compatible with
    the expected type declared in master_schema.json.

    Compatible = no drift action needed (e.g. int↔bigint, date read as string).
    Incompatible = type-change drift event.
    """
    exp = _norm_type(expected)
    inc = _norm_type(incoming)
    if exp == inc:
        return True
    # Normalise decimal variants (e.g. decimal(10,2) → decimal)
    if inc.startswith("decimal"):
        inc = "decimal"
    return inc in _SCHEMA_TO_SPARK_COMPAT.get(exp, set())

# COMMAND ----------

# ─── Column Normalization & Type Casting  (used during ingestion)

def normalize_column_names(df):
    """
    Lowercase column names and replace spaces/hyphens/dots with underscores.
    Must be applied to every incoming CSV before drift detection or Delta write.
    """
    for c in df.columns:
        clean = (
            c.strip()
            .lower()
            .replace(" ", "_")
            .replace("-", "_")
            .replace(".", "_")
        )
        if clean != c:
            df = df.withColumnRenamed(c, clean)
    return df


def cast_date_columns(df, table_name: str):
    """
    Cast date/timestamp/numeric columns based on master_schema.json type
    declarations.  Applied after normalize_column_names() and before drift
    detection so Spark inferred types match what the schema declares.

    Only casts columns that are present in both the DataFrame and the schema;
    silently skips columns that exist in only one.

    SAFE CASTING STRATEGY:
    Uses try_to_date() and try_to_timestamp() (Databricks/Spark 3.4+ SQL functions)
    which return NULL for malformed input rather than raising CAST_INVALID_INPUT.
    This is critical because:
      • ANSI mode (default in DBR 13+) makes to_date() raise on invalid input
      • Healthcare data may have null/empty dates that would otherwise crash the run

    DATE: Synthea always uses YYYY-MM-DD.
    TIMESTAMP: Synthea uses ISO 8601 — we try all three common suffix variants:
        '2020-01-15T08:30:00Z'        (UTC with Z literal)
        '2020-01-15T08:30:00+00:00'   (UTC with numeric offset)
        '2020-01-15T08:30:00'         (no timezone suffix)
    """
    from pyspark.sql.functions import col, expr, coalesce

    expected = get_expected_columns(table_name)

    for col_name, meta in expected.items():
        if col_name not in df.columns:
            continue
        t = meta["type"]
        # Backtick-quote the column name in SQL expressions to handle any
        # special characters (hyphens, spaces) that survive normalisation.
        safe_name = f"`{col_name}`"
        try:
            if t == "date":
                # try_to_date returns NULL for malformed input; never raises.
                df = df.withColumn(
                    col_name,
                    expr(f"try_to_date({safe_name}, 'yyyy-MM-dd')"),
                )

            elif t == "timestamp":
                # Try all Synthea timestamp variants; coalesce picks first non-null.
                df = df.withColumn(
                    col_name,
                    coalesce(
                        expr(f"try_to_timestamp({safe_name}, 'yyyy-MM-dd\'T\'HH:mm:ssXXX')"),  # +00:00
                        expr(f"try_to_timestamp({safe_name}, 'yyyy-MM-dd\'T\'HH:mm:ss\'Z\'')"),  # Z
                        expr(f"try_to_timestamp({safe_name}, 'yyyy-MM-dd\'T\'HH:mm:ss')"),       # no tz
                        expr(f"try_to_timestamp({safe_name}, 'yyyy-MM-dd HH:mm:ss')"),            # space
                    ),
                )

            elif t == "double":
                df = df.withColumn(col_name, col(col_name).cast("double"))

            elif t in ("long", "int", "integer"):
                df = df.withColumn(col_name, col(col_name).cast("long"))

            elif t == "boolean":
                df = df.withColumn(col_name, col(col_name).cast("boolean"))

        except Exception as e:
            log.warning(f"Type cast plan failed for '{col_name}' → '{t}': {e}")

    return df

# COMMAND ----------

# ─── Expected Columns Accessor

def get_expected_columns(table_name: str) -> OrderedDict:
    """
    Pull the expected column definitions for a table from master_schema.json.

    Returns:
        OrderedDict {col_name: {type, phi, phi_type, masking, description}}
    """
    table_meta = get_table_metadata(table_name)
    cols = OrderedDict()
    for col_name, col_def in table_meta.get("columns", {}).items():
        cols[col_name] = {
            "type":        col_def.get("type", "string"),
            "phi":         col_def.get("phi", False),
            "phi_type":    col_def.get("phi_type"),
            "masking":     col_def.get("masking"),
            "description": col_def.get("description", ""),
        }
    return cols

# COMMAND ----------

# ─── Core Drift Detection

def detect_drift(table_name: str, incoming_df) -> dict:
    """
    Compare an incoming DataFrame's schema against master_schema.json.

    Parameters:
        table_name:   Synthea table name (e.g. "patients")
        incoming_df:  Spark DataFrame read from the raw CSV (after column normalize)

    Returns:
        Drift report dict with keys:
          table, has_drift, severity,
          new_columns, missing_columns, type_changes  (each a list of detail dicts)
    """
    expected       = get_expected_columns(table_name)
    expected_names = set(expected.keys())

    # Build incoming schema map (already lowercased by normalize_column_names)
    incoming_fields = {
        f.name: f.dataType.simpleString()
        for f in incoming_df.schema.fields
    }
    incoming_names = set(incoming_fields.keys())

    report = {
        "table":           table_name,
        "has_drift":       False,
        "severity":        "NONE",
        "new_columns":     [],
        "missing_columns": [],
        "type_changes":    [],
    }

    # Additive drift: columns in CSV but not in master_schema
    for col_name in sorted(incoming_names - expected_names):
        report["new_columns"].append({
            "column":        col_name,
            "incoming_type": incoming_fields[col_name],
            "severity":      "WARNING",   # could be PHI — Advisor will evaluate
        })

    # Subtractive drift: expected but missing from CSV
    for col_name in sorted(expected_names - incoming_names):
        report["missing_columns"].append({
            "column":        col_name,
            "expected_type": expected[col_name]["type"],
            "is_phi":        expected[col_name]["phi"],
            "severity":      "CRITICAL",  # always critical — data is missing
        })

    # Type-change drift: present in both but types incompatible
    for col_name in sorted(expected_names & incoming_names):
        exp_type = expected[col_name]["type"]
        inc_type = incoming_fields[col_name]
        if not types_compatible(exp_type, inc_type):
            is_phi = expected[col_name]["phi"]
            report["type_changes"].append({
                "column":        col_name,
                "expected_type": exp_type,
                "incoming_type": inc_type,
                "is_phi":        is_phi,
                "severity":      "CRITICAL" if is_phi else "WARNING",
            })

    report["has_drift"] = bool(
        report["new_columns"] or report["missing_columns"] or report["type_changes"]
    )
    if report["has_drift"]:
        report["severity"] = _classify_overall_severity(report)

    return report


def _classify_overall_severity(drift: dict) -> str:
    """
    Derive overall severity from individual drift items.

    CRITICAL — any missing column OR a type change on a PHI column
    WARNING  — new columns (possible PHI) or type changes on non-PHI columns
    INFO     — fallback (should not normally reach here if has_drift is True)
    """
    if drift["missing_columns"]:
        return "CRITICAL"
    for tc in drift["type_changes"]:
        if tc.get("is_phi", False):
            return "CRITICAL"
    if drift["new_columns"] or drift["type_changes"]:
        return "WARNING"
    return "INFO"

# COMMAND ----------

# ─── Human-Readable Summary

def generate_drift_summary(drift: dict) -> str:
    """
    Produce a console-printable drift summary.
    Pipeline 1 logs this before deciding whether to trigger Pipeline 3.
    """
    if not drift["has_drift"]:
        return f"✅ {drift['table']}: No schema drift detected."

    lines = [
        f"\n⚠️  DRIFT DETECTED — {drift['table']}  (Severity: {drift['severity']})",
        "─" * 60,
    ]

    if drift["new_columns"]:
        lines.append(f"  + New columns ({len(drift['new_columns'])}):")
        for c in drift["new_columns"]:
            lines.append(f"      {c['column']:35s} {c['incoming_type']}")

    if drift["missing_columns"]:
        lines.append(f"  − Missing columns ({len(drift['missing_columns'])}):")
        for c in drift["missing_columns"]:
            phi_flag = "  ⛔ PHI" if c["is_phi"] else ""
            lines.append(f"      {c['column']:35s} {c['expected_type']}{phi_flag}")

    if drift["type_changes"]:
        lines.append(f"  ~ Type changes ({len(drift['type_changes'])}):")
        for c in drift["type_changes"]:
            phi_flag = "  ⛔ PHI" if c.get("is_phi") else ""
            lines.append(
                f"      {c['column']:35s} {c['expected_type']} → {c['incoming_type']}{phi_flag}"
            )

    lines.append("─" * 60)
    return "\n".join(lines)

# COMMAND ----------

# ─── Serialization Helper (Pipeline 1 → Pipeline 3)

def build_drift_event_payload(table_name: str, drift: dict) -> str:
    """
    Serialize a drift report to a JSON string for passing as a Databricks
    Job task parameter when Pipeline 1 triggers Pipeline 3.
    """
    return json.dumps(drift, indent=2)
