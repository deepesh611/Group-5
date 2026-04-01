# 06_advisor_policy.py — Deterministic Pipeline 3 guardrails and strategy selection
#
# Purpose:
#   Keep the AI Advisor narrow and safe.
#   - Auto-apply only PURE ADDITIVE drift.
#   - SUBTRACTIVE drift becomes metadata-only advisory.
#   - TYPE changes and mixed drift require manual review.
#
# This file is intentionally pure Python so it can be unit-tested outside Databricks.

from __future__ import annotations

import re
from typing import Dict, List, Tuple

_BLOCKED_KEYWORDS = re.compile(
    r"\b(DELETE|UPDATE|INSERT|TRUNCATE|DROP\s+TABLE|CREATE\s+TABLE|GRANT|REVOKE|MERGE)\b",
    re.IGNORECASE,
)
_AUTONOMOUS_DDL_PATTERN = re.compile(
    r"^\s*ALTER\s+TABLE\s+[\w.`]+\s+ADD\s+COLUMNS?\s*\(",
    re.IGNORECASE,
)


class Strategy:
    NO_ACTION = "NO_ACTION"
    AUTO_APPLY_DDL = "AUTO_APPLY_DDL"
    METADATA_ONLY = "METADATA_ONLY"
    ADVISORY_ONLY = "ADVISORY_ONLY"
    MANUAL_REVIEW = "MANUAL_REVIEW"


def drift_kind(drift: Dict) -> str:
    has_new = bool(drift.get("new_columns"))
    has_missing = bool(drift.get("missing_columns"))
    has_type = bool(drift.get("type_changes"))

    if not drift.get("has_drift", False):
        return "none"
    if has_new and not has_missing and not has_type:
        return "additive_only"
    if has_missing and not has_new and not has_type:
        return "subtractive_only"
    if has_type and not has_new and not has_missing:
        return "type_change_only"
    return "mixed"


def _split_columns_payload(columns_clause: str) -> List[str]:
    items, buf, depth = [], [], 0
    for ch in columns_clause:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        if ch == "," and depth == 0:
            piece = "".join(buf).strip()
            if piece:
                items.append(piece)
            buf = []
        else:
            buf.append(ch)
    final_piece = "".join(buf).strip()
    if final_piece:
        items.append(final_piece)
    return items


def extract_added_columns(sql_fix: str) -> List[str]:
    if not sql_fix or not sql_fix.strip():
        return []
    m = re.search(r"ADD\s+COLUMNS?\s*\((.*)\)\s*$", sql_fix.strip(), re.IGNORECASE | re.DOTALL)
    if not m:
        return []
    inner = m.group(1).strip()
    cols = []
    for piece in _split_columns_payload(inner):
        # Two alternates:
        #   1. Backtick-quoted: `any-chars-including-hyphens` — group 1
        #   2. Bare identifier: starts with letter/underscore, word chars only — group 2
        name_match = re.match(r"^`([^`]+)`\s+|^([A-Za-z_]\w*)\s+", piece)
        if name_match:
            name = name_match.group(1) if name_match.group(1) is not None else name_match.group(2)
            cols.append(name.lower())
    return cols


def validate_sql_fix(sql_fix: str, table_name: str, full_table_name: str, run_mode: str = "autonomous") -> Tuple[bool, str]:
    if not sql_fix or not sql_fix.strip():
        return True, "empty SQL_FIX — no DDL to apply"

    sql = sql_fix.strip()
    if not re.search(re.escape(table_name) + r"|" + re.escape(full_table_name), sql, re.IGNORECASE):
        return False, f"DDL does not reference '{table_name}' or '{full_table_name}'"

    blocked = _BLOCKED_KEYWORDS.search(sql)
    if blocked:
        return False, f"Blocked keyword '{blocked.group()}' present in SQL_FIX"

    if re.search(r"\bSELECT\b|\bFROM\b|\bWHERE\b", sql, re.IGNORECASE):
        return False, "SQL_FIX contains subquery keywords"

    if run_mode == "autonomous" and not _AUTONOMOUS_DDL_PATTERN.match(sql):
        return False, "Autonomous mode only permits ALTER TABLE ... ADD COLUMNS (...)"

    return True, "passed"


def validate_new_json(new_json: Dict, drift: Dict) -> Tuple[bool, str]:
    if not isinstance(new_json, dict):
        return False, "NEW_JSON must be a JSON object"

    expected_new = {c["column"].lower() for c in drift.get("new_columns", [])}
    actual_new = {c.lower() for c in new_json.keys()}

    if drift_kind(drift) == "subtractive_only" and actual_new:
        return False, "Subtractive drift must not create NEW_JSON columns"

    if actual_new and not actual_new.issubset(expected_new):
        extra = sorted(actual_new - expected_new)
        return False, f"NEW_JSON contains columns not present in drift.new_columns: {extra}"

    for col_name, col_def in new_json.items():
        if not isinstance(col_def, dict):
            return False, f"NEW_JSON['{col_name}'] must be an object"
        if "type" not in col_def:
            return False, f"NEW_JSON['{col_name}'] is missing required key: type"
    return True, "passed"


def determine_execution_strategy(drift: Dict, rec: Dict, validation_ok: bool) -> str:
    kind = drift_kind(drift)
    sql_fix = (rec or {}).get("SQL_FIX", "") or ""
    new_json = (rec or {}).get("NEW_JSON", {}) or {}

    if kind == "none":
        return Strategy.NO_ACTION
    if not validation_ok:
        return Strategy.MANUAL_REVIEW
    if kind == "subtractive_only":
        return Strategy.METADATA_ONLY
    if kind == "type_change_only":
        return Strategy.MANUAL_REVIEW
    if kind == "mixed":
        return Strategy.MANUAL_REVIEW
    if kind == "additive_only" and sql_fix and new_json:
        return Strategy.AUTO_APPLY_DDL
    if kind == "additive_only" and not sql_fix and not new_json:
        return Strategy.ADVISORY_ONLY
    return Strategy.MANUAL_REVIEW


def validate_recommendation(drift: Dict, rec: Dict, table_name: str, full_table_name: str, run_mode: str = "autonomous") -> Dict:
    rec = rec or {}
    sql_fix = rec.get("SQL_FIX", "") or ""
    new_json = rec.get("NEW_JSON", {}) or {}
    kind = drift_kind(drift)

    sql_ok, sql_reason = validate_sql_fix(sql_fix, table_name, full_table_name, run_mode)
    json_ok, json_reason = validate_new_json(new_json, drift)

    ddl_cols = set(extract_added_columns(sql_fix))
    json_cols = {c.lower() for c in new_json.keys()}
    drift_new_cols = {c["column"].lower() for c in drift.get("new_columns", [])}

    cross_ok = True
    cross_reason = "passed"
    if kind == "additive_only":
        if sql_fix and ddl_cols != drift_new_cols:
            cross_ok = False
            cross_reason = (
                f"SQL_FIX columns {sorted(ddl_cols)} do not exactly match drift.new_columns {sorted(drift_new_cols)}"
            )
        elif new_json and json_cols != drift_new_cols:
            cross_ok = False
            cross_reason = (
                f"NEW_JSON columns {sorted(json_cols)} do not exactly match drift.new_columns {sorted(drift_new_cols)}"
            )
    elif kind == "subtractive_only" and (sql_fix or new_json):
        cross_ok = False
        cross_reason = "Subtractive drift must not produce DDL or NEW_JSON"

    validation_ok = sql_ok and json_ok and cross_ok
    strategy = determine_execution_strategy(drift, rec, validation_ok)

    return {
        "drift_kind": kind,
        "validation_ok": validation_ok,
        "sql_validation": {"ok": sql_ok, "reason": sql_reason},
        "json_validation": {"ok": json_ok, "reason": json_reason},
        "cross_validation": {"ok": cross_ok, "reason": cross_reason},
        "ddl_columns": sorted(ddl_cols),
        "json_columns": sorted(json_cols),
        "execution_strategy": strategy,
    }
