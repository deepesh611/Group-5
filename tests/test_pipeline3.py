"""
Unit tests for Pipeline 3 pure-Python logic.

These tests run entirely locally (no Spark, no Databricks, no Azure) because
06_advisor_policy.py is intentionally pure Python.

Run with:
    cd "notebooks/utils"
    python -m pytest ../../tests/test_pipeline3.py -v
"""

import sys
import os
import json
import pytest

# ---------------------------------------------------------------------------
# Bootstrap — make the utils importable without Databricks globals
# ---------------------------------------------------------------------------
_UTILS = os.path.join(os.path.dirname(__file__), "..", "notebooks", "utils")
sys.path.insert(0, os.path.abspath(_UTILS))

# 06_advisor_policy.py is pure Python — import directly
from importlib.util import spec_from_file_location, module_from_spec

def _load_module(name, filename):
    path = os.path.join(os.path.abspath(_UTILS), filename)
    spec = spec_from_file_location(name, path)
    mod = module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod

policy = _load_module("policy", "06_advisor_policy.py")
Strategy = policy.Strategy
drift_kind = policy.drift_kind
validate_recommendation = policy.validate_recommendation
determine_execution_strategy = policy.determine_execution_strategy
validate_sql_fix = policy.validate_sql_fix
validate_new_json = policy.validate_new_json
extract_added_columns = policy.extract_added_columns


# ═══════════════════════════════════════════════════════════════════════════════
# Fixtures — reusable drift payloads
# ═══════════════════════════════════════════════════════════════════════════════

TABLE = "patients"
FULL_TABLE = "project5.delta_tables.patients"


def _make_drift(*, new=None, missing=None, type_changes=None):
    """Build a minimal drift dict for testing."""
    has_drift = bool(new or missing or type_changes)
    return {
        "table": TABLE,
        "has_drift": has_drift,
        "severity": "CRITICAL" if missing else ("WARNING" if has_drift else "NONE"),
        "new_columns": new or [],
        "missing_columns": missing or [],
        "type_changes": type_changes or [],
    }


@pytest.fixture
def no_drift():
    return _make_drift()


@pytest.fixture
def additive_drift():
    return _make_drift(new=[{"column": "new_col", "incoming_type": "string", "severity": "WARNING"}])


@pytest.fixture
def subtractive_drift():
    return _make_drift(missing=[{"column": "ssn", "expected_type": "string", "is_phi": True, "severity": "CRITICAL"}])


@pytest.fixture
def type_change_drift():
    return _make_drift(type_changes=[{"column": "birthdate", "expected_type": "date", "incoming_type": "string", "is_phi": True, "severity": "CRITICAL"}])


@pytest.fixture
def mixed_drift():
    return _make_drift(
        new=[{"column": "new_col", "incoming_type": "string", "severity": "WARNING"}],
        missing=[{"column": "ssn", "expected_type": "string", "is_phi": True, "severity": "CRITICAL"}],
    )


# ═══════════════════════════════════════════════════════════════════════════════
# Test Group 1 — drift_kind()
# ═══════════════════════════════════════════════════════════════════════════════

class TestDriftKind:
    def test_no_drift(self, no_drift):
        assert drift_kind(no_drift) == "none"

    def test_additive_only(self, additive_drift):
        assert drift_kind(additive_drift) == "additive_only"

    def test_subtractive_only(self, subtractive_drift):
        assert drift_kind(subtractive_drift) == "subtractive_only"

    def test_type_change_only(self, type_change_drift):
        assert drift_kind(type_change_drift) == "type_change_only"

    def test_mixed(self, mixed_drift):
        assert drift_kind(mixed_drift) == "mixed"


# ═══════════════════════════════════════════════════════════════════════════════
# Test Group 2 — validate_sql_fix()
# ═══════════════════════════════════════════════════════════════════════════════

class TestValidateSqlFix:
    def test_empty_sql_is_ok(self):
        ok, reason = validate_sql_fix("", TABLE, FULL_TABLE, "autonomous")
        assert ok
        assert "empty" in reason.lower()

    def test_valid_alter_table(self):
        sql = f"ALTER TABLE {FULL_TABLE} ADD COLUMNS (new_col STRING)"
        ok, _ = validate_sql_fix(sql, TABLE, FULL_TABLE, "autonomous")
        assert ok

    def test_blocked_drop_keyword(self):
        sql = f"DROP TABLE {FULL_TABLE}"
        ok, reason = validate_sql_fix(sql, TABLE, FULL_TABLE, "autonomous")
        assert not ok
        assert "drop" in reason.lower()

    def test_blocked_delete(self):
        sql = f"DELETE FROM {FULL_TABLE}"
        ok, reason = validate_sql_fix(sql, TABLE, FULL_TABLE, "autonomous")
        assert not ok

    def test_blocked_select_subquery(self):
        sql = f"ALTER TABLE {FULL_TABLE} ADD COLUMNS (x STRING) WHERE id IN (SELECT id FROM {FULL_TABLE})"
        ok, reason = validate_sql_fix(sql, TABLE, FULL_TABLE, "autonomous")
        assert not ok
        assert "subquery" in reason.lower()

    def test_autonomous_rejects_non_add_columns(self):
        sql = f"ALTER TABLE {FULL_TABLE} RENAME COLUMN old_col TO new_col"
        ok, reason = validate_sql_fix(sql, TABLE, FULL_TABLE, "autonomous")
        assert not ok
        assert "autonomous" in reason.lower()

    def test_interactive_allows_non_add_columns(self):
        sql = f"ALTER TABLE {FULL_TABLE} RENAME COLUMN old_col TO new_col"
        ok, _ = validate_sql_fix(sql, TABLE, FULL_TABLE, "interactive")
        assert ok

    def test_sql_missing_table_name(self):
        sql = "ALTER TABLE some_other_table ADD COLUMNS (x STRING)"
        ok, reason = validate_sql_fix(sql, TABLE, FULL_TABLE, "autonomous")
        assert not ok
        assert "reference" in reason.lower()


# ═══════════════════════════════════════════════════════════════════════════════
# Test Group 3 — validate_new_json()
# ═══════════════════════════════════════════════════════════════════════════════

class TestValidateNewJson:
    def test_valid_new_json(self, additive_drift):
        new_json = {"new_col": {"type": "string", "phi": False, "masking": None, "description": "test"}}
        ok, _ = validate_new_json(new_json, additive_drift)
        assert ok

    def test_empty_new_json_is_ok(self, additive_drift):
        ok, _ = validate_new_json({}, additive_drift)
        assert ok

    def test_not_a_dict_fails(self, additive_drift):
        ok, reason = validate_new_json(["bad"], additive_drift)
        assert not ok
        assert "object" in reason.lower()

    def test_extra_column_not_in_drift_fails(self, additive_drift):
        new_json = {"phantom_col": {"type": "string"}}
        ok, reason = validate_new_json(new_json, additive_drift)
        assert not ok
        assert "phantom_col" in reason

    def test_subtractive_drift_must_not_have_new_json(self, subtractive_drift):
        new_json = {"ssn": {"type": "string"}}
        ok, reason = validate_new_json(new_json, subtractive_drift)
        assert not ok
        assert "subtractive" in reason.lower()

    def test_col_def_missing_type_fails(self, additive_drift):
        new_json = {"new_col": {"phi": False}}   # missing 'type'
        ok, reason = validate_new_json(new_json, additive_drift)
        assert not ok
        assert "type" in reason


# ═══════════════════════════════════════════════════════════════════════════════
# Test Group 4 — extract_added_columns()
# ═══════════════════════════════════════════════════════════════════════════════

class TestExtractAddedColumns:
    def test_single_column(self):
        sql = f"ALTER TABLE {FULL_TABLE} ADD COLUMNS (new_col STRING)"
        assert extract_added_columns(sql) == ["new_col"]

    def test_multiple_columns(self):
        sql = f"ALTER TABLE {FULL_TABLE} ADD COLUMNS (col_a STRING, col_b DOUBLE, col_c DATE)"
        cols = extract_added_columns(sql)
        assert sorted(cols) == ["col_a", "col_b", "col_c"]

    def test_backtick_quoted_column(self):
        sql = f"ALTER TABLE {FULL_TABLE} ADD COLUMNS (`my-col` STRING)"
        cols = extract_added_columns(sql)
        assert "my-col" in cols

    def test_empty_sql_returns_empty(self):
        assert extract_added_columns("") == []

    def test_non_matching_sql_returns_empty(self):
        assert extract_added_columns("SELECT * FROM foo") == []


# ═══════════════════════════════════════════════════════════════════════════════
# Test Group 5 — determine_execution_strategy()
# ═══════════════════════════════════════════════════════════════════════════════

class TestDetermineExecutionStrategy:
    def test_no_drift_gives_no_action(self, no_drift):
        assert determine_execution_strategy(no_drift, {}, True) == Strategy.NO_ACTION

    def test_failed_validation_gives_manual_review(self, additive_drift):
        rec = {
            "SQL_FIX": f"ALTER TABLE {FULL_TABLE} ADD COLUMNS (new_col STRING)",
            "NEW_JSON": {"new_col": {"type": "string"}},
        }
        assert determine_execution_strategy(additive_drift, rec, False) == Strategy.MANUAL_REVIEW

    def test_additive_with_sql_and_json_gives_auto_apply(self, additive_drift):
        rec = {
            "SQL_FIX": f"ALTER TABLE {FULL_TABLE} ADD COLUMNS (new_col STRING)",
            "NEW_JSON": {"new_col": {"type": "string"}},
        }
        assert determine_execution_strategy(additive_drift, rec, True) == Strategy.AUTO_APPLY_DDL

    def test_additive_no_sql_no_json_gives_advisory(self, additive_drift):
        rec = {"SQL_FIX": "", "NEW_JSON": {}}
        assert determine_execution_strategy(additive_drift, rec, True) == Strategy.ADVISORY_ONLY

    def test_subtractive_gives_metadata_only(self, subtractive_drift):
        assert determine_execution_strategy(subtractive_drift, {}, True) == Strategy.METADATA_ONLY

    def test_type_change_gives_manual_review(self, type_change_drift):
        assert determine_execution_strategy(type_change_drift, {}, True) == Strategy.MANUAL_REVIEW

    def test_mixed_gives_manual_review(self, mixed_drift):
        assert determine_execution_strategy(mixed_drift, {}, True) == Strategy.MANUAL_REVIEW


# ═══════════════════════════════════════════════════════════════════════════════
# Test Group 6 — validate_recommendation() end-to-end
# ═══════════════════════════════════════════════════════════════════════════════

class TestValidateRecommendation:
    def test_no_drift_validates_cleanly(self, no_drift):
        result = validate_recommendation(no_drift, {}, TABLE, FULL_TABLE, "autonomous")
        assert result["validation_ok"] is True
        assert result["execution_strategy"] == Strategy.NO_ACTION

    def test_valid_additive_rec_gives_auto_apply(self, additive_drift):
        rec = {
            "SQL_FIX": f"ALTER TABLE {FULL_TABLE} ADD COLUMNS (new_col STRING)",
            "NEW_JSON": {"new_col": {"type": "string", "phi": False, "masking": None, "description": "test"}},
            "SEVERITY": "WARNING",
            "REASONING": "New column detected",
        }
        result = validate_recommendation(additive_drift, rec, TABLE, FULL_TABLE, "autonomous")
        assert result["validation_ok"] is True
        assert result["execution_strategy"] == Strategy.AUTO_APPLY_DDL

    def test_subtractive_with_sql_fix_fails(self, subtractive_drift):
        # LLM hallucination — generated DDL for subtractive drift
        rec = {
            "SQL_FIX": f"ALTER TABLE {FULL_TABLE} ADD COLUMNS (ssn STRING)",
            "NEW_JSON": {"ssn": {"type": "string"}},
            "SEVERITY": "CRITICAL",
            "REASONING": "bad LLM output",
        }
        result = validate_recommendation(subtractive_drift, rec, TABLE, FULL_TABLE, "autonomous")
        assert result["validation_ok"] is False
        # Even though validation failed, strategy should be METADATA_ONLY for subtractive
        # The cross_validation failure pushes it to MANUAL_REVIEW
        assert result["execution_strategy"] in (Strategy.MANUAL_REVIEW, Strategy.METADATA_ONLY)

    def test_subtractive_with_empty_rec_gives_metadata_only(self, subtractive_drift):
        rec = {"SQL_FIX": "", "NEW_JSON": {}, "SEVERITY": "CRITICAL", "REASONING": "Missing column"}
        result = validate_recommendation(subtractive_drift, rec, TABLE, FULL_TABLE, "autonomous")
        assert result["validation_ok"] is True
        assert result["execution_strategy"] == Strategy.METADATA_ONLY

    def test_mismatched_columns_fails_cross_validation(self, additive_drift):
        # SQL adds a different column than what's in drift.new_columns
        rec = {
            "SQL_FIX": f"ALTER TABLE {FULL_TABLE} ADD COLUMNS (wrong_col STRING)",
            "NEW_JSON": {"wrong_col": {"type": "string"}},
            "SEVERITY": "WARNING",
            "REASONING": "LLM used wrong column name",
        }
        result = validate_recommendation(additive_drift, rec, TABLE, FULL_TABLE, "autonomous")
        assert result["cross_validation"]["ok"] is False
        assert result["execution_strategy"] == Strategy.MANUAL_REVIEW

    def test_type_change_always_manual_review(self, type_change_drift):
        rec = {"SQL_FIX": "", "NEW_JSON": {}, "SEVERITY": "CRITICAL", "REASONING": "Type changed"}
        result = validate_recommendation(type_change_drift, rec, TABLE, FULL_TABLE, "autonomous")
        assert result["execution_strategy"] == Strategy.MANUAL_REVIEW

    def test_mixed_drift_always_manual_review(self, mixed_drift):
        rec = {"SQL_FIX": "", "NEW_JSON": {}, "SEVERITY": "CRITICAL", "REASONING": "Mixed drift"}
        result = validate_recommendation(mixed_drift, rec, TABLE, FULL_TABLE, "autonomous")
        assert result["execution_strategy"] == Strategy.MANUAL_REVIEW
