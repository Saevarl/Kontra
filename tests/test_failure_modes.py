# tests/test_failure_modes.py
"""Tests for rule failure modes and structured details."""

from datetime import datetime, timedelta, timezone

import polars as pl
import pytest

from kontra.state.types import FailureMode


class TestFailureModeEnum:
    """Tests for the FailureMode enum."""

    def test_failure_mode_values(self):
        """All expected failure modes exist."""
        assert FailureMode.NOVEL_CATEGORY.value == "novel_category"
        assert FailureMode.NULL_VALUES.value == "null_values"
        assert FailureMode.DUPLICATE_VALUES.value == "duplicate_values"
        assert FailureMode.RANGE_VIOLATION.value == "range_violation"
        assert FailureMode.SCHEMA_DRIFT.value == "schema_drift"
        assert FailureMode.FRESHNESS_LAG.value == "freshness_lag"
        assert FailureMode.ROW_COUNT_LOW.value == "row_count_low"
        assert FailureMode.ROW_COUNT_HIGH.value == "row_count_high"
        assert FailureMode.PATTERN_MISMATCH.value == "pattern_mismatch"
        assert FailureMode.CUSTOM_CHECK_FAILED.value == "custom_check_failed"

    def test_failure_mode_str(self):
        """FailureMode has string representation."""
        assert str(FailureMode.NOVEL_CATEGORY) == "novel_category"
        assert str(FailureMode.NULL_VALUES) == "null_values"


class TestNotNullFailureMode:
    """Tests for not_null rule failure details."""

    def test_not_null_failure_mode(self):
        """not_null rule returns failure_mode and details on failure."""
        from kontra.rules.builtin.not_null import NotNullRule

        df = pl.DataFrame({
            "id": [1, 2, None, 4, None],
        })
        rule = NotNullRule("not_null", {"column": "id"})
        result = rule.validate(df)

        assert result["passed"] is False
        assert result["failed_count"] == 2
        assert result["failure_mode"] == "null_values"
        assert "details" in result

        details = result["details"]
        assert details["null_count"] == 2
        assert details["null_rate"] == 0.4  # 2/5
        assert details["total_rows"] == 5
        assert "sample_positions" in details
        assert 2 in details["sample_positions"]  # Index of first null

    def test_not_null_no_failure_mode_on_pass(self):
        """not_null rule does not return failure_mode on pass."""
        from kontra.rules.builtin.not_null import NotNullRule

        df = pl.DataFrame({
            "id": [1, 2, 3, 4, 5],
        })
        rule = NotNullRule("not_null", {"column": "id"})
        result = rule.validate(df)

        assert result["passed"] is True
        assert "failure_mode" not in result
        assert "details" not in result


class TestUniqueFailureMode:
    """Tests for unique rule failure details."""

    def test_unique_failure_mode(self):
        """unique rule returns failure_mode and details on failure."""
        from kontra.rules.builtin.unique import UniqueRule

        df = pl.DataFrame({
            "id": [1, 2, 2, 3, 3, 3],
        })
        rule = UniqueRule("unique", {"column": "id"})
        result = rule.validate(df)

        assert result["passed"] is False
        assert result["failure_mode"] == "duplicate_values"
        assert "details" in result

        details = result["details"]
        assert details["duplicate_value_count"] == 2  # 2 and 3 are duplicates
        assert "top_duplicates" in details
        # 3 appears 3 times, 2 appears 2 times
        top = details["top_duplicates"]
        assert len(top) == 2
        assert top[0]["value"] == 3
        assert top[0]["count"] == 3

    def test_unique_no_failure_mode_on_pass(self):
        """unique rule does not return failure_mode on pass."""
        from kontra.rules.builtin.unique import UniqueRule

        df = pl.DataFrame({
            "id": [1, 2, 3, 4, 5],
        })
        rule = UniqueRule("unique", {"column": "id"})
        result = rule.validate(df)

        assert result["passed"] is True
        assert "failure_mode" not in result


class TestRangeFailureMode:
    """Tests for range rule failure details."""

    def test_range_failure_mode_below_min(self):
        """range rule returns details for values below min."""
        from kontra.rules.builtin.range import RangeRule

        df = pl.DataFrame({
            "age": [5, 10, 15, 25, 30],
        })
        rule = RangeRule("range", {"column": "age", "min": 18, "max": 65})
        result = rule.validate(df)

        assert result["passed"] is False
        assert result["failure_mode"] == "range_violation"
        assert "details" in result

        details = result["details"]
        assert details["expected_min"] == 18
        assert details["expected_max"] == 65
        assert details["actual_min"] == 5
        assert details["actual_max"] == 30
        assert details["below_min_count"] == 3  # 5, 10, 15 < 18

    def test_range_failure_mode_above_max(self):
        """range rule returns details for values above max."""
        from kontra.rules.builtin.range import RangeRule

        df = pl.DataFrame({
            "age": [20, 30, 70, 80, 90],
        })
        rule = RangeRule("range", {"column": "age", "min": 18, "max": 65})
        result = rule.validate(df)

        assert result["passed"] is False
        details = result["details"]
        assert details["above_max_count"] == 3  # 70, 80, 90 > 65


class TestAllowedValuesFailureMode:
    """Tests for allowed_values rule failure details."""

    def test_allowed_values_failure_mode(self):
        """allowed_values rule returns failure_mode and details on failure."""
        from kontra.rules.builtin.allowed_values import AllowedValuesRule

        df = pl.DataFrame({
            "status": ["active", "inactive", "unknown", "deleted", "unknown"],
        })
        rule = AllowedValuesRule("allowed_values", {
            "column": "status",
            "values": ["active", "inactive", "deleted"]
        })
        result = rule.validate(df)

        assert result["passed"] is False
        assert result["failure_mode"] == "novel_category"
        assert "details" in result

        details = result["details"]
        assert "expected" in details
        assert "unknown" not in details["expected"]
        assert "unexpected_values" in details
        unexpected = details["unexpected_values"]
        assert len(unexpected) == 1
        assert unexpected[0]["value"] == "unknown"
        assert unexpected[0]["count"] == 2


class TestDtypeFailureMode:
    """Tests for dtype rule failure details."""

    def test_dtype_failure_mode(self):
        """dtype rule returns failure_mode and details on failure."""
        from kontra.rules.builtin.dtype import DtypeRule

        df = pl.DataFrame({
            "id": ["1", "2", "3"],  # String, not int
        })
        rule = DtypeRule("dtype", {"column": "id", "type": "int64"})
        result = rule.validate(df)

        assert result["passed"] is False
        assert result["failure_mode"] == "schema_drift"
        assert "details" in result

        details = result["details"]
        assert details["expected_type"] == "int64"
        assert details["column"] == "id"


class TestMinRowsFailureMode:
    """Tests for min_rows rule failure details."""

    def test_min_rows_failure_mode(self):
        """min_rows rule returns failure_mode and details on failure."""
        from kontra.rules.builtin.min_rows import MinRowsRule

        df = pl.DataFrame({
            "id": [1, 2, 3],
        })
        rule = MinRowsRule("min_rows", {"value": 10})
        result = rule.validate(df)

        assert result["passed"] is False
        assert result["failure_mode"] == "row_count_low"
        assert "details" in result

        details = result["details"]
        assert details["actual_rows"] == 3
        assert details["minimum_required"] == 10
        assert details["shortfall"] == 7


class TestMaxRowsFailureMode:
    """Tests for max_rows rule failure details."""

    def test_max_rows_failure_mode(self):
        """max_rows rule returns failure_mode and details on failure."""
        from kontra.rules.builtin.max_rows import MaxRowsRule

        df = pl.DataFrame({
            "id": list(range(100)),
        })
        rule = MaxRowsRule("max_rows", {"value": 50})
        result = rule.validate(df)

        assert result["passed"] is False
        assert result["failure_mode"] == "row_count_high"
        assert "details" in result

        details = result["details"]
        assert details["actual_rows"] == 100
        assert details["maximum_allowed"] == 50
        assert details["excess"] == 50


class TestRegexFailureMode:
    """Tests for regex rule failure details."""

    def test_regex_failure_mode(self):
        """regex rule returns failure_mode and details on failure."""
        from kontra.rules.builtin.regex import RegexRule

        df = pl.DataFrame({
            "email": ["valid@email.com", "invalid", "also@valid.org", "bad"],
        })
        rule = RegexRule("regex", {"column": "email", "pattern": r"^[\w\.-]+@[\w\.-]+\.\w+$"})
        result = rule.validate(df)

        assert result["passed"] is False
        assert result["failure_mode"] == "pattern_mismatch"
        assert "details" in result

        details = result["details"]
        assert "pattern" in details
        assert "sample_mismatches" in details
        assert "invalid" in details["sample_mismatches"]
        assert "bad" in details["sample_mismatches"]


class TestFreshnessFailureMode:
    """Tests for freshness rule failure details."""

    def test_freshness_failure_mode(self):
        """freshness rule returns failure_mode and details on failure."""
        from kontra.rules.builtin.freshness import FreshnessRule

        # Data from 2 days ago (stale if max_age is 1 day)
        old_ts = datetime.now(timezone.utc) - timedelta(days=2)
        df = pl.DataFrame({
            "updated_at": [old_ts],
        })
        rule = FreshnessRule("freshness", {"column": "updated_at", "max_age": "1d"})
        result = rule.validate(df)

        assert result["passed"] is False
        assert result["failure_mode"] == "freshness_lag"
        assert "details" in result

        details = result["details"]
        assert "latest_timestamp" in details
        assert "threshold_timestamp" in details
        assert "actual_age_seconds" in details
        assert "max_age_seconds" in details
        assert details["max_age_spec"] == "1d"


class TestCustomSqlCheckFailureMode:
    """Tests for custom_sql_check rule failure details."""

    def test_custom_sql_check_failure_mode(self):
        """custom_sql_check rule returns failure_mode and details on failure."""
        from kontra.rules.builtin.custom_sql_check import CustomSQLCheck

        df = pl.DataFrame({
            "price": [10, -5, 20, -3, 30],
        })
        rule = CustomSQLCheck("custom_sql_check", {
            "query": "SELECT * FROM data WHERE price < 0"
        })
        result = rule.validate(df)

        assert result["passed"] is False
        assert result["failure_mode"] == "custom_check_failed"
        assert "details" in result

        details = result["details"]
        assert details["failed_row_count"] == 2  # -5 and -3
        assert "query" in details
