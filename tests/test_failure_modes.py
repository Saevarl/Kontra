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
        from kontra.rule_defs.builtin.not_null import NotNullRule

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
        from kontra.rule_defs.builtin.not_null import NotNullRule

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
        from kontra.rule_defs.builtin.unique import UniqueRule

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
        from kontra.rule_defs.builtin.unique import UniqueRule

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
        from kontra.rule_defs.builtin.range import RangeRule

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
        from kontra.rule_defs.builtin.range import RangeRule

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
        from kontra.rule_defs.builtin.allowed_values import AllowedValuesRule

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

    def test_allowed_values_with_null_in_allowed_list(self):
        """allowed_values accepts NULL when None is in allowed values list (BUG-007)."""
        from kontra.rule_defs.builtin.allowed_values import AllowedValuesRule

        df = pl.DataFrame({
            "status": ["active", "inactive", None],
        })
        # NULL is explicitly in the allowed values
        rule = AllowedValuesRule("allowed_values", {
            "column": "status",
            "values": ["active", "inactive", None]
        })
        result = rule.validate(df)

        # Should pass - all values including NULL are allowed
        assert result["passed"] is True
        assert result["failed_count"] == 0

    def test_allowed_values_rejects_null_when_not_in_list(self):
        """allowed_values rejects NULL when None is NOT in allowed values list."""
        from kontra.rule_defs.builtin.allowed_values import AllowedValuesRule

        df = pl.DataFrame({
            "status": ["active", "inactive", None],
        })
        # NULL is NOT in the allowed values
        rule = AllowedValuesRule("allowed_values", {
            "column": "status",
            "values": ["active", "inactive"]
        })
        result = rule.validate(df)

        # Should fail - NULL is not allowed
        assert result["passed"] is False
        assert result["failed_count"] == 1  # The NULL row


class TestDtypeFailureMode:
    """Tests for dtype rule failure details."""

    def test_dtype_failure_mode(self):
        """dtype rule returns failure_mode and details on failure."""
        from kontra.rule_defs.builtin.dtype import DtypeRule

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
        from kontra.rule_defs.builtin.min_rows import MinRowsRule

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
        from kontra.rule_defs.builtin.max_rows import MaxRowsRule

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
        from kontra.rule_defs.builtin.regex import RegexRule

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
        from kontra.rule_defs.builtin.freshness import FreshnessRule

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
        from kontra.rule_defs.builtin.custom_sql_check import CustomSQLCheck

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


class TestDuplicateRuleIdError:
    """Tests for duplicate rule ID detection and error messages."""

    def test_duplicate_rule_id_raises_error(self):
        """Duplicate rule IDs raise DuplicateRuleIdError."""
        import kontra
        from kontra import rules
        from kontra.errors import DuplicateRuleIdError

        df = pl.DataFrame({"email": ["a@b.com", "c@d.com"]})

        # Two not_null rules on the same column without explicit IDs
        with pytest.raises(DuplicateRuleIdError) as exc_info:
            kontra.validate(df, rules=[
                rules.not_null("email"),
                rules.not_null("email"),  # Duplicate
            ], save=False)

        error = exc_info.value
        assert error.rule_id == "COL:email:not_null"
        assert error.rule_name == "not_null"
        assert error.column == "email"

    def test_duplicate_rule_id_error_message(self):
        """DuplicateRuleIdError has helpful error message."""
        import kontra
        from kontra import rules
        from kontra.errors import DuplicateRuleIdError

        df = pl.DataFrame({"email": ["a@b.com"]})

        with pytest.raises(DuplicateRuleIdError) as exc_info:
            kontra.validate(df, rules=[
                rules.not_null("email"),
                rules.not_null("email"),
            ], save=False)

        error_str = str(exc_info.value)
        assert "COL:email:not_null" in error_str
        assert "id:" in error_str  # Suggests adding explicit ID
        assert "index" in error_str.lower()  # Shows which rules conflict

    def test_explicit_id_avoids_collision(self):
        """Explicit IDs prevent collision errors."""
        import kontra
        from kontra import rules

        df = pl.DataFrame({"email": ["a@b.com", "c@d.com"]})

        # Same rule type on same column, but with explicit IDs
        result = kontra.validate(df, rules=[
            rules.not_null("email", id="email_not_null_1"),
            rules.not_null("email", id="email_not_null_2"),
        ], save=False)

        assert result.passed
        assert len(result.rules) == 2
        assert result.rules[0].rule_id == "email_not_null_1"
        assert result.rules[1].rule_id == "email_not_null_2"

    def test_duplicate_dataset_rule_id(self):
        """Duplicate dataset-level rule IDs are detected."""
        import kontra
        from kontra.errors import DuplicateRuleIdError

        df = pl.DataFrame({"id": list(range(10))})

        # Two min_rows rules without explicit IDs
        with pytest.raises(DuplicateRuleIdError) as exc_info:
            kontra.validate(df, rules=[
                {"name": "min_rows", "params": {"value": 5}},
                {"name": "min_rows", "params": {"value": 10}},
            ], save=False)

        error = exc_info.value
        assert error.rule_id == "DATASET:min_rows"
        assert error.column is None  # Dataset-level rule

    def test_duplicate_in_contract_file(self, tmp_path):
        """Duplicate rule IDs in YAML contract raise error."""
        import kontra
        from kontra.errors import DuplicateRuleIdError

        contract_path = tmp_path / "contract.yml"
        contract_path.write_text("""
name: test_contract
datasource: placeholder
rules:
  - name: not_null
    params:
      column: email
  - name: not_null
    params:
      column: email
""")

        df = pl.DataFrame({"email": ["a@b.com"]})

        with pytest.raises(DuplicateRuleIdError):
            kontra.validate(df, contract=str(contract_path), save=False)


class TestUniqueNullHandling:
    """Tests for unique rule NULL handling (BUG-002 fix)."""

    def test_unique_nulls_not_treated_as_duplicates(self):
        """NULLs should not be treated as duplicates (SQL semantics)."""
        from kontra.rule_defs.builtin.unique import UniqueRule

        df = pl.DataFrame({"v": [None, None, None]})
        rule = UniqueRule("unique", {"column": "v"})
        result = rule.validate(df)

        # Per SQL semantics: NULL != NULL, so no duplicates
        assert result["passed"] is True
        assert result["failed_count"] == 0

    def test_unique_still_catches_real_duplicates(self):
        """Real duplicate values should still be caught."""
        from kontra.rule_defs.builtin.unique import UniqueRule

        df = pl.DataFrame({"v": ["a", "a", "b", None, None]})
        rule = UniqueRule("unique", {"column": "v"})
        result = rule.validate(df)

        # "a" is duplicated - SQL semantics: 3 non-null - 2 distinct = 1 extra
        assert result["passed"] is False
        assert result["failed_count"] == 1  # One extra "a" row

    def test_unique_all_nulls_passes(self):
        """Column with all NULLs should pass unique check."""
        import kontra
        from kontra import rules

        df = pl.DataFrame({"v": [None, None, None, None, None]})
        result = kontra.validate(df, rules=[rules.unique("v")], save=False)

        assert result.passed is True


class TestFreshnessErrorMessages:
    """Tests for freshness rule error messages (BUG-007 fix)."""

    def test_freshness_non_datetime_column_gives_clear_error(self):
        """freshness rule should give clear error for non-datetime column."""
        from kontra.rule_defs.builtin.freshness import FreshnessRule

        df = pl.DataFrame({"value": [1, 2, 3]})  # Integer column
        rule = FreshnessRule("freshness", {"column": "value", "max_age": "24h"})
        result = rule.validate(df)

        assert result["passed"] is False
        # Should have clear error, not raw Python exception
        assert "datetime" in result["message"].lower()
        assert "Rule execution failed" not in result["message"]
        assert "Int64" in result["message"]  # Shows actual type

    def test_freshness_string_column_that_cant_be_parsed(self):
        """freshness rule handles unparseable string column gracefully."""
        from kontra.rule_defs.builtin.freshness import FreshnessRule

        df = pl.DataFrame({"value": ["not", "a", "date"]})
        rule = FreshnessRule("freshness", {"column": "value", "max_age": "24h"})
        result = rule.validate(df)

        assert result["passed"] is False
        # Should mention it can't be parsed as datetime
        assert "cannot be parsed" in result["message"].lower() or "datetime" in result["message"].lower()


class TestEmptyInputHandling:
    """Tests for empty input handling."""

    def test_empty_list_with_dataset_rules_works(self):
        """Empty list with dataset-level rules should work."""
        import kontra
        from kontra import rules

        # min_rows should fail on empty list
        result = kontra.validate([], rules=[rules.min_rows(1)], save=False)
        assert result.passed is False
        assert result.failed_count == 1

        # max_rows should pass on empty list
        result = kontra.validate([], rules=[rules.max_rows(100)], save=False)
        assert result.passed is True

    def test_non_empty_list_works(self):
        """Non-empty list should work normally."""
        import kontra
        from kontra import rules

        data = [{"id": 1}, {"id": 2}]
        result = kontra.validate(data, rules=[rules.not_null("id")], save=False)

        assert result.passed is True


class TestStateCorruptedError:
    """Tests for StateCorruptedError (BUG-006 fix)."""

    def test_state_corrupted_error_exists(self):
        """StateCorruptedError is available from kontra."""
        import kontra
        from kontra.errors import StateCorruptedError

        assert StateCorruptedError is not None
        # Should be able to instantiate
        err = StateCorruptedError("test_contract", "test error")
        assert "test_contract" in str(err)
        assert "corrupted" in str(err).lower()

    def test_state_corrupted_error_exported(self):
        """StateCorruptedError is exported from kontra module."""
        import kontra

        assert hasattr(kontra, "StateCorruptedError")


class TestCustomSqlCheckFix:
    """Tests for custom_sql_check fix (BUG-001 from first round)."""

    def test_custom_sql_check_accepts_sql_param(self):
        """custom_sql_check should accept 'sql' parameter (documented name)."""
        import kontra
        from kontra import rules

        df = pl.DataFrame({"value": [10, 20, 30]})
        result = kontra.validate(df, rules=[
            rules.custom_sql_check(sql="SELECT * FROM data WHERE value > 25")
        ], save=False)

        # Should find 1 row (value=30 > 25)
        assert result.rules[0].failed_count == 1

    def test_custom_sql_check_table_substitution(self):
        """custom_sql_check should substitute {table} with 'data'."""
        import kontra
        from kontra import rules

        df = pl.DataFrame({"value": [10, 20, 30]})
        result = kontra.validate(df, rules=[
            rules.custom_sql_check(sql="SELECT * FROM {table} WHERE value < 15")
        ], save=False)

        # Should find 1 row (value=10 < 15)
        assert result.rules[0].failed_count == 1


class TestRangeValidationFix:
    """Tests for range rule validation fix (BUG-004 from first round)."""

    def test_range_no_bounds_raises_error(self):
        """range rule with no min/max should raise error at construction."""
        from kontra import rules

        with pytest.raises(ValueError) as exc_info:
            rules.range("value")  # No min or max

        assert "min" in str(exc_info.value).lower()
        assert "max" in str(exc_info.value).lower()

    def test_range_rule_class_no_bounds_raises_error(self):
        """RangeRule class should also raise error at construction."""
        from kontra.rule_defs.builtin.range import RangeRule
        from kontra.errors import RuleParameterError

        with pytest.raises(RuleParameterError):
            RangeRule("range", {"column": "value"})  # No min or max


class TestDtypeValidationFix:
    """Tests for dtype rule validation fix (BUG-005 from first round)."""

    def test_dtype_unknown_type_raises_error(self):
        """dtype rule with unknown type should raise error at construction."""
        from kontra.rule_defs.builtin.dtype import DtypeRule
        from kontra.errors import RuleParameterError

        with pytest.raises(RuleParameterError) as exc_info:
            DtypeRule("dtype", {"column": "value", "type": "unknown_type"})

        error_str = str(exc_info.value)
        assert "unknown_type" in error_str
        # Should list valid types
        assert "int64" in error_str or "Valid types" in error_str
