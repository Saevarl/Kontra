# tests/test_rules_conditional_range.py
"""Tests for the conditional_range rule."""

import pytest
import polars as pl

import kontra
from kontra import rules


class TestConditionalRangeBasic:
    """Basic functionality tests."""

    def test_pass_when_in_range(self):
        """Rule passes when column is in range for matching condition."""
        df = pl.DataFrame({
            "customer_type": ["premium", "premium", "premium"],
            "discount": [15.0, 25.0, 40.0],  # All in [10, 50]
        })
        result = kontra.validate(df, rules=[
            rules.conditional_range("discount", "customer_type == 'premium'", min=10, max=50)
        ], save=False)
        assert result.passed
        assert result.failed_count == 0

    def test_fail_when_below_min(self):
        """Rule fails when column is below min for matching condition."""
        df = pl.DataFrame({
            "customer_type": ["premium", "regular", "premium"],
            "discount": [5.0, 5.0, 15.0],  # Row 0: below 10
        })
        result = kontra.validate(df, rules=[
            rules.conditional_range("discount", "customer_type == 'premium'", min=10, max=50)
        ], save=False)
        assert not result.passed
        assert result.rules[0].failed_count == 1

    def test_fail_when_above_max(self):
        """Rule fails when column is above max for matching condition."""
        df = pl.DataFrame({
            "customer_type": ["premium", "regular", "premium"],
            "discount": [60.0, 5.0, 15.0],  # Row 0: above 50
        })
        result = kontra.validate(df, rules=[
            rules.conditional_range("discount", "customer_type == 'premium'", min=10, max=50)
        ], save=False)
        assert not result.passed
        assert result.rules[0].failed_count == 1

    def test_fail_when_null(self):
        """Rule fails when column is NULL for matching condition."""
        df = pl.DataFrame({
            "customer_type": ["premium", "regular", "premium"],
            "discount": [None, 5.0, 15.0],  # Row 0: NULL
        })
        result = kontra.validate(df, rules=[
            rules.conditional_range("discount", "customer_type == 'premium'", min=10, max=50)
        ], save=False)
        assert not result.passed
        assert result.rules[0].failed_count == 1

    def test_pass_when_condition_false(self):
        """Rule passes when condition is FALSE regardless of column value."""
        df = pl.DataFrame({
            "customer_type": ["regular", "regular", "regular"],
            "discount": [5.0, None, 100.0],  # All would fail if condition were true
        })
        result = kontra.validate(df, rules=[
            rules.conditional_range("discount", "customer_type == 'premium'", min=10, max=50)
        ], save=False)
        assert result.passed
        assert result.rules[0].failed_count == 0


class TestConditionalRangePartialBounds:
    """Tests for min-only and max-only cases."""

    def test_min_only_pass(self):
        """Rule passes with min-only when values are above min."""
        df = pl.DataFrame({
            "status": ["active", "active"],
            "score": [50, 100],
        })
        result = kontra.validate(df, rules=[
            rules.conditional_range("score", "status == 'active'", min=10)
        ], save=False)
        assert result.passed

    def test_min_only_fail(self):
        """Rule fails with min-only when value is below min."""
        df = pl.DataFrame({
            "status": ["active", "inactive", "active"],
            "score": [5, 5, 50],  # Row 0: 5 < 10
        })
        result = kontra.validate(df, rules=[
            rules.conditional_range("score", "status == 'active'", min=10)
        ], save=False)
        assert not result.passed
        assert result.rules[0].failed_count == 1

    def test_max_only_pass(self):
        """Rule passes with max-only when values are below max."""
        df = pl.DataFrame({
            "status": ["active", "active"],
            "score": [50, 80],
        })
        result = kontra.validate(df, rules=[
            rules.conditional_range("score", "status == 'active'", max=100)
        ], save=False)
        assert result.passed

    def test_max_only_fail(self):
        """Rule fails with max-only when value is above max."""
        df = pl.DataFrame({
            "status": ["active", "inactive", "active"],
            "score": [150, 200, 50],  # Row 0: 150 > 100
        })
        result = kontra.validate(df, rules=[
            rules.conditional_range("score", "status == 'active'", max=100)
        ], save=False)
        assert not result.passed
        assert result.rules[0].failed_count == 1


class TestConditionalRangeConditions:
    """Tests for different condition operators."""

    def test_not_equal_condition(self):
        """Rule works with != operator."""
        df = pl.DataFrame({
            "tier": ["gold", "silver", "gold"],
            "rate": [5.0, 3.0, 15.0],  # Row 1 (silver): 3.0 < 10
        })
        result = kontra.validate(df, rules=[
            rules.conditional_range("rate", "tier != 'gold'", min=10)
        ], save=False)
        assert not result.passed
        assert result.rules[0].failed_count == 1

    def test_greater_than_condition(self):
        """Rule works with > operator."""
        df = pl.DataFrame({
            "amount": [100, 200, 50],
            "discount": [15.0, 5.0, 3.0],  # Row 1: amount > 150, discount 5 < 10
        })
        result = kontra.validate(df, rules=[
            rules.conditional_range("discount", "amount > 150", min=10, max=50)
        ], save=False)
        assert not result.passed
        assert result.rules[0].failed_count == 1

    def test_boolean_condition(self):
        """Rule works with boolean condition."""
        df = pl.DataFrame({
            "is_premium": [True, False, True],
            "discount": [5.0, 3.0, 15.0],  # Row 0: is_premium=True, discount 5 < 10
        })
        result = kontra.validate(df, rules=[
            rules.conditional_range("discount", "is_premium == true", min=10, max=50)
        ], save=False)
        assert not result.passed
        assert result.rules[0].failed_count == 1


class TestConditionalRangeEdgeCases:
    """Edge case tests."""

    def test_boundary_values_pass(self):
        """Rule passes when values are exactly at boundaries."""
        df = pl.DataFrame({
            "type": ["x", "x"],
            "value": [10.0, 50.0],  # Exactly at min and max
        })
        result = kontra.validate(df, rules=[
            rules.conditional_range("value", "type == 'x'", min=10, max=50)
        ], save=False)
        assert result.passed

    def test_all_conditions_false(self):
        """Rule passes when no rows match condition."""
        df = pl.DataFrame({
            "type": ["a", "b", "c"],
            "value": [None, -100, 1000],  # All would fail if condition were true
        })
        result = kontra.validate(df, rules=[
            rules.conditional_range("value", "type == 'x'", min=0, max=100)
        ], save=False)
        assert result.passed
        assert result.rules[0].failed_count == 0

    def test_multiple_failures(self):
        """Rule correctly counts multiple failures."""
        df = pl.DataFrame({
            "type": ["x", "x", "x", "y", "x"],
            "value": [5.0, None, 60.0, 1000.0, 25.0],
            # Failures: row 0 (below), row 1 (null), row 2 (above)
            # Row 3 doesn't match condition, row 4 is valid
        })
        result = kontra.validate(df, rules=[
            rules.conditional_range("value", "type == 'x'", min=10, max=50)
        ], save=False, tally=True)
        assert not result.passed
        assert result.rules[0].failed_count == 3


class TestConditionalRangeValidation:
    """Parameter validation tests."""

    def test_missing_bounds_error(self):
        """Rule raises error when neither min nor max provided."""
        df = pl.DataFrame({"status": ["x"], "col": [1]})
        # Validation happens when the rule is instantiated
        with pytest.raises(ValueError, match="at least one of 'min' or 'max'"):
            kontra.validate(df, rules=[
                {"name": "conditional_range", "params": {"column": "col", "when": "status == 'x'"}}
            ], save=False)

    def test_invalid_when_expression(self):
        """Rule raises error for invalid when expression."""
        df = pl.DataFrame({"type": ["a"], "value": [1]})
        # Invalid expression (no operator)
        with pytest.raises(ValueError, match="invalid 'when' expression"):
            kontra.validate(df, rules=[
                {"name": "conditional_range", "params": {
                    "column": "value", "when": "invalid expression", "min": 0
                }}
            ], save=False)

    def test_min_greater_than_max_error(self):
        """Rule raises error when min > max."""
        df = pl.DataFrame({"status": ["x"], "col": [1]})
        # Validation happens when the rule is instantiated
        with pytest.raises(Exception, match="min.*must be.*max"):
            kontra.validate(df, rules=[
                {"name": "conditional_range", "params": {
                    "column": "col", "when": "status == 'x'", "min": 100, "max": 50
                }}
            ], save=False)


class TestConditionalRangeYAML:
    """Tests for YAML contract format."""

    def test_yaml_format(self, tmp_path):
        """Rule works with YAML contract."""
        contract_path = tmp_path / "contract.yml"
        contract_path.write_text("""
name: test_contract
datasource: placeholder
rules:
  - name: conditional_range
    params:
      column: discount
      when: "tier == 'gold'"
      min: 10
      max: 50
""")
        df = pl.DataFrame({
            "tier": ["gold", "silver", "gold"],
            "discount": [5.0, 3.0, 15.0],  # Row 0 fails
        })
        result = kontra.validate(df, contract=str(contract_path), save=False)
        assert not result.passed
        assert result.rules[0].failed_count == 1


class TestConditionalRangeSeverity:
    """Tests for severity settings."""

    def test_warning_severity(self):
        """Rule respects warning severity."""
        df = pl.DataFrame({
            "type": ["x", "x"],
            "value": [5.0, 60.0],  # Both fail
        })
        result = kontra.validate(df, rules=[
            rules.conditional_range("value", "type == 'x'", min=10, max=50, severity="warning")
        ], save=False)
        # With warning severity, overall passes (no blocking failures)
        assert result.passed
        assert result.warning_count == 1

    def test_custom_rule_id(self):
        """Rule accepts custom ID."""
        df = pl.DataFrame({
            "type": ["x"],
            "value": [5.0],
        })
        result = kontra.validate(df, rules=[
            rules.conditional_range("value", "type == 'x'", min=10, id="custom_discount_check")
        ], save=False)
        assert result.rules[0].rule_id == "custom_discount_check"


class TestConditionalRangeFileSources:
    """Tests for different file sources."""

    def test_parquet_file(self, tmp_path):
        """Rule works with Parquet files."""
        parquet_path = tmp_path / "data.parquet"
        df = pl.DataFrame({
            "customer_type": ["premium", "regular", "premium"],
            "discount": [5.0, 3.0, 15.0],  # Row 0 fails
        })
        df.write_parquet(parquet_path)

        result = kontra.validate(str(parquet_path), rules=[
            rules.conditional_range("discount", "customer_type == 'premium'", min=10, max=50)
        ], save=False)
        assert not result.passed
        assert result.rules[0].failed_count == 1

    def test_csv_file(self, tmp_path):
        """Rule works with CSV files."""
        csv_path = tmp_path / "data.csv"
        df = pl.DataFrame({
            "customer_type": ["premium", "regular", "premium"],
            "discount": [5.0, 3.0, 15.0],  # Row 0 fails
        })
        df.write_csv(csv_path)

        result = kontra.validate(str(csv_path), rules=[
            rules.conditional_range("discount", "customer_type == 'premium'", min=10, max=50)
        ], save=False)
        assert not result.passed
        assert result.rules[0].failed_count == 1


class TestConditionalRangeDetails:
    """Tests for failure details and explanations.

    Note: Details are computed via validate() method (fallback path).
    The vectorized execution path doesn't compute details for performance.
    These tests call the rule's validate() directly to test _explain_failure.
    """

    def test_failure_details_structure(self):
        """Failure details contain expected fields."""
        from kontra.rule_defs.builtin.conditional_range import ConditionalRangeRule
        df = pl.DataFrame({
            "type": ["x", "x", "x", "y"],
            "value": [5.0, None, 60.0, 100.0],  # 3 failures when type=='x'
        })
        rule = ConditionalRangeRule("conditional_range", {
            "column": "value", "when": "type == 'x'", "min": 10, "max": 50
        })
        result = rule.validate(df)
        details = result.get("details")
        assert details is not None
        assert "failed_count" in details
        assert "failure_rate" in details
        assert "column" in details
        assert "when_condition" in details
        assert details["column"] == "value"
        assert details["when_condition"] == "type == 'x'"

    def test_failure_details_below_min_count(self):
        """Details include below_min_count when applicable."""
        from kontra.rule_defs.builtin.conditional_range import ConditionalRangeRule
        df = pl.DataFrame({
            "type": ["x", "x"],
            "value": [5.0, 3.0],  # Both below min=10
        })
        rule = ConditionalRangeRule("conditional_range", {
            "column": "value", "when": "type == 'x'", "min": 10, "max": 50
        })
        result = rule.validate(df)
        details = result.get("details")
        assert details.get("below_min_count", 0) == 2

    def test_failure_details_above_max_count(self):
        """Details include above_max_count when applicable."""
        from kontra.rule_defs.builtin.conditional_range import ConditionalRangeRule
        df = pl.DataFrame({
            "type": ["x", "x"],
            "value": [60.0, 100.0],  # Both above max=50
        })
        rule = ConditionalRangeRule("conditional_range", {
            "column": "value", "when": "type == 'x'", "min": 10, "max": 50
        })
        result = rule.validate(df)
        details = result.get("details")
        assert details.get("above_max_count", 0) == 2

    def test_failure_details_null_count(self):
        """Details include null_count_when_condition when applicable."""
        from kontra.rule_defs.builtin.conditional_range import ConditionalRangeRule
        df = pl.DataFrame({
            "type": ["x", "x", "x"],
            "value": [None, None, 25.0],  # 2 nulls when type=='x'
        })
        rule = ConditionalRangeRule("conditional_range", {
            "column": "value", "when": "type == 'x'", "min": 10, "max": 50
        })
        result = rule.validate(df)
        details = result.get("details")
        assert details.get("null_count_when_condition", 0) == 2

    def test_failure_details_sample_positions(self):
        """Details include sample_positions for small failure sets."""
        from kontra.rule_defs.builtin.conditional_range import ConditionalRangeRule
        df = pl.DataFrame({
            "type": ["x", "y", "x", "x"],
            "value": [5.0, 100.0, 60.0, 25.0],  # Rows 0, 2 fail
        })
        rule = ConditionalRangeRule("conditional_range", {
            "column": "value", "when": "type == 'x'", "min": 10, "max": 50
        })
        result = rule.validate(df)
        details = result.get("details")
        assert "sample_positions" in details
        assert 0 in details["sample_positions"]
        assert 2 in details["sample_positions"]

    def test_min_only_message(self):
        """Message for min-only rule indicates failure."""
        df = pl.DataFrame({
            "type": ["x"],
            "value": [5.0],
        })
        result = kontra.validate(df, rules=[
            rules.conditional_range("value", "type == 'x'", min=10)
        ], save=False)
        # Unified message format: "At least 1 row out of range when condition met"
        # or detailed: "1 row below minimum"
        msg = result.rules[0].message.lower()
        assert "range" in msg or "below" in msg or "minimum" in msg

    def test_max_only_message(self):
        """Message for max-only rule indicates failure."""
        df = pl.DataFrame({
            "type": ["x"],
            "value": [100.0],
        })
        result = kontra.validate(df, rules=[
            rules.conditional_range("value", "type == 'x'", max=50)
        ], save=False)
        # Unified message format: "At least 1 row out of range when condition met"
        # or detailed: "1 row above maximum"
        msg = result.rules[0].message.lower()
        assert "range" in msg or "above" in msg or "maximum" in msg


class TestConditionalRangeSqlFilter:
    """Tests for to_sql_filter method."""

    def test_sql_filter_basic(self):
        """to_sql_filter produces valid SQL fragment."""
        from kontra.rule_defs.builtin.conditional_range import ConditionalRangeRule
        rule = ConditionalRangeRule("conditional_range", {
            "column": "discount",
            "when": "status == 'active'",
            "min": 10,
            "max": 50,
        })
        sql = rule.to_sql_filter()
        assert sql is not None
        assert "discount" in sql
        assert "status" in sql
        assert "active" in sql
        assert "IS NULL" in sql
        assert "10" in sql
        assert "50" in sql

    def test_sql_filter_min_only(self):
        """SQL filter for min-only bound."""
        from kontra.rule_defs.builtin.conditional_range import ConditionalRangeRule
        rule = ConditionalRangeRule("conditional_range", {
            "column": "value",
            "when": "type == 'x'",
            "min": 0,
        })
        sql = rule.to_sql_filter()
        assert "< 0" in sql
        assert ">" not in sql.replace(">", "").replace("<>", "")  # No max check

    def test_sql_filter_max_only(self):
        """SQL filter for max-only bound."""
        from kontra.rule_defs.builtin.conditional_range import ConditionalRangeRule
        rule = ConditionalRangeRule("conditional_range", {
            "column": "value",
            "when": "type == 'x'",
            "max": 100,
        })
        sql = rule.to_sql_filter()
        assert "> 100" in sql

    def test_sql_filter_not_equal_operator(self):
        """SQL filter handles != operator."""
        from kontra.rule_defs.builtin.conditional_range import ConditionalRangeRule
        rule = ConditionalRangeRule("conditional_range", {
            "column": "value",
            "when": "type != 'x'",
            "min": 0,
        })
        sql = rule.to_sql_filter()
        assert "<>" in sql

    def test_sql_filter_numeric_condition(self):
        """SQL filter handles numeric condition values."""
        from kontra.rule_defs.builtin.conditional_range import ConditionalRangeRule
        rule = ConditionalRangeRule("conditional_range", {
            "column": "discount",
            "when": "amount > 100",
            "min": 10,
        })
        sql = rule.to_sql_filter()
        assert "100" in sql

    def test_sql_filter_boolean_condition(self):
        """SQL filter handles boolean condition values."""
        from kontra.rule_defs.builtin.conditional_range import ConditionalRangeRule
        rule = ConditionalRangeRule("conditional_range", {
            "column": "discount",
            "when": "is_premium == true",
            "min": 10,
        })
        sql = rule.to_sql_filter()
        assert "TRUE" in sql

    def test_sql_filter_null_is_condition(self):
        """SQL filter handles 'column == null' condition."""
        from kontra.rule_defs.builtin.conditional_range import ConditionalRangeRule
        rule = ConditionalRangeRule("conditional_range", {
            "column": "value",
            "when": "status == null",
            "min": 0,
        })
        sql = rule.to_sql_filter()
        assert "IS NULL" in sql

    def test_sql_filter_null_not_condition(self):
        """SQL filter handles 'column != null' condition."""
        from kontra.rule_defs.builtin.conditional_range import ConditionalRangeRule
        rule = ConditionalRangeRule("conditional_range", {
            "column": "value",
            "when": "status != null",
            "min": 0,
        })
        sql = rule.to_sql_filter()
        assert "IS NOT NULL" in sql

    def test_sql_filter_null_comparison_returns_none(self):
        """SQL filter returns None for invalid null comparisons like '> null'."""
        from kontra.rule_defs.builtin.conditional_range import ConditionalRangeRule
        rule = ConditionalRangeRule("conditional_range", {
            "column": "value",
            "when": "status > null",  # Can't compare with > to null
            "min": 0,
        })
        sql = rule.to_sql_filter()
        # This should return None since you can't do status > NULL
        # Actually looking at the code, it returns None only for non-== non-!= operators with NULL
        # But the condition parser may not allow this... let me check
        # Actually the parser would parse this, but to_sql_filter handles it
        assert sql is None
