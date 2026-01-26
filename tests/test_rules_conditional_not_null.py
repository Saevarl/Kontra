# tests/test_rules_conditional_not_null.py
"""Tests for the conditional_not_null rule."""

import pytest
import polars as pl

from kontra.rule_defs.builtin.conditional_not_null import ConditionalNotNullRule
from kontra.rule_defs.condition_parser import parse_condition, condition_to_sql, ConditionParseError


class TestConditionParser:
    """Tests for the condition parser."""

    def test_parse_string_equal(self):
        """Test parsing string equality condition."""
        col, op, val = parse_condition("status == 'shipped'")
        assert col == "status"
        assert op == "=="
        assert val == "shipped"

    def test_parse_string_not_equal(self):
        """Test parsing string inequality condition."""
        col, op, val = parse_condition("category != 'test'")
        assert col == "category"
        assert op == "!="
        assert val == "test"

    def test_parse_integer_greater(self):
        """Test parsing integer comparison."""
        col, op, val = parse_condition("amount > 100")
        assert col == "amount"
        assert op == ">"
        assert val == 100

    def test_parse_integer_less_equal(self):
        """Test parsing integer comparison with <=."""
        col, op, val = parse_condition("score <= 50")
        assert col == "score"
        assert op == "<="
        assert val == 50

    def test_parse_float(self):
        """Test parsing float value."""
        col, op, val = parse_condition("price >= 99.99")
        assert col == "price"
        assert op == ">="
        assert val == 99.99

    def test_parse_negative_number(self):
        """Test parsing negative number."""
        col, op, val = parse_condition("balance < -100")
        assert col == "balance"
        assert op == "<"
        assert val == -100

    def test_parse_boolean_true(self):
        """Test parsing boolean true."""
        col, op, val = parse_condition("is_active == true")
        assert col == "is_active"
        assert op == "=="
        assert val is True

    def test_parse_boolean_false(self):
        """Test parsing boolean false."""
        col, op, val = parse_condition("is_deleted == false")
        assert col == "is_deleted"
        assert op == "=="
        assert val is False

    def test_parse_null(self):
        """Test parsing null value."""
        col, op, val = parse_condition("parent_id == null")
        assert col == "parent_id"
        assert op == "=="
        assert val is None

    def test_parse_double_quoted_string(self):
        """Test parsing double-quoted string."""
        col, op, val = parse_condition('status == "completed"')
        assert col == "status"
        assert op == "=="
        assert val == "completed"

    def test_parse_with_whitespace(self):
        """Test parsing with extra whitespace."""
        col, op, val = parse_condition("  status   ==   'shipped'  ")
        assert col == "status"
        assert op == "=="
        assert val == "shipped"

    def test_parse_underscore_column(self):
        """Test parsing column with underscores."""
        col, op, val = parse_condition("shipping_date != null")
        assert col == "shipping_date"
        assert op == "!="
        assert val is None

    def test_parse_invalid_empty(self):
        """Test parsing empty string raises error."""
        with pytest.raises(ConditionParseError):
            parse_condition("")

    def test_parse_invalid_no_operator(self):
        """Test parsing without operator raises error."""
        with pytest.raises(ConditionParseError):
            parse_condition("status 'shipped'")

    def test_parse_invalid_operator(self):
        """Test parsing with invalid operator raises error."""
        with pytest.raises(ConditionParseError):
            parse_condition("status ~= 'shipped'")


class TestConditionToSql:
    """Tests for condition_to_sql function."""

    def test_string_equal(self):
        """Test SQL generation for string equality."""
        sql = condition_to_sql("status", "==", "shipped", "duckdb")
        assert '"status" = ' in sql
        assert "'shipped'" in sql

    def test_string_not_equal(self):
        """Test SQL generation for string inequality."""
        sql = condition_to_sql("status", "!=", "deleted", "duckdb")
        assert '"status" <> ' in sql
        assert "'deleted'" in sql

    def test_integer_comparison(self):
        """Test SQL generation for integer comparison."""
        sql = condition_to_sql("amount", ">", 100, "duckdb")
        assert '"amount" > 100' in sql

    def test_float_comparison(self):
        """Test SQL generation for float comparison."""
        sql = condition_to_sql("price", ">=", 99.99, "duckdb")
        assert '"price" >= 99.99' in sql

    def test_boolean_true(self):
        """Test SQL generation for boolean true."""
        sql = condition_to_sql("is_active", "==", True, "duckdb")
        assert '"is_active" = TRUE' in sql

    def test_boolean_false(self):
        """Test SQL generation for boolean false."""
        sql = condition_to_sql("is_deleted", "==", False, "duckdb")
        assert '"is_deleted" = FALSE' in sql

    def test_null_equal(self):
        """Test SQL generation for NULL equality."""
        sql = condition_to_sql("parent_id", "==", None, "duckdb")
        assert '"parent_id" IS NULL' in sql

    def test_null_not_equal(self):
        """Test SQL generation for NULL inequality."""
        sql = condition_to_sql("parent_id", "!=", None, "duckdb")
        assert '"parent_id" IS NOT NULL' in sql

    def test_postgres_dialect(self):
        """Test SQL generation for PostgreSQL."""
        sql = condition_to_sql("status", "==", "shipped", "postgres")
        assert '"status" = ' in sql  # PostgreSQL uses double quotes

    def test_sqlserver_dialect(self):
        """Test SQL generation for SQL Server."""
        sql = condition_to_sql("status", "==", "shipped", "sqlserver")
        assert "[status] = " in sql  # SQL Server uses brackets


class TestConditionalNotNullRule:
    """Tests for ConditionalNotNullRule."""

    def test_passes_when_condition_false(self):
        """Test passes when condition is false (regardless of column value)."""
        df = pl.DataFrame({
            "status": ["pending", "pending", "pending"],
            "shipping_date": [None, None, None],  # All NULL but condition never true
        })
        rule = ConditionalNotNullRule(
            "conditional_not_null",
            {"column": "shipping_date", "when": "status == 'shipped'"}
        )
        result = rule.validate(df)
        assert result["passed"] is True
        assert result["failed_count"] == 0

    def test_passes_when_not_null(self):
        """Test passes when condition is true and column is not null."""
        df = pl.DataFrame({
            "status": ["shipped", "shipped", "shipped"],
            "shipping_date": ["2024-01-15", "2024-01-16", "2024-01-17"],
        })
        rule = ConditionalNotNullRule(
            "conditional_not_null",
            {"column": "shipping_date", "when": "status == 'shipped'"}
        )
        result = rule.validate(df)
        assert result["passed"] is True
        assert result["failed_count"] == 0

    def test_fails_when_condition_true_and_null(self):
        """Test fails when condition is true and column is null."""
        df = pl.DataFrame({
            "status": ["shipped", "pending", "shipped"],
            "shipping_date": ["2024-01-15", None, None],  # Row 3 fails
        })
        rule = ConditionalNotNullRule(
            "conditional_not_null",
            {"column": "shipping_date", "when": "status == 'shipped'"}
        )
        result = rule.validate(df)
        assert result["passed"] is False
        assert result["failed_count"] == 1

    def test_multiple_failures(self):
        """Test counting multiple failures."""
        df = pl.DataFrame({
            "status": ["shipped", "shipped", "shipped", "pending"],
            "shipping_date": [None, None, "2024-01-17", None],  # Rows 1, 2 fail
        })
        rule = ConditionalNotNullRule(
            "conditional_not_null",
            {"column": "shipping_date", "when": "status == 'shipped'"}
        )
        result = rule.validate(df)
        assert result["passed"] is False
        assert result["failed_count"] == 2

    def test_with_numeric_condition(self):
        """Test with numeric condition value."""
        df = pl.DataFrame({
            "priority": [1, 2, 1, 2],
            "assignee": ["Alice", None, None, "Bob"],  # Row 3 fails (priority 1 but null)
        })
        rule = ConditionalNotNullRule(
            "conditional_not_null",
            {"column": "assignee", "when": "priority == 1"}
        )
        result = rule.validate(df)
        assert result["passed"] is False
        assert result["failed_count"] == 1

    def test_with_greater_than_condition(self):
        """Test with > condition."""
        df = pl.DataFrame({
            "amount": [100, 500, 1000, 50],
            "approval_id": [None, "A001", None, None],  # Row 3 fails (1000 > 100 but null)
        })
        rule = ConditionalNotNullRule(
            "conditional_not_null",
            {"column": "approval_id", "when": "amount > 100"}
        )
        result = rule.validate(df)
        assert result["passed"] is False
        assert result["failed_count"] == 1  # Only row 3: 100 is not > 100

    def test_with_boolean_condition(self):
        """Test with boolean condition value."""
        df = pl.DataFrame({
            "is_premium": [True, False, True, True],
            "discount_code": ["VIP10", None, None, "VIP20"],  # Row 3 fails
        })
        rule = ConditionalNotNullRule(
            "conditional_not_null",
            {"column": "discount_code", "when": "is_premium == true"}
        )
        result = rule.validate(df)
        assert result["passed"] is False
        assert result["failed_count"] == 1

    def test_with_not_equal_condition(self):
        """Test with != condition."""
        df = pl.DataFrame({
            "status": ["active", "deleted", "active", "active"],
            "last_activity": ["2024-01-15", None, None, "2024-01-17"],  # Row 3 fails
        })
        rule = ConditionalNotNullRule(
            "conditional_not_null",
            {"column": "last_activity", "when": "status != 'deleted'"}
        )
        result = rule.validate(df)
        assert result["passed"] is False
        assert result["failed_count"] == 1

    def test_condition_column_with_null(self):
        """Test when the condition column itself has NULL."""
        df = pl.DataFrame({
            "status": ["shipped", None, "shipped"],
            "shipping_date": ["2024-01-15", None, None],  # Row 3 fails; Row 2 condition is NULL (not = 'shipped')
        })
        rule = ConditionalNotNullRule(
            "conditional_not_null",
            {"column": "shipping_date", "when": "status == 'shipped'"}
        )
        result = rule.validate(df)
        assert result["passed"] is False
        assert result["failed_count"] == 1  # Only row 3

    def test_invalid_when_expression(self):
        """Test that invalid when expression raises ValueError."""
        with pytest.raises(ValueError, match="invalid 'when' expression"):
            ConditionalNotNullRule(
                "conditional_not_null",
                {"column": "shipping_date", "when": "invalid"}
            )

    def test_missing_column_param(self):
        """Test that missing column param raises ValueError."""
        with pytest.raises(ValueError, match="requires parameter 'column'"):
            ConditionalNotNullRule(
                "conditional_not_null",
                {"when": "status == 'shipped'"}
            )

    def test_missing_when_param(self):
        """Test that missing when param raises ValueError."""
        with pytest.raises(ValueError, match="requires parameter 'when'"):
            ConditionalNotNullRule(
                "conditional_not_null",
                {"column": "shipping_date"}
            )

    def test_required_columns(self):
        """Test that required_columns returns both columns."""
        rule = ConditionalNotNullRule(
            "conditional_not_null",
            {"column": "shipping_date", "when": "status == 'shipped'"}
        )
        assert rule.required_columns() == {"shipping_date", "status"}

    def test_compile_predicate(self):
        """Test that compile_predicate returns valid predicate."""
        rule = ConditionalNotNullRule(
            "conditional_not_null",
            {"column": "shipping_date", "when": "status == 'shipped'"}
        )
        rule.rule_id = "test_conditional"
        pred = rule.compile_predicate()

        assert pred is not None
        assert pred.rule_id == "test_conditional"
        assert pred.columns == {"shipping_date", "status"}
        assert "when" in pred.message

    def test_to_sql_spec(self):
        """Test that to_sql_spec returns valid spec."""
        rule = ConditionalNotNullRule(
            "conditional_not_null",
            {"column": "shipping_date", "when": "status == 'shipped'"}
        )
        rule.rule_id = "test_conditional"
        spec = rule.to_sql_spec()

        assert spec is not None
        assert spec["kind"] == "conditional_not_null"
        assert spec["rule_id"] == "test_conditional"
        assert spec["column"] == "shipping_date"
        assert spec["when_column"] == "status"
        assert spec["when_op"] == "=="
        assert spec["when_value"] == "shipped"


class TestConditionalNotNullRuleAPI:
    """Tests for conditional_not_null rule via Python API."""

    def test_rules_helper(self):
        """Test rules.conditional_not_null() helper creates correct dict."""
        from kontra.api.rules import conditional_not_null

        rule_dict = conditional_not_null("shipping_date", "status == 'shipped'")
        assert rule_dict["name"] == "conditional_not_null"
        assert rule_dict["params"]["column"] == "shipping_date"
        assert rule_dict["params"]["when"] == "status == 'shipped'"
        assert rule_dict["severity"] == "blocking"

    def test_rules_helper_with_severity(self):
        """Test rules.conditional_not_null() helper with custom severity."""
        from kontra.api.rules import conditional_not_null

        rule_dict = conditional_not_null("col", "x > 0", severity="warning")
        assert rule_dict["severity"] == "warning"

    def test_rules_helper_with_id(self):
        """Test rules.conditional_not_null() helper with custom ID."""
        from kontra.api.rules import conditional_not_null

        rule_dict = conditional_not_null("col", "x > 0", id="custom_id")
        assert rule_dict["id"] == "custom_id"

    def test_rules_module(self):
        """Test rules.conditional_not_null() via module import."""
        from kontra import rules

        rule_dict = rules.conditional_not_null("shipping_date", "status == 'shipped'")
        assert rule_dict["name"] == "conditional_not_null"


class TestConditionalNotNullRuleIntegration:
    """Integration tests for conditional_not_null rule with kontra.validate()."""

    def test_validate_passes(self):
        """Test conditional_not_null passes with kontra.validate()."""
        import kontra
        from kontra import rules

        df = pl.DataFrame({
            "status": ["shipped", "pending", "shipped"],
            "shipping_date": ["2024-01-15", None, "2024-01-17"],
        })

        result = kontra.validate(df, rules=[
            rules.conditional_not_null("shipping_date", "status == 'shipped'"),
        ])

        assert result.passed is True
        assert len(result.rules) == 1
        assert result.rules[0].passed is True

    def test_validate_fails(self):
        """Test conditional_not_null failure with kontra.validate()."""
        import kontra
        from kontra import rules

        df = pl.DataFrame({
            "status": ["shipped", "pending", "shipped"],
            "shipping_date": ["2024-01-15", None, None],  # Row 3 fails
        })

        result = kontra.validate(df, rules=[
            rules.conditional_not_null("shipping_date", "status == 'shipped'"),
        ])

        assert result.passed is False
        assert result.rules[0].passed is False
        assert result.rules[0].failed_count == 1


class TestConditionalNotNullSqlUtils:
    """Tests for SQL utility functions for conditional_not_null rule."""

    def test_agg_conditional_not_null_string_condition(self):
        """Test agg_conditional_not_null with string condition."""
        from kontra.engine.sql_utils import agg_conditional_not_null

        sql = agg_conditional_not_null(
            "shipping_date", "status", "==", "shipped", "rule_1", "duckdb"
        )
        assert '"shipping_date"' in sql
        assert '"status"' in sql
        assert "'shipped'" in sql
        assert 'IS NULL' in sql
        assert '"rule_1"' in sql

    def test_agg_conditional_not_null_numeric_condition(self):
        """Test agg_conditional_not_null with numeric condition."""
        from kontra.engine.sql_utils import agg_conditional_not_null

        sql = agg_conditional_not_null(
            "approval_id", "amount", ">", 100, "rule_1", "duckdb"
        )
        assert '"approval_id"' in sql
        assert '"amount"' in sql
        assert '> 100' in sql
        assert 'IS NULL' in sql

    def test_agg_conditional_not_null_null_condition(self):
        """Test agg_conditional_not_null with NULL condition value."""
        from kontra.engine.sql_utils import agg_conditional_not_null

        # When checking for NULL equality
        sql = agg_conditional_not_null(
            "child_id", "parent_id", "==", None, "rule_1", "duckdb"
        )
        assert '"child_id"' in sql
        assert '"parent_id" IS NULL' in sql

    def test_agg_conditional_not_null_postgres(self):
        """Test agg_conditional_not_null generates valid PostgreSQL SQL."""
        from kontra.engine.sql_utils import agg_conditional_not_null

        sql = agg_conditional_not_null(
            "col", "status", "==", "active", "rule_1", "postgres"
        )
        assert '"col"' in sql  # PostgreSQL uses double quotes
        assert '"status"' in sql
        assert "'active'" in sql

    def test_agg_conditional_not_null_sqlserver(self):
        """Test agg_conditional_not_null generates valid SQL Server SQL."""
        from kontra.engine.sql_utils import agg_conditional_not_null

        sql = agg_conditional_not_null(
            "col", "status", "==", "active", "rule_1", "sqlserver"
        )
        assert '[col]' in sql  # SQL Server uses brackets
        assert '[status]' in sql

    def test_agg_conditional_not_null_boolean(self):
        """Test agg_conditional_not_null with boolean condition."""
        from kontra.engine.sql_utils import agg_conditional_not_null

        sql = agg_conditional_not_null(
            "discount", "is_premium", "==", True, "rule_1", "duckdb"
        )
        assert '"discount"' in sql
        assert '"is_premium"' in sql
        assert 'TRUE' in sql
