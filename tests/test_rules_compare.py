# tests/test_rules_compare.py
"""Tests for the compare rule."""

import pytest
import polars as pl

from kontra.rule_defs.builtin.compare import CompareRule, SUPPORTED_OPS


class TestCompareRule:
    """Tests for CompareRule."""

    def test_compare_greater_than_passes(self):
        """Test compare with > operator passes when condition is met."""
        df = pl.DataFrame({
            "a": [10, 20, 30],
            "b": [5, 10, 15],
        })
        rule = CompareRule("compare", {"left": "a", "right": "b", "op": ">"})
        result = rule.validate(df)
        assert result["passed"] is True
        assert result["failed_count"] == 0

    def test_compare_greater_than_fails(self):
        """Test compare with > operator fails when condition is not met."""
        df = pl.DataFrame({
            "a": [10, 5, 30],  # 5 is not > 10
            "b": [5, 10, 15],
        })
        rule = CompareRule("compare", {"left": "a", "right": "b", "op": ">"})
        result = rule.validate(df)
        assert result["passed"] is False
        assert result["failed_count"] == 1

    def test_compare_greater_equal_passes(self):
        """Test compare with >= operator passes when condition is met."""
        df = pl.DataFrame({
            "a": [10, 10, 30],
            "b": [5, 10, 15],
        })
        rule = CompareRule("compare", {"left": "a", "right": "b", "op": ">="})
        result = rule.validate(df)
        assert result["passed"] is True
        assert result["failed_count"] == 0

    def test_compare_greater_equal_fails(self):
        """Test compare with >= operator fails when condition is not met."""
        df = pl.DataFrame({
            "a": [10, 5, 30],  # 5 is not >= 10
            "b": [5, 10, 15],
        })
        rule = CompareRule("compare", {"left": "a", "right": "b", "op": ">="})
        result = rule.validate(df)
        assert result["passed"] is False
        assert result["failed_count"] == 1

    def test_compare_less_than_passes(self):
        """Test compare with < operator passes when condition is met."""
        df = pl.DataFrame({
            "a": [1, 2, 3],
            "b": [5, 10, 15],
        })
        rule = CompareRule("compare", {"left": "a", "right": "b", "op": "<"})
        result = rule.validate(df)
        assert result["passed"] is True
        assert result["failed_count"] == 0

    def test_compare_less_than_fails(self):
        """Test compare with < operator fails when condition is not met."""
        df = pl.DataFrame({
            "a": [10, 2, 3],  # 10 is not < 5
            "b": [5, 10, 15],
        })
        rule = CompareRule("compare", {"left": "a", "right": "b", "op": "<"})
        result = rule.validate(df)
        assert result["passed"] is False
        assert result["failed_count"] == 1

    def test_compare_less_equal_passes(self):
        """Test compare with <= operator passes when condition is met."""
        df = pl.DataFrame({
            "a": [5, 2, 3],
            "b": [5, 10, 15],
        })
        rule = CompareRule("compare", {"left": "a", "right": "b", "op": "<="})
        result = rule.validate(df)
        assert result["passed"] is True
        assert result["failed_count"] == 0

    def test_compare_equal_passes(self):
        """Test compare with == operator passes when condition is met."""
        df = pl.DataFrame({
            "a": [5, 10, 15],
            "b": [5, 10, 15],
        })
        rule = CompareRule("compare", {"left": "a", "right": "b", "op": "=="})
        result = rule.validate(df)
        assert result["passed"] is True
        assert result["failed_count"] == 0

    def test_compare_equal_fails(self):
        """Test compare with == operator fails when condition is not met."""
        df = pl.DataFrame({
            "a": [5, 11, 15],  # 11 != 10
            "b": [5, 10, 15],
        })
        rule = CompareRule("compare", {"left": "a", "right": "b", "op": "=="})
        result = rule.validate(df)
        assert result["passed"] is False
        assert result["failed_count"] == 1

    def test_compare_not_equal_passes(self):
        """Test compare with != operator passes when condition is met."""
        df = pl.DataFrame({
            "a": [1, 2, 3],
            "b": [5, 10, 15],
        })
        rule = CompareRule("compare", {"left": "a", "right": "b", "op": "!="})
        result = rule.validate(df)
        assert result["passed"] is True
        assert result["failed_count"] == 0

    def test_compare_not_equal_fails(self):
        """Test compare with != operator fails when values are equal."""
        df = pl.DataFrame({
            "a": [5, 2, 3],  # 5 == 5
            "b": [5, 10, 15],
        })
        rule = CompareRule("compare", {"left": "a", "right": "b", "op": "!="})
        result = rule.validate(df)
        assert result["passed"] is False
        assert result["failed_count"] == 1

    def test_compare_null_left_is_failure(self):
        """Test that NULL in left column counts as failure."""
        df = pl.DataFrame({
            "a": [10, None, 30],
            "b": [5, 10, 15],
        })
        rule = CompareRule("compare", {"left": "a", "right": "b", "op": ">"})
        result = rule.validate(df)
        assert result["passed"] is False
        assert result["failed_count"] == 1

    def test_compare_null_right_is_failure(self):
        """Test that NULL in right column counts as failure."""
        df = pl.DataFrame({
            "a": [10, 20, 30],
            "b": [5, None, 15],
        })
        rule = CompareRule("compare", {"left": "a", "right": "b", "op": ">"})
        result = rule.validate(df)
        assert result["passed"] is False
        assert result["failed_count"] == 1

    def test_compare_null_both_is_failure(self):
        """Test that NULL in both columns counts as failure."""
        df = pl.DataFrame({
            "a": [10, None, 30],
            "b": [5, None, 15],
        })
        rule = CompareRule("compare", {"left": "a", "right": "b", "op": ">"})
        result = rule.validate(df)
        assert result["passed"] is False
        assert result["failed_count"] == 1

    def test_compare_multiple_failures(self):
        """Test multiple failure counting."""
        df = pl.DataFrame({
            "a": [1, None, None, 4],  # 1 not > 5, two NULLs
            "b": [5, 10, None, 1],
        })
        rule = CompareRule("compare", {"left": "a", "right": "b", "op": ">"})
        result = rule.validate(df)
        assert result["passed"] is False
        assert result["failed_count"] == 3  # 1 comparison failure + 2 NULL failures

    def test_compare_with_dates(self):
        """Test compare works with date columns."""
        df = pl.DataFrame({
            "start_date": ["2024-01-01", "2024-02-01", "2024-03-01"],
            "end_date": ["2024-01-15", "2024-02-15", "2024-03-15"],
        }).with_columns([
            pl.col("start_date").str.to_date(),
            pl.col("end_date").str.to_date(),
        ])
        rule = CompareRule("compare", {"left": "end_date", "right": "start_date", "op": ">="})
        result = rule.validate(df)
        assert result["passed"] is True
        assert result["failed_count"] == 0

    def test_compare_with_dates_failure(self):
        """Test compare detects date comparison failures."""
        df = pl.DataFrame({
            "start_date": ["2024-01-01", "2024-02-01", "2024-03-01"],
            "end_date": ["2024-01-15", "2024-01-15", "2024-03-15"],  # row 2: end < start
        }).with_columns([
            pl.col("start_date").str.to_date(),
            pl.col("end_date").str.to_date(),
        ])
        rule = CompareRule("compare", {"left": "end_date", "right": "start_date", "op": ">="})
        result = rule.validate(df)
        assert result["passed"] is False
        assert result["failed_count"] == 1

    def test_compare_invalid_operator(self):
        """Test that invalid operator raises ValueError."""
        with pytest.raises(ValueError, match="unsupported operator"):
            CompareRule("compare", {"left": "a", "right": "b", "op": "~="})

    def test_compare_missing_left_param(self):
        """Test that missing left param raises ValueError."""
        with pytest.raises(ValueError, match="requires parameter 'left'"):
            CompareRule("compare", {"right": "b", "op": ">"})

    def test_compare_missing_right_param(self):
        """Test that missing right param raises ValueError."""
        with pytest.raises(ValueError, match="requires parameter 'right'"):
            CompareRule("compare", {"left": "a", "op": ">"})

    def test_compare_missing_op_param(self):
        """Test that missing op param raises ValueError."""
        with pytest.raises(ValueError, match="requires parameter 'op'"):
            CompareRule("compare", {"left": "a", "right": "b"})

    def test_compare_required_columns(self):
        """Test that required_columns returns both columns."""
        rule = CompareRule("compare", {"left": "a", "right": "b", "op": ">"})
        assert rule.required_columns() == {"a", "b"}

    def test_compare_compile_predicate(self):
        """Test that compile_predicate returns valid predicate."""
        rule = CompareRule("compare", {"left": "a", "right": "b", "op": ">"})
        rule.rule_id = "test_compare"
        pred = rule.compile_predicate()

        assert pred is not None
        assert pred.rule_id == "test_compare"
        assert pred.columns == {"a", "b"}
        assert "not greater than" in pred.message

    def test_compare_to_sql_spec(self):
        """Test that to_sql_spec returns valid spec."""
        rule = CompareRule("compare", {"left": "a", "right": "b", "op": ">="})
        rule.rule_id = "test_compare"
        spec = rule.to_sql_spec()

        assert spec is not None
        assert spec["kind"] == "compare"
        assert spec["rule_id"] == "test_compare"
        assert spec["left"] == "a"
        assert spec["right"] == "b"
        assert spec["op"] == ">="

    def test_all_operators_supported(self):
        """Test that all documented operators are supported."""
        expected_ops = {">", ">=", "<", "<=", "==", "!="}
        assert SUPPORTED_OPS == expected_ops


class TestCompareRuleAPI:
    """Tests for compare rule via Python API."""

    def test_rules_compare_helper(self):
        """Test rules.compare() helper creates correct dict."""
        from kontra.api.rules import compare

        rule_dict = compare("end_date", "start_date", ">=")
        assert rule_dict["name"] == "compare"
        assert rule_dict["params"]["left"] == "end_date"
        assert rule_dict["params"]["right"] == "start_date"
        assert rule_dict["params"]["op"] == ">="
        assert rule_dict["severity"] == "blocking"

    def test_rules_compare_helper_with_severity(self):
        """Test rules.compare() helper with custom severity."""
        from kontra.api.rules import compare

        rule_dict = compare("a", "b", ">", severity="warning")
        assert rule_dict["severity"] == "warning"

    def test_rules_compare_helper_with_id(self):
        """Test rules.compare() helper with custom ID."""
        from kontra.api.rules import compare

        rule_dict = compare("a", "b", ">", id="custom_compare_id")
        assert rule_dict["id"] == "custom_compare_id"

    def test_rules_module_compare(self):
        """Test rules.compare() via module import."""
        from kontra import rules

        rule_dict = rules.compare("end_date", "start_date", ">=")
        assert rule_dict["name"] == "compare"


class TestCompareRuleIntegration:
    """Integration tests for compare rule with kontra.validate()."""

    def test_validate_with_compare_rule(self):
        """Test compare rule works with kontra.validate()."""
        import kontra
        from kontra import rules

        df = pl.DataFrame({
            "start": [1, 2, 3],
            "end": [5, 6, 7],
        })

        result = kontra.validate(df, rules=[
            rules.compare("end", "start", ">="),
        ])

        assert result.passed is True
        assert len(result.rules) == 1
        assert result.rules[0].passed is True

    def test_validate_with_compare_rule_failure(self):
        """Test compare rule failure with kontra.validate()."""
        import kontra
        from kontra import rules

        df = pl.DataFrame({
            "start": [1, 10, 3],  # row 2: end (6) < start (10)
            "end": [5, 6, 7],
        })

        result = kontra.validate(df, rules=[
            rules.compare("end", "start", ">="),
        ])

        assert result.passed is False
        assert result.rules[0].passed is False
        assert result.rules[0].failed_count == 1


class TestCompareSqlUtils:
    """Tests for SQL utility functions for compare rule."""

    def test_agg_compare_duckdb(self):
        """Test agg_compare generates valid DuckDB SQL."""
        from kontra.engine.sql_utils import agg_compare

        sql = agg_compare("end_date", "start_date", ">=", "rule_1", "duckdb")
        assert '"end_date"' in sql
        assert '"start_date"' in sql
        assert '>=' in sql
        assert 'IS NULL' in sql
        assert '"rule_1"' in sql

    def test_agg_compare_postgres(self):
        """Test agg_compare generates valid PostgreSQL SQL."""
        from kontra.engine.sql_utils import agg_compare

        sql = agg_compare("a", "b", "==", "rule_1", "postgres")
        assert '"a"' in sql
        assert '"b"' in sql
        assert '=' in sql  # == maps to =
        assert '"rule_1"' in sql

    def test_agg_compare_sqlserver(self):
        """Test agg_compare generates valid SQL Server SQL."""
        from kontra.engine.sql_utils import agg_compare

        sql = agg_compare("a", "b", "!=", "rule_1", "sqlserver")
        assert '[a]' in sql  # SQL Server uses brackets
        assert '[b]' in sql
        assert '<>' in sql  # != maps to <>
        assert '[rule_1]' in sql

    def test_agg_compare_all_operators(self):
        """Test agg_compare generates valid SQL for all operators."""
        from kontra.engine.sql_utils import agg_compare

        operators = [">", ">=", "<", "<=", "==", "!="]
        for op in operators:
            sql = agg_compare("a", "b", op, "rule_1", "duckdb")
            assert 'SUM(CASE WHEN' in sql
            assert 'IS NULL' in sql  # NULL handling
