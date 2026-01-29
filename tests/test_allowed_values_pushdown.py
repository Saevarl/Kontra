# tests/test_allowed_values_pushdown.py
"""Tests for allowed_values SQL pushdown functionality."""

import pytest
import polars as pl

from kontra.rule_defs.builtin.allowed_values import AllowedValuesRule
from kontra.engine.executors.duckdb_sql import DuckDBSqlExecutor
from kontra.engine.executors.postgres_sql import PostgresSqlExecutor
from kontra.engine.executors.sqlserver_sql import SqlServerSqlExecutor
from kontra.engine.sql_utils import agg_allowed_values


def _make_rule(column: str, values: list) -> AllowedValuesRule:
    """Helper to create an AllowedValuesRule with proper rule_id."""
    rule = AllowedValuesRule(
        name="allowed_values",
        params={"column": column, "values": values}
    )
    rule.rule_id = f"COL:{column}:allowed_values"
    return rule


class TestAllowedValuesExecutorSupport:
    """Tests for executor SUPPORTED_RULES."""

    def test_duckdb_executor_supports_allowed_values(self):
        """DuckDB executor includes allowed_values in SUPPORTED_RULES."""
        assert "allowed_values" in DuckDBSqlExecutor.SUPPORTED_RULES

    def test_postgres_executor_supports_allowed_values(self):
        """PostgreSQL executor includes allowed_values in SUPPORTED_RULES."""
        assert "allowed_values" in PostgresSqlExecutor.SUPPORTED_RULES

    def test_sqlserver_executor_supports_allowed_values(self):
        """SQL Server executor includes allowed_values in SUPPORTED_RULES."""
        assert "allowed_values" in SqlServerSqlExecutor.SUPPORTED_RULES


class TestAllowedValuesRuleToSqlSpec:
    """Tests for AllowedValuesRule.to_sql_spec()."""

    def test_to_sql_spec_basic(self):
        """to_sql_spec() returns correct spec structure."""
        rule = _make_rule("status", ["active", "inactive", "pending"])
        spec = rule.to_sql_spec()

        assert spec is not None
        assert spec["kind"] == "allowed_values"
        assert spec["rule_id"] == "COL:status:allowed_values"
        assert spec["column"] == "status"
        assert spec["values"] == ["active", "inactive", "pending"]

    def test_to_sql_spec_numeric_values(self):
        """to_sql_spec() handles numeric values."""
        rule = _make_rule("rating", [1, 2, 3, 4, 5])
        spec = rule.to_sql_spec()

        assert spec is not None
        assert spec["values"] == [1, 2, 3, 4, 5]

    def test_to_sql_spec_mixed_types(self):
        """to_sql_spec() handles mixed value types."""
        rule = _make_rule("value", ["a", 1, True])
        spec = rule.to_sql_spec()

        assert spec is not None
        assert spec["values"] == ["a", 1, True]


class TestAllowedValuesSqlAgg:
    """Tests for agg_allowed_values() SQL generation."""

    def test_agg_allowed_values_duckdb(self):
        """agg_allowed_values generates correct DuckDB SQL."""
        sql = agg_allowed_values("status", ["active", "inactive"], "rule_1", "duckdb")

        assert '"status"' in sql
        assert '"rule_1"' in sql
        assert "NOT IN" in sql
        assert "'active'" in sql
        assert "'inactive'" in sql
        assert "SUM(CASE WHEN" in sql

    def test_agg_allowed_values_postgres(self):
        """agg_allowed_values generates correct Postgres SQL with type cast."""
        sql = agg_allowed_values("status", ["active", "inactive"], "rule_1", "postgres")

        assert '"status"' in sql
        assert "::text" in sql  # Postgres type cast
        assert "NOT IN" in sql

    def test_agg_allowed_values_sqlserver(self):
        """agg_allowed_values generates correct SQL Server SQL with type cast."""
        sql = agg_allowed_values("status", ["active"], "rule_1", "sqlserver")

        assert "[status]" in sql  # SQL Server uses brackets
        assert "CAST(" in sql and "AS NVARCHAR" in sql  # SQL Server type cast
        assert "NOT IN" in sql


class TestDuckDBCompileAllowedValues:
    """Tests for DuckDB executor compile() with allowed_values."""

    def test_compile_allowed_values_default_uses_exists(self):
        """compile() routes allowed_values to EXISTS by default (tally=False)."""
        executor = DuckDBSqlExecutor()
        specs = [
            {
                "kind": "allowed_values",
                "rule_id": "COL:status:allowed_values",
                "column": "status",
                "values": ["active", "inactive", "pending"],
            }
        ]
        compiled = executor.compile(specs)

        # Default tally=False routes to EXISTS
        assert len(compiled["exists_specs"]) == 1
        assert len(compiled["aggregate_selects"]) == 0
        assert len(compiled["supported_specs"]) == 1

    def test_compile_allowed_values_with_tally_uses_aggregate(self):
        """compile() routes allowed_values to aggregate when tally=True."""
        executor = DuckDBSqlExecutor()
        specs = [
            {
                "kind": "allowed_values",
                "rule_id": "COL:status:allowed_values",
                "column": "status",
                "values": ["active", "inactive", "pending"],
                "tally": True,
            }
        ]
        compiled = executor.compile(specs)

        # tally=True routes to aggregate
        assert len(compiled["exists_specs"]) == 0
        assert len(compiled["aggregate_selects"]) == 1
        assert "COL:status:allowed_values" in compiled["aggregate_selects"][0]
        assert len(compiled["supported_specs"]) == 1
        assert len(compiled["aggregate_specs"]) == 1

    def test_compile_allowed_values_multiple(self):
        """compile() handles multiple allowed_values specs with mixed tally."""
        executor = DuckDBSqlExecutor()
        specs = [
            {
                "kind": "allowed_values",
                "rule_id": "COL:status:allowed_values",
                "column": "status",
                "values": ["active", "inactive"],
                # Default tally=False -> EXISTS
            },
            {
                "kind": "allowed_values",
                "rule_id": "COL:type:allowed_values",
                "column": "type",
                "values": ["A", "B", "C"],
                "tally": True,  # tally=True -> aggregate
            },
        ]
        compiled = executor.compile(specs)

        assert len(compiled["exists_specs"]) == 1
        assert len(compiled["aggregate_selects"]) == 1
        assert len(compiled["supported_specs"]) == 2

    def test_compile_mixed_rules_with_allowed_values(self):
        """compile() handles mix of allowed_values with other rules."""
        executor = DuckDBSqlExecutor()
        specs = [
            {"kind": "not_null", "rule_id": "nn_1", "column": "id"},
            {"kind": "min_rows", "rule_id": "mr_1", "threshold": 10},
            {
                "kind": "allowed_values",
                "rule_id": "av_1",
                "column": "status",
                "values": ["active", "inactive"],
            },
        ]
        compiled = executor.compile(specs)

        # not_null and allowed_values (default tally=False) go to exists_specs
        assert len(compiled["exists_specs"]) == 2
        # min_rows (dataset rule, no tally) goes to aggregate_selects
        assert len(compiled["aggregate_selects"]) == 1
        # All three are supported
        assert len(compiled["supported_specs"]) == 3

    def test_compile_allowed_values_missing_column(self):
        """compile() skips specs with missing column."""
        executor = DuckDBSqlExecutor()
        specs = [
            {
                "kind": "allowed_values",
                "rule_id": "av_1",
                "values": ["active", "inactive"],
                # Missing column
            }
        ]
        compiled = executor.compile(specs)

        assert len(compiled["exists_specs"]) == 0
        assert len(compiled["aggregate_selects"]) == 0
        assert len(compiled["supported_specs"]) == 0

    def test_compile_allowed_values_missing_values(self):
        """compile() skips specs with missing values."""
        executor = DuckDBSqlExecutor()
        specs = [
            {
                "kind": "allowed_values",
                "rule_id": "av_1",
                "column": "status",
                # Missing values
            }
        ]
        compiled = executor.compile(specs)

        assert len(compiled["aggregate_selects"]) == 0
        assert len(compiled["supported_specs"]) == 0


class TestPostgresCompileAllowedValues:
    """Tests for Postgres executor compile() with allowed_values."""

    def test_postgres_compile_allowed_values_default_uses_exists(self):
        """PostgreSQL executor routes allowed_values to EXISTS by default (tally=False)."""
        executor = PostgresSqlExecutor()
        specs = [
            {
                "kind": "allowed_values",
                "rule_id": "COL:status:allowed_values",
                "column": "status",
                "values": ["active", "inactive"],
            }
        ]
        compiled = executor.compile(specs)

        # Default tally=False routes to EXISTS
        assert len(compiled["exists_specs"]) == 1
        assert len(compiled["aggregate_selects"]) == 0
        assert len(compiled["supported_specs"]) == 1

    def test_postgres_compile_allowed_values_with_tally(self):
        """PostgreSQL executor routes allowed_values to aggregate when tally=True."""
        executor = PostgresSqlExecutor()
        specs = [
            {
                "kind": "allowed_values",
                "rule_id": "COL:status:allowed_values",
                "column": "status",
                "values": ["active", "inactive"],
                "tally": True,
            }
        ]
        compiled = executor.compile(specs)

        # tally=True routes to aggregate
        assert len(compiled["exists_specs"]) == 0
        assert len(compiled["aggregate_selects"]) == 1
        assert "COL:status:allowed_values" in compiled["aggregate_selects"][0]
        assert len(compiled["supported_specs"]) == 1


class TestAllowedValuesIntegration:
    """Integration tests for allowed_values with actual data validation."""

    def test_allowed_values_validate_passes(self):
        """allowed_values rule passes on valid data."""
        import kontra
        from kontra import rules as r

        df = pl.DataFrame({
            "status": ["active", "inactive", "pending", "active"],
        })

        result = kontra.validate(df, rules=[
            r.allowed_values("status", ["active", "inactive", "pending"]),
        ])

        assert result.passed
        assert result.failed_count == 0

    def test_allowed_values_validate_fails(self):
        """allowed_values rule fails on invalid data."""
        import kontra
        from kontra import rules as r

        df = pl.DataFrame({
            "status": ["active", "INVALID", "pending", "UNKNOWN"],
        })

        result = kontra.validate(df, rules=[
            r.allowed_values("status", ["active", "inactive", "pending"]),
        ], tally=True)  # Need exact counts for this test

        assert not result.passed
        assert result.failed_count == 1
        # Should have 2 failing rows (INVALID and UNKNOWN)
        rule_result = result.rules[0]
        assert rule_result.rule_id == "COL:status:allowed_values"
        assert rule_result.failed_count == 2
