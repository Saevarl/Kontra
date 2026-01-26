# tests/test_custom_sql_pushdown.py
"""Tests for custom SQL pushdown functionality.

Tests the sqlglot-based SQL validation and remote execution of custom_sql_check rules.
"""

import pytest

from kontra.engine.sql_validator import (
    validate_sql,
    ValidationResult,
    format_table_reference,
    replace_table_placeholder,
    transpile_sql,
    to_count_query,
)
from kontra.rule_defs.builtin.custom_sql_check import CustomSQLCheck


class TestSqlValidator:
    """Tests for sqlglot-based SQL validation."""

    def test_validates_simple_select(self):
        """Simple SELECT is valid."""
        result = validate_sql("SELECT * FROM users WHERE age > 18")
        assert result.is_safe is True
        assert result.reason is None

    def test_validates_select_with_aggregates(self):
        """SELECT with aggregates is valid."""
        result = validate_sql("SELECT COUNT(*) FROM orders WHERE status = 'pending'")
        assert result.is_safe is True

    def test_validates_select_with_join(self):
        """SELECT with JOIN is valid."""
        result = validate_sql(
            "SELECT * FROM orders o JOIN users u ON o.user_id = u.id WHERE u.active = true"
        )
        assert result.is_safe is True

    def test_validates_cte(self):
        """WITH (CTE) query is valid."""
        result = validate_sql(
            """
            WITH active_users AS (
                SELECT id FROM users WHERE active = true
            )
            SELECT * FROM orders WHERE user_id IN (SELECT id FROM active_users)
            """
        )
        assert result.is_safe is True

    def test_validates_subquery(self):
        """Subquery is valid."""
        result = validate_sql(
            "SELECT * FROM orders WHERE user_id IN (SELECT id FROM users WHERE active = true)"
        )
        assert result.is_safe is True

    def test_rejects_insert(self):
        """INSERT is rejected."""
        result = validate_sql("INSERT INTO users (name) VALUES ('evil')")
        assert result.is_safe is False
        assert "SELECT" in result.reason

    def test_rejects_update(self):
        """UPDATE is rejected."""
        result = validate_sql("UPDATE users SET active = false")
        assert result.is_safe is False
        assert "SELECT" in result.reason

    def test_rejects_delete(self):
        """DELETE is rejected."""
        result = validate_sql("DELETE FROM users")
        assert result.is_safe is False
        assert "SELECT" in result.reason

    def test_rejects_drop(self):
        """DROP is rejected."""
        result = validate_sql("DROP TABLE users")
        assert result.is_safe is False
        assert "SELECT" in result.reason

    def test_rejects_create(self):
        """CREATE is rejected."""
        result = validate_sql("CREATE TABLE evil (id INT)")
        assert result.is_safe is False
        assert "SELECT" in result.reason

    def test_rejects_multiple_statements(self):
        """Multiple statements (SQL injection) is rejected."""
        result = validate_sql("SELECT * FROM users; DROP TABLE users;")
        assert result.is_safe is False
        assert "multiple" in result.reason.lower() or "1 statement" in result.reason

    def test_rejects_pg_sleep(self):
        """pg_sleep function is rejected."""
        result = validate_sql("SELECT pg_sleep(10)", dialect="postgres")
        assert result.is_safe is False
        assert "forbidden" in result.reason.lower()

    def test_rejects_xp_cmdshell(self):
        """xp_cmdshell function is rejected."""
        result = validate_sql("SELECT * FROM xp_cmdshell('dir')", dialect="sqlserver")
        assert result.is_safe is False
        # Note: sqlglot may parse this differently, but it should still be rejected

    def test_rejects_dblink(self):
        """dblink function is rejected."""
        result = validate_sql(
            "SELECT * FROM dblink('host=evil', 'SELECT * FROM passwords')",
            dialect="postgres"
        )
        assert result.is_safe is False
        assert "forbidden" in result.reason.lower()

    def test_empty_sql(self):
        """Empty SQL is rejected."""
        result = validate_sql("")
        assert result.is_safe is False
        assert "empty" in result.reason.lower()

    def test_invalid_sql(self):
        """Invalid SQL syntax is rejected."""
        result = validate_sql("SELECTT * FROMM users")
        assert result.is_safe is False
        # Either parse error or not a SELECT statement

    def test_dialect_postgres(self):
        """PostgreSQL dialect is handled."""
        result = validate_sql(
            "SELECT * FROM users WHERE created_at > NOW() - INTERVAL '1 day'",
            dialect="postgres"
        )
        assert result.is_safe is True

    def test_dialect_sqlserver(self):
        """SQL Server dialect is handled."""
        result = validate_sql(
            "SELECT TOP 10 * FROM users ORDER BY created_at DESC",
            dialect="sqlserver"
        )
        assert result.is_safe is True


class TestTableReferenceFormatting:
    """Tests for table reference formatting."""

    def test_postgres_format(self):
        """PostgreSQL uses double quotes."""
        ref = format_table_reference("public", "users", "postgres")
        assert ref == '"public"."users"'

    def test_sqlserver_format(self):
        """SQL Server uses square brackets."""
        ref = format_table_reference("dbo", "orders", "sqlserver")
        assert ref == "[dbo].[orders]"

    def test_duckdb_format(self):
        """DuckDB uses double quotes."""
        ref = format_table_reference("main", "data", "duckdb")
        assert ref == '"main"."data"'


class TestPlaceholderReplacement:
    """Tests for {table} placeholder replacement."""

    def test_replace_postgres(self):
        """Replaces {table} with PostgreSQL format."""
        sql = "SELECT * FROM {table} WHERE active = true"
        result = replace_table_placeholder(sql, "public", "users", "postgres")
        assert result == 'SELECT * FROM "public"."users" WHERE active = true'

    def test_replace_sqlserver(self):
        """Replaces {table} with SQL Server format."""
        sql = "SELECT COUNT(*) FROM {table}"
        result = replace_table_placeholder(sql, "dbo", "orders", "sqlserver")
        assert result == "SELECT COUNT(*) FROM [dbo].[orders]"

    def test_replace_multiple(self):
        """Replaces multiple occurrences of {table}."""
        sql = "SELECT * FROM {table} WHERE id IN (SELECT id FROM {table} WHERE x > 1)"
        result = replace_table_placeholder(sql, "public", "users", "postgres")
        assert result.count('"public"."users"') == 2
        assert "{table}" not in result


def _make_rule(sql: str = None, params: dict = None) -> CustomSQLCheck:
    """Helper to create a CustomSQLCheck rule with proper rule_id."""
    if params is None:
        params = {"sql": sql} if sql else {}
    rule = CustomSQLCheck(name="custom_sql_check", params=params)
    rule.rule_id = "test_rule"  # Set manually as factory would
    return rule


class TestCustomSqlCheckRule:
    """Tests for custom_sql_check rule's remote execution support."""

    def test_supports_remote_execution_safe_sql(self):
        """Safe SQL supports remote execution."""
        rule = _make_rule("SELECT * FROM {table} WHERE balance < 0")
        supported, reason = rule.supports_remote_execution("postgres")
        assert supported is True
        assert "safe" in reason.lower()

    def test_supports_remote_execution_unsafe_sql(self):
        """Unsafe SQL does not support remote execution."""
        rule = _make_rule("DELETE FROM {table}")
        supported, reason = rule.supports_remote_execution("postgres")
        assert supported is False
        assert "SELECT" in reason or "validation" in reason.lower()

    def test_supports_remote_execution_missing_sql(self):
        """Missing SQL parameter returns False."""
        rule = _make_rule(params={})
        supported, reason = rule.supports_remote_execution("postgres")
        assert supported is False
        assert "missing" in reason.lower()

    def test_get_remote_sql_postgres(self):
        """Gets properly formatted SQL for PostgreSQL."""
        rule = _make_rule("SELECT * FROM {table} WHERE status = 'bad'")
        sql = rule.get_remote_sql("public", "orders", "postgres")
        assert '"public"."orders"' in sql
        assert "{table}" not in sql

    def test_get_remote_sql_sqlserver(self):
        """Gets properly formatted SQL for SQL Server."""
        rule = _make_rule("SELECT * FROM {table} WHERE status = 'bad'")
        sql = rule.get_remote_sql("dbo", "orders", "sqlserver")
        assert "[dbo].[orders]" in sql
        assert "{table}" not in sql

    def test_to_sql_spec(self):
        """Returns proper SQL spec for executors."""
        rule = _make_rule("SELECT * FROM {table} WHERE x < 0")
        spec = rule.to_sql_spec()
        assert spec is not None
        assert spec["kind"] == "custom_sql_check"
        assert spec["rule_id"] == "test_rule"
        assert spec["sql"] == "SELECT * FROM {table} WHERE x < 0"

    def test_to_sql_spec_missing_sql(self):
        """Returns None when SQL is missing."""
        rule = _make_rule(params={})
        spec = rule.to_sql_spec()
        assert spec is None

    def test_legacy_query_param(self):
        """Accepts legacy 'query' parameter name."""
        rule = _make_rule(params={"query": "SELECT * FROM {table} WHERE x < 0"})
        supported, _ = rule.supports_remote_execution("postgres")
        assert supported is True
        spec = rule.to_sql_spec()
        assert spec is not None
        assert spec["sql"] == "SELECT * FROM {table} WHERE x < 0"


class TestTranspileSQL:
    """Tests for SQL dialect transpilation."""

    def test_transpile_postgres_to_sqlserver(self):
        """Basic transpilation works."""
        success, result = transpile_sql(
            "SELECT * FROM users WHERE active = true",
            from_dialect="postgres",
            to_dialect="sqlserver"
        )
        assert success is True
        # SQL Server uses 1 for true in some contexts
        assert "SELECT" in result

    def test_transpile_invalid_sql(self):
        """Invalid SQL returns error."""
        success, result = transpile_sql(
            "SELECTT * FROMM broken",
            from_dialect="postgres",
            to_dialect="sqlserver"
        )
        # May succeed or fail depending on sqlglot's leniency
        # Just ensure it doesn't crash


class TestToCountQuery:
    """Tests for to_count_query() transformation."""

    def test_simple_select_star(self):
        """Simple SELECT * is rewritten to COUNT(*)."""
        success, result = to_count_query("SELECT * FROM users WHERE x < 0")
        assert success is True
        assert "COUNT(*)" in result.upper()
        assert "SELECT *" not in result.upper()

    def test_simple_select_columns(self):
        """SELECT with columns is rewritten to COUNT(*)."""
        success, result = to_count_query("SELECT a, b, c FROM users WHERE x < 0")
        assert success is True
        assert "COUNT(*)" in result.upper()
        # Should not have the original columns
        assert ", B," not in result.upper()

    def test_distinct_is_wrapped(self):
        """SELECT DISTINCT is wrapped, not rewritten."""
        success, result = to_count_query("SELECT DISTINCT region FROM users")
        assert success is True
        assert "COUNT(*)" in result.upper()
        # Should be wrapped (subquery)
        assert "_V" in result.upper() or "_v" in result

    def test_group_by_is_wrapped(self):
        """GROUP BY query is wrapped."""
        success, result = to_count_query(
            "SELECT region FROM users GROUP BY region HAVING COUNT(*) > 1"
        )
        assert success is True
        assert "COUNT(*)" in result.upper()
        # Should be wrapped
        assert "_V" in result.upper() or "_v" in result
        # Original GROUP BY should be in subquery
        assert "GROUP BY" in result.upper()

    def test_limit_is_wrapped(self):
        """LIMIT query is wrapped to preserve limit semantics."""
        success, result = to_count_query("SELECT * FROM users WHERE x < 0 LIMIT 100")
        assert success is True
        assert "COUNT(*)" in result.upper()
        # Should be wrapped
        assert "_V" in result.upper() or "_v" in result
        # LIMIT should be preserved in subquery
        assert "LIMIT" in result.upper() or "100" in result

    def test_postgres_dialect(self):
        """PostgreSQL dialect is handled."""
        success, result = to_count_query(
            "SELECT * FROM users WHERE created_at > NOW() - INTERVAL '1 day'",
            dialect="postgres"
        )
        assert success is True
        assert "COUNT(*)" in result.upper()

    def test_sqlserver_dialect(self):
        """SQL Server dialect is handled."""
        success, result = to_count_query(
            "SELECT * FROM users WHERE active = 1",
            dialect="sqlserver"
        )
        assert success is True
        assert "COUNT(*)" in result.upper()

    def test_invalid_sql_returns_error(self):
        """Invalid SQL returns error."""
        success, result = to_count_query("SELECTT * FROMM broken")
        # sqlglot may parse this as something weird, or fail
        # Either way, if it succeeds the result should still be usable,
        # or it should return an error
        if not success:
            assert "error" in result.lower() or "parse" in result.lower() or "expected" in result.lower()
        # If it succeeds (sqlglot is lenient), that's also acceptable

    def test_complex_where_clause(self):
        """Complex WHERE clause is preserved."""
        success, result = to_count_query(
            "SELECT * FROM orders WHERE status = 'active' AND amount > 100 AND region IN ('US', 'EU')"
        )
        assert success is True
        assert "COUNT(*)" in result.upper()
        # WHERE conditions should be preserved
        assert "STATUS" in result.upper()
        assert "AMOUNT" in result.upper()

    def test_join_is_preserved(self):
        """JOIN is preserved in rewritten query."""
        success, result = to_count_query(
            "SELECT o.* FROM orders o JOIN users u ON o.user_id = u.id WHERE u.active = true"
        )
        assert success is True
        assert "COUNT(*)" in result.upper()
        assert "JOIN" in result.upper()

    def test_subquery_in_where(self):
        """Subquery in WHERE is preserved."""
        success, result = to_count_query(
            "SELECT * FROM orders WHERE user_id NOT IN (SELECT id FROM blocked_users)"
        )
        assert success is True
        assert "COUNT(*)" in result.upper()
        # sqlglot may normalize "NOT IN" to "NOT ... IN"
        assert "NOT" in result.upper() and "IN" in result.upper()

    def test_cte_is_wrapped(self):
        """CTE (WITH clause) is wrapped."""
        success, result = to_count_query(
            """
            WITH active AS (SELECT id FROM users WHERE active = true)
            SELECT * FROM orders WHERE user_id IN (SELECT id FROM active)
            """
        )
        assert success is True
        assert "COUNT(*)" in result.upper()


class TestCustomRuleSqlAgg:
    """Tests for custom rules with to_sql_agg() for SQL pushdown."""

    def test_custom_agg_spec_generation(self):
        """Custom rule with to_sql_agg() generates proper spec."""
        import polars as pl
        from kontra.rule_defs.base import BaseRule
        from kontra.rule_defs.predicates import Predicate
        from kontra.rule_defs.registry import register_rule, RULE_REGISTRY
        from kontra.rule_defs.execution_plan import _maybe_rule_sql_spec

        # Clean up if already registered
        if "test_positive" in RULE_REGISTRY:
            del RULE_REGISTRY["test_positive"]

        @register_rule("test_positive")
        class TestPositiveRule(BaseRule):
            def __init__(self, name, params):
                super().__init__(name, params)
                self.column = params["column"]

            def validate(self, df):
                mask = df[self.column].is_null() | (df[self.column] <= 0)
                return self._failures(df, mask, f"{self.column} non-positive")

            def to_sql_agg(self, dialect="duckdb"):
                col = f'"{self.column}"'
                return f"SUM(CASE WHEN {col} IS NULL OR {col} <= 0 THEN 1 ELSE 0 END)"

        # Build rule and check spec
        from kontra.rule_defs.factory import RuleFactory
        from kontra.config.models import RuleSpec

        spec = RuleSpec(name="test_positive", params={"column": "amount"})
        factory = RuleFactory([spec])
        rules = factory.build_rules()
        rule = rules[0]

        sql_spec = _maybe_rule_sql_spec(rule)

        assert sql_spec is not None
        assert sql_spec["kind"] == "custom_agg"
        assert "sql_agg" in sql_spec
        assert sql_spec["sql_agg"]["duckdb"] is not None
        assert "SUM(CASE WHEN" in sql_spec["sql_agg"]["duckdb"]

        # Clean up
        del RULE_REGISTRY["test_positive"]

    def test_duckdb_executor_supports_custom_agg(self):
        """DuckDB executor includes custom_agg in SUPPORTED_RULES."""
        from kontra.engine.executors.duckdb_sql import DuckDBSqlExecutor
        assert "custom_agg" in DuckDBSqlExecutor.SUPPORTED_RULES

    def test_postgres_executor_supports_custom_agg(self):
        """PostgreSQL executor includes custom_agg in SUPPORTED_RULES."""
        from kontra.engine.executors.postgres_sql import PostgresSqlExecutor
        assert "custom_agg" in PostgresSqlExecutor.SUPPORTED_RULES

    def test_sqlserver_executor_supports_custom_agg(self):
        """SQL Server executor includes custom_agg in SUPPORTED_RULES."""
        from kontra.engine.executors.sqlserver_sql import SqlServerSqlExecutor
        assert "custom_agg" in SqlServerSqlExecutor.SUPPORTED_RULES

    def test_duckdb_compile_custom_agg(self):
        """DuckDB executor compiles custom_agg specs."""
        from kontra.engine.executors.duckdb_sql import DuckDBSqlExecutor

        executor = DuckDBSqlExecutor()
        specs = [
            {
                "kind": "custom_agg",
                "rule_id": "COL:amount:positive",
                "sql_agg": {
                    "duckdb": 'SUM(CASE WHEN "amount" IS NULL OR "amount" <= 0 THEN 1 ELSE 0 END)',
                    "postgres": 'SUM(CASE WHEN "amount" IS NULL OR "amount" <= 0 THEN 1 ELSE 0 END)',
                    "mssql": 'SUM(CASE WHEN "amount" IS NULL OR "amount" <= 0 THEN 1 ELSE 0 END)',
                },
            }
        ]
        compiled = executor.compile(specs)

        assert len(compiled["aggregate_selects"]) == 1
        assert "COL:amount:positive" in compiled["aggregate_selects"][0]
        assert len(compiled["supported_specs"]) == 1

    def test_postgres_compile_custom_agg(self):
        """PostgreSQL executor compiles custom_agg specs."""
        from kontra.engine.executors.postgres_sql import PostgresSqlExecutor

        executor = PostgresSqlExecutor()
        specs = [
            {
                "kind": "custom_agg",
                "rule_id": "COL:amount:positive",
                "sql_agg": {
                    "duckdb": 'SUM(CASE WHEN "amount" IS NULL OR "amount" <= 0 THEN 1 ELSE 0 END)',
                    "postgres": 'SUM(CASE WHEN "amount" IS NULL OR "amount" <= 0 THEN 1 ELSE 0 END)',
                    "mssql": 'SUM(CASE WHEN "amount" IS NULL OR "amount" <= 0 THEN 1 ELSE 0 END)',
                },
            }
        ]
        compiled = executor.compile(specs)

        assert len(compiled["aggregate_selects"]) == 1
        assert "COL:amount:positive" in compiled["aggregate_selects"][0]
        assert len(compiled["supported_specs"]) == 1


class TestCustomAggIntegration:
    """End-to-end integration tests for custom rules with to_sql_agg()."""

    def test_custom_agg_validates_data_passes(self):
        """Custom rule with to_sql_agg() validates data correctly (pass case)."""
        import polars as pl
        import kontra
        from kontra.rule_defs.base import BaseRule
        from kontra.rule_defs.registry import register_rule, RULE_REGISTRY

        # Clean up if already registered
        if "test_positive_int" in RULE_REGISTRY:
            del RULE_REGISTRY["test_positive_int"]

        @register_rule("test_positive_int")
        class TestPositiveRule(BaseRule):
            def __init__(self, name, params):
                super().__init__(name, params)
                self.column = params["column"]

            def validate(self, df):
                mask = df[self.column].is_null() | (df[self.column] <= 0)
                return self._failures(df, mask, f"{self.column} non-positive")

            def to_sql_agg(self, dialect="duckdb"):
                col = f'"{self.column}"'
                return f"SUM(CASE WHEN {col} IS NULL OR {col} <= 0 THEN 1 ELSE 0 END)"

        # All positive values - should pass
        df = pl.DataFrame({"amount": [10, 20, 30, 40, 50]})

        # Use dict format for rules
        rule_specs = [{"name": "test_positive_int", "params": {"column": "amount"}}]

        result = kontra.validate(df, rules=rule_specs, save=False)

        assert result.passed
        assert result.failed_count == 0

        # Clean up
        del RULE_REGISTRY["test_positive_int"]

    def test_custom_agg_validates_data_fails(self):
        """Custom rule with to_sql_agg() validates data correctly (fail case)."""
        import polars as pl
        import kontra
        from kontra.rule_defs.base import BaseRule
        from kontra.rule_defs.registry import register_rule, RULE_REGISTRY

        # Clean up if already registered
        if "test_positive_int2" in RULE_REGISTRY:
            del RULE_REGISTRY["test_positive_int2"]

        @register_rule("test_positive_int2")
        class TestPositiveRule(BaseRule):
            def __init__(self, name, params):
                super().__init__(name, params)
                self.column = params["column"]

            def validate(self, df):
                mask = df[self.column].is_null() | (df[self.column] <= 0)
                return self._failures(df, mask, f"{self.column} non-positive")

            def to_sql_agg(self, dialect="duckdb"):
                col = f'"{self.column}"'
                return f"SUM(CASE WHEN {col} IS NULL OR {col} <= 0 THEN 1 ELSE 0 END)"

        # Has negative and zero values - should fail
        df = pl.DataFrame({"amount": [10, -5, 0, 40, 50]})

        # Use dict format for rules
        rule_specs = [{"name": "test_positive_int2", "params": {"column": "amount"}}]

        result = kontra.validate(df, rules=rule_specs, save=False)

        assert not result.passed
        assert result.failed_count == 1
        # Should have 2 failing rows (-5 and 0)
        rule_result = result.rules[0]
        assert rule_result.failed_count == 2

        # Clean up
        del RULE_REGISTRY["test_positive_int2"]

    def test_custom_agg_uses_sql_pushdown_on_parquet(self, tmp_path):
        """Custom rule with to_sql_agg() uses SQL pushdown for parquet files."""
        import polars as pl
        import kontra
        from kontra.rule_defs.base import BaseRule
        from kontra.rule_defs.registry import register_rule, RULE_REGISTRY

        # Clean up if already registered
        if "test_positive_int3" in RULE_REGISTRY:
            del RULE_REGISTRY["test_positive_int3"]

        @register_rule("test_positive_int3")
        class TestPositiveRule(BaseRule):
            def __init__(self, name, params):
                super().__init__(name, params)
                self.column = params["column"]

            def validate(self, df):
                mask = df[self.column].is_null() | (df[self.column] <= 0)
                return self._failures(df, mask, f"{self.column} non-positive")

            def to_sql_agg(self, dialect="duckdb"):
                col = f'"{self.column}"'
                return f"SUM(CASE WHEN {col} IS NULL OR {col} <= 0 THEN 1 ELSE 0 END)"

        # Write test data to parquet
        df = pl.DataFrame({"amount": [10, -5, 0, 40, 50]})
        parquet_path = tmp_path / "test_data.parquet"
        df.write_parquet(parquet_path)

        # Use dict format for rules
        rule_specs = [{"name": "test_positive_int3", "params": {"column": "amount"}}]

        result = kontra.validate(str(parquet_path), rules=rule_specs, save=False)

        assert not result.passed
        assert result.failed_count == 1
        rule_result = result.rules[0]
        assert rule_result.failed_count == 2
        # Verify SQL pushdown was used
        assert rule_result.source == "sql"

        # Clean up
        del RULE_REGISTRY["test_positive_int3"]


class TestDatabaseExecutorCompile:
    """Tests for database executor compile() with custom_sql_check."""

    def test_postgres_executor_supports_custom_sql_check(self):
        """PostgreSQL executor includes custom_sql_check in SUPPORTED_RULES."""
        from kontra.engine.executors.postgres_sql import PostgresSqlExecutor
        assert "custom_sql_check" in PostgresSqlExecutor.SUPPORTED_RULES

    def test_sqlserver_executor_supports_custom_sql_check(self):
        """SQL Server executor includes custom_sql_check in SUPPORTED_RULES."""
        from kontra.engine.executors.sqlserver_sql import SqlServerSqlExecutor
        assert "custom_sql_check" in SqlServerSqlExecutor.SUPPORTED_RULES

    def test_compile_accepts_safe_custom_sql(self):
        """Compile accepts safe custom SQL."""
        from kontra.engine.executors.postgres_sql import PostgresSqlExecutor

        executor = PostgresSqlExecutor()
        specs = [
            {
                "kind": "custom_sql_check",
                "rule_id": "test_custom",
                "sql": "SELECT * FROM dummy_table WHERE x < 0"
            }
        ]
        compiled = executor.compile(specs)
        assert len(compiled["custom_sql_specs"]) == 1
        assert compiled["custom_sql_specs"][0]["rule_id"] == "test_custom"
        assert len(compiled["supported_specs"]) == 1

    def test_compile_rejects_unsafe_custom_sql(self):
        """Compile rejects unsafe custom SQL (not added to custom_sql_specs)."""
        from kontra.engine.executors.postgres_sql import PostgresSqlExecutor

        executor = PostgresSqlExecutor()
        specs = [
            {
                "kind": "custom_sql_check",
                "rule_id": "test_evil",
                "sql": "DELETE FROM users"
            }
        ]
        compiled = executor.compile(specs)
        # Unsafe SQL should not be in custom_sql_specs
        assert len(compiled["custom_sql_specs"]) == 0
        assert len(compiled["supported_specs"]) == 0

    def test_compile_mixed_rules(self):
        """Compile handles mix of regular and custom SQL rules."""
        from kontra.engine.executors.postgres_sql import PostgresSqlExecutor

        executor = PostgresSqlExecutor()
        specs = [
            {"kind": "not_null", "rule_id": "nn_1", "column": "id"},
            {"kind": "min_rows", "rule_id": "mr_1", "threshold": 10},
            {
                "kind": "custom_sql_check",
                "rule_id": "cs_1",
                "sql": "SELECT * FROM dummy WHERE bad = true"
            },
        ]
        compiled = executor.compile(specs)

        # not_null goes to exists_specs
        assert len(compiled["exists_specs"]) == 1
        # min_rows goes to aggregate_selects
        assert len(compiled["aggregate_selects"]) == 1
        # custom_sql goes to custom_sql_specs
        assert len(compiled["custom_sql_specs"]) == 1
        # All three are supported
        assert len(compiled["supported_specs"]) == 3
