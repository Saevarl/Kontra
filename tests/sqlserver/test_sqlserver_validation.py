# tests/sqlserver/test_sqlserver_validation.py
"""
Integration tests for SQL Server validation.

Requires SQL Server container to be running:
    cd tests/sqlserver && docker compose up -d
"""

import pytest


@pytest.mark.integration
class TestSqlServerValidation:
    """Test validation rules against SQL Server tables."""

    def test_materializer_loads_data(self, sqlserver_uri):
        """Test that SqlServerMaterializer can load data."""
        from kontra.connectors.handle import DatasetHandle
        from kontra.engine.materializers.registry import (
            register_default_materializers,
            pick_materializer,
        )

        register_default_materializers()
        handle = DatasetHandle.from_uri(sqlserver_uri)
        mat = pick_materializer(handle)

        assert mat.materializer_name == "sqlserver"

        # Get schema
        schema = mat.schema()
        assert "user_id" in schema
        assert "email" in schema

        # Load with projection
        df = mat.to_polars(["user_id", "email", "status"])
        assert len(df) == 1002
        assert list(df.columns) == ["user_id", "email", "status"]

    def test_executor_not_null_rule(self, sqlserver_uri):
        """Test not_null rule execution."""
        from kontra.connectors.handle import DatasetHandle
        from kontra.engine.executors.sqlserver_sql import SqlServerSqlExecutor

        handle = DatasetHandle.from_uri(sqlserver_uri)
        executor = SqlServerSqlExecutor()

        # Test username (no nulls)
        specs = [{"kind": "not_null", "column": "username", "rule_id": "test_not_null"}]
        plan = executor.compile(specs)
        result = executor.execute(handle, plan)

        assert len(result["results"]) == 1
        assert result["results"][0]["passed"] is True
        assert result["results"][0]["failed_count"] == 0

    def test_executor_not_null_fails(self, sqlserver_uri):
        """Test not_null rule failure (email has nulls)."""
        from kontra.connectors.handle import DatasetHandle
        from kontra.engine.executors.sqlserver_sql import SqlServerSqlExecutor

        handle = DatasetHandle.from_uri(sqlserver_uri)
        executor = SqlServerSqlExecutor()

        specs = [{"kind": "not_null", "column": "email", "rule_id": "test_email_not_null"}]
        plan = executor.compile(specs)
        result = executor.execute(handle, plan)

        assert len(result["results"]) == 1
        assert result["results"][0]["passed"] is False
        # EXISTS returns 1 (has violation) instead of exact count
        assert result["results"][0]["failed_count"] >= 1

    def test_executor_unique_rule(self, sqlserver_uri):
        """Test unique rule execution."""
        from kontra.connectors.handle import DatasetHandle
        from kontra.engine.executors.sqlserver_sql import SqlServerSqlExecutor

        handle = DatasetHandle.from_uri(sqlserver_uri)
        executor = SqlServerSqlExecutor()

        # Test user_id (unique)
        specs = [{"kind": "unique", "column": "user_id", "rule_id": "test_unique"}]
        plan = executor.compile(specs)
        result = executor.execute(handle, plan)

        assert len(result["results"]) == 1
        assert result["results"][0]["passed"] is True

    def test_executor_unique_fails(self, sqlserver_uri):
        """Test unique rule failure (email has duplicates)."""
        from kontra.connectors.handle import DatasetHandle
        from kontra.engine.executors.sqlserver_sql import SqlServerSqlExecutor

        handle = DatasetHandle.from_uri(sqlserver_uri)
        executor = SqlServerSqlExecutor()

        specs = [{"kind": "unique", "column": "email", "rule_id": "test_email_unique"}]
        plan = executor.compile(specs)
        result = executor.execute(handle, plan)

        assert len(result["results"]) == 1
        assert result["results"][0]["passed"] is False
        assert result["results"][0]["failed_count"] > 0

    def test_executor_min_rows(self, sqlserver_uri):
        """Test min_rows rule."""
        from kontra.connectors.handle import DatasetHandle
        from kontra.engine.executors.sqlserver_sql import SqlServerSqlExecutor

        handle = DatasetHandle.from_uri(sqlserver_uri)
        executor = SqlServerSqlExecutor()

        # Should pass: 1002 >= 1000
        specs = [{"kind": "min_rows", "threshold": 1000, "rule_id": "test_min_rows"}]
        plan = executor.compile(specs)
        result = executor.execute(handle, plan)

        assert result["results"][0]["passed"] is True

        # Should fail: 1002 < 2000
        specs = [{"kind": "min_rows", "threshold": 2000, "rule_id": "test_min_rows_fail"}]
        plan = executor.compile(specs)
        result = executor.execute(handle, plan)

        assert result["results"][0]["passed"] is False
        assert result["results"][0]["failed_count"] == 998  # 2000 - 1002

    def test_executor_max_rows(self, sqlserver_uri):
        """Test max_rows rule."""
        from kontra.connectors.handle import DatasetHandle
        from kontra.engine.executors.sqlserver_sql import SqlServerSqlExecutor

        handle = DatasetHandle.from_uri(sqlserver_uri)
        executor = SqlServerSqlExecutor()

        # Should pass: 1002 <= 2000
        specs = [{"kind": "max_rows", "threshold": 2000, "rule_id": "test_max_rows"}]
        plan = executor.compile(specs)
        result = executor.execute(handle, plan)

        assert result["results"][0]["passed"] is True

        # Should fail: 1002 > 500
        specs = [{"kind": "max_rows", "threshold": 500, "rule_id": "test_max_rows_fail"}]
        plan = executor.compile(specs)
        result = executor.execute(handle, plan)

        assert result["results"][0]["passed"] is False
        assert result["results"][0]["failed_count"] == 502  # 1002 - 500

    def test_executor_allowed_values(self, sqlserver_uri):
        """Test allowed_values rule."""
        from kontra.connectors.handle import DatasetHandle
        from kontra.engine.executors.sqlserver_sql import SqlServerSqlExecutor

        handle = DatasetHandle.from_uri(sqlserver_uri)
        executor = SqlServerSqlExecutor()

        # Should pass: status values are active, inactive, pending, suspended
        specs = [{
            "kind": "allowed_values",
            "column": "status",
            "values": ["active", "inactive", "pending", "suspended"],
            "rule_id": "test_allowed",
        }]
        plan = executor.compile(specs)
        result = executor.execute(handle, plan)

        assert result["results"][0]["passed"] is True

        # Should fail: missing 'suspended' from allowed values
        specs = [{
            "kind": "allowed_values",
            "column": "status",
            "values": ["active", "inactive", "pending"],
            "rule_id": "test_allowed_fail",
        }]
        plan = executor.compile(specs)
        result = executor.execute(handle, plan)

        assert result["results"][0]["passed"] is False
        assert result["results"][0]["failed_count"] == 250  # ~25% are 'suspended'

    def test_introspect(self, sqlserver_uri):
        """Test executor introspection."""
        from kontra.connectors.handle import DatasetHandle
        from kontra.engine.executors.sqlserver_sql import SqlServerSqlExecutor

        handle = DatasetHandle.from_uri(sqlserver_uri)
        executor = SqlServerSqlExecutor()

        result = executor.introspect(handle)

        assert result["row_count"] == 1002
        assert "user_id" in result["available_cols"]
        assert "email" in result["available_cols"]
        assert len(result["available_cols"]) == 9


@pytest.mark.integration
class TestSqlServerPreplan:
    """Test metadata-based preplan."""

    def test_preplan_not_null(self, sqlserver_uri):
        """Test preplan for not_null rules using metadata."""
        from kontra.connectors.handle import DatasetHandle
        from kontra.preplan.sqlserver import preplan_sqlserver

        handle = DatasetHandle.from_uri(sqlserver_uri)

        # username is NOT NULL column -> should pass_meta
        # email is nullable -> unknown
        predicates = [
            ("test_username", "username", "not_null", None),
            ("test_email", "email", "not_null", None),
        ]

        result = preplan_sqlserver(handle, ["username", "email"], predicates)

        assert result.rule_decisions["test_username"] == "pass_meta"
        assert result.rule_decisions["test_email"] == "unknown"

    def test_preplan_unique(self, sqlserver_uri):
        """Test preplan for unique rules using metadata."""
        from kontra.connectors.handle import DatasetHandle
        from kontra.preplan.sqlserver import preplan_sqlserver

        handle = DatasetHandle.from_uri(sqlserver_uri)

        predicates = [
            ("test_user_id", "user_id", "unique", None),
            ("test_email", "email", "unique", None),
        ]

        result = preplan_sqlserver(handle, ["user_id", "email"], predicates)

        # user_id is identity with primary key -> pass_meta
        assert result.rule_decisions["test_user_id"] == "pass_meta"
        # email has no unique constraint -> unknown
        assert result.rule_decisions["test_email"] == "unknown"
