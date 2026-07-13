# tests/postgres/test_postgres_validation.py
"""
Integration tests for PostgreSQL validation.

Requires PostgreSQL container to be running:
    cd tests/postgres && docker compose up -d
"""

import pytest


@pytest.mark.integration
class TestPostgresValidation:
    """Test validation rules against PostgreSQL tables."""

    def test_uri_validation_reuses_one_connection_across_phases(
        self, postgres_container, monkeypatch
    ):
        """URI validation shares one run-owned connection across PostgreSQL phases."""
        import psycopg
        import kontra
        from kontra import rules

        uri = (
            "postgres://kontra:kontra_test@localhost:5433/"
            "kontra_test/public.tiny"
        )
        with psycopg.connect(
            host=postgres_container["host"],
            port=postgres_container["port"],
            user=postgres_container["user"],
            password=postgres_container["password"],
            dbname=postgres_container["database"],
        ) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "CREATE TABLE IF NOT EXISTS tiny AS "
                    "SELECT g AS id, (g % 90)::int AS age "
                    "FROM generate_series(0, 999) g"
                )

        original_connect = psycopg.connect
        connect_calls = []

        def counting_connect(*args, **kwargs):
            connect_calls.append((args, kwargs))
            return original_connect(*args, **kwargs)

        monkeypatch.setattr(psycopg, "connect", counting_connect)

        result = kontra.validate(
            uri,
            rules=[
                rules.not_null("age"),
                rules.range("age", min=0, max=89),
                rules.unique("id"),
            ],
            save=False,
        )

        assert result.passed
        assert len(connect_calls) == 1

    def test_materializer_loads_data(self, postgres_uri):
        """Test that PostgresMaterializer can load data."""
        from kontra.connectors.handle import DatasetHandle
        from kontra.engine.materializers.registry import (
            register_default_materializers,
            pick_materializer,
        )

        register_default_materializers()
        handle = DatasetHandle.from_uri(postgres_uri)
        mat = pick_materializer(handle)

        assert mat.materializer_name == "postgres"

        # Get schema
        schema = mat.schema()
        assert "user_id" in schema
        assert "email" in schema

        # Load with projection
        df = mat.to_polars(["user_id", "email", "status"])
        assert len(df) == 1002
        assert list(df.columns) == ["user_id", "email", "status"]

    def test_executor_not_null_rule(self, postgres_uri):
        """Test not_null rule execution."""
        from kontra.connectors.handle import DatasetHandle
        from kontra.engine.executors.postgres_sql import PostgresSqlExecutor

        handle = DatasetHandle.from_uri(postgres_uri)
        executor = PostgresSqlExecutor()

        # Test username (no nulls)
        specs = [{"kind": "not_null", "column": "username", "rule_id": "test_not_null"}]
        plan = executor.compile(specs)
        result = executor.execute(handle, plan)

        assert len(result["results"]) == 1
        assert result["results"][0]["passed"] is True
        assert result["results"][0]["failed_count"] == 0

    def test_executor_not_null_fails(self, postgres_uri):
        """Test not_null rule failure (email has nulls)."""
        from kontra.connectors.handle import DatasetHandle
        from kontra.engine.executors.postgres_sql import PostgresSqlExecutor

        handle = DatasetHandle.from_uri(postgres_uri)
        executor = PostgresSqlExecutor()

        specs = [{"kind": "not_null", "column": "email", "rule_id": "test_email_not_null"}]
        plan = executor.compile(specs)
        result = executor.execute(handle, plan)

        assert len(result["results"]) == 1
        assert result["results"][0]["passed"] is False
        # EXISTS returns 1 (has violation) instead of exact count
        # This is intentional - EXISTS is faster than COUNT
        assert result["results"][0]["failed_count"] >= 1

    def test_executor_unique_rule(self, postgres_uri):
        """Test unique rule execution."""
        from kontra.connectors.handle import DatasetHandle
        from kontra.engine.executors.postgres_sql import PostgresSqlExecutor

        handle = DatasetHandle.from_uri(postgres_uri)
        executor = PostgresSqlExecutor()

        # Test user_id (unique)
        specs = [{"kind": "unique", "column": "user_id", "rule_id": "test_unique"}]
        plan = executor.compile(specs)
        result = executor.execute(handle, plan)

        assert len(result["results"]) == 1
        assert result["results"][0]["passed"] is True

    def test_executor_unique_fails(self, postgres_uri):
        """Test unique rule failure (email has duplicates)."""
        from kontra.connectors.handle import DatasetHandle
        from kontra.engine.executors.postgres_sql import PostgresSqlExecutor

        handle = DatasetHandle.from_uri(postgres_uri)
        executor = PostgresSqlExecutor()

        specs = [{"kind": "unique", "column": "email", "rule_id": "test_email_unique"}]
        plan = executor.compile(specs)
        result = executor.execute(handle, plan)

        assert len(result["results"]) == 1
        assert result["results"][0]["passed"] is False
        # 2 duplicates + 20 nulls (nulls count as distinct) = ~21 failures
        assert result["results"][0]["failed_count"] > 0

    def test_executor_min_rows(self, postgres_uri):
        """Test min_rows rule."""
        from kontra.connectors.handle import DatasetHandle
        from kontra.engine.executors.postgres_sql import PostgresSqlExecutor

        handle = DatasetHandle.from_uri(postgres_uri)
        executor = PostgresSqlExecutor()

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

    def test_executor_max_rows(self, postgres_uri):
        """Test max_rows rule."""
        from kontra.connectors.handle import DatasetHandle
        from kontra.engine.executors.postgres_sql import PostgresSqlExecutor

        handle = DatasetHandle.from_uri(postgres_uri)
        executor = PostgresSqlExecutor()

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

    def test_executor_allowed_values(self, postgres_uri):
        """Test allowed_values rule."""
        from kontra.connectors.handle import DatasetHandle
        from kontra.engine.executors.postgres_sql import PostgresSqlExecutor

        handle = DatasetHandle.from_uri(postgres_uri)
        executor = PostgresSqlExecutor()

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
        # Use tally=True to get exact count (default uses EXISTS which returns 1)
        specs = [{
            "kind": "allowed_values",
            "column": "status",
            "values": ["active", "inactive", "pending"],
            "rule_id": "test_allowed_fail",
            "tally": True,
        }]
        plan = executor.compile(specs)
        result = executor.execute(handle, plan)

        assert result["results"][0]["passed"] is False
        assert result["results"][0]["failed_count"] == 250  # ~25% are 'suspended'

    def test_introspect(self, postgres_uri):
        """Test executor introspection."""
        from kontra.connectors.handle import DatasetHandle
        from kontra.engine.executors.postgres_sql import PostgresSqlExecutor

        handle = DatasetHandle.from_uri(postgres_uri)
        executor = PostgresSqlExecutor()

        result = executor.introspect(handle)

        assert result["row_count"] == 1002
        assert "user_id" in result["available_cols"]
        assert "email" in result["available_cols"]
        assert len(result["available_cols"]) == 9


@pytest.mark.integration
class TestPostgresPreplan:
    """Test pg_stats-based preplan."""

    def test_preplan_not_null(self, postgres_uri):
        """Test preplan for not_null rules using pg_stats."""
        from kontra.connectors.handle import DatasetHandle
        from kontra.preplan.postgres import preplan_postgres

        handle = DatasetHandle.from_uri(postgres_uri)

        # username has null_frac = 0 -> should pass
        predicates = [
            ("test_username", "username", "not_null", None),
            ("test_email", "email", "not_null", None),  # has nulls -> unknown
        ]

        result = preplan_postgres(handle, ["username", "email"], predicates)

        assert result.rule_decisions["test_username"] == "pass_meta"
        assert result.rule_decisions["test_email"] == "unknown"

    def test_preplan_unique(self, postgres_uri):
        """Test preplan for unique rules using pg_stats."""
        from kontra.connectors.handle import DatasetHandle
        from kontra.preplan.postgres import preplan_postgres

        handle = DatasetHandle.from_uri(postgres_uri)

        predicates = [
            ("test_user_id", "user_id", "unique", None),
            ("test_email", "email", "unique", None),
        ]

        result = preplan_postgres(handle, ["user_id", "email"], predicates)

        # user_id has n_distinct = -1 (all unique)
        assert result.rule_decisions["test_user_id"] == "pass_meta"
        # email has duplicates -> unknown
        assert result.rule_decisions["test_email"] == "unknown"
