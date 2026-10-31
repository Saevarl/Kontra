# tests/postgres/test_postgres_state_store.py
"""
PostgresStore (validation history/state) against the live container.

Runs in a dedicated ``kontra_state_test`` database so the state tables can be
dropped and recreated without touching the fixtures other suites use.

Requires PostgreSQL container to be running:
    cd tests/postgres && docker compose up -d
"""

from __future__ import annotations

import pytest

from kontra.state.backends.postgres import PostgresStore
from tests.state_backend_cases import STATE_TABLES, StateStoreCases

STATE_DB = "kontra_state_test"


@pytest.fixture(scope="session")
def postgres_state_db(postgres_container: dict) -> str:
    import psycopg

    params = dict(
        host=postgres_container["host"],
        port=postgres_container["port"],
        user=postgres_container["user"],
        password=postgres_container["password"],
    )
    with psycopg.connect(dbname=postgres_container["database"], autocommit=True, **params) as conn:
        exists = conn.execute(
            "SELECT 1 FROM pg_database WHERE datname = %s", (STATE_DB,)
        ).fetchone()
        if not exists:
            conn.execute(f"CREATE DATABASE {STATE_DB}")

    # Start from no tables so the store's own DDL is what gets exercised
    with psycopg.connect(dbname=STATE_DB, autocommit=True, **params) as conn:
        for table in STATE_TABLES:
            conn.execute(f"DROP TABLE IF EXISTS {table} CASCADE")

    return (
        f"postgres://{params['user']}:{params['password']}@{params['host']}:{params['port']}"
        f"/{STATE_DB}"
    )


@pytest.mark.integration
class TestPostgresStateStore(StateStoreCases):
    store_class = PostgresStore

    @pytest.fixture
    def store_uri(self, postgres_state_db: str) -> str:
        return postgres_state_db

    def test_schema_types(self, store):
        store.list_contracts()
        cur = store._get_conn().cursor()
        cur.execute(
            """
            SELECT table_name, column_name, data_type
            FROM information_schema.columns
            WHERE table_name IN ('kontra_runs', 'kontra_rule_results', 'kontra_annotations')
              AND column_name IN ('run_at', 'details', 'payload', 'passed', 'created_at')
            ORDER BY table_name, column_name
            """
        )
        assert cur.fetchall() == [
            ("kontra_annotations", "created_at", "timestamp with time zone"),
            ("kontra_annotations", "payload", "jsonb"),
            ("kontra_rule_results", "details", "jsonb"),
            ("kontra_rule_results", "passed", "boolean"),
            ("kontra_runs", "passed", "boolean"),
            ("kontra_runs", "run_at", "timestamp with time zone"),
        ]
        cur.close()

    def test_repr(self, store):
        assert repr(store) == "PostgresStore(host=localhost, dbname=kontra_state_test)"
