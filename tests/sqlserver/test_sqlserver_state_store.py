# tests/sqlserver/test_sqlserver_state_store.py
"""
SQLServerStore (validation history/state) against the live container.

Runs in a dedicated ``kontra_state_test`` database so the state tables can be
dropped and recreated without touching the fixtures other suites use.

SQLServerStore connects through pyodbc with "ODBC Driver 17 for SQL Server"
by default; the tests skip when that driver is not installed.

Requires SQL Server container to be running:
    cd tests/sqlserver && docker compose up -d
"""

from __future__ import annotations

import pytest

from kontra.state.backends.sqlserver import SQLServerStore
from kontra.state.types import Annotation
from tests.state_backend_cases import (
    FP,
    STATE_TABLES,
    StateStoreCases,
    _CountingConnection,
)

STATE_DB = "kontra_state_test"
DRIVER = "ODBC Driver 17 for SQL Server"
_SERVER = "localhost,1433"
_USER = "sa"
_PASSWORD = "Kontra_Test123!"


def _connect(database: str):
    pyodbc = pytest.importorskip("pyodbc")
    if DRIVER not in pyodbc.drivers():
        pytest.skip(f"{DRIVER} not installed")
    try:
        return pyodbc.connect(
            f"DRIVER={{{DRIVER}}};SERVER={_SERVER};DATABASE={database};"
            f"UID={_USER};PWD={_PASSWORD}",
            autocommit=True,
            timeout=5,
        )
    except pyodbc.Error as e:
        pytest.skip(f"SQL Server not reachable: {e}")


@pytest.fixture(scope="session")
def sqlserver_state_db() -> str:
    conn = _connect("master")
    conn.execute(f"IF DB_ID('{STATE_DB}') IS NULL CREATE DATABASE {STATE_DB}")
    conn.close()

    # Start from no tables so the store's own DDL is what gets exercised
    conn = _connect(STATE_DB)
    for table in STATE_TABLES:
        conn.execute(f"IF OBJECT_ID('{table}', 'U') IS NOT NULL DROP TABLE {table}")
    conn.close()

    return f"mssql://{_USER}:{_PASSWORD}@localhost:1433/{STATE_DB}"


@pytest.mark.integration
class TestSQLServerStateStore(StateStoreCases):
    store_class = SQLServerStore

    @pytest.fixture
    def store_uri(self, sqlserver_state_db: str) -> str:
        return sqlserver_state_db

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
        assert [tuple(r) for r in cur.fetchall()] == [
            ("kontra_annotations", "created_at", "datetimeoffset"),
            ("kontra_annotations", "payload", "nvarchar"),
            ("kontra_rule_results", "details", "nvarchar"),
            ("kontra_rule_results", "passed", "bit"),
            ("kontra_runs", "passed", "bit"),
            ("kontra_runs", "run_at", "datetimeoffset"),
        ]

    def test_history_chunks_run_ids_under_parameter_limit(self, store, monkeypatch):
        from kontra.state.backends import sqlserver

        states = self._save_three(store)
        for s in states:
            store.save_annotation(
                Annotation(run_id=s.id, actor_id="x", annotation_type="t", summary=str(s.id))
            )
        expected = [(h.id, [r.id for r in h.rules]) for h in store.get_history(FP)]

        monkeypatch.setattr(sqlserver, "_MAX_IN_PARAMS", 2)
        counting = _CountingConnection(store._get_conn())
        store._conn = counting
        try:
            history = store.get_history(FP)
            assert len(counting.statements) == 1 + 2
            with_anns = store.get_history_with_annotations(FP)
        finally:
            store._conn = counting._conn

        assert [(h.id, [r.id for r in h.rules]) for h in history] == expected
        assert [[a.summary for a in h.annotations] for h in with_anns] == [
            [str(h.id)] for h in with_anns
        ]

    def test_repr(self, store):
        assert repr(store) == "SQLServerStore(server=localhost, database=kontra_state_test)"
