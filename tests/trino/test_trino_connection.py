"""Live Trino connection tests: one connection per validation, a caller's session untouched."""

from __future__ import annotations

import pytest

import kontra
from kontra import rules

pytestmark = pytest.mark.integration


# Preplan, the scan (a unique rule takes mark_distinct on Kontra's own
# transaction), custom SQL and a rule left to Polars, so every phase connects.
def _checks(column: str, key: str) -> list:
    return [
        rules.not_null(column),
        rules.unique(key),
        rules.custom_sql_check(f"SELECT * FROM {{table}} WHERE {key} < 0"),
        rules.allowed_values(key, ["1"]),  # string values on an integer column: Polars
    ]


def connect(**kwargs):
    import trino

    return trino.dbapi.connect(host="localhost", port=8095, user="kontra", timezone="UTC", **kwargs)


USERS = ("memory.kontra.users", "email", "user_id")
EVENTS = ("iceberg.kontra.events", "kind", "event_id")


def _uri(fq: str) -> str:
    catalog, schema, table = fq.split(".")
    return f"trino://kontra@localhost:8095/{catalog}/{schema}.{table}"


@pytest.fixture
def connections(monkeypatch):
    """Every trino.dbapi connection opened while the test runs, and whether it was closed."""
    import trino

    opened = []
    real = trino.dbapi.connect

    def counting(*args, **kwargs):
        conn = real(*args, **kwargs)
        opened.append(conn)
        close = conn.close

        def closing():
            conn.kontra_test_closed = True
            close()

        conn.close = closing
        return conn

    monkeypatch.setattr(trino.dbapi, "connect", counting)
    return opened


def _session(conn) -> dict:
    s = conn._client_session
    return {
        "properties": dict(s.properties),
        "catalog": s.catalog,
        "schema": s.schema,
        "timezone": s.timezone,
        "source": s.source,
        "client_tags": list(s.client_tags or []),
        "isolation_level": conn.isolation_level,
    }


class TestOneConnection:
    @pytest.mark.parametrize("table", [USERS, EVENTS], ids=["memory", "iceberg"])
    def test_a_uri_validation_opens_one_connection(
        self, trino_users_uri, trino_iceberg_uri, connections, table
    ):
        fq, column, key = table
        r = kontra.validate(_uri(fq), rules=_checks(column, key), tally=True, save=False)

        sources = {x.rule_id: x.source for x in r.rules}
        assert sources[f"COL:{key}:unique"] == "sql"
        assert sources[f"COL:{key}:allowed_values"] == "polars"
        assert len(connections) == 1
        assert getattr(connections[0], "kontra_test_closed", False)

    @pytest.mark.parametrize("table", [USERS, EVENTS], ids=["memory", "iceberg"])
    def test_the_connection_is_closed_when_a_validation_fails(
        self, trino_users_uri, trino_iceberg_uri, connections, monkeypatch, table
    ):
        from kontra.engine import engine

        def failing(*args, **kwargs):
            raise RuntimeError("merge failed")

        # After every phase has queried: a pushdown error would fall back to Polars.
        monkeypatch.setattr(engine, "merge_results", failing)
        fq, column, key = table
        with pytest.raises(RuntimeError, match="merge failed"):
            kontra.validate(
                _uri(fq),
                rules=_checks(column, key),
                tally=True,
                save=False,
            )
        assert len(connections) == 1
        assert all(getattr(c, "kontra_test_closed", False) for c in connections)


class TestCallerConnection:
    """Kontra never sets a session property, an isolation level, a commit or a rollback on it."""

    @pytest.mark.parametrize("table", [USERS, EVENTS], ids=["memory", "iceberg"])
    def test_autocommit_session_is_unchanged(
        self, trino_users_uri, trino_iceberg_uri, connections, table
    ):
        fq, column, key = table
        conn = connect()
        try:
            cur = conn.cursor()
            cur.execute("SET SESSION query_max_run_time = '17m'")  # the caller's own
            cur.fetchall()
            before = _session(conn)
            opened = len(connections)
            r = kontra.validate(conn, table=fq, rules=_checks(column, key), tally=True, save=False)
            assert _session(conn) == before
            assert conn.transaction is None
            assert len(connections) == opened  # nothing of Kontra's own
        finally:
            conn.close()
        assert {x.rule_id: x.source for x in r.rules}[f"COL:{key}:unique"] == "sql"

    @pytest.mark.parametrize("table", [USERS, EVENTS], ids=["memory", "iceberg"])
    def test_transaction_and_session_are_unchanged(
        self, trino_users_uri, trino_iceberg_uri, connections, table
    ):
        from trino.transaction import IsolationLevel

        fq, column, key = table
        conn = connect(isolation_level=IsolationLevel.READ_UNCOMMITTED)
        try:
            cur = conn.cursor()  # opens the caller's transaction
            cur.execute("SELECT 1")
            cur.fetchall()
            transaction, transaction_id = conn.transaction, conn.transaction.id
            before = _session(conn)
            opened = len(connections)
            kontra.validate(conn, table=fq, rules=_checks(column, key), tally=True, save=False)
            assert _session(conn) == before
            # Neither committed nor rolled back: the same transaction is still open.
            assert conn.transaction is transaction
            assert transaction.id == transaction_id
            cur = conn.cursor()
            cur.execute("SELECT 1")
            assert cur.fetchall() == [[1]]
            assert len(connections) == opened
            conn.commit()
        finally:
            conn.close()
