# tests/test_sql_state_backends.py
"""
Database-free checks for the SQL state backends. The live behaviour is
covered by tests/postgres/test_postgres_state_store.py and
tests/sqlserver/test_sqlserver_state_store.py.
"""

from __future__ import annotations

import struct
import sys
from datetime import datetime, timedelta, timezone

import pytest

from kontra.state.backends import postgres, sqlserver
from kontra.state.backends.base import StateBackend
from kontra.state.types import Annotation, StateSummary, ValidationState

BACKENDS = [
    pytest.param(
        postgres, postgres.PostgresStore, "psycopg", "_PsycopgError",
        "postgres://u:p@localhost:5432/db", id="postgres",
    ),
    pytest.param(
        sqlserver, sqlserver.SQLServerStore, "pyodbc", "_PyodbcError",
        "mssql://u:p@localhost:1433/db", id="sqlserver",
    ),
]


def _state() -> ValidationState:
    return ValidationState(
        contract_fingerprint="fp",
        dataset_fingerprint=None,
        contract_name="c",
        dataset_uri="d",
        run_at=datetime.now(timezone.utc),
        summary=StateSummary(passed=True, total_rules=0, passed_rules=0, failed_rules=0),
        rules=[],
    )


@pytest.mark.parametrize("module, cls, driver, error_global, uri", BACKENDS)
def test_missing_driver_raises_from_every_method(
    monkeypatch, module, cls, driver, error_global, uri
):
    # Without the driver the lazy error class falls back to Exception; the
    # install hint must still surface instead of being swallowed as a read error.
    monkeypatch.setitem(sys.modules, driver, None)
    monkeypatch.setattr(module, error_global, None)
    store = cls(uri)

    calls = [
        lambda: store.save(_state()),
        lambda: store.get_latest("fp"),
        lambda: store.get_history("fp"),
        lambda: store.get_at("fp", datetime.now(timezone.utc)),
        lambda: store.delete_old("fp"),
        lambda: store.list_contracts(),
        lambda: store.clear(),
        lambda: store.save_annotation(Annotation(run_id=1)),
        lambda: store.get_annotations(1),
        lambda: store.get_annotations_for_contract("fp"),
        lambda: store.get_run_with_annotations("fp"),
        lambda: store.get_history_with_annotations("fp"),
    ]
    for call in calls:
        with pytest.raises(RuntimeError, match=f"requires '{driver}'"):
            call()


@pytest.mark.parametrize("module, cls, driver, error_global, uri", BACKENDS)
def test_public_names_still_importable(module, cls, driver, error_global, uri):
    assert issubclass(cls, StateBackend)
    assert (cls.RUNS_TABLE, cls.RULE_RESULTS_TABLE, cls.ANNOTATIONS_TABLE) == (
        "kontra_runs", "kontra_rule_results", "kontra_annotations",
    )
    assert "CREATE TABLE" in cls.CREATE_TABLES_SQL
    assert callable(module._get_db_error)
    assert isinstance(cls._parse_connection_params(uri), dict)


def test_postgres_dialect_sql():
    store = postgres.PostgresStore("postgres://u:p@h/db")
    assert store._insert_returning_id("t", ["a", "b"]) == (
        "INSERT INTO t (a, b) VALUES (%s, %s) RETURNING id"
    )
    assert store._select_top("x", "FROM t ORDER BY y", ("p",)) == (
        "SELECT x FROM t ORDER BY y LIMIT 1", ("p",),
    )
    assert store._select_top("x", "FROM t ORDER BY y", ("p",), 5) == (
        "SELECT x FROM t ORDER BY y LIMIT %s", ("p", 5),
    )
    ids = list(range(5000))
    assert list(store._in_clauses("run_id", ids)) == [("run_id = ANY(%s)", (ids,))]
    assert store._decode_json({"a": 1}) == {"a": 1}


def test_sqlserver_dialect_sql(monkeypatch):
    store = sqlserver.SQLServerStore("mssql://u:p@h/db")
    assert store._insert_returning_id("t", ["a", "b"]) == (
        "INSERT INTO t (a, b) OUTPUT INSERTED.id VALUES (?, ?)"
    )
    assert store._select_top("x", "FROM t ORDER BY y", ("p",)) == (
        "SELECT TOP 1 x FROM t ORDER BY y", ("p",),
    )
    assert store._select_top("x", "FROM t ORDER BY y", ("p",), 5) == (
        "SELECT TOP (?) x FROM t ORDER BY y", (5, "p"),
    )

    # Every chunk stays under SQL Server's 2100-parameter cap
    clauses = list(store._in_clauses("run_id", list(range(4500))))
    assert [len(params) for _, params in clauses] == [2000, 2000, 500]
    assert clauses[2] == ("run_id IN (" + ", ".join(["?"] * 500) + ")", tuple(range(4000, 4500)))
    assert list(store._in_clauses("run_id", [])) == []

    assert store._decode_json('{"a": [1, 2]}') == {"a": [1, 2]}
    assert store._decode_json(None) is None
    assert store._decode_json("") is None
    assert store._decode_json("not json") is None


def test_sqlserver_ddl_splits_into_statements():
    store = sqlserver.SQLServerStore("mssql://u:p@h/db")
    statements = store._ddl_statements()
    assert len(statements) == 9  # 3 tables, 6 indexes
    assert all(s.startswith("IF NOT EXISTS") for s in statements)


@pytest.mark.parametrize(
    "fields, expected",
    [
        (
            (2026, 3, 1, 12, 0, 5, 123456700, 0, 0),
            datetime(2026, 3, 1, 12, 0, 5, 123456, tzinfo=timezone.utc),
        ),
        (
            (2025, 12, 31, 23, 59, 59, 0, -5, -30),
            datetime(2025, 12, 31, 23, 59, 59,
                     tzinfo=timezone(-timedelta(hours=5, minutes=30))),
        ),
    ],
)
def test_sqlserver_datetimeoffset_converter(fields, expected):
    raw = struct.pack("<6hI2h", *fields)
    value = sqlserver._datetimeoffset_to_datetime(raw)
    assert value == expected
    assert value.utcoffset() == expected.utcoffset()
    assert sqlserver._datetimeoffset_to_datetime(None) is None
