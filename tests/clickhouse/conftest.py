"""Shared fixtures for live ClickHouse tests. Skips the whole directory if the
ClickHouse container is unreachable."""

from __future__ import annotations

import pytest

_HOST, _PORT = "localhost", 8123
_USER, _PASSWORD, _DB = "kontra", "kontra_test", "kontra_test"
_URI = f"clickhouse://{_USER}:{_PASSWORD}@{_HOST}:{_PORT}/{_DB}/ch_test_users"


def _client():
    import clickhouse_connect

    return clickhouse_connect.get_client(
        host=_HOST, port=_PORT, username=_USER, password=_PASSWORD, database=_DB
    )


def _clickhouse_ready() -> bool:
    try:
        _client().command("SELECT 1")
        return True
    except Exception:
        return False


@pytest.fixture(scope="session", autouse=True)
def _require_clickhouse():
    if not _clickhouse_ready():
        pytest.skip("ClickHouse container not available", allow_module_level=False)


@pytest.fixture(scope="session")
def clickhouse_seed():
    """Create a deterministic ch_test_users table; drop it at the end."""
    c = _client()
    c.command("DROP TABLE IF EXISTS ch_test_users")
    c.command(
        "CREATE TABLE ch_test_users ("
        "user_id UInt32, email String, status Nullable(String), "
        "age Nullable(Int32), note String"
        ") ENGINE = MergeTree ORDER BY user_id"
    )
    c.command(
        "INSERT INTO ch_test_users VALUES "
        "(1,'a@b.com','active',25,'abc   '),"
        "(2,'bad','inactive',NULL,'x'),"
        "(3,'c@d.com',NULL,120,'yy'),"
        "(4,'e@f.com','active',-1,'zzz')"
    )
    yield
    c.command("DROP TABLE IF EXISTS ch_test_users")


@pytest.fixture(scope="session")
def clickhouse_uri(clickhouse_seed) -> str:
    return _URI
