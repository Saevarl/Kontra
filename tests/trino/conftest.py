# tests/trino/conftest.py
"""
Pytest fixtures for Trino integration tests.

Usage:
    pytest tests/trino/ -v

Requires the Trino container (memory, Iceberg, and Iceberg on a JDBC catalog):
    cd tests/trino && docker compose up -d

The fixture starts it if Docker is available and skips the directory if Trino
cannot be reached. Tables are created in the throwaway container and dropped
at the end of the session.
"""

from __future__ import annotations

import os
import subprocess
import time

import pytest

_HOST, _PORT, _USER = "localhost", 8095, "kontra"
URI_USERS = f"trino://{_USER}@{_HOST}:{_PORT}/memory/kontra.users"
URI_ICEBERG = f"trino://{_USER}@{_HOST}:{_PORT}/iceberg/kontra.events"

# Deterministic users table with known violations (see the comments per row).
_USERS_DDL = """
CREATE TABLE memory.kontra.users (
    user_id    bigint NOT NULL,
    email      varchar,
    status     varchar,
    age        integer,
    balance    decimal(10, 2),
    score      double,
    code       char(3),
    note       varchar,
    created_at timestamp(6),
    updated_at timestamp(6) with time zone,
    signup     date
)
"""
_USERS_ROWS = """
INSERT INTO memory.kontra.users VALUES
  (1, 'a@b.com', 'active',   25, 10.50, 1.5,   'ab',  'abc   ', TIMESTAMP '2026-01-01 00:00:00',
      TIMESTAMP '2026-01-01 00:00:00 UTC', DATE '2020-01-01'),
  (2, 'bad',     'inactive', NULL, 0.00, nan(), 'x',  'x',      TIMESTAMP '2026-01-02 00:00:00',
      TIMESTAMP '2026-01-02 00:00:00 UTC', DATE '2020-01-02'),
  (3, 'c@d.com', NULL,       120, NULL, 3.0,   NULL,  'yy',     NULL,
      NULL, DATE '2020-01-03'),
  (3, 'e@f.com', 'active',   -1, -5.25, NULL,  'zz',  'zzz',    TIMESTAMP '2026-01-03 00:00:00',
      TIMESTAMP '2026-01-03 00:00:00 UTC', NULL),
  (5, 'Ünï@cødé.com', 'pending', 40, 99.99, 0.5, 'ab', 'abc' || chr(10), TIMESTAMP '2026-01-04 00:00:00',
      TIMESTAMP '2026-01-04 00:00:00 UTC', DATE '2020-01-05')
"""


def connect():
    import trino

    return trino.dbapi.connect(host=_HOST, port=_PORT, user=_USER, timezone="UTC")


def run_sql(*statements: str) -> None:
    with connect() as conn:
        cur = conn.cursor()
        for sql in statements:
            cur.execute(sql)
            cur.fetchall()


def _is_trino_ready() -> bool:
    import trino

    try:
        with connect() as conn:
            cur = conn.cursor()
            cur.execute("SELECT 1")
            cur.fetchall()
        return True
    except (OSError, trino.exceptions.HttpError, trino.exceptions.DatabaseError):
        return False


@pytest.fixture(scope="session", autouse=True)
def trino_container():
    """Ensure the Trino container is running; skip the directory if it is not."""
    if _is_trino_ready():
        return
    try:
        subprocess.run(
            ["docker", "compose", "up", "-d"],
            cwd=os.path.dirname(__file__),
            check=True,
            capture_output=True,
        )
    except (OSError, subprocess.CalledProcessError):
        pytest.skip("Trino container not available")
    for _ in range(90):
        if _is_trino_ready():
            return
        time.sleep(1)
    pytest.skip("Trino container did not become ready within 90 seconds")


@pytest.fixture(scope="session")
def trino_connect(trino_container):
    """Factory for caller-owned Trino connections (bring-your-own-connection)."""
    return connect


@pytest.fixture(scope="session")
def trino_users_uri(trino_container) -> str:
    run_sql(
        "CREATE SCHEMA IF NOT EXISTS memory.kontra",
        "DROP TABLE IF EXISTS memory.kontra.users",
        _USERS_DDL,
        _USERS_ROWS,
    )
    yield URI_USERS
    run_sql("DROP TABLE IF EXISTS memory.kontra.users")


@pytest.fixture(scope="session")
def trino_iceberg_uri(trino_container) -> str:
    run_sql(
        "CREATE SCHEMA IF NOT EXISTS iceberg.kontra",
        "DROP TABLE IF EXISTS iceberg.kontra.events",
        "CREATE TABLE iceberg.kontra.events (event_id bigint NOT NULL, kind varchar, "
        "amount decimal(12, 2), ts timestamp(6) with time zone)",
        "INSERT INTO iceberg.kontra.events VALUES "
        "(1, 'click', 1.00, TIMESTAMP '2026-01-01 00:00:00 UTC'), "
        "(2, 'view', NULL, TIMESTAMP '2026-01-02 00:00:00 UTC'), "
        "(3, NULL, 2.50, NULL), "
        "(3, 'click', -1.00, TIMESTAMP '2026-01-03 00:00:00 UTC')",
    )
    yield URI_ICEBERG
    run_sql("DROP TABLE IF EXISTS iceberg.kontra.events")
