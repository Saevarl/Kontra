# src/kontra/connectors/clickhouse.py
"""
ClickHouse connection utilities for Kontra.

ClickHouse is accessed over HTTP via ``clickhouse-connect`` rather than a DBAPI
driver, so this module wraps the client in a small DBAPI-shaped cursor/connection
shim. That lets the shared executor and materializer use the same
``conn.cursor()`` / ``cursor.execute()`` / ``cursor.fetchall()`` pattern as the
PostgreSQL and SQL Server backends.

URI form:
    clickhouse://user:pass@host:8123/database/table

ClickHouse has no schema layer between database and table, so the path is simply
``/<database>/<table>``.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, List, Optional, Sequence, Tuple
from urllib.parse import unquote, urlparse

# Default HTTP port for the ClickHouse interface.
_DEFAULT_HTTP_PORT = 8123


@dataclass
class ClickHouseConnectionParams:
    """Resolved ClickHouse connection parameters."""

    host: str
    port: int
    user: str
    password: Optional[str]
    database: str
    table: str
    # Kept for interface parity with the other backends (ClickHouse has no
    # schema layer; the "schema" is the database).
    schema: str = ""
    secure: bool = False

    def client_kwargs(self) -> dict:
        return {
            "host": self.host,
            "port": self.port,
            "username": self.user,
            "password": self.password or "",
            "database": self.database,
            "secure": self.secure,
        }


def resolve_connection_params(uri: str) -> ClickHouseConnectionParams:
    """
    Parse a ``clickhouse://`` (or ``clickhouses://`` for TLS) URI into params.

    Raises:
        ValueError: If the URI is missing a database or table.
    """
    parsed = urlparse(uri)
    secure = parsed.scheme == "clickhouses"

    host = parsed.hostname or "localhost"
    port = parsed.port or (_DEFAULT_HTTP_PORT if not secure else 8443)
    user = unquote(parsed.username) if parsed.username else "default"
    password = unquote(parsed.password) if parsed.password else None

    # Path: /<database>/<table>
    parts = [p for p in (parsed.path or "").split("/") if p]
    if len(parts) < 2:
        raise ValueError(
            "ClickHouse URI must include a database and table: "
            "clickhouse://user:pass@host:8123/database/table "
            f"(got: {uri!r})"
        )
    database, table = parts[0], parts[1]

    return ClickHouseConnectionParams(
        host=host, port=port, user=user, password=password,
        database=database, table=table, schema=database, secure=secure,
    )


class _ClickHouseCursor:
    """Minimal DBAPI-style cursor over a clickhouse-connect client.

    Only the surface the executor/materializer use is implemented:
    execute / fetchone / fetchall / description / close.
    """

    def __init__(self, client: Any):
        self._client = client
        self._rows: List[Tuple] = []
        self._names: List[str] = []
        self._pos = 0

    def execute(self, sql: str, params: Optional[Sequence[Any]] = None) -> "_ClickHouseCursor":
        # Kontra builds fully-formed SQL (identifiers escaped, literals inlined
        # via lit_str), so positional params are not used on this path. If a
        # caller ever passes them, splice them in ClickHouse's {} style.
        if params:
            raise NotImplementedError(
                "Parameterized queries are not used on the ClickHouse path; "
                "SQL is built with escaped literals."
            )
        stripped = sql.lstrip().lower()
        if stripped.startswith(("select", "with", "show", "describe", "desc")):
            result = self._client.query(sql)
            self._rows = list(result.result_rows)
            self._names = list(result.column_names)
        else:
            self._client.command(sql)
            self._rows = []
            self._names = []
        self._pos = 0
        return self

    def fetchone(self) -> Optional[Tuple]:
        if self._pos >= len(self._rows):
            return None
        row = self._rows[self._pos]
        self._pos += 1
        return row

    def fetchall(self) -> List[Tuple]:
        rows = self._rows[self._pos:]
        self._pos = len(self._rows)
        return rows

    @property
    def description(self):
        # DBAPI description: 7-tuples; only name (index 0) is read by callers.
        return [(name,) + (None,) * 6 for name in self._names]

    def close(self) -> None:
        self._rows = []
        self._names = []


class ClickHouseConnection:
    """DBAPI-shaped connection wrapping a clickhouse-connect client."""

    def __init__(self, client: Any, owns: bool = True):
        self._client = client
        self._owns = owns

    def cursor(self) -> _ClickHouseCursor:
        return _ClickHouseCursor(self._client)

    def query_arrow(self, sql: str):
        """Fast path: return an Arrow table (used by the materializer)."""
        return self._client.query_arrow(sql)

    def close(self) -> None:
        if self._owns:
            self._client.close()

    def __enter__(self) -> "ClickHouseConnection":
        return self

    def __exit__(self, *exc) -> None:
        self.close()


def get_client(params: ClickHouseConnectionParams) -> Any:
    """Create a raw clickhouse-connect client."""
    try:
        import clickhouse_connect
    except ImportError as e:
        raise ImportError(
            "ClickHouse support requires 'clickhouse-connect'.\n"
            "Install with: pip install 'kontra[clickhouse]'"
        ) from e
    return clickhouse_connect.get_client(**params.client_kwargs())


@contextmanager
def get_connection(params: ClickHouseConnectionParams):
    """Yield a DBAPI-shaped ClickHouse connection, closed on exit."""
    conn = ClickHouseConnection(get_client(params), owns=True)
    try:
        yield conn
    finally:
        conn.close()
