# src/kontra/connectors/db_utils.py
"""
Shared utilities for database connectors.

This module provides common functionality for resolving connection parameters
from URIs and environment variables, reducing duplication between
postgres.py and sqlserver.py.
"""

from __future__ import annotations

import re
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple, TYPE_CHECKING
from urllib.parse import urlparse, unquote
import os

if TYPE_CHECKING:
    from kontra.connectors.handle import DatasetHandle


@dataclass
class DbConnectionConfig:
    """Configuration for resolving database connection parameters."""

    # Defaults
    default_host: str
    default_port: int
    default_user: str
    default_schema: str

    # Environment variable names
    env_host: str
    env_port: str
    env_user: str
    env_password: str
    env_database: str
    env_url: Optional[str]  # e.g., DATABASE_URL, SQLSERVER_URL

    # Error message context
    db_name: str  # e.g., "PostgreSQL", "SQL Server"
    uri_example: str  # e.g., "postgres://user:pass@host:5432/database/schema.table"
    env_example: str  # e.g., "PGDATABASE"


@dataclass
class ResolvedConnectionParams:
    """
    Generic resolved connection parameters.

    Dialect-specific connectors convert this to their own dataclass.
    """

    host: str
    port: int
    user: str
    password: Optional[str]
    database: Optional[str]
    schema: str
    table: Optional[str]


def resolve_connection_params(
    uri: str,
    config: DbConnectionConfig,
) -> ResolvedConnectionParams:
    """
    Resolve database connection parameters from URI + environment.

    Three-layer resolution with later layers overriding earlier:
      1. Environment variables (PGXXX, MSSQL_XXX, etc.)
      2. URL environment variable (DATABASE_URL, SQLSERVER_URL)
      3. Explicit URI values (highest priority)

    Args:
        uri: The connection URI
        config: Dialect-specific configuration

    Returns:
        ResolvedConnectionParams with all values resolved

    Raises:
        ValueError: If required parameters (database, table) cannot be resolved
    """
    parsed = urlparse(uri)

    # Start with defaults
    host = config.default_host
    port = config.default_port
    user = config.default_user
    password: Optional[str] = None
    database: Optional[str] = None
    schema = config.default_schema
    table: Optional[str] = None

    # Layer 1: Standard environment variables
    host, port, user, password, database = _apply_env_vars(
        host, port, user, password, database, config
    )

    # Layer 2: URL environment variable (if configured)
    if config.env_url:
        host, port, user, password, database = _apply_url_env_var(
            host, port, user, password, database, config.env_url
        )

    # Layer 3: Explicit URI values (highest priority)
    host, port, user, password = _apply_uri_connection(
        host, port, user, password, parsed
    )

    # Extract database and schema.table from path
    database, schema, table = _parse_uri_path(
        parsed.path, database, config.default_schema
    )

    # Validate required fields
    _validate_required_fields(database, table, config)

    return ResolvedConnectionParams(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
        schema=schema,
        table=table,
    )


def _apply_env_vars(
    host: str,
    port: int,
    user: str,
    password: Optional[str],
    database: Optional[str],
    config: DbConnectionConfig,
) -> Tuple[str, int, str, Optional[str], Optional[str]]:
    """Apply environment variables (Layer 1)."""
    if os.getenv(config.env_host):
        host = os.getenv(config.env_host, host)
    if os.getenv(config.env_port):
        try:
            port = int(os.getenv(config.env_port, str(port)))
        except ValueError:
            pass
    if os.getenv(config.env_user):
        user = os.getenv(config.env_user, user)
    if os.getenv(config.env_password):
        password = os.getenv(config.env_password)
    if os.getenv(config.env_database):
        database = os.getenv(config.env_database)

    return host, port, user, password, database


def _apply_url_env_var(
    host: str,
    port: int,
    user: str,
    password: Optional[str],
    database: Optional[str],
    env_url_name: str,
) -> Tuple[str, int, str, Optional[str], Optional[str]]:
    """Apply URL environment variable like DATABASE_URL (Layer 2)."""
    url_value = os.getenv(env_url_name)
    if not url_value:
        return host, port, user, password, database

    db_parsed = urlparse(url_value)
    if db_parsed.hostname:
        host = db_parsed.hostname
    if db_parsed.port:
        port = db_parsed.port
    if db_parsed.username:
        user = unquote(db_parsed.username)
    if db_parsed.password:
        password = unquote(db_parsed.password)
    if db_parsed.path and db_parsed.path != "/":
        database = db_parsed.path.strip("/").split("/")[0]

    return host, port, user, password, database


def _apply_uri_connection(
    host: str,
    port: int,
    user: str,
    password: Optional[str],
    parsed,
) -> Tuple[str, int, str, Optional[str]]:
    """Apply explicit URI connection values (Layer 3)."""
    if parsed.hostname:
        host = parsed.hostname
    if parsed.port:
        port = parsed.port
    if parsed.username:
        user = unquote(parsed.username)
    if parsed.password:
        password = unquote(parsed.password)

    return host, port, user, password


def _parse_uri_path(
    path: str,
    current_database: Optional[str],
    default_schema: str,
) -> Tuple[Optional[str], str, Optional[str]]:
    """
    Parse database, schema, and table from URI path.

    Format: /database/schema.table or /database/table (uses default schema)
    """
    database = current_database
    schema = default_schema
    table: Optional[str] = None

    path_parts = [p for p in path.strip("/").split("/") if p]

    if len(path_parts) >= 1:
        database = path_parts[0]

    if len(path_parts) >= 2:
        schema_table = path_parts[1]
        if "." in schema_table:
            schema, table = schema_table.split(".", 1)
        else:
            schema = default_schema
            table = schema_table

    return database, schema, table


def _validate_required_fields(
    database: Optional[str],
    table: Optional[str],
    config: DbConnectionConfig,
) -> None:
    """Validate that required fields are present."""
    if not database:
        raise ValueError(
            f"{config.db_name} database name is required.\n\n"
            f"Set {config.env_database} environment variable or use full URI:\n"
            f"  {config.uri_example}"
        )

    if not table:
        raise ValueError(
            f"{config.db_name} table name is required.\n\n"
            f"Specify schema.table in URI:\n"
            f"  {config.uri_example}\n"
            f"  {config.uri_example.split('/')[0]}///{config.default_schema}.users "
            f"(with {config.env_database} set)"
        )


def mask_credentials(uri: str) -> str:
    """
    Mask credentials in a URI for safe display in logs/output.

    Handles patterns like:
    - postgres://user:password@host/db -> postgres://user:***@host/db
    - mssql://sa:Secret123!@host/db -> mssql://sa:***@host/db
    - Any URI with ://user:password@ pattern (including @ in password)

    Args:
        uri: URI that may contain credentials

    Returns:
        URI with password masked as '***'
    """
    if not uri or "://" not in uri:
        return uri

    # Primary: mask greedily from the first ':' after '://' to the LAST '@'.
    # urlparse cannot be trusted here — a password containing '/', '#', '?' or
    # '@' makes it split the netloc at the wrong place (password=None, or a
    # partial mask that leaks the rest of the secret into the "host"). Host and
    # path carry no '@' in Kontra URIs, so the last '@' is the userinfo/host
    # boundary. Over-masks a path containing '@' rather than risk a leak —
    # masking must fail safe. Proper fix for such URIs is percent-encoding at
    # build time (resolve_datasource); this stays defensive.
    m = re.match(r"^([^:]+://[^:/@]+:).+@([^@]*)$", uri)
    if m:
        return f"{m.group(1)}***@{m.group(2)}"

    # Fallback for shapes the regex doesn't cover (e.g. no explicit password).
    try:
        parsed = urlparse(uri)
        if parsed.password:
            if parsed.port:
                new_netloc = f"{parsed.username}:***@{parsed.hostname}:{parsed.port}"
            else:
                new_netloc = f"{parsed.username}:***@{parsed.hostname}"
            return uri.replace(parsed.netloc, new_netloc)
    except (ValueError, AttributeError):
        pass

    return uri


# --------------------------------------------------------------------------- #
# Connection context manager
# --------------------------------------------------------------------------- #

@contextmanager
def get_connection_ctx(handle: "DatasetHandle", dialect: str):
    """
    Get a connection context for either BYOC or URI-based handles.

    For BYOC, yields the external connection directly (not owned by us).
    For a run-scoped URI connection, yields the engine-owned connection and
    rolls back at the phase boundary. Otherwise, yields a fresh connection.

    Args:
        handle: DatasetHandle with connection info
        dialect: "postgres" or "sqlserver"
    """
    if handle.scheme == "byoc" and handle.external_conn is not None:
        # BYOC: yield external connection directly, don't close it
        yield handle.external_conn
    elif handle.owned_conn is not None:
        try:
            yield handle.owned_conn
        finally:
            # Kontra's validation queries are read-only. Reset the transaction
            # between phases so a prior query cannot affect the next one.
            handle.owned_conn.rollback()
    elif handle.db_params:
        # URI-based: use our connection manager
        if dialect == "postgres":
            from kontra.connectors.postgres import get_connection
        elif dialect in ("sqlserver", "mssql"):
            from kontra.connectors.sqlserver import get_connection
        elif dialect in ("clickhouse", "clickhouses"):
            from kontra.connectors.clickhouse import get_connection
        else:
            raise ValueError(f"Unknown dialect: {dialect}")
        with get_connection(handle.db_params) as conn:
            yield conn
    else:
        raise ValueError("Handle has neither external_conn nor db_params")


def open_shared_postgres_connection(handle: "DatasetHandle") -> None:
    """Open and stash the PostgreSQL connection owned by one validation run."""
    if handle.scheme not in ("postgres", "postgresql") or not handle.db_params:
        return
    if handle.owned_conn is not None:
        return

    from kontra.connectors.postgres import get_connection

    object.__setattr__(handle, "owned_conn", get_connection(handle.db_params))


def close_shared_postgres_connection(handle: "DatasetHandle | None") -> None:
    """Close the URI-owned PostgreSQL connection exactly once, if present."""
    if (
        handle is None
        or handle.scheme not in ("postgres", "postgresql")
        or handle.owned_conn is None
    ):
        return

    conn = handle.owned_conn
    try:
        conn.rollback()
    finally:
        try:
            conn.close()
        finally:
            object.__setattr__(handle, "owned_conn", None)


# --------------------------------------------------------------------------- #
# Paramstyle compatibility shim
# --------------------------------------------------------------------------- #

def _is_pyodbc(obj: Any) -> bool:
    """
    True if a connection/cursor belongs to the pyodbc driver.

    Uses the same module-name trick as connectors/detection.py. pyodbc's
    cursor/connection classes both live in the top-level ``pyodbc`` module.
    """
    return type(obj).__module__.split(".")[0] == "pyodbc"


def execute_with_params(cursor: Any, sql: str, params: Any = None) -> Any:
    """
    Execute a parameterized *static catalog query*, adapting paramstyle.

    pymssql uses ``%s`` placeholders; pyodbc uses ``?``. When the cursor belongs
    to pyodbc (the Entra ID path, or a BYOC pyodbc connection), rewrite ``%s`` to
    ``?`` so the same query text works on both drivers.

    IMPORTANT: This performs a literal ``%s`` -> ``?`` substitution and is ONLY
    safe for the fixed, internal catalog/metadata queries it is applied to. Those
    queries contain no literal ``%s`` outside of parameter placeholders. Do NOT
    use this helper for arbitrary or user-supplied SQL.

    postgres (psycopg) uses ``%s`` natively and must never be routed here.

    Args:
        cursor: A DBAPI cursor (pymssql or pyodbc).
        sql: The query text using ``%s`` placeholders.
        params: Parameter tuple/sequence, or None for no parameters.

    Returns:
        The cursor (for convenience / chaining).
    """
    if _is_pyodbc(cursor) and "%s" in sql:
        sql = sql.replace("%s", "?")
    if params is None:
        cursor.execute(sql)
    else:
        cursor.execute(sql, params)
    return cursor


# --------------------------------------------------------------------------- #
# Identifier quoting
# --------------------------------------------------------------------------- #

def pg_quote_ident(name: str) -> str:
    """Escape a PostgreSQL identifier (column/table name)."""
    return '"' + name.replace('"', '""') + '"'


def ss_quote_ident(name: str) -> str:
    """Escape a SQL Server identifier (column/table name)."""
    return "[" + name.replace("]", "]]") + "]"
