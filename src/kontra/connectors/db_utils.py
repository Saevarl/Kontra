# src/kontra/connectors/db_utils.py
"""
Shared utilities for database connectors.

This module provides common functionality for resolving connection parameters
from URIs and environment variables, reducing duplication between
postgres.py and sqlserver.py.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse, unquote
import os


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
