# src/kontra/connectors/sqlserver.py
"""
SQL Server connection utilities for Kontra.

Supports multiple authentication methods:
1. Full URI: mssql://user:pass@host:port/database/schema.table
2. Environment variables: MSSQL_HOST, MSSQL_PORT, MSSQL_USER, MSSQL_PASSWORD, MSSQL_DATABASE
3. SQLSERVER_URL (similar to DATABASE_URL pattern)

Priority: URI values > SQLSERVER_URL > MSSQL_XXX env vars > defaults
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

from .db_utils import (
    DbConnectionConfig,
    resolve_connection_params as _resolve_params,
)


# SQL Server-specific configuration for parameter resolution
_MSSQL_CONFIG = DbConnectionConfig(
    default_host="localhost",
    default_port=1433,
    default_user="sa",
    default_schema="dbo",
    env_host="MSSQL_HOST",
    env_port="MSSQL_PORT",
    env_user="MSSQL_USER",
    env_password="MSSQL_PASSWORD",
    env_database="MSSQL_DATABASE",
    env_url="SQLSERVER_URL",
    db_name="SQL Server",
    uri_example="mssql://user:pass@host:1433/database/schema.table",
    env_example="MSSQL_DATABASE",
)


@dataclass
class SqlServerConnectionParams:
    """Resolved SQL Server connection parameters."""

    host: str
    port: int
    user: str
    password: Optional[str]
    database: str
    schema: str
    table: str

    def to_dict(self) -> Dict[str, Any]:
        """Return connection kwargs for pymssql.connect()."""
        return {
            "server": self.host,
            "port": self.port,
            "user": self.user,
            "password": self.password,
            "database": self.database,
        }

    @property
    def qualified_table(self) -> str:
        """Return schema.table identifier."""
        return f"{self.schema}.{self.table}"


def resolve_connection_params(uri: str) -> SqlServerConnectionParams:
    """
    Resolve SQL Server connection parameters from URI + environment.

    URI format:
        mssql://user:pass@host:port/database/schema.table
        mssql:///dbo.users  (uses env vars for connection)
        sqlserver://...  (alias for mssql://)

    Priority: URI values > SQLSERVER_URL > MSSQL_XXX env vars > defaults

    Raises:
        ValueError: If required parameters (database, table) cannot be resolved.
    """
    resolved = _resolve_params(uri, _MSSQL_CONFIG)

    return SqlServerConnectionParams(
        host=resolved.host,
        port=resolved.port,
        user=resolved.user,
        password=resolved.password,
        database=resolved.database,  # type: ignore (validated in _resolve_params)
        schema=resolved.schema,
        table=resolved.table,  # type: ignore (validated in _resolve_params)
    )


def get_connection(params: SqlServerConnectionParams):
    """
    Create a pymssql connection from resolved parameters.

    Returns:
        pymssql.Connection
    """
    try:
        import pymssql
    except ImportError as e:
        raise ImportError(
            "pymssql is required for SQL Server support.\n"
            "Install with: pip install pymssql"
        ) from e

    try:
        return pymssql.connect(**params.to_dict())
    except pymssql.OperationalError as e:
        raise ConnectionError(
            f"SQL Server connection failed: {e}\n\n"
            f"Connection details:\n"
            f"  Host: {params.host}:{params.port}\n"
            f"  Database: {params.database}\n"
            f"  User: {params.user}\n\n"
            "Check your connection settings or set environment variables:\n"
            "  export MSSQL_HOST=localhost\n"
            "  export MSSQL_PORT=1433\n"
            "  export MSSQL_USER=your_user\n"
            "  export MSSQL_PASSWORD=your_password\n"
            "  export MSSQL_DATABASE=your_database"
        ) from e
