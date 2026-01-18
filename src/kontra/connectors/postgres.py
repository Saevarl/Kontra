# src/kontra/connectors/postgres.py
"""
PostgreSQL connection utilities for Kontra.

Supports multiple authentication methods:
1. Full URI: postgres://user:pass@host:port/database/schema.table
2. Environment variables (libpq standard): PGHOST, PGPORT, PGUSER, PGPASSWORD, PGDATABASE
3. DATABASE_URL (common in PaaS like Heroku, Railway)

Priority: URI values > DATABASE_URL > PGXXX env vars > defaults
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Dict, Optional

from .db_utils import (
    DbConnectionConfig,
    resolve_connection_params as _resolve_params,
)


# PostgreSQL-specific configuration for parameter resolution
_PG_CONFIG = DbConnectionConfig(
    default_host="localhost",
    default_port=5432,
    default_user=os.getenv("USER", "postgres"),
    default_schema="public",
    env_host="PGHOST",
    env_port="PGPORT",
    env_user="PGUSER",
    env_password="PGPASSWORD",
    env_database="PGDATABASE",
    env_url="DATABASE_URL",
    db_name="PostgreSQL",
    uri_example="postgres://user:pass@host:5432/database/schema.table",
    env_example="PGDATABASE",
)


@dataclass
class PostgresConnectionParams:
    """Resolved PostgreSQL connection parameters."""

    host: str
    port: int
    user: str
    password: Optional[str]
    database: str
    schema: str
    table: str

    def to_dict(self) -> Dict[str, Any]:
        """Return connection kwargs for psycopg.connect()."""
        return {
            "host": self.host,
            "port": self.port,
            "user": self.user,
            "password": self.password,
            "dbname": self.database,
        }

    @property
    def qualified_table(self) -> str:
        """Return schema.table identifier."""
        return f"{self.schema}.{self.table}"


def resolve_connection_params(uri: str) -> PostgresConnectionParams:
    """
    Resolve PostgreSQL connection parameters from URI + environment.

    URI format:
        postgres://user:pass@host:port/database/schema.table
        postgres:///public.users  (uses env vars for connection)

    Priority: URI values > DATABASE_URL > PGXXX env vars > defaults

    Raises:
        ValueError: If required parameters (database, table) cannot be resolved.
    """
    resolved = _resolve_params(uri, _PG_CONFIG)

    return PostgresConnectionParams(
        host=resolved.host,
        port=resolved.port,
        user=resolved.user,
        password=resolved.password,
        database=resolved.database,  # type: ignore (validated in _resolve_params)
        schema=resolved.schema,
        table=resolved.table,  # type: ignore (validated in _resolve_params)
    )


def get_connection(params: PostgresConnectionParams):
    """
    Create a psycopg connection from resolved parameters.

    Returns:
        psycopg.Connection
    """
    try:
        import psycopg
    except ImportError as e:
        raise ImportError(
            "psycopg is required for PostgreSQL support.\n"
            "Install with: pip install 'psycopg[binary]'"
        ) from e

    try:
        return psycopg.connect(**params.to_dict())
    except psycopg.OperationalError as e:
        raise ConnectionError(
            f"PostgreSQL connection failed: {e}\n\n"
            f"Connection details:\n"
            f"  Host: {params.host}:{params.port}\n"
            f"  Database: {params.database}\n"
            f"  User: {params.user}\n\n"
            "Check your connection settings or set environment variables:\n"
            "  export PGHOST=localhost\n"
            "  export PGPORT=5432\n"
            "  export PGUSER=your_user\n"
            "  export PGPASSWORD=your_password\n"
            "  export PGDATABASE=your_database"
        ) from e
