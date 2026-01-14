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
from urllib.parse import urlparse, unquote


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

    def to_dsn(self) -> str:
        """Return a psycopg-compatible DSN string."""
        parts = [f"host={self.host}", f"port={self.port}", f"dbname={self.database}"]
        if self.user:
            parts.append(f"user={self.user}")
        if self.password:
            parts.append(f"password={self.password}")
        return " ".join(parts)

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
    parsed = urlparse(uri)

    # Start with defaults
    host = "localhost"
    port = 5432
    user = os.getenv("USER", "postgres")
    password: Optional[str] = None
    database: Optional[str] = None
    schema = "public"
    table: Optional[str] = None

    # Layer 1: Standard PGXXX environment variables
    if os.getenv("PGHOST"):
        host = os.getenv("PGHOST", host)
    if os.getenv("PGPORT"):
        try:
            port = int(os.getenv("PGPORT", str(port)))
        except ValueError:
            pass
    if os.getenv("PGUSER"):
        user = os.getenv("PGUSER", user)
    if os.getenv("PGPASSWORD"):
        password = os.getenv("PGPASSWORD")
    if os.getenv("PGDATABASE"):
        database = os.getenv("PGDATABASE")

    # Layer 2: DATABASE_URL (common in PaaS)
    database_url = os.getenv("DATABASE_URL")
    if database_url:
        db_parsed = urlparse(database_url)
        if db_parsed.hostname:
            host = db_parsed.hostname
        if db_parsed.port:
            port = db_parsed.port
        if db_parsed.username:
            user = unquote(db_parsed.username)
        if db_parsed.password:
            password = unquote(db_parsed.password)
        if db_parsed.path and db_parsed.path != "/":
            # DATABASE_URL path is just /database
            database = db_parsed.path.strip("/").split("/")[0]

    # Layer 3: Explicit URI values (highest priority)
    if parsed.hostname:
        host = parsed.hostname
    if parsed.port:
        port = parsed.port
    if parsed.username:
        user = unquote(parsed.username)
    if parsed.password:
        password = unquote(parsed.password)

    # Extract database and schema.table from path
    # Format: /database/schema.table or /database/table (assumes public schema)
    path_parts = [p for p in parsed.path.strip("/").split("/") if p]

    if len(path_parts) >= 1:
        database = path_parts[0]

    if len(path_parts) >= 2:
        schema_table = path_parts[1]
        if "." in schema_table:
            schema, table = schema_table.split(".", 1)
        else:
            schema = "public"
            table = schema_table

    # Validate required fields
    if not database:
        raise ValueError(
            "PostgreSQL database name is required.\n\n"
            "Set PGDATABASE environment variable or use full URI:\n"
            "  postgres://user:pass@host:5432/database/schema.table"
        )

    if not table:
        raise ValueError(
            "PostgreSQL table name is required.\n\n"
            "Specify schema.table in URI:\n"
            "  postgres://user:pass@host:5432/database/schema.table\n"
            "  postgres:///public.users (with PGDATABASE set)"
        )

    return PostgresConnectionParams(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
        schema=schema,
        table=table,
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


def fetch_pg_stats(params: PostgresConnectionParams) -> Dict[str, Dict[str, Any]]:
    """
    Fetch PostgreSQL statistics from pg_stats and pg_class.

    Returns a dict keyed by column name with stats:
        {
            "column_name": {
                "null_frac": 0.02,        # Fraction of nulls
                "n_distinct": -1,          # -1 = unique, >0 = count, <0 = fraction
                "most_common_vals": [...],
                "most_common_freqs": [...],
            },
            "__table__": {
                "row_estimate": 1000,
                "page_count": 10,
            }
        }
    """
    import psycopg

    with get_connection(params) as conn:
        with conn.cursor() as cur:
            # Table-level stats from pg_class
            cur.execute(
                """
                SELECT reltuples::bigint AS row_estimate,
                       relpages AS page_count
                FROM pg_class
                WHERE relname = %s
                  AND relnamespace = %s::regnamespace
                """,
                (params.table, params.schema),
            )
            row = cur.fetchone()
            table_stats = {
                "row_estimate": row[0] if row else 0,
                "page_count": row[1] if row else 0,
            }

            # Column-level stats from pg_stats
            cur.execute(
                """
                SELECT attname AS column_name,
                       null_frac,
                       n_distinct,
                       most_common_vals::text,
                       most_common_freqs::text
                FROM pg_stats
                WHERE schemaname = %s AND tablename = %s
                """,
                (params.schema, params.table),
            )

            result: Dict[str, Dict[str, Any]] = {"__table__": table_stats}
            for row in cur.fetchall():
                col_name, null_frac, n_distinct, mcv, mcf = row
                result[col_name] = {
                    "null_frac": null_frac,
                    "n_distinct": n_distinct,
                    "most_common_vals": mcv,
                    "most_common_freqs": mcf,
                }

            return result
