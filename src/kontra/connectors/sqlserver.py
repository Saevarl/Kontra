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


def fetch_sqlserver_stats(params: SqlServerConnectionParams) -> Dict[str, Dict[str, Any]]:
    """
    Fetch SQL Server statistics from sys.dm_db_stats_properties and related DMVs.

    Returns a dict keyed by column name with stats:
        {
            "column_name": {
                "null_frac": 0.02,        # Estimated fraction of nulls
                "n_distinct": 1000,       # Estimated distinct values (-1 = unique)
                "rows": 10000,            # Rows when stats were computed
            },
            "__table__": {
                "row_estimate": 10000,
                "page_count": 100,
            }
        }
    """
    import pymssql

    with get_connection(params) as conn:
        with conn.cursor() as cursor:
            # Table-level stats from sys.dm_db_partition_stats
            cursor.execute(
                """
                SELECT SUM(row_count) AS row_estimate,
                       SUM(used_page_count) AS page_count
                FROM sys.dm_db_partition_stats ps
                JOIN sys.objects o ON ps.object_id = o.object_id
                JOIN sys.schemas s ON o.schema_id = s.schema_id
                WHERE s.name = %s AND o.name = %s AND ps.index_id IN (0, 1)
                """,
                (params.schema, params.table),
            )
            row = cursor.fetchone()
            table_stats = {
                "row_estimate": row[0] if row and row[0] else 0,
                "page_count": row[1] if row and row[1] else 0,
            }

            # Column-level stats from sys.dm_db_stats_properties + DBCC SHOW_STATISTICS
            # We use density_vector from stats to estimate distinct values
            cursor.execute(
                """
                SELECT
                    c.name AS column_name,
                    s.name AS stat_name,
                    sp.rows,
                    sp.modification_counter
                FROM sys.stats s
                JOIN sys.stats_columns sc ON s.stats_id = sc.stats_id AND s.object_id = sc.object_id
                JOIN sys.columns c ON sc.column_id = c.column_id AND sc.object_id = c.object_id
                CROSS APPLY sys.dm_db_stats_properties(s.object_id, s.stats_id) sp
                WHERE s.object_id = OBJECT_ID(%s)
                """,
                (f"{params.schema}.{params.table}",),
            )

            result: Dict[str, Dict[str, Any]] = {"__table__": table_stats}

            for row in cursor.fetchall():
                col_name, stat_name, rows, mod_counter = row
                if col_name not in result:
                    result[col_name] = {
                        "rows": rows,
                        "modification_counter": mod_counter,
                        "stat_name": stat_name,
                    }

            # For each column with stats, get density (1/distinct) from DBCC SHOW_STATISTICS
            # This requires more complex parsing, so we'll do a simpler approach:
            # Query actual distinct counts for key columns (more reliable for preplan)
            for col_name in list(result.keys()):
                if col_name == "__table__":
                    continue
                try:
                    # Get null fraction
                    cursor.execute(
                        f"""
                        SELECT
                            CAST(SUM(CASE WHEN [{col_name}] IS NULL THEN 1 ELSE 0 END) AS FLOAT)
                            / NULLIF(COUNT(*), 0) AS null_frac,
                            COUNT(DISTINCT [{col_name}]) AS n_distinct
                        FROM [{params.schema}].[{params.table}]
                        """,
                    )
                    stats_row = cursor.fetchone()
                    if stats_row:
                        result[col_name]["null_frac"] = stats_row[0] or 0.0
                        result[col_name]["n_distinct"] = stats_row[1] or 0
                        # Mark as unique if distinct = row count
                        if result[col_name]["n_distinct"] == table_stats["row_estimate"]:
                            result[col_name]["n_distinct"] = -1  # Convention: -1 = all unique
                except (KeyError, TypeError, IndexError):
                    # Stats query failed or malformed, leave partial data
                    pass

            return result
