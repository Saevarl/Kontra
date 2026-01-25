# src/kontra/engine/executors/postgres_sql.py
"""
PostgreSQL SQL Executor - executes validation rules via SQL pushdown.

Uses DatabaseSqlExecutor base class for shared compile/execute logic.

Supports rules:
  - not_null(column) - uses EXISTS (fast early termination)
  - unique(column) - uses COUNT DISTINCT
  - min_rows(threshold) - uses COUNT
  - max_rows(threshold) - uses COUNT
  - allowed_values(column, values) - uses SUM CASE
  - freshness(column, max_age) - uses MAX
  - range(column, min, max) - uses SUM CASE
  - regex(column, pattern) - uses ~ operator
  - compare(left, right, op) - uses SUM CASE
  - conditional_not_null(column, when) - uses SUM CASE
  - conditional_range(column, when, min, max) - uses SUM CASE
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Dict, List, Tuple

from kontra.connectors.handle import DatasetHandle
from kontra.connectors.postgres import PostgresConnectionParams, get_connection
from kontra.connectors.detection import parse_table_reference, get_default_schema, POSTGRESQL

from .database_base import DatabaseSqlExecutor
from .registry import register_executor


@register_executor("postgres")
class PostgresSqlExecutor(DatabaseSqlExecutor):
    """
    PostgreSQL SQL pushdown executor.

    Inherits compile() and execute() from DatabaseSqlExecutor.
    Provides PostgreSQL-specific connection and table handling.
    """

    DIALECT = "postgres"
    SUPPORTED_RULES = {
        "not_null", "unique", "min_rows", "max_rows",
        "allowed_values", "disallowed_values",
        "freshness", "range", "length",
        "regex", "contains", "starts_with", "ends_with",
        "compare", "conditional_not_null", "conditional_range",
        "custom_sql_check", "custom_agg"
    }

    @property
    def name(self) -> str:
        return "postgres"

    def _supports_scheme(self, scheme: str, handle: DatasetHandle) -> bool:
        """Check if this executor supports the given URI scheme."""
        # BYOC: check dialect for external connections
        if scheme == "byoc" and handle.dialect == "postgresql":
            return handle.external_conn is not None

        # URI-based: handle postgres:// URIs
        return scheme in {"postgres", "postgresql"}

    @contextmanager
    def _get_connection_ctx(self, handle: DatasetHandle):
        """
        Get a PostgreSQL connection context.

        For BYOC, yields the external connection directly (not owned by us).
        For URI-based, yields a new connection (owned by context manager).
        """
        if handle.scheme == "byoc" and handle.external_conn is not None:
            yield handle.external_conn
        elif handle.db_params:
            with get_connection(handle.db_params) as conn:
                yield conn
        else:
            raise ValueError("Handle has neither external_conn nor db_params")

    def _get_table_reference(self, handle: DatasetHandle) -> str:
        """
        Get the fully-qualified table reference for PostgreSQL.

        Returns: "schema"."table" format.
        """
        if handle.scheme == "byoc" and handle.table_ref:
            _db, schema, table = parse_table_reference(handle.table_ref)
            schema = schema or get_default_schema(POSTGRESQL)
            return f"{self._esc(schema)}.{self._esc(table)}"
        elif handle.db_params:
            params: PostgresConnectionParams = handle.db_params
            return f"{self._esc(params.schema)}.{self._esc(params.table)}"
        else:
            raise ValueError("Handle has neither table_ref nor db_params")

    def _get_schema_and_table(self, handle: DatasetHandle) -> Tuple[str, str]:
        """
        Get schema and table name separately for custom SQL placeholder replacement.

        Returns: Tuple of (schema, table_name)
        """
        if handle.scheme == "byoc" and handle.table_ref:
            _db, schema, table = parse_table_reference(handle.table_ref)
            schema = schema or get_default_schema(POSTGRESQL)
            return schema, table
        elif handle.db_params:
            params: PostgresConnectionParams = handle.db_params
            return params.schema, params.table
        else:
            raise ValueError("Handle has neither table_ref nor db_params")

    def _get_cursor(self, conn):
        """Get a cursor using context manager."""
        # PostgreSQL cursors support context managers
        return conn.cursor()

    def _close_cursor(self, cursor):
        """Close the cursor."""
        cursor.close()

    def introspect(self, handle: DatasetHandle, **kwargs) -> Dict[str, Any]:
        """
        Introspect the PostgreSQL table for metadata.

        Returns:
            {"row_count": int, "available_cols": [...], "staging": None}
        """
        table = self._get_table_reference(handle)

        # Get schema and table name for information_schema query
        if handle.scheme == "byoc" and handle.table_ref:
            _db, schema, table_name = parse_table_reference(handle.table_ref)
            schema = schema or get_default_schema(POSTGRESQL)
        elif handle.db_params:
            params: PostgresConnectionParams = handle.db_params
            schema = params.schema
            table_name = params.table
        else:
            raise ValueError("Handle has neither table_ref nor db_params")

        with self._get_connection_ctx(handle) as conn:
            with conn.cursor() as cur:
                # Get row count
                cur.execute(f"SELECT COUNT(*) FROM {table}")
                row_count = cur.fetchone()
                n = int(row_count[0]) if row_count else 0

                # Get column names
                cur.execute(
                    """
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = %s AND table_name = %s
                    ORDER BY ordinal_position
                    """,
                    (schema, table_name),
                )
                cols = [row[0] for row in cur.fetchall()]

        return {"row_count": n, "available_cols": cols, "staging": None}
