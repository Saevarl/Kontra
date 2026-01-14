# src/kontra/engine/materializers/sqlserver.py
"""
SQL Server Materializer - loads SQL Server tables to Polars DataFrames.

Uses pymssql for database connectivity.
"""

from __future__ import annotations

import os
import time
from typing import Any, Dict, List, Optional

import polars as pl

from kontra.connectors.handle import DatasetHandle
from kontra.connectors.sqlserver import SqlServerConnectionParams, get_connection

from .base import BaseMaterializer
from .registry import register_materializer


@register_materializer("sqlserver")
class SqlServerMaterializer(BaseMaterializer):
    """
    Materialize SQL Server tables as Polars DataFrames with column projection.

    Features:
      - Efficient data loading via pymssql
      - Column projection at source (SELECT only needed columns)
    """

    materializer_name = "sqlserver"

    def __init__(self, handle: DatasetHandle):
        super().__init__(handle)

        if not handle.db_params:
            raise ValueError("SQL Server handle missing db_params")

        self.params: SqlServerConnectionParams = handle.db_params
        self._io_debug_enabled = bool(os.getenv("KONTRA_IO_DEBUG"))
        self._last_io_debug: Optional[Dict[str, Any]] = None

    def schema(self) -> List[str]:
        """Return column names without loading data."""
        with get_connection(self.params) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
                """,
                (self.params.schema, self.params.table),
            )
            return [row[0] for row in cursor.fetchall()]

    def to_polars(self, columns: Optional[List[str]]) -> pl.DataFrame:
        """
        Load table data as a Polars DataFrame with optional column projection.

        Args:
            columns: List of columns to load. If None, loads all columns.

        Returns:
            Polars DataFrame with the requested columns.
        """
        t0 = time.perf_counter()

        # Build column list for SELECT
        if columns:
            cols_sql = ", ".join(_esc_ident(c) for c in columns)
        else:
            cols_sql = "*"

        # SQL Server uses [schema].[table] syntax
        qualified_table = f"[{self.params.schema}].[{self.params.table}]"
        query = f"SELECT {cols_sql} FROM {qualified_table}"

        with get_connection(self.params) as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            # Fetch all rows - for large tables, consider chunked loading
            rows = cursor.fetchall()
            col_names = [desc[0] for desc in cursor.description] if cursor.description else []

        t1 = time.perf_counter()

        # Convert to Polars DataFrame
        if rows:
            df = pl.DataFrame(rows, schema=col_names, orient="row")
        else:
            # Empty DataFrame with correct schema
            df = pl.DataFrame(schema={name: pl.Utf8 for name in col_names})

        if self._io_debug_enabled:
            self._last_io_debug = {
                "materializer": "sqlserver",
                "mode": "pymssql_fetch",
                "table": self.params.qualified_table,
                "columns_requested": list(columns or []),
                "column_count": len(columns or col_names),
                "row_count": len(rows) if rows else 0,
                "elapsed_ms": int((t1 - t0) * 1000),
            }
        else:
            self._last_io_debug = None

        return df

    def io_debug(self) -> Optional[Dict[str, Any]]:
        return self._last_io_debug


def _esc_ident(name: str) -> str:
    """Escape a SQL Server identifier (column/table name)."""
    # SQL Server uses [brackets] for quoting identifiers
    # Double any internal brackets
    return "[" + name.replace("]", "]]") + "]"
