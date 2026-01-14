# src/kontra/engine/materializers/postgres.py
"""
PostgreSQL Materializer - loads PostgreSQL tables to Polars DataFrames.

Uses psycopg3's efficient binary COPY protocol for streaming data.
"""

from __future__ import annotations

import os
import time
from typing import Any, Dict, List, Optional

import polars as pl

from kontra.connectors.handle import DatasetHandle
from kontra.connectors.postgres import PostgresConnectionParams, get_connection

from .base import BaseMaterializer
from .registry import register_materializer


@register_materializer("postgres")
class PostgresMaterializer(BaseMaterializer):
    """
    Materialize PostgreSQL tables as Polars DataFrames with column projection.

    Features:
      - Efficient data loading via psycopg3
      - Column projection at source (SELECT only needed columns)
      - Binary protocol for faster transfers (when available)
    """

    def __init__(self, handle: DatasetHandle):
        super().__init__(handle)

        if not handle.db_params:
            raise ValueError("PostgreSQL handle missing db_params")

        self.params: PostgresConnectionParams = handle.db_params
        self._io_debug_enabled = bool(os.getenv("KONTRA_IO_DEBUG"))
        self._last_io_debug: Optional[Dict[str, Any]] = None

    def schema(self) -> List[str]:
        """Return column names without loading data."""
        with get_connection(self.params) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = %s AND table_name = %s
                    ORDER BY ordinal_position
                    """,
                    (self.params.schema, self.params.table),
                )
                return [row[0] for row in cur.fetchall()]

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

        query = f"SELECT {cols_sql} FROM {self.params.qualified_table}"

        with get_connection(self.params) as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                # Fetch all rows - for large tables, consider chunked loading
                rows = cur.fetchall()
                col_names = [desc[0] for desc in cur.description] if cur.description else []

        t1 = time.perf_counter()

        # Convert to Polars DataFrame
        if rows:
            df = pl.DataFrame(rows, schema=col_names, orient="row")
        else:
            # Empty DataFrame with correct schema
            df = pl.DataFrame(schema={name: pl.Utf8 for name in col_names})

        if self._io_debug_enabled:
            self._last_io_debug = {
                "materializer": "postgres",
                "mode": "psycopg_fetch",
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
    """Escape a PostgreSQL identifier (column/table name)."""
    # Double any internal quotes and wrap in quotes
    return '"' + name.replace('"', '""') + '"'
