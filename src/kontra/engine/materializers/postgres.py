# src/kontra/engine/materializers/postgres.py
"""
PostgreSQL Materializer - loads PostgreSQL tables to Polars DataFrames.

Uses psycopg3's efficient binary COPY protocol for streaming data.
"""

from __future__ import annotations

import os
import time
from typing import TYPE_CHECKING, Any, Dict, List, Optional

if TYPE_CHECKING:
    import polars as pl

from kontra.connectors.handle import DatasetHandle
from kontra.connectors.postgres import PostgresConnectionParams, get_connection
from kontra.connectors.detection import parse_table_reference, get_default_schema, POSTGRESQL
from contextlib import contextmanager

from .base import BaseMaterializer
from .registry import register_materializer


@contextmanager
def _get_connection_ctx(handle: DatasetHandle):
    """
    Get a connection context for either BYOC or URI-based handles.

    For BYOC, yields the external connection directly (not owned by us).
    For URI-based, yields a new connection (owned by context manager).
    """
    if handle.scheme == "byoc" and handle.external_conn is not None:
        # BYOC: yield external connection directly, don't close it
        yield handle.external_conn
    elif handle.db_params:
        # URI-based: use our connection manager
        with get_connection(handle.db_params) as conn:
            yield conn
    else:
        raise ValueError("Handle has neither external_conn nor db_params")


@register_materializer("postgres")
class PostgresMaterializer(BaseMaterializer):
    """
    Materialize PostgreSQL tables as Polars DataFrames with column projection.

    Features:
      - Efficient data loading via psycopg3
      - Column projection at source (SELECT only needed columns)
      - Binary protocol for faster transfers (when available)
      - BYOC (Bring Your Own Connection) support
    """

    def __init__(self, handle: DatasetHandle):
        super().__init__(handle)

        self._is_byoc = handle.scheme == "byoc" and handle.external_conn is not None

        if self._is_byoc:
            # BYOC: get table info from handle
            if not handle.table_ref:
                raise ValueError("BYOC handle missing table_ref")
            _db, schema, table = parse_table_reference(handle.table_ref)
            self._schema_name = schema or get_default_schema(POSTGRESQL)
            self._table_name = table
            self._qualified_table = f'{_esc_ident(self._schema_name)}.{_esc_ident(self._table_name)}'
        elif handle.db_params:
            # URI-based: use params
            self.params: PostgresConnectionParams = handle.db_params
            self._schema_name = self.params.schema
            self._table_name = self.params.table
            self._qualified_table = self.params.qualified_table
        else:
            raise ValueError("PostgreSQL handle missing db_params or external_conn")

        self._io_debug_enabled = bool(os.getenv("KONTRA_IO_DEBUG"))
        self._last_io_debug: Optional[Dict[str, Any]] = None

    def schema(self) -> List[str]:
        """Return column names without loading data."""
        with _get_connection_ctx(self.handle) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = %s AND table_name = %s
                    ORDER BY ordinal_position
                    """,
                    (self._schema_name, self._table_name),
                )
                return [row[0] for row in cur.fetchall()]

    def to_polars(self, columns: Optional[List[str]]) -> "pl.DataFrame":
        """
        Load table data as a Polars DataFrame with optional column projection.

        Supports both URI-based connections (handle.db_params) and
        BYOC connections (handle.external_conn).

        Args:
            columns: List of columns to load. If None, loads all columns.

        Returns:
            Polars DataFrame with the requested columns.

        Raises:
            ImportError: If polars is not installed.
        """
        try:
            import polars as pl  # Lazy import - only needed when residual rules exist
        except ImportError as e:
            raise ImportError(
                "Polars is required to materialize data for validation but is not installed. "
                "Install with: pip install polars"
            ) from e

        t0 = time.perf_counter()

        # Build column list for SELECT
        if columns:
            cols_sql = ", ".join(_esc_ident(c) for c in columns)
        else:
            cols_sql = "*"

        query = f"SELECT {cols_sql} FROM {self._qualified_table}"

        with _get_connection_ctx(self.handle) as conn:
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
                "mode": "psycopg_fetch" if not self._is_byoc else "byoc_fetch",
                "table": self._qualified_table,
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
