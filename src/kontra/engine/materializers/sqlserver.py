# src/kontra/engine/materializers/sqlserver.py
"""
SQL Server Materializer - loads SQL Server tables to Polars DataFrames.

Uses pymssql for database connectivity.
"""

from __future__ import annotations

import os
import time
from typing import TYPE_CHECKING, Any, Dict, List, Optional

if TYPE_CHECKING:
    import polars as pl

from kontra.connectors.handle import DatasetHandle
from kontra.connectors.sqlserver import SqlServerConnectionParams
from kontra.connectors.detection import parse_table_reference, get_default_schema, SQLSERVER
from kontra.connectors.db_utils import (
    get_connection_ctx,
    execute_with_params,
    ss_quote_ident as _esc_ident,
)

from .base import BaseMaterializer
from .registry import register_materializer


def _get_connection_ctx(handle: DatasetHandle):
    """Get a connection context for SQL Server handles."""
    return get_connection_ctx(handle, "sqlserver")


@register_materializer("sqlserver")
class SqlServerMaterializer(BaseMaterializer):
    """
    Materialize SQL Server tables as Polars DataFrames with column projection.

    Features:
      - Efficient data loading via pymssql
      - Column projection at source (SELECT only needed columns)
      - BYOC (Bring Your Own Connection) support
    """

    materializer_name = "sqlserver"

    def __init__(self, handle: DatasetHandle):
        super().__init__(handle)

        self._sql: Optional[str] = getattr(handle, "sql", None)
        self._is_byoc = handle.external_conn is not None and handle.scheme in ("byoc", "query")

        if self._sql:
            # Query source: materialize the SELECT as a subquery (projection still
            # applies). No table to qualify.
            self._schema_name = None
            self._table_name = None
            self._qualified_table = f"({self._sql}) AS _kontra_q"
        elif self._is_byoc:
            # BYOC: get table info from handle
            if not handle.table_ref:
                raise ValueError("BYOC handle missing table_ref")
            _db, schema, table = parse_table_reference(handle.table_ref)
            self._schema_name = schema or get_default_schema(SQLSERVER)
            self._table_name = table
            self._qualified_table = f'[{self._schema_name}].[{self._table_name}]'
        elif handle.db_params:
            # URI-based: use params
            self.params: SqlServerConnectionParams = handle.db_params
            self._schema_name = self.params.schema
            self._table_name = self.params.table
            self._qualified_table = f'[{self.params.schema}].[{self.params.table}]'
        else:
            raise ValueError("SQL Server handle missing db_params or external_conn")

        self._io_debug_enabled = bool(os.getenv("KONTRA_IO_DEBUG"))
        self._last_io_debug: Optional[Dict[str, Any]] = None

    def schema(self) -> List[str]:
        """Return column names without loading data."""
        with _get_connection_ctx(self.handle) as conn:
            cursor = conn.cursor()
            if self._sql:
                # Query source: describe via an empty result set.
                cursor.execute(f"SELECT TOP 0 * FROM {self._qualified_table}")
                return [d[0] for d in cursor.description] if cursor.description else []
            # %s placeholders; adapted to ? automatically for pyodbc connections
            execute_with_params(
                cursor,
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
                """,
                (self._schema_name, self._table_name),
            )
            return [row[0] for row in cursor.fetchall()]

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
            cursor = conn.cursor()
            cursor.execute(query)
            # Fetch all rows - for large tables, consider chunked loading
            rows = cursor.fetchall()
            col_names = [desc[0] for desc in cursor.description] if cursor.description else []

        t1 = time.perf_counter()

        # Convert to Polars DataFrame
        if rows:
            # BUG 4 fix: pyodbc (Entra ID / BYOC pyodbc path) returns pyodbc.Row
            # objects from fetchall(). Polars does not unpack pyodbc.Row when
            # building a row-oriented DataFrame, which raises ShapeError ("data
            # does not match the number of columns"). pymssql returns plain
            # tuples, so this only bites the pyodbc driver. Coerce every row to a
            # plain tuple so both drivers construct identically.
            rows = [tuple(r) for r in rows]

            # BUG 5 fix: Polars' default infer_schema_length=100 only inspects
            # the first 100 rows to pick each column's dtype. A column that is
            # NULL for its first 100 rows infers Null/Utf8, then dies at the
            # first real value (e.g. ComputeError appending a datetime to an
            # inferred Null column). infer_schema_length=None scans all rows so
            # the dtype is always inferred from real data.
            #
            # We infer rather than build an explicit schema from
            # cursor.description because the driver type codes are not reliable
            # cross-driver: pymssql reports a coarse NUMBER type that conflates
            # INT and FLOAT, so it cannot distinguish Int64 from Float64. Full
            # inference is correct for both pyodbc and pymssql.
            #
            # Perf caveat: scanning all rows for inference adds a pass over the
            # fetched rows. For very large tables (e.g. 1M+ rows) this is a
            # measurable but bounded cost; correctness (loading the table at all)
            # takes precedence over the truncated-inference fast path.
            df = pl.DataFrame(
                rows, schema=col_names, orient="row", infer_schema_length=None
            )
        else:
            # Empty DataFrame with correct schema
            df = pl.DataFrame(schema={name: pl.Utf8 for name in col_names})

        if self._io_debug_enabled:
            self._last_io_debug = {
                "materializer": "sqlserver",
                "mode": "pymssql_fetch" if not self._is_byoc else "byoc_fetch",
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


