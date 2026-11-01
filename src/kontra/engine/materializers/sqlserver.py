# src/kontra/engine/materializers/sqlserver.py
"""
SQL Server Materializer - loads SQL Server tables to Polars DataFrames.

Uses pymssql for database connectivity.
"""

from __future__ import annotations

import datetime
import decimal
import os
import time
from collections.abc import Sequence
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
from .row_frame import DeclaredTypes, frame_from_rows


def _get_connection_ctx(handle: DatasetHandle):
    """Get a connection context for SQL Server handles."""
    return get_connection_ctx(handle, "sqlserver")


_SS_INT = ("tinyint", "smallint", "int", "bigint")
_SS_TEXT = ("char", "varchar", "nchar", "nvarchar", "text", "ntext")
_SS_DATETIME = ("datetime", "datetime2", "smalldatetime")


def _ss_type_dtype(type_name: str) -> Any:
    """Polars dtype for a declared type (``decimal(10,2)``), as inference types its values.

    Matches what Polars makes of pymssql's Python values: integers as Int64,
    decimals as Decimal(38, scale), money as Decimal(38, 4), datetimeoffset
    in UTC. None for types without a fixed dtype (uniqueidentifier, xml, ...).
    """
    import polars as pl

    t = type_name.strip().lower()
    base = t.split("(", 1)[0].strip()
    if base in _SS_INT:
        return pl.Int64
    if base == "bit":
        return pl.Boolean
    if base in ("real", "float"):
        return pl.Float64
    if base in ("decimal", "numeric"):
        args = t[len(base) :].strip("() ").split(",")
        return pl.Decimal(38, int(args[1]) if len(args) == 2 else 0)
    if base in ("money", "smallmoney"):
        return pl.Decimal(38, 4)
    if base in _SS_TEXT:
        return pl.Utf8
    if base == "date":
        return pl.Date
    if base in _SS_DATETIME:
        return pl.Datetime("us")
    if base == "datetimeoffset":
        return pl.Datetime("us", "UTC")
    if base == "time":
        return pl.Time
    if base in ("binary", "varbinary", "image"):
        return pl.Binary
    return None


def _pyodbc_declared_types(description: Sequence[Any]) -> DeclaredTypes | None:
    """Polars dtypes from a pyodbc description, or None for any other driver.

    pyodbc reports each column's Python type plus its precision and scale,
    which is enough. pymssql reports coarse codes (one for every number,
    and NULL-only datetimes come back as binary), so it needs the lookup.
    """
    import polars as pl

    if not description or not all(isinstance(d[1], type) for d in description):
        return None
    by_type = {
        int: pl.Int64,
        bool: pl.Boolean,
        float: pl.Float64,
        str: pl.Utf8,
        datetime.datetime: pl.Datetime("us"),
        datetime.date: pl.Date,
        datetime.time: pl.Time,
        bytes: pl.Binary,
        bytearray: pl.Binary,
    }
    out: dict[str, Any] = {}
    for desc in description:
        if desc[1] is decimal.Decimal:
            out[desc[0]] = pl.Decimal(38, desc[5] or 0)
        else:
            out[desc[0]] = by_type.get(desc[1])
    return out


def _describe_result_types(conn: Any, query: str) -> DeclaredTypes:
    """Declared type of each column ``query`` returns, without running it.

    One catalog call, also exact for a Query source's SELECT. If the server
    refuses it (permissions, a temp table in the query), the frame keeps the
    inferred types: no worse than without the lookup.
    """
    cursor = conn.cursor()
    try:
        execute_with_params(
            cursor,
            "SELECT name, system_type_name FROM sys.dm_exec_describe_first_result_set(%s, NULL, 0)",
            (query,),
        )
        rows = cursor.fetchall()
    except Exception:  # noqa: BLE001 - each driver raises its own error classes
        return {}
    return {name: _ss_type_dtype(type_name) for name, type_name in rows if name and type_name}


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
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
                ORDER BY ORDINAL_POSITION
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
            import polars  # noqa: F401 - lazy, only needed when residual rules exist
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
            description = list(cursor.description or [])
            t1 = time.perf_counter()

            # pyodbc (Entra ID / BYOC pyodbc path) returns pyodbc.Row objects,
            # which Polars does not unpack when building a row-oriented frame
            # (ShapeError). pymssql returns plain tuples. Coerce every row so
            # both drivers construct identically.
            rows = [tuple(r) for r in rows]

            col_names = [desc[0] for desc in description]
            pyodbc_types = _pyodbc_declared_types(description)

            def declared() -> DeclaredTypes:
                # pymssql: one lookup, only for an empty result or an all-NULL
                # column, on the connection still open here.
                if pyodbc_types is not None:
                    return pyodbc_types
                return _describe_result_types(conn, query)

            df = frame_from_rows(rows, col_names, declared)

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


