# src/kontra/engine/materializers/postgres.py
"""
PostgreSQL Materializer - loads PostgreSQL tables to Polars DataFrames.

Uses psycopg3's efficient binary COPY protocol for streaming data.
"""

from __future__ import annotations

import os
import time
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, Dict, List, Optional

if TYPE_CHECKING:
    import polars as pl

from kontra.connectors.handle import DatasetHandle
from kontra.connectors.postgres import PostgresConnectionParams
from kontra.connectors.detection import parse_table_reference, get_default_schema, POSTGRESQL
from kontra.connectors.db_utils import get_connection_ctx, pg_quote_ident as _esc_ident

from .base import BaseMaterializer
from .registry import register_materializer
from .row_frame import DeclaredTypes, frame_from_rows


def _get_connection_ctx(handle: DatasetHandle):
    """Get a connection context for PostgreSQL handles."""
    return get_connection_ctx(handle, "postgres")


# Type OIDs (pg_type), the same for every driver.
_PG_INT = (20, 21, 23, 26)  # int8, int2, int4, oid
_PG_FLOAT = (700, 701)  # float4, float8
_PG_TEXT = (18, 19, 25, 1042, 1043)  # "char", name, text, bpchar, varchar
_PG_NUMERIC = 1700
_PG_TIMESTAMPTZ = 1184


def _pg_declared_types(description: Sequence[Any], time_zone: str) -> DeclaredTypes:
    """Polars dtype per result column from its type OID, as inference types its values.

    Integers load as Int64 and numerics as Decimal(38, scale), which is what
    Polars makes of the driver's Python ints and Decimals. Types left out
    (uuid, json, arrays, ...) have no fixed dtype and map to None.
    """
    import polars as pl

    fixed = {
        **{oid: pl.Int64 for oid in _PG_INT},
        **{oid: pl.Float64 for oid in _PG_FLOAT},
        **{oid: pl.Utf8 for oid in _PG_TEXT},
        16: pl.Boolean,
        17: pl.Binary,  # bytea
        1082: pl.Date,
        1083: pl.Time,
        1266: pl.Time,  # timetz: Polars keeps no zone on a time
        1114: pl.Datetime("us"),
        1186: pl.Duration("us"),  # interval
        _PG_TIMESTAMPTZ: pl.Datetime("us", time_zone),
    }
    out: dict[str, Any] = {}
    for desc in description:
        oid = desc[1]
        if oid == _PG_NUMERIC:
            # Unconstrained numeric reports no scale; its values set one.
            scale = desc[5] or 0
            out[desc[0]] = pl.Decimal(38, scale) if scale <= 38 else None
        else:
            out[desc[0]] = fixed.get(oid)
    return out


def _session_time_zone(conn: Any) -> str:
    """The zone timestamptz values arrive in, read from the connection without a query.

    psycopg 3 tracks the session TimeZone; its values are inferred as that
    zone, and as UTC when it is a fixed offset or the driver can't tell.
    """
    tz = getattr(getattr(conn, "info", None), "timezone", None)
    return getattr(tz, "key", None) or "UTC"


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

        self._sql: Optional[str] = getattr(handle, "sql", None)
        self._is_byoc = handle.external_conn is not None and handle.scheme in ("byoc", "query")

        if self._sql:
            # Query source: materialize the SELECT as a subquery so column
            # projection still applies. No table to qualify.
            self._schema_name = None
            self._table_name = None
            self._qualified_table = f"({self._sql}) AS _kontra_q"
        elif self._is_byoc:
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
                if self._sql:
                    # Query source: describe via an empty result set.
                    cur.execute(f"SELECT * FROM {self._qualified_table} LIMIT 0")
                    return [d[0] for d in cur.description] if cur.description else []
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
            with conn.cursor() as cur:
                cur.execute(query)
                # Fetch all rows - for large tables, consider chunked loading
                rows = cur.fetchall()
                description = list(cur.description or [])
                time_zone = _session_time_zone(conn)

        t1 = time.perf_counter()

        col_names = [desc[0] for desc in description]
        df = frame_from_rows(rows, col_names, lambda: _pg_declared_types(description, time_zone))

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


