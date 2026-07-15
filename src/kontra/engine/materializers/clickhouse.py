# src/kontra/engine/materializers/clickhouse.py
"""
ClickHouse Materializer - loads ClickHouse tables into Polars DataFrames.

Uses clickhouse-connect's Arrow output for a zero-copy hand-off to Polars.
This path only runs when a rule cannot be pushed down to SQL (rare for
ClickHouse, which handles almost every validation as a native aggregate).
"""

from __future__ import annotations

import os
import time
from typing import TYPE_CHECKING, Any, Dict, List, Optional

if TYPE_CHECKING:
    import polars as pl

from kontra.connectors.handle import DatasetHandle
from kontra.connectors.clickhouse import ClickHouseConnectionParams
from kontra.connectors.detection import parse_table_reference
from kontra.engine.sql_ir import esc_ident

from .base import BaseMaterializer
from .registry import register_materializer


def _ch_ident(name: str) -> str:
    return esc_ident(name, "clickhouse")


@register_materializer("clickhouse")
class ClickHouseMaterializer(BaseMaterializer):
    """Materialize ClickHouse tables as Polars DataFrames via Arrow."""

    materializer_name = "clickhouse"

    def __init__(self, handle: DatasetHandle):
        super().__init__(handle)
        self._sql: Optional[str] = getattr(handle, "sql", None)
        self._is_byoc = handle.external_conn is not None and handle.scheme in ("byoc", "query")

        if self._sql:
            # Query source: materialize the SELECT as a subquery.
            self._database = ""
            self._table_name = None
            self._qualified_table = f"({self._sql}) AS _kontra_q"
        elif self._is_byoc:
            if not handle.table_ref:
                raise ValueError("BYOC handle missing table_ref")
            _db, _schema, table = parse_table_reference(handle.table_ref)
            self._database = _db or ""
            self._table_name = table
        elif handle.db_params:
            self.params: ClickHouseConnectionParams = handle.db_params
            self._database = self.params.database
            self._table_name = self.params.table
        else:
            raise ValueError("ClickHouse handle missing db_params or external_conn")

        if not self._sql:
            self._qualified_table = (
                f"{_ch_ident(self._database)}.{_ch_ident(self._table_name)}"
                if self._database
                else _ch_ident(self._table_name)
            )
        self._io_debug_enabled = bool(os.getenv("KONTRA_IO_DEBUG"))
        self._last_io_debug: Optional[Dict[str, Any]] = None

    def _connection_ctx(self):
        from kontra.connectors.db_utils import get_connection_ctx

        return get_connection_ctx(self.handle, "clickhouse")

    def schema(self) -> List[str]:
        """Return column names without loading data (system.columns, no scan)."""
        from kontra.connectors.db_utils import get_connection_ctx
        from kontra.engine.sql_ir import lit_str

        with get_connection_ctx(self.handle, "clickhouse") as conn:
            if self._sql:
                arrow = conn.query_arrow(f"SELECT * FROM {self._qualified_table} LIMIT 0")
                return list(arrow.column_names)
            cur = conn.cursor()
            cur.execute(
                "SELECT name FROM system.columns "
                f"WHERE database = {lit_str(self._database, 'clickhouse')} "
                f"AND table = {lit_str(self._table_name, 'clickhouse')} "
                "ORDER BY position"
            )
            return [row[0] for row in cur.fetchall()]

    def to_polars(self, columns: Optional[List[str]]) -> "pl.DataFrame":
        """Load table data as a Polars DataFrame with optional projection."""
        import polars as pl

        cols_sql = ", ".join(_ch_ident(c) for c in columns) if columns else "*"
        query = f"SELECT {cols_sql} FROM {self._qualified_table}"

        t0 = time.perf_counter()
        from kontra.connectors.db_utils import get_connection_ctx

        with get_connection_ctx(self.handle, "clickhouse") as conn:
            # Fast path: Arrow -> Polars (zero-copy). BYOC connections are the
            # DBAPI shim, which also exposes query_arrow.
            arrow_table = conn.query_arrow(query)
            df = pl.from_arrow(arrow_table)
        t1 = time.perf_counter()

        if not isinstance(df, pl.DataFrame):  # from_arrow can yield a Series
            df = pl.DataFrame(df)

        if self._io_debug_enabled:
            self._last_io_debug = {
                "materializer": "clickhouse",
                "mode": "arrow_zero_copy",
                "table": self._qualified_table,
                "columns_requested": list(columns or []),
                "row_count": df.height,
                "elapsed_ms": int((t1 - t0) * 1000),
            }
        else:
            self._last_io_debug = None

        return df

    def io_debug(self) -> Optional[Dict[str, Any]]:
        return self._last_io_debug
