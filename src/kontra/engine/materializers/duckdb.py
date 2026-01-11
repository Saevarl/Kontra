# src/kontra/engine/materializers/duckdb.py
from __future__ import annotations

import os
import time
from typing import Any, Dict, List, Optional

import polars as pl
import duckdb

# --- Kontra Imports ---
from kontra.engine.backends.duckdb_session import create_duckdb_connection
from kontra.engine.backends.duckdb_utils import (
    esc_ident,
    lit_str,
)
from kontra.connectors.handle import DatasetHandle

from .base import BaseMaterializer  # Import from new base file
from .registry import register_materializer


@register_materializer("duckdb")
class DuckDBMaterializer(BaseMaterializer):
    """
    Column-pruned materialization via DuckDB httpfs → Arrow → Polars.

    Guarantees:
      - **Format aware**: Parquet via read_parquet(), CSV via read_csv_auto().
      - **Projection**: SELECT only requested columns at source (true pruning).
      - **Low copy**: Arrow table handoff → Polars DataFrame.
      - **Remote support**: S3/HTTP via DuckDB httpfs (loaded in session factory).

    Scope:
      - I/O only. Does NOT execute rule SQL; the SQL executor handles pushdown.
    """

    def __init__(self, handle: DatasetHandle):
        super().__init__(handle)
        self.source = handle.uri
        self._io_debug_enabled = bool(os.getenv("KONTRA_IO_DEBUG"))
        self._last_io_debug: Optional[Dict[str, Any]] = None
        self.con = create_duckdb_connection(self.handle)

    # ---------- Materializer API ----------

    def schema(self) -> List[str]:
        """
        Return column names without materializing data (best effort, format-aware).
        """
        read_fn = self._get_read_function()
        cur = self.con.execute(
            f"SELECT * FROM {read_fn}({lit_str(self.source)}) LIMIT 0"
        )
        return [d[0] for d in cur.description] if cur.description else []

    def to_polars(self, columns: Optional[List[str]]) -> pl.DataFrame:
        """
        Materialize the requested columns as a Polars DataFrame via Arrow.
        """
        # Route through Arrow for consistent, low-copy materialization.
        import pyarrow as pa  # noqa: F401

        cols_sql = (
            ", ".join(esc_ident(c) for c in (columns or [])) if columns else "*"
        )
        read_func = self._get_read_function()

        t0 = time.perf_counter()
        query = f"SELECT {cols_sql} FROM {read_func}({lit_str(self.source)})"
        cur = self.con.execute(query)
        table = cur.fetch_arrow_table()
        t1 = time.perf_counter()

        if self._io_debug_enabled:
            self._last_io_debug = {
                "materializer": "duckdb",
                "mode": "duckdb_project_to_arrow",
                "columns_requested": list(columns or []),
                "column_count": len(columns or []),
                "elapsed_ms": int((t1 - t0) * 1000),
            }
        else:
            self._last_io_debug = None

        return pl.from_arrow(table)

    def io_debug(self) -> Optional[Dict[str, Any]]:
        return self._last_io_debug

    # ---------- Internals ----------

    def _get_read_function(self) -> str:
        """
        Return the correct DuckDB read function based on file format.

        Notes:
          - For CSV, we prefer DuckDB's read_csv_auto() for performance.
          - CSV options (delimiter, header, etc.) can be threaded from
            connector handle in future (TODO), but auto inference is robust
            for most standardized data lake dumps.
        """
        fmt = (self.handle.format or "").lower()
        if fmt == "parquet":
            return "read_parquet"
        if fmt == "csv":
            return "read_csv_auto"

        # Fallback: attempt format autodetection; Parquet is most common.
        return "read_parquet"
