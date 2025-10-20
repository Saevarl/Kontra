# src/kontra/engine/materializers/duckdb.py
from __future__ import annotations

import os
import time
from typing import Any, Dict, List, Optional

import polars as pl

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
    Column-pruned materialization via DuckDB httpfs/azure → Arrow → Polars.

    - True column projection for Parquet/CSV on local or remote object stores.
    - Zero/low-copy handoff to Polars via Arrow.
    - Focused on I/O; it does NOT execute rule SQL (that’s the SQL executor’s job).
    """

    def __init__(self, handle: DatasetHandle):
        super().__init__(handle)
        self.source = handle.uri
        self._io_debug_enabled = bool(os.getenv("KONTRA_IO_DEBUG"))
        self._last_io_debug: Optional[Dict[str, Any]] = None

        # Call the centralized factory to get a connection
        # configured for this specific handle.
        self.con = create_duckdb_connection(self.handle)

    # ---------- Materializer API ----------

    def schema(self) -> List[str]:
        """Return column names without materializing data (best effort)."""
        # TODO: This should be format-aware like to_polars
        cur = self.con.execute(
            f"SELECT * FROM read_parquet({lit_str(self.source)}) LIMIT 0"
        )
        return [d[0] for d in cur.description] if cur.description else []

    def to_polars(self, columns: Optional[List[str]]) -> pl.DataFrame:
        """Materialize the requested columns as a Polars DataFrame via Arrow."""
        # Route through Arrow for consistent, zero/low-copy columnar materialization.
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
        """Return the correct DuckDB read function based on file format."""
        if self.handle.format == "parquet":
            return "read_parquet"
        if self.handle.format == "csv":
            # TODO: Pass CSV options from handle.fs_opts
            return "read_csv_auto"

        # Fallback: try DuckDB's autodetect (best effort)
        # We default to parquet as it's the most common case.
        return "read_parquet"

    # Note: _safe_set, _configure_perf, and _configure_storage
    # have been removed. All connection logic is now centralized
    # in kontra.engine.backends.duckdb_session.