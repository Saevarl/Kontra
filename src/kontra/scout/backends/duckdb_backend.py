# src/kontra/scout/backends/duckdb_backend.py
"""
DuckDB backend for Scout profiler.

Supports Parquet and CSV files (local + S3/HTTP).
"""

from __future__ import annotations

import os
from typing import Any, Dict, List, Optional, Tuple

import duckdb

try:
    import pyarrow.parquet as pq
    import pyarrow.fs as pafs

    _HAS_PYARROW = True
except ImportError:
    _HAS_PYARROW = False

from kontra.connectors.handle import DatasetHandle
from kontra.engine.backends.duckdb_session import create_duckdb_connection
from kontra.engine.backends.duckdb_utils import esc_ident as duckdb_esc_ident
from kontra.engine.backends.duckdb_utils import lit_str


class DuckDBBackend:
    """
    DuckDB-based profiler backend for Parquet and CSV files.

    Features:
    - Parquet metadata extraction (row count from footer)
    - Single-pass aggregation queries
    - Sampling support
    - S3/HTTP support via DuckDB httpfs
    """

    def __init__(
        self,
        handle: DatasetHandle,
        *,
        sample_size: Optional[int] = None,
    ):
        self.handle = handle
        self.sample_size = sample_size
        self.con: Optional[duckdb.DuckDBPyConnection] = None
        self._parquet_metadata: Optional[Any] = None
        self._view_name = "_scout"

    def connect(self) -> None:
        """Create DuckDB connection and source view."""
        self.con = create_duckdb_connection(self.handle)
        self._create_source_view()

    def close(self) -> None:
        """Clean up resources."""
        if self.con:
            try:
                self.con.execute(f"DROP VIEW IF EXISTS {self._view_name}")
            except Exception:
                pass

    def get_schema(self) -> List[Tuple[str, str]]:
        """Return [(column_name, raw_type), ...]"""
        cur = self.con.execute(f"SELECT * FROM {self._view_name} LIMIT 0")
        return [(d[0], str(d[1])) for d in cur.description]

    def get_row_count(self) -> int:
        """
        Get row count, using Parquet metadata if available.

        For Parquet files, the row count is extracted from the footer
        without scanning data (fast). For CSV/other formats, a COUNT query is used.
        """
        # Try Parquet metadata first (no scan)
        if self.handle.format == "parquet" and _HAS_PYARROW and self.sample_size is None:
            try:
                meta = self._get_parquet_metadata()
                if meta:
                    if os.getenv("KONTRA_VERBOSE"):
                        print(f"[INFO] Parquet metadata: {meta.num_rows} rows from footer")
                    return meta.num_rows
            except Exception:
                pass

        # Fall back to query
        result = self.con.execute(f"SELECT COUNT(*) FROM {self._view_name}").fetchone()
        return int(result[0]) if result else 0

    def get_estimated_size_bytes(self) -> Optional[int]:
        """Get estimated size from Parquet metadata."""
        if self.handle.format == "parquet" and _HAS_PYARROW:
            try:
                meta = self._get_parquet_metadata()
                if meta:
                    return meta.serialized_size
            except Exception:
                pass
        return None

    def execute_stats_query(self, exprs: List[str]) -> Dict[str, Any]:
        """Execute aggregation query with multiple expressions."""
        if not exprs:
            return {}

        sql = f"SELECT {', '.join(exprs)} FROM {self._view_name}"
        cur = self.con.execute(sql)
        row = cur.fetchone()
        col_names = [d[0] for d in cur.description]
        return dict(zip(col_names, row)) if row else {}

    def fetch_top_values(self, column: str, limit: int) -> List[Tuple[Any, int]]:
        """Fetch top N most frequent values."""
        col = self.esc_ident(column)
        sql = f"""
            SELECT {col} AS val, COUNT(*) AS cnt
            FROM {self._view_name}
            WHERE {col} IS NOT NULL
            GROUP BY {col}
            ORDER BY cnt DESC
            LIMIT {limit}
        """
        try:
            rows = self.con.execute(sql).fetchall()
            return [(r[0], int(r[1])) for r in rows]
        except Exception:
            return []

    def fetch_distinct_values(self, column: str) -> List[Any]:
        """Fetch all distinct values for a column."""
        col = self.esc_ident(column)
        sql = f"""
            SELECT DISTINCT {col}
            FROM {self._view_name}
            WHERE {col} IS NOT NULL
            ORDER BY {col}
        """
        try:
            rows = self.con.execute(sql).fetchall()
            return [r[0] for r in rows]
        except Exception:
            return []

    def fetch_sample_values(self, column: str, limit: int) -> List[Any]:
        """Fetch sample values for pattern detection."""
        col = self.esc_ident(column)
        sql = f"""
            SELECT {col}
            FROM {self._view_name}
            WHERE {col} IS NOT NULL
            LIMIT {limit}
        """
        try:
            rows = self.con.execute(sql).fetchall()
            return [r[0] for r in rows if r[0] is not None]
        except Exception:
            return []

    def esc_ident(self, name: str) -> str:
        """Escape identifier for DuckDB."""
        return duckdb_esc_ident(name)

    @property
    def source_format(self) -> str:
        """Return source format."""
        return self.handle.format or "unknown"

    # ----------------------------- Internal methods -----------------------------

    def _create_source_view(self) -> None:
        """Create a DuckDB view over the source, optionally with sampling."""
        fmt = (self.handle.format or "").lower()
        uri = self.handle.uri

        if fmt == "parquet":
            read_fn = f"read_parquet({lit_str(uri)})"
        elif fmt == "csv":
            read_fn = f"read_csv_auto({lit_str(uri)})"
        else:
            # Try parquet first
            read_fn = f"read_parquet({lit_str(uri)})"

        if self.sample_size:
            sql = f"""
                CREATE OR REPLACE VIEW {self._view_name} AS
                SELECT * FROM {read_fn}
                USING SAMPLE {int(self.sample_size)} ROWS
            """
        else:
            sql = f"CREATE OR REPLACE VIEW {self._view_name} AS SELECT * FROM {read_fn}"

        self.con.execute(sql)

    def _get_parquet_metadata(self) -> Optional[Any]:
        """Extract Parquet metadata without reading data."""
        if not _HAS_PYARROW:
            return None

        if self._parquet_metadata is not None:
            return self._parquet_metadata

        try:
            uri = self.handle.uri
            fs = None

            # Handle S3
            if self.handle.scheme == "s3":
                opts = self.handle.fs_opts or {}
                kwargs: Dict[str, Any] = {}
                if opts.get("s3_access_key_id") and opts.get("s3_secret_access_key"):
                    kwargs["access_key"] = opts["s3_access_key_id"]
                    kwargs["secret_key"] = opts["s3_secret_access_key"]
                if opts.get("s3_endpoint"):
                    endpoint = opts["s3_endpoint"]
                    if endpoint.startswith("http://"):
                        endpoint = endpoint[7:]
                        kwargs["scheme"] = "http"
                    elif endpoint.startswith("https://"):
                        endpoint = endpoint[8:]
                        kwargs["scheme"] = "https"
                    kwargs["endpoint_override"] = endpoint
                if opts.get("s3_url_style", "").lower() == "path" or opts.get("s3_endpoint"):
                    kwargs["force_virtual_addressing"] = False

                fs = pafs.S3FileSystem(**kwargs)
                if uri.lower().startswith("s3://"):
                    uri = uri[5:]

            pf = pq.ParquetFile(uri, filesystem=fs)
            self._parquet_metadata = pf.metadata
            return self._parquet_metadata

        except Exception:
            return None
