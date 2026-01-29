# src/kontra/scout/backends/duckdb_backend.py
"""
DuckDB backend for Scout profiler.

Supports Parquet and CSV files (local + S3/HTTP).
"""

from __future__ import annotations

import logging
import os
from typing import Any, Dict, List, Optional, Tuple

import duckdb

_logger = logging.getLogger(__name__)

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
            except duckdb.Error:
                pass  # View cleanup is best-effort

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
            except (OSError, IOError, ValueError) as e:
                _logger.debug(f"Could not get row count from Parquet metadata: {e}")

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
            except (OSError, IOError, ValueError) as e:
                _logger.debug(f"Could not get size from Parquet metadata: {e}")
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
        except duckdb.Error as e:
            _logger.debug(f"Query error getting null counts: {e}")
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
        except duckdb.Error as e:
            _logger.debug(f"Query error fetching distinct values for {column}: {e}")
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
        except duckdb.Error as e:
            _logger.debug(f"Query error fetching sample values for {column}: {e}")
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

            # Handle Azure (ADLS Gen2, Azure Blob)
            if self.handle.scheme in ("abfs", "abfss", "az"):
                opts = self.handle.fs_opts or {}
                kwargs: Dict[str, Any] = {}

                if opts.get("azure_account_name"):
                    kwargs["account_name"] = opts["azure_account_name"]

                # Use only ONE auth method - account_key takes priority over sas_token
                # PyArrow can crash or behave unexpectedly when both are provided
                if opts.get("azure_account_key"):
                    kwargs["account_key"] = opts["azure_account_key"]
                elif opts.get("azure_sas_token"):
                    # PyArrow requires SAS token WITH the leading '?'
                    sas = opts["azure_sas_token"]
                    if not sas.startswith("?"):
                        sas = "?" + sas
                    kwargs["sas_token"] = sas

                try:
                    fs = pafs.AzureFileSystem(**kwargs)
                    # Parse Azure URI to get container/path for PyArrow
                    # Format: abfss://container@account.dfs.core.windows.net/path
                    from urllib.parse import urlparse
                    parsed = urlparse(uri)
                    # netloc is "container@account.dfs.core.windows.net"
                    if "@" in parsed.netloc:
                        container = parsed.netloc.split("@", 1)[0]
                    else:
                        container = parsed.netloc.split(".")[0]
                    path_part = parsed.path.lstrip("/")
                    uri = f"{container}/{path_part}"
                except (ImportError, OSError, ValueError) as e:
                    # Azure filesystem not available or credentials invalid
                    # Fall back to DuckDB-based profiling
                    _logger.debug(f"Could not create Azure filesystem: {e}")
                    return None

            pf = pq.ParquetFile(uri, filesystem=fs)
            self._parquet_metadata = pf.metadata
            return self._parquet_metadata

        except (OSError, IOError, ValueError) as e:
            _logger.debug(f"Could not read Parquet metadata: {e}")
            return None

    def supports_metadata_only(self) -> bool:
        """
        Check if this backend supports metadata-only profiling.

        Returns True only for Parquet files when PyArrow is available.
        CSV files don't have metadata statistics.
        """
        return (
            self.handle.format == "parquet"
            and _HAS_PYARROW
            and self.sample_size is None
        )

    def profile_metadata_only(
        self, schema: List[Tuple[str, str]], row_count: int
    ) -> Dict[str, Dict[str, Any]]:
        """
        Profile columns using only Parquet metadata (no data scan).

        Returns dict mapping column_name -> {null_count, distinct_count, ...}

        Parquet row group statistics provide:
        - null_count: Exact count of nulls (sum across row groups)
        - num_values: Non-null values per row group
        - min/max: Column min/max (for potential use)

        Note: Parquet does NOT store distinct_count. We estimate from
        num_values (assuming all non-null values are distinct as upper bound).

        This is used for the 'lite' preset to achieve fast profiling
        without scanning the actual data.
        """
        meta = self._get_parquet_metadata()
        if not meta:
            raise RuntimeError("Cannot get Parquet metadata")

        # Build column stats by aggregating across row groups
        col_stats: Dict[str, Dict[str, Any]] = {}

        # Initialize stats for each column
        for col_name, _ in schema:
            col_stats[col_name] = {
                "null_count": 0,
                "num_values": 0,
                "has_statistics": False,
            }

        # Aggregate stats from all row groups
        for rg_idx in range(meta.num_row_groups):
            rg = meta.row_group(rg_idx)

            for col_idx in range(rg.num_columns):
                col_chunk = rg.column(col_idx)
                # Get column name from path (handles nested columns)
                col_path = col_chunk.path_in_schema
                col_name = col_path.split(".")[-1] if "." in col_path else col_path

                if col_name not in col_stats:
                    continue

                stats = col_chunk.statistics
                if stats is not None:
                    col_stats[col_name]["has_statistics"] = True
                    if stats.null_count is not None:
                        col_stats[col_name]["null_count"] += stats.null_count
                    if stats.num_values is not None:
                        col_stats[col_name]["num_values"] += stats.num_values

        # Build result dict
        result: Dict[str, Dict[str, Any]] = {}

        for col_name, raw_type in schema:
            stats = col_stats.get(col_name, {})

            null_count = stats.get("null_count", 0)
            num_values = stats.get("num_values", 0)
            has_stats = stats.get("has_statistics", False)

            # Estimate distinct_count:
            # - If no stats: use non-null count as upper bound
            # - Parquet doesn't track distinct count
            non_null = row_count - null_count if has_stats else row_count
            distinct_count = non_null  # Upper bound estimate

            result[col_name] = {
                "null_count": null_count if has_stats else 0,
                "distinct_count": distinct_count,
                "has_statistics": has_stats,
                "is_estimate": True,  # Flag that distinct_count is estimated
                "is_upper_bound": True,  # Parquet doesn't track distinct, this is always upper bound
            }

        return result
