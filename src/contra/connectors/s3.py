# src/contra/connectors/s3.py
from __future__ import annotations
from typing import Optional, List, Dict

import polars as pl

from .base import BaseConnector
from .capabilities import ConnectorCapabilities as CC
from .handle import DatasetHandle


class S3Connector(BaseConnector):
    """
    S3/MinIO connector (CSV/Parquet).

    - .load() kept for backward compatibility (engine may still call it if .handle() is missing).
    - .handle() is preferred; it allows the materializer registry (DuckDBMaterializer)
      to perform true column projection via Arrow.
    """

    # Remote object store with pushdown potential and partial reads
    capabilities: int = CC.PUSHDOWN | CC.REMOTE_PARTIAL

    def load(self, uri: str, columns: Optional[List[str]] = None) -> pl.DataFrame:
        # Legacy path — works, but may load extra columns.
        lower = uri.lower()
        if lower.endswith(".parquet"):
            lf = pl.scan_parquet(uri)
            if columns:
                lf = lf.select([pl.col(c) for c in columns])
            return lf.collect()
        elif lower.endswith(".csv"):
            lf = pl.scan_csv(uri)
            if columns:
                lf = lf.select([pl.col(c) for c in columns])
            return lf.collect()
        else:
            raise ValueError(f"Unsupported S3 file format: {uri}")

    # NEW: preferred by engine/materializers
    def handle(self, uri: str) -> DatasetHandle:
        fmt = (
            "parquet" if uri.lower().endswith(".parquet")
            else "csv" if uri.lower().endswith(".csv")
            else "other"
        )
        # Keep fs_opts minimal; DuckDBMaterializer will also look at DUCKDB_S3_* / AWS_* envs.
        fs_opts: Dict[str, str] = {}
        return DatasetHandle(uri=uri, format=fmt, caps=self.capabilities, fs_opts=fs_opts)
