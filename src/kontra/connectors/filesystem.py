# src/contra/connectors/filesystem.py
from __future__ import annotations
from typing import Optional, List

import polars as pl

from .base import BaseConnector
from .capabilities import ConnectorCapabilities as CC
from .handle import DatasetHandle


class FilesystemConnector(BaseConnector):
    """
    Local filesystem connector (CSV/Parquet).

    Existing .load() remains; the engine now prefers .handle() so a materializer
    (DuckDB or fallback Polars) can materialize efficiently.
    """

    capabilities: int = CC.LOCAL

    def load(self, path: str, columns: Optional[List[str]] = None) -> pl.DataFrame:
        lower = path.lower()
        if lower.endswith(".parquet"):
            # Polars will still read the full file; we prefer materializers for pruning.
            lf = pl.scan_parquet(path)
            if columns:
                lf = lf.select([pl.col(c) for c in columns])
            return lf.collect()
        elif lower.endswith(".csv"):
            lf = pl.scan_csv(path)
            if columns:
                lf = lf.select([pl.col(c) for c in columns])
            return lf.collect()
        else:
            raise ValueError(f"Unsupported local file format: {path}")

    # NEW: preferred by the materializer registry
    def handle(self, path: str) -> DatasetHandle:
        fmt = (
            "parquet" if path.lower().endswith(".parquet")
            else "csv" if path.lower().endswith(".csv")
            else "other"
        )
        return DatasetHandle(uri=path, format=fmt, caps=self.capabilities, fs_opts={})
