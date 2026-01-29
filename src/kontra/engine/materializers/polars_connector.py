# src/kontra/engine/materializers/polars_connector.py
from __future__ import annotations

"""
PolarsConnectorMaterializer

Purpose
-------
Local, dependency-light materializer that produces a Polars DataFrame from
file-based datasets (Parquet/CSV). Supports column projection. Does *not*
require the legacy connectors package.

Design
------
- First tries legacy `ConnectorFactory` (for back-compat if present).
- Otherwise uses native Polars lazy scans:
    - scan_*  → optional .select(projection) → collect()

Notes
-----
Polars ≥ 1.34 removed `columns=` from scan_*; apply projection via `.select()`.
"""

from typing import Any, Dict, List, Optional

import polars as pl

from kontra.connectors.handle import DatasetHandle
from .base import BaseMaterializer
from .registry import register_materializer


def _infer_format(uri: str, explicit: Optional[str]) -> str:
    """Resolve file format from explicit handle.format or file extension."""
    if explicit:
        return explicit.lower()
    low = uri.lower()
    if low.endswith(".parquet"):
        return "parquet"
    if low.endswith(".csv"):
        return "csv"
    return ""


@register_materializer("polars-connector")
class PolarsConnectorMaterializer(BaseMaterializer):
    """
    Minimal, deterministic materializer for local files.

    Responsibilities
    ----------------
    - Cheap schema peek (names only)
    - DataFrame materialization with optional projection
    - No side effects; no hidden state
    """

    name = "polars-connector"

    def __init__(self, handle: DatasetHandle):
        super().__init__(handle)
        self._io_debug: Optional[Dict[str, Any]] = None  # retained for parity with duckdb materializer

    # ------------------------------------------------------------------ #
    # Introspection
    # ------------------------------------------------------------------ #

    def schema(self) -> List[str]:
        """
        Return column names using a lazy scan. Never raises — empty list on failure.
        """
        uri = self.handle.uri
        fmt = _infer_format(uri, getattr(self.handle, "format", None))

        try:
            if fmt == "parquet":
                return list(pl.scan_parquet(uri).collect_schema().names())
            if fmt == "csv":
                return list(pl.scan_csv(uri).collect_schema().names())
        except (OSError, IOError, pl.exceptions.ComputeError, ValueError):
            # File not found or unreadable - return empty columns
            pass
        return []

    # ------------------------------------------------------------------ #
    # Materialization
    # ------------------------------------------------------------------ #

    def to_polars(self, columns: Optional[List[str]]) -> pl.DataFrame:
        """
        Materialize dataset into a Polars DataFrame.

        Strategy
        --------
        1) Attempt legacy connectors path (if installed) to preserve behavior.
        2) Otherwise, native Polars scan with projection via `.select()`.
        """
        # --- Legacy path (optional/back-compat) --------------------------------
        try:
            from kontra.connectors.factory import ConnectorFactory  # type: ignore

            connector = ConnectorFactory.from_source(self.handle.uri)
            # The legacy API accepts `columns=` (best-effort).
            return connector.load(self.handle.uri, columns=columns)
        except (ImportError, ModuleNotFoundError):
            # Fall back to native Polars path
            pass

        # --- Native Polars path -------------------------------------------------
        uri = self.handle.uri
        fmt = _infer_format(uri, getattr(self.handle, "format", None))

        if fmt == "parquet":
            lf = pl.scan_parquet(uri)
        elif fmt == "csv":
            # Add CSV options here if your data requires (delimiter, nulls, dtypes).
            lf = pl.scan_csv(uri)
        else:
            # Provide helpful error message based on URI
            if uri == "inline" or not uri:
                raise IOError(
                    f"No data path specified. Either:\n"
                    f"  1. Add 'data: path/to/file.parquet' to your contract YAML\n"
                    f"  2. Use --data to specify the data path: kontra validate contract.yml --data path/to/file.parquet"
                )
            elif "://" in uri:
                raise IOError(f"Unsupported data source URI: {uri}")
            else:
                # Assume it's a file path - check if it exists
                from pathlib import Path
                if not Path(uri).exists():
                    raise IOError(f"Data file not found: '{uri}'. Check that the path is correct.")
                else:
                    raise IOError(f"Unsupported file format: '{uri}'. Kontra supports .parquet and .csv files.")

        if columns:
            lf = lf.select([pl.col(c) for c in columns])

        # NOTE: streaming=True is deprecated; default engine suffices for tests and CI.
        return lf.collect()

    # ------------------------------------------------------------------ #
    # Diagnostics
    # ------------------------------------------------------------------ #

    def io_debug(self) -> Optional[Dict[str, Any]]:
        """Reserved hook for I/O diagnostics (none for this materializer)."""
        return None
