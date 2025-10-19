# src/contra/connectors/base.py
from __future__ import annotations
from abc import ABC, abstractmethod
from typing import List, Optional
import warnings
import polars as pl

class BaseConnector(ABC):
    """Abstract base class for all data connectors."""

    # Safe default: subclasses can override a capabilities bitmask if they want
    capabilities: int = 0

    @abstractmethod
    def load(self, source: str, columns: Optional[List[str]] = None) -> pl.DataFrame:
        """
        Load data from a source path/URI.

        NOTE: In the new materializer flow, the engine prefers connector.handle()
        + pick_materializer(...). This .load() method remains for backward
        compatibility and for connectors that don't yet implement handle().
        """
        ...

    # -------- DEPRECATED: no longer used by the engine/materializers ----------
    def duckdb_config(self, con, source: str) -> None:  # pragma: no cover - deprecated
        """
        DEPRECATED — no-op.

        Old design had connectors mutating a DuckDB connection (httpfs creds, endpoint).
        New design pushes storage config into the DatasetHandle.fs_opts and lets
        DuckDBMaterializer configure the session generically.
        """
        warnings.warn(
            "BaseConnector.duckdb_config() is deprecated and ignored. "
            "Provide fs_opts via connector.handle() instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return None
