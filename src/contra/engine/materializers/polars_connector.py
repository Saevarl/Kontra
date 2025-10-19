# src/contra/engine/materializers/polars_connector.py
from __future__ import annotations
from typing import List, Optional, Dict, Any

import polars as pl

from contra.connectors.handle import DatasetHandle
from .registry import BaseMaterializer, register_materializer

@register_materializer("polars-connector")
class PolarsConnectorMaterializer(BaseMaterializer):
    """
    Fallback materializer that uses the existing ConnectorFactory.load()
    to produce a Polars DataFrame. Projection is best-effort (connectors may ignore it).
    """

    def __init__(self, handle: DatasetHandle):
        super().__init__(handle)
        self._io_debug: Optional[Dict[str, Any]] = None  # always None for this path

    def schema(self) -> List[str]:
        # Best-effort peek via Polars lazy scan.
        uri = self.handle.uri.lower()
        if uri.endswith(".parquet"):
            return list(pl.scan_parquet(self.handle.uri).collect_schema().names())
        if uri.endswith(".csv"):
            return list(pl.scan_csv(self.handle.uri).collect_schema().names())
        # Unknown format; load a tiny sample if needed (skip for now)
        return []

    def to_polars(self, columns: Optional[List[str]]):
        from contra.connectors.factory import ConnectorFactory
        connector = ConnectorFactory.from_source(self.handle.uri)
        # Connector API allows "columns="; not all connectors can honor it.
        return connector.load(self.handle.uri, columns=columns)

    def io_debug(self) -> Optional[Dict[str, Any]]:
        # No special diagnostics here; reserved for duckdb materializer.
        return None
