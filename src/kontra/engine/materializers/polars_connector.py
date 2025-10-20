# src/kontra/engine/materializers/polars_connector.py
from __future__ import annotations

from typing import Any, Dict, List, Optional

import polars as pl

from kontra.connectors.handle import DatasetHandle

from .base import BaseMaterializer  # <-- Import from new base file
from .registry import register_materializer


@register_materializer("polars-connector")
class PolarsConnectorMaterializer(BaseMaterializer):
    """
    Fallback materializer that uses the (old) ConnectorFactory.load()
    to produce a Polars DataFrame. Projection is best-effort.

    This is kept for local files and as a fallback for unknown
    remote file types that Polars might support.
    """

    def __init__(self, handle: DatasetHandle):
        super().__init__(handle)
        self._io_debug: Optional[Dict[str, Any]] = None  # always None for this path

    def schema(self) -> List[str]:
        """Best-effort peek via Polars lazy scan."""
        uri = self.handle.uri.lower()
        try:
            if uri.endswith(".parquet"):
                return list(
                    pl.scan_parquet(self.handle.uri).collect_schema().names()
                )
            if uri.endswith(".csv"):
                return list(
                    pl.scan_csv(self.handle.uri).collect_schema().names()
                )
        except Exception:
            pass  # Fallback to empty list
        return []

    def to_polars(self, columns: Optional[List[str]]) -> pl.DataFrame:
        """
        Materialize using the old ConnectorFactory.

        NOTE: This relies on the old `connectors` code. This materializer
        will be fully deprecated once all sources are handled by
        the DuckDB materializer or a future native Polars one.
        """
        # TODO: This is a temporary bridge to the old (soon to be deleted)
        # connector code. This should be replaced with a native
        # Polars scan_parquet/scan_csv.
        try:
            from kontra.connectors.factory import ConnectorFactory

            connector = ConnectorFactory.from_source(self.handle.uri)
            # Connector API allows "columns="; not all connectors can honor it.
            return connector.load(self.handle.uri, columns=columns)
        except ImportError:
            # Fallback if old connectors are deleted
            if self.handle.format == "parquet":
                return pl.scan_parquet(self.handle.uri, columns=columns).collect()
            if self.handle.format == "csv":
                return pl.scan_csv(self.handle.uri, columns=columns).collect()
            raise IOError(
                f"Unsupported format for PolarsConnectorMaterializer: {self.handle.uri}"
            )

    def io_debug(self) -> Optional[Dict[str, Any]]:
        # No special diagnostics here; reserved for duckdb materializer.
        return None