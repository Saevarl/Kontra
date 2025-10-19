from __future__ import annotations
from typing import Optional
from ...connectors.capabilities import ConnectorCapabilities as CC  # re-export path differs when imported from engine
from .duckdb import DuckDBMaterializer
from .polars_connector import PolarsConnectorMaterializer

def is_s3_uri(val: str | None) -> bool:
    return isinstance(val, str) and val.lower().startswith("s3://")

class MaterializerFactory:
    @staticmethod
    def from_source(source: str, connector, caps: int, prefer_remote_pruning: bool):
        """
        Choose the best materializer for a given source and connector capabilities.

        Strategy (v1):
          - If remote S3 and we prefer pruning → DuckDBMaterializer (httpfs + Arrow)
          - Else → PolarsConnectorMaterializer (direct connector.load)
        """
        if is_s3_uri(source) and prefer_remote_pruning and (caps & (CC.PUSHDOWN | CC.REMOTE_PARTIAL)):
            return DuckDBMaterializer(source)
        return PolarsConnectorMaterializer(source, connector)
