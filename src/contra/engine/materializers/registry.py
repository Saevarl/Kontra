from __future__ import annotations
from typing import Callable, Dict, Optional, List

from contra.connectors.handle import DatasetHandle

# Registry: materializer_name -> ctor(handle) function
_MATS: Dict[str, Callable[[DatasetHandle], "BaseMaterializer"]] = {}
# Simple order for picking when multiple can handle a handle
_ORDER: List[str] = []


def register_materializer(name: str):
    """
    Decorator to register a materializer class under a stable name.
    The class must implement the BaseMaterializer interface (duck-typed).
    """
    def deco(cls):
        _MATS[name] = lambda handle: cls(handle)
        if name not in _ORDER:
            _ORDER.append(name)
        cls.materializer_name = name  # friendly label for stats.io
        return cls
    return deco


def pick_materializer(handle: DatasetHandle, prefer_pruning: bool = True) -> "BaseMaterializer":
    """
    Choose the best materializer for the given dataset handle.

    Policy (v1):
      - If Parquet/CSV and URI is local/remote → DuckDB materializer (true projection).
      - Otherwise → PolarsConnector materializer (fallback).
    """
    if prefer_pruning and handle.format in ("parquet", "csv"):
        ctor = _MATS.get("duckdb")
        if ctor:
            return ctor(handle)

    ctor = _MATS.get("polars-connector")
    if not ctor:
        raise RuntimeError("No default materializer registered (polars-connector missing)")
    return ctor(handle)


def get_materializer_for(source_uri: str, prefer_pruning: bool = True) -> "BaseMaterializer":
    """
    Convenience wrapper used by the engine:
      URI string -> DatasetHandle -> best materializer
    """
    handle = DatasetHandle.from_uri(source_uri)
    return pick_materializer(handle, prefer_pruning)


def register_default_materializers() -> None:
    """
    Eagerly import built-in materializers so their @register_materializer
    decorators run and populate the registry.

    Kept as a function (not a module-level import) to avoid circular imports
    during packaging and to make tests able to override/patch easily.
    """
    # Local imports to trigger decorator side-effects without creating
    # hard dependencies at import time.
    from .duckdb import DuckDBMaterializer  # noqa: F401
    from .polars_connector import PolarsConnectorMaterializer  # noqa: F401
    # Nothing else to do: importing is enough.


class BaseMaterializer:
    """
    Minimal protocol (duck-typed) for materializers:
      - to_polars(columns: Optional[List[str]]) -> pl.DataFrame
      - schema() -> list[str]
      - io_debug() -> Optional[dict]  (stats/diagnostics)
    """
    materializer_name: str = "unknown"

    def __init__(self, handle: DatasetHandle):
        self.handle = handle
