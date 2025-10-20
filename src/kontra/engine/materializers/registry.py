# src/kontra/engine/materializers/registry.py
from __future__ import annotations

from typing import TYPE_CHECKING, Callable, Dict, List

from kontra.connectors.handle import DatasetHandle

if TYPE_CHECKING:
    # Import from the new base file
    from .base import BaseMaterializer as Materializer
    from .duckdb import DuckDBMaterializer  # noqa: F401
    from .polars_connector import PolarsConnectorMaterializer  # noqa: F401


# Registry: materializer_name -> ctor(handle) function
_MATS: Dict[str, Callable[[DatasetHandle], Materializer]] = {}
# Simple order for picking when multiple can handle a handle
_ORDER: List[str] = []


def register_materializer(name: str):
    """
    Decorator to register a materializer class under a stable name.
    The class must implement the Materializer protocol.
    """

    def deco(cls: Callable[[DatasetHandle], Materializer]) -> Callable[
        [DatasetHandle], Materializer
    ]:
        if name in _MATS:
            raise ValueError(f"Materializer '{name}' is already registered.")
        _MATS[name] = cls
        if name not in _ORDER:
            _ORDER.append(name)
        cls.materializer_name = name  # friendly label for stats.io
        return cls

    return deco


def pick_materializer(handle: DatasetHandle) -> Materializer:
    """
    Choose the best materializer for the given dataset handle.

    Policy (v1.1 - Refactored):
      - If the URI is remote (s3, http, etc.) AND the format is known
        (parquet, csv), we *always* use the DuckDB materializer
        for its superior I/O and column pruning.
      - Otherwise, we fall back to the PolarsConnector materializer.

    This logic is now INDEPENDENT of the projection flag.
    """
    # --- BUG FIX ---
    # We no longer check `prefer_pruning`. We check the handle's scheme.
    is_remote = handle.scheme in ("s3", "http", "https")
    is_known_format = handle.format in ("parquet", "csv")

    if is_remote and is_known_format:
        ctor = _MATS.get("duckdb")
        if ctor:
            return ctor(handle)
    # --- END BUG FIX ---

    # Fallback for local files or unknown formats
    ctor = _MATS.get("polars-connector")
    if not ctor:
        raise RuntimeError(
            "No default materializer registered (polars-connector missing)"
        )
    return ctor(handle)


def register_default_materializers() -> None:
    """
    Eagerly import built-in materializers so their @register_materializer
    decorators run and populate the registry.
    """
    # Local imports to trigger decorator side-effects
    from . import duckdb  # noqa: F401
    from . import polars_connector  # noqa: F401