# src/kontra/engine/materializers/base.py
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, List, Optional

if TYPE_CHECKING:
    import polars as pl

from kontra.connectors.handle import DatasetHandle


class BaseMaterializer:
    """
    Minimal base class for materializers.

    Defines the interface for:
      - Loading a source into a Polars DataFrame (with projection)
      - Peeking the schema
      - Reporting I/O diagnostics
    """

    materializer_name: str = "unknown"

    def __init__(self, handle: DatasetHandle):
        """
        Initialize the materializer with a data source handle.

        Args:
            handle: The DatasetHandle containing the URI and fs_opts.
        """
        self.handle = handle

    def schema(self) -> List[str]:
        """Return column names without materializing data (best effort)."""
        raise NotImplementedError

    def to_polars(self, columns: Optional[List[str]]) -> "pl.DataFrame":
        """Materialize directly as a Polars DataFrame."""
        raise NotImplementedError

    def io_debug(self) -> Optional[Dict[str, Any]]:
        """Return last I/O diagnostics for observability (or None)."""
        return None  # Default implementation