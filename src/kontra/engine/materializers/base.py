from __future__ import annotations
from typing import List, Optional, Protocol, runtime_checkable, Dict, Any

# NOTE: keep imports inside method bodies to avoid hard deps (pyarrow, polars)
# Materializers should be lightweight and dependency-safe.

@runtime_checkable
class Materializer(Protocol):
    """
    Source → (Arrow/Polars) with optional column projection.

    Implementations MUST:
      - be side-effect free (no global/env mutation)
      - prefer columnar paths (Arrow/Polars) when possible
      - expose lightweight I/O diagnostics via io_debug()
    """

    def schema(self) -> List[str]:
        """Return column names without materializing data (best effort)."""
        ...

    def to_arrow(self, columns: Optional[List[str]]) -> "pa.Table":
        """Materialize as a pyarrow.Table (preferred for zero/low-copy handoff)."""
        ...

    def to_polars(self, columns: Optional[List[str]]) -> "pl.DataFrame":
        """Materialize directly as a Polars DataFrame."""
        ...

    def io_debug(self) -> Optional[Dict[str, Any]]:
        """Return last I/O diagnostics for observability (or None)."""
        ...
