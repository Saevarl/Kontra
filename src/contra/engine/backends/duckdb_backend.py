# src/contra/engine/backends/duckdb_backend.py
from __future__ import annotations

"""
DEPRECATED MODULE

The old DuckDBBackend is replaced by:
  - SQL executors:   contra.engine.executors.*
  - Materializers:   contra.engine.materializers.*

This shim remains importable to avoid immediate breakage, but will raise at use.
"""

from typing import Any, Dict

import warnings


class DuckDBBackend:  # pragma: no cover - deprecation shim
    name = "duckdb (deprecated)"

    def __init__(self, *args, **kwargs):
        warnings.warn(
            "contra.engine.backends.duckdb_backend.DuckDBBackend is deprecated.\n"
            "Use the SQL executor registry (contra.engine.executors.*) and the DuckDB materializer instead.",
            DeprecationWarning,
            stacklevel=2,
        )

    def supports(self, *args, **kwargs) -> bool:
        return False

    def compile(self, *args, **kwargs) -> str:
        raise RuntimeError(
            "DuckDBBackend.compile() is deprecated. "
            "The engine now uses SQL executors and materializers."
        )

    def execute(self, *args, **kwargs) -> Dict[str, Any]:
        raise RuntimeError(
            "DuckDBBackend.execute() is deprecated. "
            "The engine now uses SQL executors (for rule pushdown) "
            "and DuckDBMaterializer (for column-projected I/O)."
        )

    def introspect(self, *args, **kwargs) -> Dict[str, Any]:
        return {"row_count": None, "available_cols": []}
