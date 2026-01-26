# src/kontra/scout/__init__.py
"""
Kontra Scout - Contract-free data profiling for LLM context compression.

Note: Heavy imports (ScoutProfiler) are lazy-loaded to keep `import kontra` fast.
Use `from kontra.scout.profiler import ScoutProfiler` for direct access.
"""

from kontra.scout.types import ColumnProfile, DatasetProfile

__all__ = ["ScoutProfiler", "ColumnProfile", "DatasetProfile"]


def __getattr__(name: str):
    """Lazy load ScoutProfiler to avoid importing polars on package import."""
    if name == "ScoutProfiler":
        from kontra.scout.profiler import ScoutProfiler
        return ScoutProfiler
    raise AttributeError(f"module 'kontra.scout' has no attribute '{name}'")
