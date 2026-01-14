# src/kontra/scout/__init__.py
"""
Kontra Scout - Contract-free data profiling for LLM context compression.
"""

from kontra.scout.profiler import ScoutProfiler
from kontra.scout.types import ColumnProfile, DatasetProfile

__all__ = ["ScoutProfiler", "ColumnProfile", "DatasetProfile"]
