# src/kontra/probes/__init__.py
"""
Transformation probes for Kontra.

Probes measure the structural effects of data transformations without
assigning meaning or judgment. They provide deterministic, structured,
token-efficient measurements for agents to reason about.

Available probes:
- compare: Measure differences between before/after transformation
- profile_relationship: Measure JOIN viability between datasets
"""

from kontra.probes.compare import compare
from kontra.probes.relationship import profile_relationship

__all__ = [
    "compare",
    "profile_relationship",
]
