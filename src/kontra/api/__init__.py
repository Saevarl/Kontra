# src/kontra/api/__init__.py
"""
Kontra Python API - Public interface classes and functions.
"""

from kontra.api.results import (
    ValidationResult,
    RuleResult,
    Diff,
    Suggestions,
    SuggestedRule,
)
from kontra.api.rules import rules

__all__ = [
    "ValidationResult",
    "RuleResult",
    "Diff",
    "Suggestions",
    "SuggestedRule",
    "rules",
]
