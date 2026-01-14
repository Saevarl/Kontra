# src/kontra/state/__init__.py
"""
Kontra State Management - Validation state persistence and comparison.

Enables time-based reasoning for agentic workflows by tracking validation
results across runs.
"""

from .types import ValidationState, RuleState, StateSummary, StateDiff, RuleDiff, FailureMode, Severity
from .fingerprint import fingerprint_contract, fingerprint_dataset
from .backends import StateBackend, LocalStore, get_default_store

__all__ = [
    # Types
    "ValidationState",
    "RuleState",
    "StateSummary",
    "StateDiff",
    "RuleDiff",
    "FailureMode",
    "Severity",
    # Fingerprinting
    "fingerprint_contract",
    "fingerprint_dataset",
    # Backends
    "StateBackend",
    "LocalStore",
    "get_default_store",
]
