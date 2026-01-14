# src/kontra/engine/types.py
"""
Type definitions for engine result dictionaries.

These TypedDicts provide IDE support and documentation for the
dict-based results returned by the validation engine.

Usage:
    from kontra.engine.types import RuleResultDict, ValidationResultDict

    def process_result(result: RuleResultDict) -> None:
        print(result["rule_id"])  # IDE knows this is str
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, TypedDict, Literal


class RuleResultDict(TypedDict, total=False):
    """
    Result of validating a single rule.

    Required fields:
        rule_id: Unique identifier for the rule
        passed: Whether the rule passed validation
        failed_count: Number of violations found
        message: Human-readable result message

    Optional fields:
        severity: blocking | warning | info
        execution_source: Where rule was executed (polars | sql | metadata)
        failure_mode: Type of failure (null_values, duplicate_values, etc.)
        details: Additional details (unexpected values, suggestions, etc.)
        actions_executed: List of post-validation actions run
    """
    # Required
    rule_id: str
    passed: bool
    failed_count: int
    message: str
    # Optional
    severity: str
    execution_source: str
    failure_mode: str
    details: Dict[str, Any]
    actions_executed: List[str]


class SummaryDict(TypedDict, total=False):
    """
    Validation summary for a dataset.

    Contains aggregate pass/fail counts and optional severity breakdowns.
    """
    passed: bool
    total_rules: int
    rules_passed: int
    rules_failed: int
    dataset_name: str
    # Severity breakdown
    blocking_failures: int
    warning_failures: int
    info_failures: int


class ValidationResultDict(TypedDict, total=False):
    """
    Complete validation result returned by ValidationEngine.run().

    Contains summary, individual rule results, and optional stats.
    """
    dataset: str
    summary: SummaryDict
    results: List[RuleResultDict]
    stats: Dict[str, Any]
    run_meta: Dict[str, Any]


class PreplanSummaryDict(TypedDict, total=False):
    """
    Preplan (metadata analysis) summary.

    Reports how many rules were resolved via metadata without data scan.
    """
    enabled: bool
    effective: bool
    rules_pass_meta: int
    rules_fail_meta: int
    rules_unknown: int
    row_groups_kept: Optional[int]
    row_groups_total: Optional[int]
    row_groups_pruned: Optional[int]


class ProjectionDict(TypedDict, total=False):
    """
    Column projection statistics.

    Reports column pruning effectiveness.
    """
    enabled: bool
    available_count: int
    full: Dict[str, Any]
    residual: Dict[str, Any]


class PushdownDict(TypedDict, total=False):
    """
    SQL pushdown statistics.

    Reports SQL execution details and timing.
    """
    enabled: bool
    effective: bool
    executor: str
    rules_pushed: int
    breakdown_ms: Dict[str, int]


class StatsDict(TypedDict, total=False):
    """
    Full validation statistics.

    Optional stats block attached to validation results when
    stats_mode is "summary" or "profile".
    """
    stats_version: str
    run_meta: Dict[str, Any]
    dataset: Dict[str, Any]
    preplan: PreplanSummaryDict
    pushdown: PushdownDict
    projection: ProjectionDict
    residual: Dict[str, Any]
    columns_touched: List[str]
    columns_validated: List[str]
    columns_loaded: List[str]
    profile: Dict[str, Any]
