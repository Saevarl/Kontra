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

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, TypedDict, TYPE_CHECKING

if TYPE_CHECKING:
    from kontra.rule_defs.execution_plan import CompiledPlan, RuleExecutionPlan


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


# --------------------------------------------------------------------------- #
# Phase Dataclasses
# --------------------------------------------------------------------------- #
# These dataclasses represent the outputs of each phase in the validation
# engine pipeline. They enable clean separation of concerns and make the
# _run_impl() method easier to follow.


@dataclass
class CompilationContext:
    """
    Output of the rule compilation phase.

    Contains everything needed to execute rules across tiers:
    - Built rule objects
    - Execution plan with compiled predicates
    - Severity, tally, and context mappings by rule_id
    """
    rules: List[Any]
    plan: "RuleExecutionPlan"
    compiled_full: "CompiledPlan"
    severity_map: Dict[str, str]
    tally_map: Dict[str, bool]
    context_map: Dict[str, Dict[str, Any]]


@dataclass
class PreplanResult:
    """
    Output of the preplan (metadata-only) phase.

    Reports which rules were resolved via metadata without data scan,
    and provides a scan manifest for remaining rules.
    """
    effective: bool
    handled_ids: Set[str]
    results_by_id: Dict[str, Dict[str, Any]]
    # Parquet-specific manifest
    row_groups: Optional[List[int]] = None
    columns: Optional[List[str]] = None
    total_rows: Optional[int] = None
    # Timing
    analyze_ms: int = 0
    # Summary for stats
    summary: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PushdownResult:
    """
    Output of the SQL pushdown phase.

    Reports which rules were executed via SQL (DuckDB/PostgreSQL/SQLServer)
    and provides metadata for subsequent phases.
    """
    effective: bool
    handled_ids: Set[str]
    results_by_id: Dict[str, Dict[str, Any]]
    row_count: Optional[int] = None
    available_cols: List[str] = field(default_factory=list)
    executor_name: str = "none"
    # Timing breakdown
    compile_ms: int = 0
    execute_ms: int = 0
    introspect_ms: int = 0
    # Staging info (CSV -> Parquet)
    staged_path: Optional[str] = None
    staging_tmpdir: Any = None  # tempfile.TemporaryDirectory


@dataclass
class ResidualResult:
    """
    Output of the residual Polars execution phase.

    Contains rule results executed via Polars and the loaded DataFrame.
    """
    results: List[Dict[str, Any]]
    df: Optional[Any] = None  # pl.DataFrame
    # Timing
    load_ms: int = 0
    execute_ms: int = 0
