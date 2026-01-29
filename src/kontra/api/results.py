# src/kontra/api/results.py
"""
Public API result types for Kontra.

These classes wrap the internal state/result types with a cleaner interface
for the public Python API.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple, Union

import yaml

_logger = logging.getLogger(__name__)


# --- Unique rule sampling helpers (shared by multiple methods) ---


def _is_unique_rule(rule: Any) -> bool:
    """Check if a rule is a unique rule."""
    return getattr(rule, "name", None) == "unique"


def _filter_samples_polars(
    source: Any,  # pl.DataFrame or pl.LazyFrame
    rule: Any,
    predicate: Any,
    n: int,
) -> Any:  # pl.DataFrame
    """
    Filter samples with special handling for unique rule.

    Works with both DataFrame and LazyFrame sources. Adds row_index,
    and for unique rules adds _duplicate_count sorted by worst offenders.

    Args:
        source: Polars DataFrame or LazyFrame
        rule: Rule object (used to detect unique rule)
        predicate: Polars expression for filtering
        n: Maximum rows to return

    Returns:
        Polars DataFrame with filtered samples
    """
    import polars as pl

    # Convert DataFrame to LazyFrame if needed
    if isinstance(source, pl.DataFrame):
        lf = source.lazy()
    else:
        lf = source

    # Add row index
    lf = lf.with_row_index("row_index")

    # Special case: unique rule - add duplicate count, sort by worst offenders
    if _is_unique_rule(rule):
        column = rule.params.get("column")
        return (
            lf.with_columns(
                pl.col(column).count().over(column).alias("_duplicate_count")
            )
            .filter(predicate)
            .sort("_duplicate_count", descending=True)
            .head(n)
            .collect()
        )
    else:
        return lf.filter(predicate).head(n).collect()


def _build_unique_sample_query_sql(
    table: str,
    column: str,
    n: int,
    dialect: str,
) -> str:
    """
    Build SQL query for sampling unique rule violations.

    Returns query that finds duplicate values, orders by worst offenders,
    and includes _duplicate_count and row_index.

    Args:
        table: Fully qualified table name
        column: Column being checked for uniqueness
        n: Maximum rows to return
        dialect: SQL dialect ("postgres", "mssql")

    Returns:
        SQL query string
    """
    col = f'"{column}"'

    if dialect == "mssql":
        return f"""
            SELECT t.*, dup._duplicate_count,
                   ROW_NUMBER() OVER (ORDER BY dup._duplicate_count DESC) - 1 AS row_index
            FROM {table} t
            JOIN (
                SELECT {col}, COUNT(*) as _duplicate_count
                FROM {table}
                GROUP BY {col}
                HAVING COUNT(*) > 1
            ) dup ON t.{col} = dup.{col}
            ORDER BY dup._duplicate_count DESC
            OFFSET 0 ROWS FETCH FIRST {n} ROWS ONLY
        """
    else:
        return f"""
            SELECT t.*, dup._duplicate_count,
                   ROW_NUMBER() OVER (ORDER BY dup._duplicate_count DESC) - 1 AS row_index
            FROM {table} t
            JOIN (
                SELECT {col}, COUNT(*) as _duplicate_count
                FROM {table}
                GROUP BY {col}
                HAVING COUNT(*) > 1
            ) dup ON t.{col} = dup.{col}
            ORDER BY dup._duplicate_count DESC
            LIMIT {n}
        """


# --- End unique rule helpers ---


class FailureSamples:
    """
    Collection of sample rows that failed a validation rule.

    This class wraps a list of failing rows with serialization methods.
    It's iterable and indexable like a list.

    Properties:
        rule_id: The rule ID these samples are from
        count: Number of samples in this collection

    Methods:
        to_dict(): Convert to list of dicts
        to_json(): Convert to JSON string
        to_llm(): Token-optimized format for LLM context
        to_dataframe(): Convert to Polars DataFrame for analysis
    """

    def __init__(self, samples: List[Dict[str, Any]], rule_id: str):
        self._samples = samples
        self.rule_id = rule_id

    def __repr__(self) -> str:
        return f"FailureSamples({self.rule_id}, {len(self._samples)} rows)"

    def __len__(self) -> int:
        return len(self._samples)

    def __iter__(self) -> Iterator[Dict[str, Any]]:
        return iter(self._samples)

    def __getitem__(self, index: int) -> Dict[str, Any]:
        return self._samples[index]

    def __bool__(self) -> bool:
        return len(self._samples) > 0

    @property
    def count(self) -> int:
        """Number of sample rows."""
        return len(self._samples)

    def to_dict(self) -> List[Dict[str, Any]]:
        """Convert to list of dicts."""
        return self._samples

    def to_json(self, indent: Optional[int] = None) -> str:
        """Convert to JSON string."""
        return json.dumps(self._samples, indent=indent, default=str)

    def to_llm(self) -> str:
        """
        Token-optimized format for LLM context.

        Example output:
            SAMPLES: COL:email:not_null (2 rows)
            [0] row=1: id=2, email=None, status=active
            [1] row=3: id=4, email=None, status=active

        For unique rule:
            SAMPLES: COL:user_id:unique (2 rows)
            [0] row=5, dupes=3: user_id=123, name=Alice
            [1] row=8, dupes=3: user_id=123, name=Bob
        """
        if not self._samples:
            return f"SAMPLES: {self.rule_id} (0 rows)"

        lines = [f"SAMPLES: {self.rule_id} ({len(self._samples)} rows)"]

        for i, row in enumerate(self._samples[:10]):  # Limit to 10 for token efficiency
            # Extract special columns for prefix
            row_idx = row.get("row_index")
            dup_count = row.get("_duplicate_count")

            # Build prefix with metadata
            prefix_parts = []
            if row_idx is not None:
                prefix_parts.append(f"row={row_idx}")
            if dup_count is not None:
                prefix_parts.append(f"dupes={dup_count}")
            prefix = ", ".join(prefix_parts) + ": " if prefix_parts else ""

            # Format remaining columns as compact key=value pairs
            parts = []
            for k, v in row.items():
                # Skip special columns (already in prefix)
                if k in ("row_index", "_duplicate_count"):
                    continue
                if v is None:
                    parts.append(f"{k}=None")
                elif isinstance(v, str) and len(v) > 20:
                    parts.append(f"{k}={v[:20]}...")
                else:
                    parts.append(f"{k}={v}")
            lines.append(f"[{i}] {prefix}" + ", ".join(parts))

        if len(self._samples) > 10:
            lines.append(f"... +{len(self._samples) - 10} more rows")

        return "\n".join(lines)

    def to_dataframe(self) -> "pl.DataFrame":
        """
        Convert samples to a Polars DataFrame.

        Returns:
            Polars DataFrame with sample rows

        Example:
            >>> samples = result.sample_failures("COL:email:not_null")
            >>> df = samples.to_dataframe()
            >>> df.filter(pl.col("status") == "active")
        """
        import polars as pl
        if not self._samples:
            return pl.DataFrame()
        return pl.DataFrame(self._samples)


class SampleReason:
    """Constants for why samples may be unavailable."""

    UNAVAILABLE_METADATA = "unavailable_from_metadata"  # Preplan tier - knows existence, not location
    UNAVAILABLE_PASSED = "rule_passed"  # No failures to sample
    UNAVAILABLE_UNSUPPORTED = "rule_unsupported"  # dtype, min_rows, etc. - no row-level samples
    TRUNCATED_BUDGET = "budget_exhausted"  # Global budget hit
    TRUNCATED_LIMIT = "per_rule_limit"  # Per-rule cap hit


@dataclass
class RuleResult:
    """
    Result for a single validation rule.

    Properties:
        rule_id: Unique identifier (e.g., "COL:user_id:not_null")
        name: Rule type name (e.g., "not_null")
        passed: Whether the rule passed
        failed_count: Number of failing rows (exact if tally=True, ≥1 if tally=False)
        violation_rate: Fraction of rows that failed (0.0-1.0), or None if passed
        message: Human-readable result message
        severity: "blocking" | "warning" | "info"
        source: Measurement source ("metadata", "sql", "polars")
        tally: Whether failed_count is exact (True) or a lower bound (False)
        column: Column name if applicable
        context: Consumer-defined metadata (owner, tags, fix_hint, etc.)
        annotations: List of annotations on this rule (opt-in, loaded via get_run_with_annotations)
        severity_weight: User-defined numeric weight (None if unconfigured)

    Sampling properties:
        samples: List of sample failing rows, or None if unavailable
        samples_source: Where samples came from ("sql", "polars"), or None
        samples_reason: Why samples unavailable (see SampleReason)
        samples_truncated: True if more samples exist but were cut off
    """

    rule_id: str
    name: str
    passed: bool
    failed_count: int
    message: str
    severity: str = "blocking"
    source: str = "polars"
    tally: bool = False  # True = exact count, False = early stop (failed_count may be ≥1)
    column: Optional[str] = None
    details: Optional[Dict[str, Any]] = None
    context: Optional[Dict[str, Any]] = None
    failure_mode: Optional[str] = None  # Semantic failure type (e.g., "config_error", "null_values")

    # Sampling fields (eager sampling)
    samples: Optional[List[Dict[str, Any]]] = None
    samples_source: Optional[str] = None
    samples_reason: Optional[str] = None
    samples_truncated: bool = False

    # Annotations (opt-in, loaded via get_run_with_annotations)
    annotations: Optional[List[Dict[str, Any]]] = None

    # LLM juice: user-defined severity weight (None if unconfigured)
    severity_weight: Optional[float] = None

    # For violation_rate computation (populated during result creation)
    _total_rows: Optional[int] = field(default=None, repr=False)

    @property
    def violation_rate(self) -> Optional[float]:
        """
        Fraction of rows that failed this rule.

        Returns:
            Float 0.0-1.0, or None if:
            - Rule passed (failed_count == 0)
            - total_rows is 0 or unknown

        Example:
            for rule in result.rules:
                if rule.violation_rate:
                    print(f"{rule.rule_id}: {rule.violation_rate:.2%} of rows failed")
        """
        if self.passed or self.failed_count == 0:
            return None
        if self._total_rows is None or self._total_rows == 0:
            return None
        return self.failed_count / self._total_rows

    def __repr__(self) -> str:
        status = "PASS" if self.passed else "FAIL"
        if self.failed_count > 0:
            failure_word = "failure" if self.failed_count == 1 else "failures"
            return f"RuleResult({self.rule_id}) {status} - {self.failed_count:,} {failure_word}"
        return f"RuleResult({self.rule_id}) {status}"

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "RuleResult":
        """Create from engine result dict."""
        rule_id = d.get("rule_id", "")

        # Extract column from rule_id if present
        column = None
        if rule_id.startswith("COL:"):
            parts = rule_id.split(":")
            if len(parts) >= 2:
                column = parts[1]

        # Extract rule name
        name = d.get("rule_name", d.get("name", ""))
        if not name and ":" in rule_id:
            name = rule_id.split(":")[-1]

        return cls(
            rule_id=rule_id,
            name=name,
            passed=d.get("passed", False),
            failed_count=d.get("failed_count", 0),
            message=d.get("message", ""),
            severity=d.get("severity", "blocking"),
            source=d.get("execution_source", d.get("source", "polars")),
            tally=d.get("tally", False),
            column=column,
            details=d.get("details"),
            context=d.get("context"),
            failure_mode=d.get("failure_mode"),
            # Sampling fields
            samples=d.get("samples"),
            samples_source=d.get("samples_source"),
            samples_reason=d.get("samples_reason"),
            samples_truncated=d.get("samples_truncated", False),
            # LLM juice
            severity_weight=d.get("severity_weight"),
        )

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        d = {
            "rule_id": self.rule_id,
            "name": self.name,
            "passed": self.passed,
            "failed_count": self.failed_count,
            "tally": self.tally,
            "message": self.message,
            "severity": self.severity,
            "source": self.source,
        }
        # Include violation_rate if available
        if self.violation_rate is not None:
            d["violation_rate"] = self.violation_rate
        if self.column:
            d["column"] = self.column
        if self.details:
            d["details"] = self.details
        if self.context:
            d["context"] = self.context
        if self.failure_mode:
            d["failure_mode"] = self.failure_mode

        # Sampling fields - always include for clarity
        d["samples"] = self.samples  # None = unavailable, [] = none found
        if self.samples_source:
            d["samples_source"] = self.samples_source
        if self.samples_reason:
            d["samples_reason"] = self.samples_reason
        if self.samples_truncated:
            d["samples_truncated"] = self.samples_truncated

        # Annotations (opt-in)
        if self.annotations is not None:
            d["annotations"] = self.annotations

        # LLM juice (only include if configured)
        if self.severity_weight is not None:
            d["severity_weight"] = self.severity_weight

        return d

    def to_llm(self) -> str:
        """Token-optimized format for LLM context."""
        status = "PASS" if self.passed else "FAIL"
        parts = [f"{self.rule_id}: {status}"]

        if self.failed_count > 0:
            if self.tally:
                parts.append(f"({self.failed_count:,} failures)")
            else:
                # Early-stop mode: count is a lower bound
                parts.append(f"(≥{self.failed_count:,} failures)")

        # Include violation_rate for failed rules (LLM juice)
        if self.violation_rate is not None:
            parts.append(f"[{self.violation_rate:.1%}]")

        # Include severity weight if configured (LLM juice)
        if self.severity_weight is not None:
            parts.append(f"[w={self.severity_weight}]")

        # Include context metadata (owner, sla, tags, etc.)
        if self.context:
            ctx_parts = [f"{k}={v}" for k, v in self.context.items()]
            parts.append(f"ctx=[{', '.join(ctx_parts)}]")

        # Add samples if available
        if self.samples:
            parts.append(f"\n  Samples ({len(self.samples)}):")
            for i, row in enumerate(self.samples[:5]):
                # Extract metadata
                row_idx = row.get("row_index")
                dup_count = row.get("_duplicate_count")
                prefix_parts = []
                if row_idx is not None:
                    prefix_parts.append(f"row={row_idx}")
                if dup_count is not None:
                    prefix_parts.append(f"dupes={dup_count}")
                prefix = ", ".join(prefix_parts) + ": " if prefix_parts else ""

                # Format data columns
                data_parts = []
                for k, v in row.items():
                    if k in ("row_index", "_duplicate_count"):
                        continue
                    if v is None:
                        data_parts.append(f"{k}=None")
                    elif isinstance(v, str) and len(v) > 15:
                        data_parts.append(f"{k}={v[:15]}...")
                    else:
                        data_parts.append(f"{k}={v}")
                parts.append(f"    [{i}] {prefix}" + ", ".join(data_parts[:5]))
            if len(self.samples) > 5:
                parts.append(f"    ... +{len(self.samples) - 5} more")
        elif self.samples_reason:
            parts.append(f"\n  Samples: {self.samples_reason}")

        # Add annotations if available
        if self.annotations:
            parts.append(f"\n  Annotations ({len(self.annotations)}):")
            for ann in self.annotations[:3]:
                ann_type = ann.get("annotation_type", "note")
                actor = ann.get("actor_id", "unknown")
                summary = ann.get("summary", "")[:40]
                if len(ann.get("summary", "")) > 40:
                    summary += "..."
                parts.append(f'    [{ann_type}] by {actor}: "{summary}"')
            if len(self.annotations) > 3:
                parts.append(f"    ... +{len(self.annotations) - 3} more")

        return " ".join(parts[:2]) + "".join(parts[2:])


@dataclass
class ValidationResult:
    """
    Result of a validation run.

    Properties:
        passed: True if all blocking rules passed
        dataset: Dataset name/path
        total_rows: Number of rows in the validated dataset
        total_rules: Total number of rules evaluated
        passed_count: Number of rules that passed
        failed_count: Number of blocking rules that failed
        warning_count: Number of warning rules that failed
        rules: List of RuleResult objects
        blocking_failures: List of failed blocking rules
        warnings: List of failed warning rules
        quality_score: Deterministic score 0.0-1.0 (None if weights unconfigured)
        data: The validated DataFrame (if loaded), None if preplan/pushdown handled everything
        stats: Optional statistics dict
        annotations: List of run-level annotations (opt-in, loaded via get_run_with_annotations)

    Methods:
        sample_failures(rule_id, n=5): Get sample of failing rows for a rule

    Note:
        Each RuleResult has a `violation_rate` property for per-rule failure rates.
    """

    passed: bool
    dataset: str
    total_rows: int
    total_rules: int
    passed_count: int
    failed_count: int
    warning_count: int
    rules: List[RuleResult]
    stats: Optional[Dict[str, Any]] = None
    annotations: Optional[List[Dict[str, Any]]] = None  # Run-level annotations (opt-in)
    _raw: Optional[Dict[str, Any]] = field(default=None, repr=False)
    # For sample_failures() - lazy evaluation
    _data_source: Optional[Any] = field(default=None, repr=False)
    _rule_objects: Optional[List[Any]] = field(default=None, repr=False)
    # Loaded data (if Polars execution occurred)
    _data: Optional[Any] = field(default=None, repr=False)

    @property
    def data(self) -> Optional["pl.DataFrame"]:
        """
        The validated DataFrame, if data was loaded.

        Returns the Polars DataFrame that was validated when:
        - Polars execution occurred (residual rules needed data)
        - A DataFrame was passed directly to validate()

        Returns None when:
        - All rules were resolved by preplan/pushdown (no data loaded)
        - Data source was a file path and wasn't materialized

        Example:
            result = kontra.validate("data.parquet", rules=[...], preplan="off", pushdown="off")
            if result.passed and result.data is not None:
                # Use the already-loaded data
                process(result.data)
        """
        return self._data

    def __repr__(self) -> str:
        from kontra.connectors.handle import mask_credentials

        status = "PASSED" if self.passed else "FAILED"
        safe_dataset = mask_credentials(self.dataset) if self.dataset else self.dataset
        parts = [f"ValidationResult({safe_dataset}) {status}"]
        parts.append(f"  Total: {self.total_rules} rules | Passed: {self.passed_count} | Failed: {self.failed_count}")
        if self.warning_count > 0:
            parts.append(f"  Warnings: {self.warning_count}")
        if not self.passed:
            blocking = [r.rule_id for r in self.blocking_failures[:3]]
            if blocking:
                parts.append(f"  Blocking: {', '.join(blocking)}")
                if len(self.blocking_failures) > 3:
                    parts.append(f"    ... and {len(self.blocking_failures) - 3} more")
        return "\n".join(parts)

    def __bool__(self) -> bool:
        """Return True if validation passed, False otherwise.

        Enables intuitive boolean checks:
            if result:
                print("Validation passed")
        """
        return self.passed

    def __iter__(self) -> Iterator[RuleResult]:
        """Iterate over rule results.

        Enables natural iteration:
            for rule in result:
                print(rule.rule_id, rule.passed)
        """
        return iter(self.rules)

    def __len__(self) -> int:
        """Return number of rules.

        Enables len(result) to return the number of rules.
        """
        return len(self.rules)

    @property
    def blocking_failures(self) -> List[RuleResult]:
        """Get all failed blocking rules."""
        return [r for r in self.rules if not r.passed and r.severity == "blocking"]

    @property
    def warnings(self) -> List[RuleResult]:
        """Get all failed warning rules."""
        return [r for r in self.rules if not r.passed and r.severity == "warning"]

    @property
    def info_failures(self) -> List[RuleResult]:
        """Get all failed info rules."""
        return [r for r in self.rules if not r.passed and r.severity == "info"]

    @property
    def quality_score(self) -> Optional[float]:
        """
        Deterministic quality score derived from violation data.

        Formula:
            quality_score = 1.0 - weighted_violation_rate
            weighted_violation_rate = Σ(failed_count * severity_weight) / (total_rows * Σ(weights))

        Returns:
            Float 0.0-1.0, or None if:
            - severity_weights not configured
            - total_rows is 0
            - No rules have weights

        Note:
            This score is pure data - Kontra never interprets it as "good" or "bad".
            Consumers/agents use it for trend reasoning.
        """
        # Check if any rule has a weight (weights configured)
        rules_with_weights = [r for r in self.rules if r.severity_weight is not None]
        if not rules_with_weights:
            return None

        # Avoid division by zero
        if self.total_rows == 0:
            return None

        # Calculate weighted violation sum
        weighted_violations = sum(
            r.failed_count * r.severity_weight
            for r in rules_with_weights
        )

        # Calculate total possible weighted violations
        # (if every row failed every rule)
        total_weight = sum(r.severity_weight for r in rules_with_weights)
        max_weighted_violations = self.total_rows * total_weight

        if max_weighted_violations == 0:
            return 1.0  # No possible violations

        # Quality = 1 - violation_rate
        violation_rate = weighted_violations / max_weighted_violations
        return max(0.0, min(1.0, 1.0 - violation_rate))

    @classmethod
    def from_engine_result(
        cls,
        result: Dict[str, Any],
        dataset: str = "unknown",
        data_source: Optional[Any] = None,
        rule_objects: Optional[List[Any]] = None,
        sample: int = 5,
        sample_budget: int = 50,
        sample_columns: Optional[Union[List[str], str]] = None,
        severity_weights: Optional[Dict[str, float]] = None,
        data: Optional[Any] = None,
    ) -> "ValidationResult":
        """Create from ValidationEngine.run() result dict.

        Args:
            result: Engine result dict
            dataset: Dataset name (fallback)
            data_source: Original data source for lazy sample_failures()
            rule_objects: Rule objects for sample_failures() predicates
            sample: Per-rule sample cap (0 to disable)
            sample_budget: Global sample cap across all rules
            sample_columns: Columns to include in samples (None=all, list=specific, "relevant"=rule columns)
            severity_weights: User-defined severity weights from config (None if unconfigured)
            data: Loaded DataFrame (if Polars execution occurred)
        """
        summary = result.get("summary", {})
        results_list = result.get("results", [])

        # Convert raw results to RuleResult objects
        rules = [RuleResult.from_dict(r) for r in results_list]

        # Populate context from rule objects if available
        if rule_objects is not None:
            context_map = {
                getattr(r, "rule_id", r.name): getattr(r, "context", {})
                for r in rule_objects
            }
            for rule_result in rules:
                ctx = context_map.get(rule_result.rule_id)
                if ctx:
                    rule_result.context = ctx

        # Populate severity weights from config (LLM juice)
        if severity_weights is not None:
            for rule_result in rules:
                weight = severity_weights.get(rule_result.severity)
                if weight is not None:
                    rule_result.severity_weight = weight

        # Calculate counts
        total = summary.get("total_rules", len(rules))
        passed_count = summary.get("rules_passed", sum(1 for r in rules if r.passed))

        # Count by severity
        blocking_failed = sum(1 for r in rules if not r.passed and r.severity == "blocking")
        warning_failed = sum(1 for r in rules if not r.passed and r.severity == "warning")

        # Extract total_rows from summary
        total_rows = summary.get("total_rows", 0)

        # Populate _total_rows on each rule for violation_rate property
        for rule_result in rules:
            rule_result._total_rows = total_rows

        # Create instance first (need it for sampling methods)
        instance = cls(
            passed=summary.get("passed", blocking_failed == 0),
            dataset=summary.get("dataset_name", dataset),
            total_rows=total_rows,
            total_rules=total,
            passed_count=passed_count,
            failed_count=blocking_failed,
            warning_count=warning_failed,
            rules=rules,
            stats=result.get("stats"),
            _raw=result,
            _data_source=data_source,
            _rule_objects=rule_objects,
            _data=data,
        )

        # Perform eager sampling if enabled
        if sample > 0 and rule_objects is not None:
            instance._perform_eager_sampling(sample, sample_budget, rule_objects, sample_columns)

        return instance

    def _perform_eager_sampling(
        self,
        per_rule_cap: int,
        global_budget: int,
        rule_objects: List[Any],
        sample_columns: Optional[Union[List[str], str]] = None,
    ) -> None:
        """
        Populate samples for each rule (eager sampling).

        Uses batched SQL queries when possible (1 query for all rules).
        Falls back to per-rule Polars sampling when SQL not supported.

        Args:
            per_rule_cap: Max samples per rule
            global_budget: Total samples across all rules
            rule_objects: Rule objects for predicates
            sample_columns: Columns to include (None=all, list=specific, "relevant"=rule columns)
        """
        import polars as pl

        # Build rule_id -> rule_object map
        rule_map = {getattr(r, "rule_id", None): r for r in rule_objects}

        # Sort rules by failed_count descending (worst offenders first)
        sorted_rules = sorted(
            self.rules,
            key=lambda r: r.failed_count if not r.passed else 0,
            reverse=True,
        )

        remaining_budget = global_budget

        # Phase 1: Collect rules that can use SQL batching
        sql_rules: List[Tuple[Any, Any, int, Optional[List[str]]]] = []  # (rule_result, rule_obj, n, columns)
        polars_rules: List[Tuple[Any, Any, Any, int, Optional[List[str]]]] = []  # (rule_result, rule_obj, predicate, n, columns)

        for rule_result in sorted_rules:
            # Handle passing rules
            if rule_result.passed:
                rule_result.samples = []
                rule_result.samples_reason = SampleReason.UNAVAILABLE_PASSED
                continue

            # Check budget
            if remaining_budget <= 0:
                rule_result.samples = None
                rule_result.samples_reason = SampleReason.TRUNCATED_BUDGET
                rule_result.samples_truncated = True
                continue

            # Get corresponding rule object
            rule_obj = rule_map.get(rule_result.rule_id)
            if rule_obj is None:
                rule_result.samples = None
                rule_result.samples_reason = SampleReason.UNAVAILABLE_UNSUPPORTED
                continue

            # Check if rule was resolved via metadata (preplan)
            if rule_result.source == "metadata":
                if not self._can_sample_source():
                    rule_result.samples = None
                    rule_result.samples_reason = SampleReason.UNAVAILABLE_METADATA
                    continue

            # Calculate samples to get for this rule
            n = min(per_rule_cap, remaining_budget)
            remaining_budget -= n  # Reserve budget

            # Determine columns to include
            cols_to_include = self._resolve_sample_columns(sample_columns, rule_obj)

            # Check if rule supports SQL filter (for batching)
            # Note: to_sql_filter can return None to indicate no SQL support for this dialect
            sql_filter_available = False
            if hasattr(rule_obj, "to_sql_filter"):
                # Probe to see if SQL filter is actually available
                sql_filter = rule_obj.to_sql_filter("duckdb")
                if sql_filter is not None:
                    sql_rules.append((rule_result, rule_obj, n, cols_to_include))
                    sql_filter_available = True

            # Fall back to sample_predicate/compile_predicate if SQL not available
            if not sql_filter_available:
                # Check sample_predicate() first (for rules like unique that have
                # different counting vs sampling semantics)
                pred_obj = None
                if hasattr(rule_obj, "sample_predicate"):
                    pred_obj = rule_obj.sample_predicate()
                if pred_obj is None and hasattr(rule_obj, "compile_predicate"):
                    pred_obj = rule_obj.compile_predicate()

                if pred_obj is not None:
                    polars_rules.append((rule_result, rule_obj, pred_obj.expr, n, cols_to_include))
                else:
                    rule_result.samples = None
                    rule_result.samples_reason = SampleReason.UNAVAILABLE_UNSUPPORTED

        # Phase 2: Execute batched SQL sampling if applicable
        if sql_rules:
            self._execute_batched_sql_sampling(sql_rules, per_rule_cap)

        # Phase 3: Execute per-rule Polars sampling for remaining rules
        for rule_result, rule_obj, predicate, n, cols_to_include in polars_rules:
            try:
                samples, samples_source = self._collect_samples_for_rule(rule_obj, predicate, n, cols_to_include)
                rule_result.samples = samples
                rule_result.samples_source = samples_source

                if len(samples) == per_rule_cap and rule_result.failed_count > per_rule_cap:
                    rule_result.samples_truncated = True
                    rule_result.samples_reason = SampleReason.TRUNCATED_LIMIT

            except Exception as e:
                rule_result.samples = None
                rule_result.samples_reason = f"error: {str(e)[:50]}"

    def _execute_batched_sql_sampling(
        self,
        sql_rules: List[Tuple[Any, Any, int, Optional[List[str]]]],
        per_rule_cap: int,
    ) -> None:
        """
        Execute batched SQL sampling for rules that support to_sql_filter().

        Builds a single UNION ALL query for all rules, executes once,
        and distributes results.
        """
        import polars as pl

        source = self._data_source
        if source is None:
            return

        # Determine dialect and source type
        dialect = "duckdb"
        is_parquet = False
        is_database = False
        db_conn = None
        db_table = None
        parquet_path = None
        owns_connection = False  # Track if we created the connection (need to close it)

        is_s3 = False

        if isinstance(source, str):
            lower = source.lower()
            if source.startswith("s3://") or source.startswith("http://") or source.startswith("https://"):
                # Remote file - use DuckDB for efficient sampling
                is_parquet = True
                is_s3 = True
                parquet_path = source
            elif lower.endswith(".parquet") or lower.endswith(".csv"):
                # Local file - use Polars (more efficient for local)
                is_parquet = False  # Will fall through to Polars path
        elif hasattr(source, "scheme"):
            handle = source
            if handle.scheme in ("postgres", "postgresql"):
                is_database = True
                dialect = "postgres"
                db_conn = getattr(handle, "external_conn", None)
                # If no external connection but we have db_params, create one
                if db_conn is None and handle.db_params is not None:
                    try:
                        from kontra.connectors.postgres import get_connection
                        db_conn = get_connection(handle.db_params)
                        owns_connection = True  # We need to close this
                    except ImportError as e:
                        _logger.debug(f"PostgreSQL driver not available: {e}")
                        db_conn = None
                    except (OSError, ConnectionError) as e:
                        _logger.debug(f"Could not connect to PostgreSQL for sampling: {e}")
                        db_conn = None
                # Get table reference from handle or db_params
                if handle.table_ref:
                    db_table = handle.table_ref
                elif handle.db_params:
                    db_table = f'"{handle.db_params.schema}"."{handle.db_params.table}"'
                else:
                    db_table = None
            elif handle.scheme == "mssql":
                is_database = True
                dialect = "mssql"
                db_conn = getattr(handle, "external_conn", None)
                # If no external connection but we have db_params, create one
                if db_conn is None and handle.db_params is not None:
                    try:
                        from kontra.connectors.sqlserver import get_connection
                        db_conn = get_connection(handle.db_params)
                        owns_connection = True  # We need to close this
                    except ImportError as e:
                        _logger.debug(f"SQL Server driver not available: {e}")
                        db_conn = None
                    except (OSError, ConnectionError) as e:
                        _logger.debug(f"Could not connect to SQL Server for sampling: {e}")
                        db_conn = None
                # Get table reference from handle or db_params
                if handle.table_ref:
                    db_table = handle.table_ref
                elif handle.db_params:
                    db_table = f"[{handle.db_params.schema}].[{handle.db_params.table}]"
                else:
                    db_table = None
            elif handle.scheme == "s3":
                # S3 via handle - use DuckDB
                is_parquet = True
                is_s3 = True
                parquet_path = handle.uri
            elif handle.scheme in ("", "file"):
                # Local file via handle - use Polars
                is_parquet = False

        # Build list of (rule_id, sql_filter, limit, columns) for batching
        rules_to_sample: List[Tuple[str, str, int, Optional[List[str]]]] = []

        for rule_result, rule_obj, n, cols_to_include in sql_rules:
            sql_filter = rule_obj.to_sql_filter(dialect)
            if sql_filter:
                rules_to_sample.append((rule_result.rule_id, sql_filter, n, cols_to_include))
            else:
                # Rule doesn't support this dialect, mark as unsupported
                rule_result.samples = None
                rule_result.samples_reason = SampleReason.UNAVAILABLE_UNSUPPORTED

        if not rules_to_sample:
            return

        # Execute batched query
        try:
            if is_parquet and is_s3 and parquet_path:
                # S3/remote: Use DuckDB batched sampling (much faster than Polars)
                results = self._batch_sample_parquet_duckdb(parquet_path, rules_to_sample)
                samples_source = "sql"
            elif is_database and db_conn and db_table:
                results = self._batch_sample_db(db_conn, db_table, rules_to_sample, dialect)
                samples_source = "sql"
            else:
                # Fall back to per-rule sampling
                results = {}
                samples_source = "polars"
                for rule_result, rule_obj, n, cols_to_include in sql_rules:
                    if hasattr(rule_obj, "compile_predicate"):
                        pred_obj = rule_obj.compile_predicate()
                        if pred_obj:
                            try:
                                samples, src = self._collect_samples_for_rule(rule_obj, pred_obj.expr, n, cols_to_include)
                                results[rule_result.rule_id] = samples
                                samples_source = src
                            except (ValueError, TypeError, KeyError, OSError) as e:
                                _logger.debug(f"Could not collect samples for rule {rule_result.rule_id}: {e}")
                                results[rule_result.rule_id] = []

            # Distribute results to rule_result objects
            for rule_result, rule_obj, n, cols_to_include in sql_rules:
                samples = results.get(rule_result.rule_id, [])
                rule_result.samples = samples
                rule_result.samples_source = samples_source

                if len(samples) == n and rule_result.failed_count > n:
                    rule_result.samples_truncated = True
                    rule_result.samples_reason = SampleReason.TRUNCATED_LIMIT

        except Exception as e:
            # Batched sampling failed, mark all rules
            for rule_result, rule_obj, n, cols_to_include in sql_rules:
                rule_result.samples = None
                rule_result.samples_reason = f"error: {str(e)[:50]}"

        finally:
            # Close connection if we created it
            if owns_connection and db_conn is not None:
                try:
                    db_conn.close()
                except (OSError, AttributeError):
                    pass  # Connection cleanup is best-effort

    def _can_sample_source(self) -> bool:
        """
        Check if the data source supports sampling.

        File-based sources (Parquet, CSV, S3) can always be sampled.
        Database sources need a live connection.

        Returns:
            True if sampling is possible, False otherwise.
        """
        import polars as pl

        source = self._data_source

        if source is None:
            return False

        # DataFrame - always sampleable
        if isinstance(source, pl.DataFrame):
            return True

        # String path - file based, always sampleable
        if isinstance(source, str):
            return True

        # DatasetHandle - check scheme and connection
        if hasattr(source, "scheme"):
            scheme = getattr(source, "scheme", None)

            # File-based schemes - always sampleable
            if scheme in (None, "file") or (hasattr(source, "uri") and source.uri):
                uri = getattr(source, "uri", "")
                if uri.lower().endswith((".parquet", ".csv")) or uri.startswith("s3://"):
                    return True

            # BYOC or database with connection - check if connection exists
            if hasattr(source, "external_conn") and source.external_conn is not None:
                return True

            # Database without connection - can't sample
            if scheme in ("postgres", "postgresql", "mssql"):
                return False

        return True  # Default to sampleable

    def _resolve_sample_columns(
        self,
        sample_columns: Optional[Union[List[str], str]],
        rule_obj: Any,
    ) -> Optional[List[str]]:
        """
        Resolve sample_columns to a list of column names.

        Args:
            sample_columns: None (all), list of names, or "relevant"
            rule_obj: Rule object for "relevant" mode

        Returns:
            List of column names to include, or None for all columns
        """
        if sample_columns is None:
            return None

        if isinstance(sample_columns, list):
            return sample_columns

        if sample_columns == "relevant":
            # Get columns from rule's required_columns() if available
            cols = set()
            if hasattr(rule_obj, "required_columns"):
                cols.update(rule_obj.required_columns())

            # Also check params for column names (required_columns() may be incomplete)
            if hasattr(rule_obj, "params"):
                params = rule_obj.params
                if "column" in params:
                    cols.add(params["column"])
                if "left" in params:
                    cols.add(params["left"])
                if "right" in params:
                    cols.add(params["right"])
                if "when_column" in params:
                    cols.add(params["when_column"])

            return list(cols) if cols else None

        # Unknown value - return all columns
        return None

    def _collect_samples_for_rule(
        self,
        rule_obj: Any,
        predicate: Any,
        n: int,
        columns: Optional[List[str]] = None,
    ) -> Tuple[List[Dict[str, Any]], str]:
        """
        Collect sample rows for a single rule.

        Uses the existing sampling infrastructure (SQL pushdown, Parquet predicate, etc.)

        Args:
            rule_obj: Rule object
            predicate: Polars expression for filtering
            n: Number of samples to collect
            columns: Columns to include (None = all)

        Returns:
            Tuple of (samples list, source string "sql" or "polars")
        """
        import polars as pl

        source = self._data_source

        if source is None:
            return [], "polars"

        # Reuse existing loading/filtering logic
        df, load_source = self._load_data_for_sampling(rule_obj, n)

        # If data was already filtered by SQL/DuckDB, just apply column projection
        if load_source == "sql":
            result_df = df.head(n)
            return self._apply_column_projection(result_df, columns), "sql"

        # For Polars path, filter with predicate (unique rule handled by helper)
        result_df = _filter_samples_polars(df, rule_obj, predicate, n)

        # For unique rule, always include _duplicate_count in projection
        if _is_unique_rule(rule_obj) and columns is not None:
            columns = list(columns) + ["_duplicate_count"]

        return self._apply_column_projection(result_df, columns), "polars"

    def _apply_column_projection(
        self,
        df: Any,
        columns: Optional[List[str]],
    ) -> List[Dict[str, Any]]:
        """
        Apply column projection to a DataFrame before converting to dicts.

        Always includes row_index if present.

        Args:
            df: Polars DataFrame
            columns: Columns to include (None = all)

        Returns:
            List of row dicts
        """
        if columns is None:
            return df.to_dicts()

        # Always include row_index and _duplicate_count if present
        cols_to_select = set(columns)
        if "row_index" in df.columns:
            cols_to_select.add("row_index")
        if "_duplicate_count" in df.columns:
            cols_to_select.add("_duplicate_count")

        # Only select columns that exist in the DataFrame
        available_cols = set(df.columns)
        cols_to_select = cols_to_select & available_cols

        if not cols_to_select:
            return df.to_dicts()

        return df.select(sorted(cols_to_select)).to_dicts()

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        d = {
            "passed": self.passed,
            "dataset": self.dataset,
            "total_rows": self.total_rows,
            "total_rules": self.total_rules,
            "passed_count": self.passed_count,
            "failed_count": self.failed_count,
            "warning_count": self.warning_count,
            "rules": [r.to_dict() for r in self.rules],
            "stats": self.stats,
        }
        if self.annotations is not None:
            d["annotations"] = self.annotations
        # LLM juice (only include if configured)
        if self.quality_score is not None:
            d["quality_score"] = self.quality_score
        return d

    def to_json(self, indent: Optional[int] = None) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict(), indent=indent, default=str)

    def to_llm(self) -> str:
        """
        Token-optimized format for LLM context.

        Example output:
            VALIDATION: my_contract FAILED (1000 rows)
            BLOCKING: COL:email:not_null (523 nulls), COL:status:allowed_values (12 invalid)
            WARNING: COL:age:range (3 out of bounds)
            PASSED: 15 rules
        """
        from kontra.connectors.handle import mask_credentials

        lines = []

        status = "PASSED" if self.passed else "FAILED"
        rows_str = f" ({self.total_rows:,} rows)" if self.total_rows > 0 else ""
        score_str = f" [score={self.quality_score:.2f}]" if self.quality_score is not None else ""
        # Mask credentials in dataset URI for safe display
        safe_dataset = mask_credentials(self.dataset) if self.dataset else self.dataset
        lines.append(f"VALIDATION: {safe_dataset} {status}{rows_str}{score_str}")

        # Blocking failures
        blocking = self.blocking_failures
        if blocking:
            parts = []
            for r in blocking[:5]:
                # dtype is binary - show message instead of count
                if r.rule_id.endswith(":dtype"):
                    parts.append(f"{r.rule_id} ({r.message})")
                elif r.failed_count > 0:
                    parts.append(f"{r.rule_id} ({r.failed_count:,})")
                else:
                    parts.append(r.rule_id)
            line = "BLOCKING: " + ", ".join(parts)
            if len(blocking) > 5:
                line += f" ... +{len(blocking) - 5} more"
            lines.append(line)

        # Warnings
        warnings = self.warnings
        if warnings:
            parts = []
            for r in warnings[:5]:
                # dtype is binary - show message instead of count
                if r.rule_id.endswith(":dtype"):
                    parts.append(f"{r.rule_id} ({r.message})")
                elif r.failed_count > 0:
                    parts.append(f"{r.rule_id} ({r.failed_count:,})")
                else:
                    parts.append(r.rule_id)
            line = "WARNING: " + ", ".join(parts)
            if len(warnings) > 5:
                line += f" ... +{len(warnings) - 5} more"
            lines.append(line)

        # Info failures
        info = self.info_failures
        if info:
            parts = []
            for r in info[:5]:
                # dtype is binary - show message instead of count
                if r.rule_id.endswith(":dtype"):
                    parts.append(f"{r.rule_id} ({r.message})")
                elif r.failed_count > 0:
                    parts.append(f"{r.rule_id} ({r.failed_count:,})")
                else:
                    parts.append(r.rule_id)
            line = "INFO: " + ", ".join(parts)
            if len(info) > 5:
                line += f" ... +{len(info) - 5} more"
            lines.append(line)

        # Passed summary
        lines.append(f"PASSED: {self.passed_count} rules")

        # Context metadata from rules (if any rules have context)
        rules_with_context = [(r, r.context) for r in self.rules if r.context]
        if rules_with_context:
            ctx_parts = []
            for rule, ctx in rules_with_context[:5]:
                ctx_str = ", ".join(f"{k}={v}" for k, v in ctx.items())
                ctx_parts.append(f"{rule.rule_id}: {ctx_str}")
            lines.append("CONTEXT: " + "; ".join(ctx_parts))
            if len(rules_with_context) > 5:
                lines.append(f"  ... +{len(rules_with_context) - 5} more rules with context")

        # Run-level annotations
        if self.annotations:
            lines.append(f"ANNOTATIONS ({len(self.annotations)}):")
            for ann in self.annotations[:3]:
                ann_type = ann.get("annotation_type", "note")
                actor = ann.get("actor_id", "unknown")
                summary = ann.get("summary", "")[:50]
                if len(ann.get("summary", "")) > 50:
                    summary += "..."
                lines.append(f'  [{ann_type}] by {actor}: "{summary}"')
            if len(self.annotations) > 3:
                lines.append(f"  ... +{len(self.annotations) - 3} more")

        return "\n".join(lines)

    def sample_failures(
        self,
        rule_id: str,
        n: int = 5,
        *,
        upgrade_tier: bool = False,
    ) -> FailureSamples:
        """
        Get a sample of rows that failed a specific rule.

        If eager sampling is enabled (default), this returns cached samples.
        Otherwise, it lazily re-queries the data source.

        Args:
            rule_id: The rule ID to get failures for (e.g., "COL:email:not_null")
            n: Number of sample rows to return (default: 5, max: 100)
            upgrade_tier: If True, re-execute rules resolved via metadata
                tier to get actual samples. Required for preplan rules.

        Returns:
            FailureSamples: Collection of failing rows with "row_index" field.
            Supports to_dict(), to_json(), to_llm() methods.
            Empty if the rule passed (no failures).

        Raises:
            ValueError: If rule_id not found or rule doesn't support row-level samples
            RuntimeError: If data source is unavailable for re-query,
                or if samples unavailable from metadata tier without upgrade_tier=True

        Example:
            result = kontra.validate("data.parquet", contract)
            if not result.passed:
                samples = result.sample_failures("COL:email:not_null", n=5)
                for row in samples:
                    print(f"Row {row['row_index']}: {row}")

            # For metadata-tier rules, use upgrade_tier to get samples:
            samples = result.sample_failures("COL:id:not_null", upgrade_tier=True)
        """
        import polars as pl

        # Cap n at 100
        n = min(n, 100)

        # Find the rule result
        rule_result = None
        for r in self.rules:
            if r.rule_id == rule_id:
                rule_result = r
                break

        if rule_result is None:
            raise ValueError(f"Rule not found: {rule_id}")

        # If rule passed, return empty FailureSamples
        if rule_result.passed:
            return FailureSamples([], rule_id)

        # Check for cached samples first
        if rule_result.samples is not None:
            # Have cached samples - return if n <= cached, else fetch more
            if len(rule_result.samples) >= n:
                return FailureSamples(rule_result.samples[:n], rule_id)
            # Need more samples than cached - fall through to lazy path

        # Handle unavailable samples
        if rule_result.samples_reason == SampleReason.UNAVAILABLE_METADATA:
            if not upgrade_tier:
                raise RuntimeError(
                    f"Samples unavailable for {rule_id}: rule was resolved via metadata tier. "
                    "Use upgrade_tier=True to re-execute and get samples."
                )
            # Fall through to lazy path for tier upgrade

        elif rule_result.samples_reason == SampleReason.UNAVAILABLE_UNSUPPORTED:
            raise ValueError(
                f"Rule '{rule_result.name}' does not support row-level samples. "
                "Dataset-level rules (min_rows, max_rows, freshness, etc.) "
                "cannot identify specific failing rows."
            )

        # Find the rule object to get the failure predicate
        if self._rule_objects is None:
            raise RuntimeError(
                "sample_failures() requires rule objects. "
                "This may happen if ValidationResult was created manually."
            )

        rule_obj = None
        for r in self._rule_objects:
            if getattr(r, "rule_id", None) == rule_id:
                rule_obj = r
                break

        if rule_obj is None:
            raise ValueError(f"Rule object not found for: {rule_id}")

        # Get the failure predicate
        # Check sample_predicate() first (used by rules like unique that have
        # different counting vs sampling semantics), then fall back to compile_predicate()
        predicate = None
        if hasattr(rule_obj, "sample_predicate"):
            pred_obj = rule_obj.sample_predicate()
            if pred_obj is not None:
                predicate = pred_obj.expr
        if predicate is None and hasattr(rule_obj, "compile_predicate"):
            pred_obj = rule_obj.compile_predicate()
            if pred_obj is not None:
                predicate = pred_obj.expr

        if predicate is None:
            raise ValueError(
                f"Rule '{rule_obj.name}' does not support row-level samples. "
                "Dataset-level rules (min_rows, max_rows, freshness, etc.) "
                "cannot identify specific failing rows."
            )

        # Load the data
        if self._data_source is None:
            raise RuntimeError(
                "sample_failures() requires data source reference. "
                "This may happen if ValidationResult was created manually "
                "or the data source is no longer available."
            )

        # Load data based on source type
        # Try SQL pushdown for database sources
        df, load_source = self._load_data_for_sampling(rule_obj, n)

        # For non-database sources (or if SQL filter wasn't available),
        # we need to filter with Polars
        if load_source != "sql":
            # Filter to failing rows, add index, limit (unique rule handled by helper)
            try:
                failing = _filter_samples_polars(df, rule_obj, predicate, n).to_dicts()
            except Exception as e:
                raise RuntimeError(f"Failed to query failing rows: {e}") from e
        else:
            # SQL pushdown already applied filter and added row index
            failing = df.head(n).to_dicts()

        return FailureSamples(failing, rule_id)

    def _load_data_for_sampling(
        self, rule: Any = None, n: int = 5
    ) -> Tuple["pl.DataFrame", str]:
        """
        Load data from the stored data source for sample_failures().

        For database sources with rules that support SQL filters,
        pushes the filter to SQL for performance.

        Returns:
            Tuple of (DataFrame, source) where source is "sql" or "polars"
        """
        import polars as pl

        source = self._data_source

        if source is None:
            raise RuntimeError("No data source available")

        # String path/URI
        if isinstance(source, str):
            # Try to load as file with predicate pushdown for Parquet
            if source.lower().endswith(".parquet") or source.startswith("s3://"):
                return self._load_parquet_with_filter(source, rule, n)
            elif source.lower().endswith(".csv"):
                return pl.read_csv(source), "polars"
            else:
                # Try parquet first, then CSV
                try:
                    return self._load_parquet_with_filter(source, rule, n)
                except (OSError, IOError, ValueError) as parquet_err:
                    try:
                        return pl.read_csv(source), "polars"
                    except (OSError, IOError, ValueError) as csv_err:
                        raise RuntimeError(f"Cannot load data from: {source} (parquet: {parquet_err}, csv: {csv_err})")

        # Polars DataFrame (was passed directly)
        if isinstance(source, pl.DataFrame):
            return source, "polars"

        # DatasetHandle (BYOC or parsed URI)
        if hasattr(source, "scheme") and hasattr(source, "uri"):
            # It's a DatasetHandle
            handle = source

            # Check for BYOC (external connection)
            if handle.scheme == "byoc" or (handle.external_conn is not None):
                conn = handle.external_conn
                if conn is None:
                    raise RuntimeError(
                        "Database connection is closed. "
                        "For BYOC, keep the connection open until done with sample_failures()."
                    )
                table = getattr(handle, "table_ref", None) or handle.path
                dialect = handle.dialect or "postgres"
                return self._query_db_with_filter(conn, table, rule, n, dialect), "sql"

            elif handle.scheme in ("postgres", "postgresql"):
                # PostgreSQL via URI
                conn = getattr(handle, "external_conn", None)
                owns_conn = False
                if conn is None and handle.db_params is not None:
                    # Create connection from stored params
                    try:
                        from kontra.connectors.postgres import get_connection
                        conn = get_connection(handle.db_params)
                        owns_conn = True
                    except Exception as e:
                        raise RuntimeError(
                            f"Failed to connect to PostgreSQL for sampling: {e}"
                        ) from e
                if conn is None:
                    raise RuntimeError(
                        "Database connection is not available. "
                        "For URI-based connections, sample_failures() requires re-connection."
                    )
                table = getattr(handle, "table_ref", None) or handle.path
                try:
                    return self._query_db_with_filter(conn, table, rule, n, "postgres"), "sql"
                finally:
                    if owns_conn:
                        conn.close()

            elif handle.scheme == "mssql":
                # SQL Server
                conn = getattr(handle, "external_conn", None)
                owns_conn = False
                if conn is None and handle.db_params is not None:
                    # Create connection from stored params
                    try:
                        from kontra.connectors.sqlserver import get_connection
                        conn = get_connection(handle.db_params)
                        owns_conn = True
                    except Exception as e:
                        raise RuntimeError(
                            f"Failed to connect to SQL Server for sampling: {e}"
                        ) from e
                if conn is None:
                    raise RuntimeError(
                        "Database connection is not available."
                    )
                table = getattr(handle, "table_ref", None) or handle.path
                try:
                    return self._query_db_with_filter(conn, table, rule, n, "mssql"), "sql"
                finally:
                    if owns_conn:
                        conn.close()

            elif handle.scheme in ("file", None) or (handle.uri and not handle.scheme):
                # File-based
                uri = handle.uri
                if uri.lower().endswith(".parquet"):
                    return self._load_parquet_with_filter(uri, rule, n)
                elif uri.lower().endswith(".csv"):
                    return pl.read_csv(uri), "polars"
                else:
                    return self._load_parquet_with_filter(uri, rule, n)

        raise RuntimeError(f"Unsupported data source type: {type(source)}")

    def _query_db_with_filter(
        self,
        conn: Any,
        table: str,
        rule: Any,
        n: int,
        dialect: str,
    ) -> "pl.DataFrame":
        """
        Query database with SQL filter if rule supports it.

        Uses the rule's to_sql_filter() method to push the filter to SQL,
        avoiding loading the entire table.
        """
        import polars as pl

        sql_filter = None

        # Special case: unique rule needs subquery with table name
        if _is_unique_rule(rule):
            column = rule.params.get("column")
            if column:
                query = _build_unique_sample_query_sql(table, column, n, dialect)
                return pl.read_database(query, conn)

        if rule is not None and hasattr(rule, "to_sql_filter"):
            sql_filter = rule.to_sql_filter(dialect)

        if sql_filter:
            # Build query with filter and row number
            # ROW_NUMBER() gives us the original row index
            if dialect == "mssql":
                # SQL Server syntax
                query = f"""
                    SELECT *, ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1 AS row_index
                    FROM {table}
                    WHERE {sql_filter}
                    ORDER BY (SELECT NULL)
                    OFFSET 0 ROWS FETCH FIRST {n} ROWS ONLY
                """
            else:
                # PostgreSQL / DuckDB syntax
                query = f"""
                    SELECT *, ROW_NUMBER() OVER () - 1 AS row_index
                    FROM {table}
                    WHERE {sql_filter}
                    LIMIT {n}
                """
            return pl.read_database(query, conn)
        else:
            # Fall back to loading all data (rule doesn't support SQL filter)
            return pl.read_database(f"SELECT * FROM {table}", conn)

    def _load_parquet_with_filter(
        self,
        path: str,
        rule: Any,
        n: int,
    ) -> Tuple["pl.DataFrame", str]:
        """
        Load Parquet file with predicate pushdown for performance.

        - S3/remote files: Uses DuckDB SQL pushdown (doesn't download whole file)
        - Local files: Uses Polars scan_parquet (efficient for local)

        Returns:
            Tuple of (DataFrame, source) where source is "sql" or "polars"
        """
        import polars as pl

        # Check if this is a remote file (S3, HTTP)
        is_remote = path.startswith("s3://") or path.startswith("http://") or path.startswith("https://")

        # For remote files, use DuckDB SQL pushdown (much faster - doesn't download whole file)
        if is_remote and rule is not None and hasattr(rule, "to_sql_filter"):
            sql_filter = rule.to_sql_filter("duckdb")
            if sql_filter:
                try:
                    return self._query_parquet_with_duckdb(path, sql_filter, n), "sql"
                except (ImportError, OSError, ValueError) as e:
                    _logger.debug(f"DuckDB query failed, falling back to Polars: {e}")
                    pass  # Fall through to Polars

        # For local files, just return the raw data - caller will filter
        # (Don't filter here to avoid double-filtering in _collect_samples_for_rule)
        return pl.read_parquet(path), "polars"

    def _query_parquet_with_duckdb(
        self,
        path: str,
        sql_filter: str,
        n: int,
        columns: Optional[List[str]] = None,
    ) -> "pl.DataFrame":
        """
        Query Parquet file using DuckDB with SQL filter.

        Much faster than Polars for S3 files because DuckDB pushes
        the filter and LIMIT to the row group level.
        """
        import duckdb
        import polars as pl

        con = duckdb.connect()

        # Configure S3 if needed
        if path.startswith("s3://"):
            import os
            con.execute("INSTALL httpfs; LOAD httpfs;")
            if os.environ.get("AWS_ACCESS_KEY_ID"):
                con.execute(f"SET s3_access_key_id='{os.environ['AWS_ACCESS_KEY_ID']}';")
            if os.environ.get("AWS_SECRET_ACCESS_KEY"):
                con.execute(f"SET s3_secret_access_key='{os.environ['AWS_SECRET_ACCESS_KEY']}';")
            if os.environ.get("AWS_ENDPOINT_URL"):
                endpoint = os.environ["AWS_ENDPOINT_URL"].replace("http://", "").replace("https://", "")
                con.execute(f"SET s3_endpoint='{endpoint}';")
                con.execute("SET s3_use_ssl=false;")
                con.execute("SET s3_url_style='path';")
            if os.environ.get("AWS_REGION"):
                con.execute(f"SET s3_region='{os.environ['AWS_REGION']}';")

        # Escape path for SQL
        escaped_path = path.replace("'", "''")

        # Build column list (with projection if specified)
        if columns:
            col_list = ", ".join(f'"{c}"' for c in columns)
        else:
            col_list = "*"

        # Build query with filter and row number
        query = f"""
            SELECT {col_list}, ROW_NUMBER() OVER () - 1 AS row_index
            FROM read_parquet('{escaped_path}')
            WHERE {sql_filter}
            LIMIT {n}
        """

        result = con.execute(query).pl()
        con.close()
        return result

    def _batch_sample_parquet_duckdb(
        self,
        path: str,
        rules_to_sample: List[Tuple[str, str, int, Optional[List[str]]]],
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Batch sample multiple rules from Parquet using a single DuckDB query.

        Args:
            path: Parquet file path or S3 URI
            rules_to_sample: List of (rule_id, sql_filter, limit, columns)

        Returns:
            Dict mapping rule_id to list of sample dicts
        """
        import duckdb
        import polars as pl

        if not rules_to_sample:
            return {}

        con = duckdb.connect()

        # Configure S3 if needed
        if path.startswith("s3://"):
            import os
            con.execute("INSTALL httpfs; LOAD httpfs;")
            if os.environ.get("AWS_ACCESS_KEY_ID"):
                con.execute(f"SET s3_access_key_id='{os.environ['AWS_ACCESS_KEY_ID']}';")
            if os.environ.get("AWS_SECRET_ACCESS_KEY"):
                con.execute(f"SET s3_secret_access_key='{os.environ['AWS_SECRET_ACCESS_KEY']}';")
            if os.environ.get("AWS_ENDPOINT_URL"):
                endpoint = os.environ["AWS_ENDPOINT_URL"].replace("http://", "").replace("https://", "")
                con.execute(f"SET s3_endpoint='{endpoint}';")
                con.execute("SET s3_use_ssl=false;")
                con.execute("SET s3_url_style='path';")
            if os.environ.get("AWS_REGION"):
                con.execute(f"SET s3_region='{os.environ['AWS_REGION']}';")

        escaped_path = path.replace("'", "''")

        # Collect all columns needed across all rules
        all_columns: set = set()
        for rule_id, sql_filter, limit, columns in rules_to_sample:
            if columns:
                all_columns.update(columns)

        # If any rule needs all columns, use *
        needs_all = any(cols is None for _, _, _, cols in rules_to_sample)

        if needs_all or not all_columns:
            col_list = "*"
        else:
            col_list = ", ".join(f'"{c}"' for c in sorted(all_columns))

        # Build UNION ALL query - one subquery per rule (wrapped in parens for DuckDB)
        subqueries = []
        for rule_id, sql_filter, limit, columns in rules_to_sample:
            escaped_rule_id = rule_id.replace("'", "''")
            subquery = f"""(
                SELECT '{escaped_rule_id}' AS _rule_id, {col_list}, ROW_NUMBER() OVER () - 1 AS row_index
                FROM read_parquet('{escaped_path}')
                WHERE {sql_filter}
                LIMIT {limit}
            )"""
            subqueries.append(subquery)

        query = " UNION ALL ".join(subqueries)

        try:
            result_df = con.execute(query).pl()
        finally:
            con.close()

        # Distribute results to each rule
        results: Dict[str, List[Dict[str, Any]]] = {rule_id: [] for rule_id, _, _, _ in rules_to_sample}

        for row in result_df.to_dicts():
            rule_id = row.pop("_rule_id")
            if rule_id in results:
                results[rule_id].append(row)

        return results

    def _batch_sample_db(
        self,
        conn: Any,
        table: str,
        rules_to_sample: List[Tuple[str, str, int, Optional[List[str]]]],
        dialect: str,
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Batch sample multiple rules from database using a single query.

        Args:
            conn: Database connection
            table: Table name (with schema if needed)
            rules_to_sample: List of (rule_id, sql_filter, limit, columns)
            dialect: "postgres" or "mssql"

        Returns:
            Dict mapping rule_id to list of sample dicts
        """
        import polars as pl

        if not rules_to_sample:
            return {}

        # Collect all columns needed across all rules
        all_columns: set = set()
        for rule_id, sql_filter, limit, columns in rules_to_sample:
            if columns:
                all_columns.update(columns)

        needs_all = any(cols is None for _, _, _, cols in rules_to_sample)

        if dialect == "mssql":
            # SQL Server syntax
            if needs_all or not all_columns:
                col_list = "*"
            else:
                col_list = ", ".join(f"[{c}]" for c in sorted(all_columns))

            subqueries = []
            for rule_id, sql_filter, limit, columns in rules_to_sample:
                escaped_rule_id = rule_id.replace("'", "''")
                subquery = f"""(
                    SELECT TOP {limit} '{escaped_rule_id}' AS _rule_id, {col_list},
                           ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1 AS row_index
                    FROM {table}
                    WHERE {sql_filter}
                )"""
                subqueries.append(subquery)
        else:
            # PostgreSQL syntax
            if needs_all or not all_columns:
                col_list = "*"
            else:
                col_list = ", ".join(f'"{c}"' for c in sorted(all_columns))

            subqueries = []
            for rule_id, sql_filter, limit, columns in rules_to_sample:
                escaped_rule_id = rule_id.replace("'", "''")
                subquery = f"""(
                    SELECT '{escaped_rule_id}' AS _rule_id, {col_list},
                           ROW_NUMBER() OVER () - 1 AS row_index
                    FROM {table}
                    WHERE {sql_filter}
                    LIMIT {limit}
                )"""
                subqueries.append(subquery)

        query = " UNION ALL ".join(subqueries)

        result_df = pl.read_database(query, conn)

        # Distribute results to each rule
        results: Dict[str, List[Dict[str, Any]]] = {rule_id: [] for rule_id, _, _, _ in rules_to_sample}

        for row in result_df.to_dicts():
            rule_id = row.pop("_rule_id")
            if rule_id in results:
                results[rule_id].append(row)

        return results


@dataclass
class DryRunResult:
    """
    Result of a dry run (contract validation without execution).

    Properties:
        valid: Whether the contract is syntactically valid
        rules_count: Number of rules that would run
        columns_needed: Columns the contract requires
        contract_name: Name of the contract (if any)
        errors: List of errors found during validation
        datasource: Datasource from contract
    """

    valid: bool
    rules_count: int
    columns_needed: List[str]
    contract_name: Optional[str] = None
    datasource: Optional[str] = None
    errors: List[str] = field(default_factory=list)

    def __repr__(self) -> str:
        status = "VALID" if self.valid else "INVALID"
        parts = [f"DryRunResult({self.contract_name or 'inline'}) {status}"]
        if self.valid:
            parts.append(f"  Rules: {self.rules_count}, Columns: {len(self.columns_needed)}")
            if self.columns_needed:
                cols = ", ".join(self.columns_needed[:5])
                if len(self.columns_needed) > 5:
                    cols += f" ... +{len(self.columns_needed) - 5} more"
                parts.append(f"  Needs: {cols}")
        else:
            for err in self.errors[:3]:
                parts.append(f"  ERROR: {err}")
            if len(self.errors) > 3:
                parts.append(f"  ... +{len(self.errors) - 3} more errors")
        return "\n".join(parts)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "valid": self.valid,
            "rules_count": self.rules_count,
            "columns_needed": self.columns_needed,
            "contract_name": self.contract_name,
            "datasource": self.datasource,
            "errors": self.errors,
        }

    def to_json(self, indent: Optional[int] = None) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict(), indent=indent, default=str)

    def to_llm(self) -> str:
        """Token-optimized format for LLM context."""
        if self.valid:
            cols = ",".join(self.columns_needed[:10])
            if len(self.columns_needed) > 10:
                cols += f"...+{len(self.columns_needed) - 10}"
            return f"DRYRUN: {self.contract_name or 'inline'} VALID rules={self.rules_count} cols=[{cols}]"
        else:
            errs = "; ".join(self.errors[:3])
            return f"DRYRUN: {self.contract_name or 'inline'} INVALID errors=[{errs}]"


@dataclass
class Diff:
    """
    Diff between two validation runs.

    Properties:
        has_changes: Whether there are any changes
        improved: Fewer failures than before
        regressed: More failures than before
        before: Summary of before run
        after: Summary of after run
        new_failures: Rules that started failing
        resolved: Rules that stopped failing
        count_changes: Rules where failure count changed
    """

    has_changes: bool
    improved: bool
    regressed: bool
    before: Dict[str, Any]
    after: Dict[str, Any]
    new_failures: List[Dict[str, Any]]
    resolved: List[Dict[str, Any]]
    regressions: List[Dict[str, Any]]
    improvements: List[Dict[str, Any]]
    _state_diff: Optional[Any] = field(default=None, repr=False)

    def __repr__(self) -> str:
        if self.regressed:
            status = "REGRESSED"
        elif self.improved:
            status = "IMPROVED"
        else:
            status = "NO CHANGE"

        contract = self.after.get("contract_name", "unknown")
        before_date = self.before.get("run_at", "")[:10]
        after_date = self.after.get("run_at", "")[:10]

        parts = [f"Diff({contract}) {status}"]
        parts.append(f"  {before_date} -> {after_date}")
        if self.new_failures:
            parts.append(f"  New failures: {len(self.new_failures)}")
        if self.resolved:
            parts.append(f"  Resolved: {len(self.resolved)}")
        return "\n".join(parts)

    @property
    def count_changes(self) -> List[Dict[str, Any]]:
        """Rules where failure count changed (both regressions and improvements)."""
        return self.regressions + self.improvements

    @classmethod
    def from_state_diff(cls, state_diff: "StateDiff") -> "Diff":
        """Create from internal StateDiff object."""
        return cls(
            has_changes=state_diff.has_regressions or state_diff.has_improvements,
            improved=state_diff.has_improvements and not state_diff.has_regressions,
            regressed=state_diff.has_regressions,
            before={
                "run_at": state_diff.before.run_at.isoformat(),
                "passed": state_diff.before.summary.passed,
                "total_rules": state_diff.before.summary.total_rules,
                "failed_count": state_diff.before.summary.failed_rules,
                "contract_name": state_diff.before.contract_name,
            },
            after={
                "run_at": state_diff.after.run_at.isoformat(),
                "passed": state_diff.after.summary.passed,
                "total_rules": state_diff.after.summary.total_rules,
                "failed_count": state_diff.after.summary.failed_rules,
                "contract_name": state_diff.after.contract_name,
            },
            new_failures=[rd.to_dict() for rd in state_diff.new_failures],
            resolved=[rd.to_dict() for rd in state_diff.resolved],
            regressions=[rd.to_dict() for rd in state_diff.regressions],
            improvements=[rd.to_dict() for rd in state_diff.improvements],
            _state_diff=state_diff,
        )

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "has_changes": self.has_changes,
            "improved": self.improved,
            "regressed": self.regressed,
            "before": self.before,
            "after": self.after,
            "new_failures": self.new_failures,
            "resolved": self.resolved,
            "regressions": self.regressions,
            "improvements": self.improvements,
        }

    def to_json(self, indent: Optional[int] = None) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict(), indent=indent, default=str)

    def to_llm(self) -> str:
        """Token-optimized format for LLM context."""
        if self._state_diff is not None:
            return self._state_diff.to_llm()

        # Fallback if no state_diff
        lines = []
        contract = self.after.get("contract_name", "unknown")

        if self.regressed:
            status = "REGRESSION"
        elif self.improved:
            status = "IMPROVED"
        else:
            status = "NO_CHANGE"

        lines.append(f"DIFF: {contract} {status}")
        lines.append(f"{self.before.get('run_at', '')[:10]} -> {self.after.get('run_at', '')[:10]}")

        if self.new_failures:
            lines.append(f"NEW_FAILURES: {len(self.new_failures)}")
            for nf in self.new_failures[:3]:
                lines.append(f"  - {nf.get('rule_id', '')}")

        if self.resolved:
            lines.append(f"RESOLVED: {len(self.resolved)}")

        return "\n".join(lines)


@dataclass
class SuggestedRule:
    """A suggested validation rule from profile analysis."""

    name: str
    params: Dict[str, Any]
    confidence: float
    reason: str

    def __repr__(self) -> str:
        return f"SuggestedRule({self.name}, confidence={self.confidence:.2f})"

    def to_dict(self) -> Dict[str, Any]:
        """Convert to rule dict format (for inline rules)."""
        return {
            "name": self.name,
            "params": self.params,
        }

    def to_full_dict(self) -> Dict[str, Any]:
        """Convert to dict including metadata."""
        return {
            "name": self.name,
            "params": self.params,
            "confidence": self.confidence,
            "reason": self.reason,
        }


class Suggestions:
    """
    Collection of suggested validation rules from profile analysis.

    Methods:
        to_yaml(): Export as YAML contract
        to_json(): Export as JSON
        to_dict(): Export as list of rule dicts (for inline rules)
        save(path): Save to file
        filter(min_confidence=None, name=None): Filter suggestions
    """

    def __init__(
        self,
        rules: List[SuggestedRule],
        source: str = "unknown",
    ):
        self._rules = rules
        self.source = source

    def __repr__(self) -> str:
        return f"Suggestions({len(self._rules)} rules from {self.source})"

    def __len__(self) -> int:
        return len(self._rules)

    def __iter__(self) -> Iterator[SuggestedRule]:
        return iter(self._rules)

    def __getitem__(self, index: int) -> SuggestedRule:
        return self._rules[index]

    def filter(
        self,
        min_confidence: Optional[float] = None,
        name: Optional[str] = None,
    ) -> "Suggestions":
        """
        Filter suggestions by criteria.

        Args:
            min_confidence: Minimum confidence score (0.0-1.0)
            name: Filter by rule name

        Returns:
            New Suggestions with filtered rules
        """
        filtered = self._rules

        if min_confidence is not None:
            filtered = [r for r in filtered if r.confidence >= min_confidence]

        if name is not None:
            filtered = [r for r in filtered if r.name == name]

        return Suggestions(filtered, self.source)

    def to_dict(self) -> List[Dict[str, Any]]:
        """Convert to list of rule dicts (usable with kontra.validate(rules=...))."""
        return [r.to_dict() for r in self._rules]

    def to_json(self, indent: Optional[int] = None) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict(), indent=indent, default=str)

    def to_llm(self) -> str:
        """
        Token-optimized format for LLM context.

        Example output:
            SUGGESTIONS: my_data.parquet (5 rules)
            HIGH (0.9+): not_null(id), unique(id)
            MEDIUM (0.7-0.9): range(amount), allowed_values(status)
            LOW (<0.7): regex(email)
        """
        lines = []

        lines.append(f"SUGGESTIONS: {self.source} ({len(self._rules)} rules)")

        # Group by confidence level
        high = [r for r in self._rules if r.confidence >= 0.9]
        medium = [r for r in self._rules if 0.7 <= r.confidence < 0.9]
        low = [r for r in self._rules if r.confidence < 0.7]

        if high:
            parts = [f"{r.name}({r.params.get('column', '')})" for r in high[:5]]
            line = "HIGH (0.9+): " + ", ".join(parts)
            if len(high) > 5:
                line += f" +{len(high) - 5} more"
            lines.append(line)

        if medium:
            parts = [f"{r.name}({r.params.get('column', '')})" for r in medium[:5]]
            line = "MEDIUM (0.7-0.9): " + ", ".join(parts)
            if len(medium) > 5:
                line += f" +{len(medium) - 5} more"
            lines.append(line)

        if low:
            parts = [f"{r.name}({r.params.get('column', '')})" for r in low[:3]]
            line = "LOW (<0.7): " + ", ".join(parts)
            if len(low) > 3:
                line += f" +{len(low) - 3} more"
            lines.append(line)

        return "\n".join(lines)

    def to_yaml(self, contract_name: str = "suggested_contract") -> str:
        """
        Convert to YAML contract format.

        Args:
            contract_name: Name for the contract

        Returns:
            YAML string
        """
        contract = {
            "name": contract_name,
            "dataset": self.source,
            "rules": self.to_dict(),
        }
        return yaml.dump(contract, default_flow_style=False, sort_keys=False)

    def save(self, path: Union[str, Path]) -> None:
        """
        Save suggestions to file.

        Args:
            path: Output path (YAML format)
        """
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(self.to_yaml(contract_name=path.stem))

    @classmethod
    def from_profile(
        cls,
        profile: "DatasetProfile",
        min_confidence: float = 0.5,
    ) -> "Suggestions":
        """
        Generate rule suggestions from a profile.

        This is a basic implementation. More sophisticated analysis
        could be added based on profile depth/preset.
        """
        rules: List[SuggestedRule] = []

        for col in profile.columns:
            # not_null suggestion
            if col.null_rate == 0:
                rules.append(SuggestedRule(
                    name="not_null",
                    params={"column": col.name},
                    confidence=1.0,
                    reason=f"Column {col.name} has no nulls",
                ))
            elif col.null_rate < 0.01:  # < 1% nulls
                rules.append(SuggestedRule(
                    name="not_null",
                    params={"column": col.name},
                    confidence=0.8,
                    reason=f"Column {col.name} has very few nulls ({col.null_rate:.1%})",
                ))

            # unique suggestion
            if col.uniqueness_ratio == 1.0 and col.distinct_count > 1:
                rules.append(SuggestedRule(
                    name="unique",
                    params={"column": col.name},
                    confidence=1.0,
                    reason=f"Column {col.name} has all unique values",
                ))
            elif col.uniqueness_ratio > 0.99:
                rules.append(SuggestedRule(
                    name="unique",
                    params={"column": col.name},
                    confidence=0.7,
                    reason=f"Column {col.name} is nearly unique ({col.uniqueness_ratio:.1%})",
                ))

            # dtype suggestion
            rules.append(SuggestedRule(
                name="dtype",
                params={"column": col.name, "type": col.dtype},
                confidence=1.0,
                reason=f"Column {col.name} is {col.dtype}",
            ))

            # allowed_values for low cardinality
            if col.is_low_cardinality and col.values:
                rules.append(SuggestedRule(
                    name="allowed_values",
                    params={"column": col.name, "values": col.values},
                    confidence=0.9,
                    reason=f"Column {col.name} has {len(col.values)} distinct values",
                ))

            # range for numeric
            if col.numeric and col.numeric.min is not None and col.numeric.max is not None:
                rules.append(SuggestedRule(
                    name="range",
                    params={
                        "column": col.name,
                        "min": col.numeric.min,
                        "max": col.numeric.max,
                    },
                    confidence=0.7,
                    reason=f"Column {col.name} ranges from {col.numeric.min} to {col.numeric.max}",
                ))

        # min_rows suggestion
        if profile.row_count > 0:
            # Suggest minimum as 80% of current count (or 1 if small dataset)
            min_rows = max(1, int(profile.row_count * 0.8))
            rules.append(SuggestedRule(
                name="min_rows",
                params={"threshold": min_rows},
                confidence=0.6,
                reason=f"Dataset has {profile.row_count:,} rows",
            ))

        # Filter by confidence
        filtered = [r for r in rules if r.confidence >= min_confidence]

        return cls(filtered, source=profile.source_uri)
