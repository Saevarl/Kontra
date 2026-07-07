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
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Union, TYPE_CHECKING


from kontra.api.sampling import (
    SamplingContext,
    SamplingOrchestrator,
    SampleReason,
)

if TYPE_CHECKING:
    import polars as pl

_logger = logging.getLogger(__name__)


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


# SampleReason is imported from sampling.py


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

        # Extract rule name from various sources
        name = d.get("rule_name", d.get("name", ""))
        if not name and ":" in rule_id:
            # Standard format: COL:col:name or DATASET:name
            name = rule_id.split(":")[-1]
        if not name:
            # Custom id with no colons — use rule_id as name fallback
            name = rule_id

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
        # Always include violation_rate for consistent schema (BUG-012)
        d["violation_rate"] = self.violation_rate if self.violation_rate is not None else 0.0
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

    def to_json(self, indent: Optional[int] = None) -> str:
        """Convert to JSON string."""
        import json
        return json.dumps(self.to_dict(), indent=indent, default=str)


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
    # For sample_failures() - lazy evaluation (kept for backwards compatibility)
    _data_source: Optional[Any] = field(default=None, repr=False)
    _rule_objects: Optional[List[Any]] = field(default=None, repr=False)
    # Loaded data (if Polars execution occurred)
    _data: Optional[Any] = field(default=None, repr=False)
    # Sampling orchestrator (lazy-initialized)
    _sampler: Optional[SamplingOrchestrator] = field(default=None, repr=False)

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
        tally: bool = False,
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
            tally: Whether tally mode was used (affects sample cap)
        """
        # In fail-fast mode (tally=False), cap samples at 1 per rule
        if not tally and sample > 1:
            sample = 1
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

        total_rows = summary.get("total_rows", 0)

        # Populate _total_rows on each rule for violation_rate property
        for rule_result in rules:
            rule_result._total_rows = total_rows

        # Create sampling context and orchestrator
        sampler = None
        if rule_objects is not None:
            ctx = SamplingContext(
                data_source=data_source,
                rule_objects=rule_objects,
                cached_data=data,
            )
            sampler = SamplingOrchestrator(ctx)

        # Create instance
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
            _sampler=sampler,
        )

        # Perform eager sampling if enabled
        if sample > 0 and sampler is not None:
            sampler.perform_eager_sampling(rules, sample, sample_budget, sample_columns)

        return instance

    def _get_sampler(self) -> SamplingOrchestrator:
        """
        Get or create the sampling orchestrator.

        Returns:
            SamplingOrchestrator instance

        Raises:
            RuntimeError: If rule objects are not available
        """
        if self._sampler is not None:
            return self._sampler

        if self._rule_objects is None:
            raise RuntimeError(
                "sample_failures() requires rule objects. "
                "This may happen if ValidationResult was created manually."
            )

        ctx = SamplingContext(
            data_source=self._data_source,
            rule_objects=self._rule_objects,
            cached_data=self._data,
        )
        self._sampler = SamplingOrchestrator(ctx)
        return self._sampler

    # Sampling methods moved to kontra.api.sampling.SamplingOrchestrator

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

        # Check if data source is available
        if self._data_source is None:
            raise RuntimeError(
                "sample_failures() requires data source reference. "
                "This may happen if ValidationResult was created manually "
                "or the data source is no longer available."
            )

        # Delegate to the sampling orchestrator
        sampler = self._get_sampler()
        failing = sampler.sample_failures_for_rule(rule_id, rule_result, n, upgrade_tier)

        return FailureSamples(failing, rule_id)


# The remaining sampling methods have been moved to kontra.api.sampling.SamplingOrchestrator


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
        errs = "; ".join(self.errors[:3])
        return f"DRYRUN: {self.contract_name or 'inline'} INVALID errors=[{errs}]"


@dataclass
class RuleExplainEntry:
    """Tier assignment for a single rule in the execution plan."""
    rule_id: str
    name: str
    tier: str  # "metadata", "sql", "polars"
    reason: str
    column: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        d: Dict[str, Any] = {
            "rule_id": self.rule_id,
            "name": self.name,
            "tier": self.tier,
            "reason": self.reason,
        }
        if self.column:
            d["column"] = self.column
        return d


@dataclass
class ExplainResult:
    """
    Result of an execution plan preview (explain mode).

    Shows which tier each rule will execute on without running validation.

    Properties:
        contract_name: Name of the contract
        total_rules: Number of rules
        rules: List of RuleExplainEntry with tier assignments
        summary: Counts by tier (metadata, sql, polars)
    """
    contract_name: str
    total_rules: int
    rules: List[RuleExplainEntry]
    summary: Dict[str, int]

    def __repr__(self) -> str:
        parts = [f"ExplainResult({self.contract_name}) {self.total_rules} rules"]
        for tier, count in sorted(self.summary.items()):
            parts.append(f"  {tier}: {count}")
        return "\n".join(parts)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "contract_name": self.contract_name,
            "total_rules": self.total_rules,
            "rules": [r.to_dict() for r in self.rules],
            "summary": self.summary,
        }

    def to_json(self, indent: Optional[int] = None) -> str:
        return json.dumps(self.to_dict(), indent=indent, default=str)

    def render(self) -> str:
        """Render a human-readable table for CLI output."""
        lines = [f"\nExecution Plan: {self.contract_name} ({self.total_rules} rules)\n"]
        for entry in self.rules:
            lines.append(f"  {entry.rule_id:<35s} {entry.tier:<10s} {entry.reason}")
        lines.append("")
        summary_parts = [f"{count} {tier}" for tier, count in sorted(self.summary.items()) if count > 0]
        lines.append(f"Summary: {', '.join(summary_parts)}")
        return "\n".join(lines)

    def to_llm(self) -> str:
        """Token-optimized format for LLM context."""
        lines = [f"EXPLAIN: {self.contract_name} ({self.total_rules} rules)"]
        for entry in self.rules:
            lines.append(f"  {entry.rule_id}: {entry.tier} ({entry.reason})")
        summary_parts = [f"{count} {tier}" for tier, count in sorted(self.summary.items()) if count > 0]
        lines.append(f"TIERS: {', '.join(summary_parts)}")
        return "\n".join(lines)


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
        warnings: List of warnings about metadata differences (e.g., tally mode change)
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
    warnings: List[str] = field(default_factory=list)
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
    def empty(cls, reason: str = "No history available") -> "Diff":
        """Create an empty Diff for cases with no history.

        This prevents callers from needing None guards when accessing
        properties like .regressed or .has_changes.
        """
        return cls(
            has_changes=False,
            improved=False,
            regressed=False,
            before={"run_at": "", "passed": None, "total_rules": 0, "failed_count": 0, "contract_name": ""},
            after={"run_at": "", "passed": None, "total_rules": 0, "failed_count": 0, "contract_name": ""},
            new_failures=[],
            resolved=[],
            regressions=[],
            improvements=[],
            _state_diff=None,
        )

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
            warnings=list(state_diff.warnings),
            _state_diff=state_diff,
        )

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        d: Dict[str, Any] = {
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
        if self.warnings:
            d["warnings"] = self.warnings
        return d

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

        # Warnings about metadata differences
        if self.warnings:
            for warning in self.warnings:
                lines.append(f"WARNING: {warning}")

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
        profile: Optional["DatasetProfile"] = None,
    ):
        self._rules = rules
        self.source = source
        self._profile = profile  # Used for nice YAML generation

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

        return Suggestions(filtered, self.source, self._profile)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary with metadata and rules list."""
        return {
            "source": self.source,
            "count": len(self._rules),
            "rules": [r.to_dict() for r in self._rules],
        }

    def to_rules_list(self) -> List[Dict[str, Any]]:
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
            YAML string with comments and nice formatting
        """
        # Use shared generator for nice output with comments
        if self._profile is not None:
            from kontra.scout.suggest import generate_rules_yaml
            return generate_rules_yaml(self._profile)

        # Fallback for Suggestions created without profile
        import yaml

        contract = {
            "name": contract_name,
            "datasource": self.source,
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

        return cls(filtered, source=profile.source_uri, profile=profile)


