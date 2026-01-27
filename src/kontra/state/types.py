# src/kontra/state/types.py
"""
State data types for validation result persistence.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional

from kontra.version import VERSION


# -----------------------------------------------------------------------------
# Annotation Types (v0.5)
# -----------------------------------------------------------------------------


@dataclass
class Annotation:
    """
    Annotation on a validation run or specific rule result.

    Annotations provide "memory without authority" - agents and humans
    can record context about runs (resolutions, root causes, acknowledgments)
    without affecting Kontra's behavior.

    Invariants:
    - Append-only: annotations are never updated or deleted
    - Uninterpreted: Kontra stores annotation_type but doesn't define vocabulary
    - Opt-in reads: annotations never appear unless explicitly requested
    - Never read during validation or diff

    Common annotation_type values (suggested, not enforced):
    - "resolution": I fixed this
    - "root_cause": This failed because...
    - "false_positive": This isn't actually a problem
    - "acknowledged": I saw this, will address later
    - "suppressed": Intentionally ignoring this
    - "note": General comment
    """

    # Identity
    id: Optional[int] = None  # Database-assigned ID (None for new annotations)
    run_id: int = 0  # Reference to kontra_runs.id
    rule_result_id: Optional[int] = None  # Reference to kontra_rule_results.id (None for run-level)
    rule_id: Optional[str] = None  # Semantic rule ID (e.g., "COL:email:not_null") for cross-run queries

    # Who created it
    actor_type: str = "agent"  # "agent" | "human" | "system"
    actor_id: str = ""  # e.g., "repair-agent-v2", "alice@example.com"

    # What it says
    annotation_type: str = ""  # Uninterpreted by Kontra
    summary: str = ""  # Human-readable summary
    payload: Optional[Dict[str, Any]] = None  # Arbitrary structured data

    # When
    created_at: Optional[datetime] = None

    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now(timezone.utc)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        d: Dict[str, Any] = {
            "run_id": self.run_id,
            "actor_type": self.actor_type,
            "actor_id": self.actor_id,
            "annotation_type": self.annotation_type,
            "summary": self.summary,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }
        if self.id is not None:
            d["id"] = self.id
        if self.rule_result_id is not None:
            d["rule_result_id"] = self.rule_result_id
        if self.rule_id is not None:
            d["rule_id"] = self.rule_id
        if self.payload is not None:
            d["payload"] = self.payload
        return d

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "Annotation":
        """Create from dictionary."""
        created_at = d.get("created_at")
        if isinstance(created_at, str):
            created_at = created_at.replace("Z", "+00:00")
            created_at = datetime.fromisoformat(created_at)

        return cls(
            id=d.get("id"),
            run_id=d.get("run_id", 0),
            rule_result_id=d.get("rule_result_id"),
            rule_id=d.get("rule_id"),
            actor_type=d.get("actor_type", "agent"),
            actor_id=d.get("actor_id", ""),
            annotation_type=d.get("annotation_type", ""),
            summary=d.get("summary", ""),
            payload=d.get("payload"),
            created_at=created_at,
        )

    def to_json(self) -> str:
        """Serialize to JSON string (single line for JSONL)."""
        return json.dumps(self.to_dict(), default=str)

    @classmethod
    def from_json(cls, json_str: str) -> "Annotation":
        """Deserialize from JSON string."""
        return cls.from_dict(json.loads(json_str))

    def to_llm(self) -> str:
        """Token-optimized format for LLM context."""
        ts = self.created_at.strftime("%Y-%m-%d %H:%M") if self.created_at else "?"
        parts = [
            f"[{self.annotation_type}]",
            f"by {self.actor_type}:{self.actor_id}",
            f"@ {ts}",
        ]
        if self.summary:
            parts.append(f'"{self.summary}"')
        return " ".join(parts)


# -----------------------------------------------------------------------------
# Severity and Failure Mode Enums
# -----------------------------------------------------------------------------


class Severity(str, Enum):
    """
    Rule severity levels for pipeline control.

    Severity determines how failures affect pipeline execution:
    - BLOCKING: Fails the pipeline (exit code 1)
    - WARNING: Warns but continues (exit code 0)
    - INFO: Logs only, no warning (exit code 0)

    Default severity is BLOCKING for backwards compatibility.
    """

    BLOCKING = "blocking"  # Fails pipeline, exit code 1
    WARNING = "warning"  # Warns but continues, exit code 0
    INFO = "info"  # Logs only, exit code 0

    def __str__(self) -> str:
        return self.value

    @classmethod
    def from_str(cls, value: Optional[str]) -> "Severity":
        """Parse severity from string, defaulting to BLOCKING."""
        if value is None:
            return cls.BLOCKING
        try:
            return cls(value.lower())
        except ValueError:
            return cls.BLOCKING


class FailureMode(str, Enum):
    """
    Semantic failure modes for agent reasoning.

    Each failure mode indicates WHY a rule failed, enabling:
    - Better error messages for humans
    - Structured reasoning for LLM agents
    - Targeted remediation suggestions
    """

    # Value-level failures
    NOVEL_CATEGORY = "novel_category"  # Unexpected values in allowed_values
    NULL_VALUES = "null_values"  # NULL values found
    DUPLICATE_VALUES = "duplicate_values"  # Uniqueness violated

    # Range/bound failures
    RANGE_VIOLATION = "range_violation"  # Values outside min/max bounds

    # Schema failures
    SCHEMA_DRIFT = "schema_drift"  # Column type doesn't match expected

    # Temporal failures
    FRESHNESS_LAG = "freshness_lag"  # Data is stale

    # Dataset-level failures
    ROW_COUNT_LOW = "row_count_low"  # Below minimum rows
    ROW_COUNT_HIGH = "row_count_high"  # Above maximum rows

    # Pattern failures
    PATTERN_MISMATCH = "pattern_mismatch"  # Regex pattern not matched

    # Custom rule failures
    CUSTOM_CHECK_FAILED = "custom_check_failed"  # custom_sql_check failed

    # Cross-column failures
    COMPARISON_FAILED = "comparison_failed"  # Compare rule failed
    CONDITIONAL_NULL = "conditional_null"  # Conditional not-null failed
    CONDITIONAL_RANGE_VIOLATION = "conditional_range_violation"  # Conditional range failed

    # Configuration/contract errors
    CONFIG_ERROR = "config_error"  # Contract/config issue (e.g., column not found)

    def __str__(self) -> str:
        return self.value


@dataclass
class RuleDiff:
    """Diff for a single rule between two states."""

    rule_id: str
    change_type: str  # "new_failure", "resolved", "regression", "improvement", "unchanged"

    # Counts
    before_count: int = 0
    after_count: int = 0
    delta: int = 0

    # Status change
    was_passing: bool = True
    now_passing: bool = True

    # Details from the after state
    severity: str = "blocking"  # blocking, warning, info
    failure_mode: Optional[str] = None
    message: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "rule_id": self.rule_id,
            "change_type": self.change_type,
            "before_count": self.before_count,
            "after_count": self.after_count,
            "delta": self.delta,
            "was_passing": self.was_passing,
            "now_passing": self.now_passing,
            "severity": self.severity,
            "failure_mode": self.failure_mode,
            "message": self.message,
        }


@dataclass
class RuleState:
    """State for a single rule execution."""

    rule_id: str
    rule_name: str
    passed: bool
    failed_count: int
    execution_source: str  # "metadata", "sql", "polars"

    # Severity level
    severity: str = "blocking"  # "blocking", "warning", "info"

    # Optional details for failure analysis
    failure_mode: Optional[str] = None  # "novel_category", "null_spike", etc.
    details: Optional[Dict[str, Any]] = None
    message: Optional[str] = None

    # Column info (if applicable)
    column: Optional[str] = None

    # Database-assigned ID for normalized schema (v0.5)
    id: Optional[int] = None

    # Annotations (opt-in, never loaded by default)
    annotations: Optional[List["Annotation"]] = None

    def to_dict(self, include_annotations: bool = False) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        d: Dict[str, Any] = {
            "rule_id": self.rule_id,
            "rule_name": self.rule_name,
            "passed": self.passed,
            "failed_count": self.failed_count,
            "execution_source": self.execution_source,
            "severity": self.severity,
        }
        if self.failure_mode:
            d["failure_mode"] = self.failure_mode
        if self.details:
            d["details"] = self.details
        if self.message:
            d["message"] = self.message
        if self.column:
            d["column"] = self.column
        if self.id is not None:
            d["id"] = self.id
        if include_annotations and self.annotations:
            d["annotations"] = [a.to_dict() for a in self.annotations]
        return d

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "RuleState":
        """Create from dictionary."""
        annotations = None
        if "annotations" in d and d["annotations"]:
            annotations = [Annotation.from_dict(a) for a in d["annotations"]]

        return cls(
            rule_id=d["rule_id"],
            rule_name=d["rule_name"],
            passed=d["passed"],
            failed_count=d["failed_count"],
            execution_source=d["execution_source"],
            severity=d.get("severity", "blocking"),
            failure_mode=d.get("failure_mode"),
            details=d.get("details"),
            message=d.get("message"),
            column=d.get("column"),
            id=d.get("id"),
            annotations=annotations,
        )

    @classmethod
    def from_result(cls, result: Dict[str, Any]) -> RuleState:
        """Create from validation engine result dict."""
        # Extract column from rule_id if present (COL:column:rule_name format)
        rule_id = result.get("rule_id", "")
        column = None
        if rule_id.startswith("COL:"):
            parts = rule_id.split(":")
            if len(parts) >= 2:
                column = parts[1]

        return cls(
            rule_id=rule_id,
            rule_name=result.get("rule_name", result.get("name", "")),
            passed=result.get("passed", False),
            failed_count=result.get("failed_count", 0),
            execution_source=result.get("execution_source", "polars"),
            severity=result.get("severity", "blocking"),
            failure_mode=result.get("failure_mode"),
            details=result.get("details"),
            message=result.get("message"),
            column=column,
        )


@dataclass
class StateSummary:
    """Summary statistics for a validation run."""

    passed: bool
    total_rules: int
    passed_rules: int
    failed_rules: int
    row_count: Optional[int] = None
    column_count: Optional[int] = None

    # Severity-based failure counts
    blocking_failures: int = 0
    warning_failures: int = 0
    info_failures: int = 0

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        d: Dict[str, Any] = {
            "passed": self.passed,
            "total_rules": self.total_rules,
            "passed_rules": self.passed_rules,
            "failed_rules": self.failed_rules,
            "blocking_failures": self.blocking_failures,
            "warning_failures": self.warning_failures,
            "info_failures": self.info_failures,
        }
        if self.row_count is not None:
            d["row_count"] = self.row_count
        if self.column_count is not None:
            d["column_count"] = self.column_count
        return d

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> StateSummary:
        """Create from dictionary."""
        return cls(
            passed=d["passed"],
            total_rules=d["total_rules"],
            passed_rules=d["passed_rules"],
            failed_rules=d["failed_rules"],
            row_count=d.get("row_count"),
            column_count=d.get("column_count"),
            blocking_failures=d.get("blocking_failures", 0),
            warning_failures=d.get("warning_failures", 0),
            info_failures=d.get("info_failures", 0),
        )


@dataclass
class ValidationState:
    """
    Complete state snapshot for a validation run.

    Designed for:
    - Persistence to local filesystem, S3, or database
    - Comparison across runs (diff)
    - Agent reasoning about changes over time
    """

    # Identification
    contract_fingerprint: str
    dataset_fingerprint: Optional[str]
    contract_name: str
    dataset_uri: str

    # Timing
    run_at: datetime

    # Results
    summary: StateSummary
    rules: List[RuleState]

    # Metadata
    schema_version: str = "2.0"  # v2.0 for normalized schema
    engine_version: str = field(default_factory=lambda: VERSION)

    # Optional context
    duration_ms: Optional[int] = None

    # Database-assigned ID for normalized schema (v0.5)
    id: Optional[int] = None

    # Annotations (opt-in, never loaded by default)
    annotations: Optional[List["Annotation"]] = None

    def to_dict(self, include_annotations: bool = False) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        d: Dict[str, Any] = {
            "schema_version": self.schema_version,
            "engine_version": self.engine_version,
            "contract_fingerprint": self.contract_fingerprint,
            "dataset_fingerprint": self.dataset_fingerprint,
            "contract_name": self.contract_name,
            "dataset_uri": self.dataset_uri,
            "run_at": self.run_at.isoformat(),
            "summary": self.summary.to_dict(),
            "rules": [r.to_dict(include_annotations=include_annotations) for r in self.rules],
            "duration_ms": self.duration_ms,
        }
        if self.id is not None:
            d["id"] = self.id
        if include_annotations and self.annotations:
            d["annotations"] = [a.to_dict() for a in self.annotations]
        return d

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "ValidationState":
        """Create from dictionary."""
        run_at = d["run_at"]
        if isinstance(run_at, str):
            # Parse ISO format, handle both Z and +00:00 suffixes
            run_at = run_at.replace("Z", "+00:00")
            run_at = datetime.fromisoformat(run_at)

        annotations = None
        if "annotations" in d and d["annotations"]:
            annotations = [Annotation.from_dict(a) for a in d["annotations"]]

        return cls(
            schema_version=d.get("schema_version", "1.0"),
            engine_version=d.get("engine_version", "unknown"),
            contract_fingerprint=d["contract_fingerprint"],
            dataset_fingerprint=d.get("dataset_fingerprint"),
            contract_name=d["contract_name"],
            dataset_uri=d["dataset_uri"],
            run_at=run_at,
            summary=StateSummary.from_dict(d["summary"]),
            rules=[RuleState.from_dict(r) for r in d["rules"]],
            duration_ms=d.get("duration_ms"),
            id=d.get("id"),
            annotations=annotations,
        )

    def to_json(self, indent: int = 2, include_annotations: bool = False) -> str:
        """Serialize to JSON string."""
        return json.dumps(
            self.to_dict(include_annotations=include_annotations),
            indent=indent,
            default=str,
        )

    @classmethod
    def from_json(cls, json_str: str) -> "ValidationState":
        """Deserialize from JSON string."""
        return cls.from_dict(json.loads(json_str))

    @classmethod
    def from_validation_result(
        cls,
        result: Dict[str, Any],
        contract_fingerprint: str,
        dataset_fingerprint: Optional[str],
        contract_name: str,
        dataset_uri: str,
    ) -> ValidationState:
        """
        Create a ValidationState from engine.run() result.

        Args:
            result: The dict returned by ValidationEngine.run()
            contract_fingerprint: Hash of the contract
            dataset_fingerprint: Hash of dataset metadata (optional)
            contract_name: Name from contract
            dataset_uri: URI of the dataset
        """
        summary_data = result.get("summary", {})
        results_list = result.get("results", [])
        stats = result.get("stats", {})

        # Build summary
        total = summary_data.get("total_rules", len(results_list))
        passed_count = sum(1 for r in results_list if r.get("passed", False))
        failed_count = total - passed_count

        # Count failures by severity
        blocking_failures = 0
        warning_failures = 0
        info_failures = 0
        for r in results_list:
            if not r.get("passed", False):
                severity = r.get("severity", "blocking")
                if severity == "blocking":
                    blocking_failures += 1
                elif severity == "warning":
                    warning_failures += 1
                elif severity == "info":
                    info_failures += 1

        # Get row/column counts from stats if available
        dataset_stats = stats.get("dataset", {}) if stats else {}
        row_count = dataset_stats.get("nrows")
        column_count = dataset_stats.get("ncols")

        # Use summary_data if available, otherwise calculate
        summary = StateSummary(
            passed=summary_data.get("passed", blocking_failures == 0),
            total_rules=total,
            passed_rules=passed_count,
            failed_rules=failed_count,
            row_count=row_count,
            column_count=column_count,
            blocking_failures=summary_data.get("blocking_failures", blocking_failures),
            warning_failures=summary_data.get("warning_failures", warning_failures),
            info_failures=summary_data.get("info_failures", info_failures),
        )

        # Build rule states
        rules = [RuleState.from_result(r) for r in results_list]

        # Duration
        run_meta = stats.get("run_meta", {}) if stats else {}
        duration_ms = run_meta.get("duration_ms_total")

        return cls(
            contract_fingerprint=contract_fingerprint,
            dataset_fingerprint=dataset_fingerprint,
            contract_name=contract_name,
            dataset_uri=dataset_uri,
            run_at=datetime.now(timezone.utc),
            summary=summary,
            rules=rules,
            duration_ms=duration_ms,
        )

    def get_rule(self, rule_id: str) -> Optional[RuleState]:
        """Get a specific rule state by ID."""
        for rule in self.rules:
            if rule.rule_id == rule_id:
                return rule
        return None

    def get_failed_rules(self) -> List[RuleState]:
        """Get all failed rules."""
        return [r for r in self.rules if not r.passed]

    def get_passed_rules(self) -> List[RuleState]:
        """Get all passed rules."""
        return [r for r in self.rules if r.passed]

    def to_llm(self) -> str:
        """
        Render state in token-optimized format for LLM context.

        Design:
        - Failed rules get detail, passed rules get summarized
        - Failure modes and severity surfaced for reasoning
        - Compact but complete enough for agent decisions
        """
        lines = []

        # Header
        ts = self.run_at.strftime("%Y-%m-%dT%H:%M")
        status = "PASSED" if self.summary.passed else "FAILED"
        lines.append(f"# State: {self.contract_name} @ {ts}")
        lines.append(f"result: {status} ({self.summary.passed_rules}/{self.summary.total_rules} passed)")

        # Show severity breakdown if there are failures
        if self.summary.failed_rules > 0:
            severity_parts = []
            if self.summary.blocking_failures > 0:
                severity_parts.append(f"{self.summary.blocking_failures} blocking")
            if self.summary.warning_failures > 0:
                severity_parts.append(f"{self.summary.warning_failures} warning")
            if self.summary.info_failures > 0:
                severity_parts.append(f"{self.summary.info_failures} info")
            if severity_parts:
                lines.append(f"failures: {', '.join(severity_parts)}")

        if self.summary.row_count:
            lines.append(f"rows: {self.summary.row_count:,}")

        # Failed rules with details
        failed = self.get_failed_rules()
        if failed:
            lines.append("")
            lines.append(f"## Failed ({len(failed)})")
            for rule in failed[:10]:  # Limit to top 10
                parts = [rule.rule_id]
                # Include severity if not blocking
                if rule.severity != "blocking":
                    parts.append(f"[{rule.severity}]")
                if rule.failed_count > 0:
                    count_str = f"{rule.failed_count:,}" if rule.failed_count < 1000000 else f"{rule.failed_count/1000000:.1f}M"
                    failure_word = "failure" if rule.failed_count == 1 else "failures"
                    parts.append(f"{count_str} {failure_word}")
                if rule.failure_mode:
                    parts.append(rule.failure_mode)
                if rule.message:
                    # Truncate long messages
                    msg = rule.message[:50] + "..." if len(rule.message) > 50 else rule.message
                    parts.append(msg)
                lines.append(f"- {' | '.join(parts)}")

            if len(failed) > 10:
                lines.append(f"  ... and {len(failed) - 10} more")

        # Passed rules summary (grouped by execution source)
        passed = self.get_passed_rules()
        if passed:
            lines.append("")
            lines.append(f"## Passed ({len(passed)})")

            # Group by execution source
            by_source: Dict[str, List[RuleState]] = {}
            for rule in passed:
                src = rule.execution_source or "unknown"
                by_source.setdefault(src, []).append(rule)

            # Group by rule name within source
            summary_parts = []
            for src, rules in sorted(by_source.items()):
                # Count by rule name
                by_name: Dict[str, int] = {}
                for r in rules:
                    name = r.rule_id.split(":")[-1] if ":" in r.rule_id else r.rule_id
                    by_name[name] = by_name.get(name, 0) + 1

                for name, count in sorted(by_name.items(), key=lambda x: -x[1]):
                    summary_parts.append(f"{count}x {name} [{src}]")

            lines.append(", ".join(summary_parts[:8]))
            if len(summary_parts) > 8:
                lines.append(f"  ... and {len(summary_parts) - 8} more categories")

        # Footer with fingerprint (for agent tracking)
        lines.append("")
        lines.append(f"fingerprint: {self.contract_fingerprint}")

        return "\n".join(lines)


@dataclass
class RunSummary:
    """
    Lightweight summary of a validation run for history listing.

    Used by get_history() to return a list of runs without loading
    full rule details. Optimized for quick scanning and display.
    """

    run_id: str  # Unique identifier for the run
    timestamp: datetime  # When the run occurred
    passed: bool  # Overall pass/fail status
    failed_count: int  # Total failures across all rules
    total_rows: Optional[int]  # Row count if available
    contract_name: str  # Name of the contract
    contract_fingerprint: str  # Fingerprint for filtering

    # Optional metadata
    total_rules: int = 0
    blocking_failures: int = 0
    warning_failures: int = 0
    duration_ms: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        d: Dict[str, Any] = {
            "run_id": self.run_id,
            "timestamp": self.timestamp.isoformat(),
            "passed": self.passed,
            "failed_count": self.failed_count,
            "contract_name": self.contract_name,
            "contract_fingerprint": self.contract_fingerprint,
            "total_rules": self.total_rules,
            "blocking_failures": self.blocking_failures,
            "warning_failures": self.warning_failures,
        }
        if self.total_rows is not None:
            d["total_rows"] = self.total_rows
        if self.duration_ms is not None:
            d["duration_ms"] = self.duration_ms
        return d

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "RunSummary":
        """Create from dictionary."""
        timestamp = d["timestamp"]
        if isinstance(timestamp, str):
            timestamp = timestamp.replace("Z", "+00:00")
            timestamp = datetime.fromisoformat(timestamp)

        return cls(
            run_id=d["run_id"],
            timestamp=timestamp,
            passed=d["passed"],
            failed_count=d["failed_count"],
            total_rows=d.get("total_rows"),
            contract_name=d["contract_name"],
            contract_fingerprint=d["contract_fingerprint"],
            total_rules=d.get("total_rules", 0),
            blocking_failures=d.get("blocking_failures", 0),
            warning_failures=d.get("warning_failures", 0),
            duration_ms=d.get("duration_ms"),
        )

    @classmethod
    def from_validation_state(cls, state: "ValidationState", run_id: str) -> "RunSummary":
        """Create from a full ValidationState."""
        return cls(
            run_id=run_id,
            timestamp=state.run_at,
            passed=state.summary.passed,
            failed_count=state.summary.failed_rules,
            total_rows=state.summary.row_count,
            contract_name=state.contract_name,
            contract_fingerprint=state.contract_fingerprint,
            total_rules=state.summary.total_rules,
            blocking_failures=state.summary.blocking_failures,
            warning_failures=state.summary.warning_failures,
            duration_ms=state.duration_ms,
        )

    def to_llm(self) -> str:
        """Token-optimized format for LLM context."""
        ts = self.timestamp.strftime("%Y-%m-%d %H:%M")
        status = "PASS" if self.passed else "FAIL"
        rows = f"{self.total_rows:,}" if self.total_rows else "?"
        return f"{ts} | {status} | {self.failed_count} failures | {rows} rows"


@dataclass
class StateDiff:
    """
    Diff between two validation states.

    Captures what changed between runs to enable:
    - Human-readable diff output
    - LLM reasoning about regressions
    - Automated alerting on changes
    """

    # Source states
    before: "ValidationState"
    after: "ValidationState"

    # Summary
    status_changed: bool = False
    has_regressions: bool = False
    has_improvements: bool = False

    # Rule-level changes
    new_failures: List[RuleDiff] = field(default_factory=list)
    resolved: List[RuleDiff] = field(default_factory=list)
    regressions: List[RuleDiff] = field(default_factory=list)  # count increased
    improvements: List[RuleDiff] = field(default_factory=list)  # count decreased
    unchanged: List[RuleDiff] = field(default_factory=list)

    @classmethod
    def compute(cls, before: "ValidationState", after: "ValidationState") -> "StateDiff":
        """
        Compute diff between two states.

        Args:
            before: Earlier state
            after: Later state

        Returns:
            StateDiff with categorized changes
        """
        diff = cls(before=before, after=after)
        diff.status_changed = before.summary.passed != after.summary.passed

        # Index rules by ID
        before_rules = {r.rule_id: r for r in before.rules}
        after_rules = {r.rule_id: r for r in after.rules}

        # Process all rules in after state
        for rule_id, after_rule in after_rules.items():
            before_rule = before_rules.get(rule_id)

            if before_rule is None:
                # New rule (not in before state)
                if not after_rule.passed:
                    rule_diff = RuleDiff(
                        rule_id=rule_id,
                        change_type="new_failure",
                        after_count=after_rule.failed_count,
                        delta=after_rule.failed_count,
                        was_passing=True,  # Didn't exist, treat as "was passing"
                        now_passing=False,
                        severity=after_rule.severity,
                        failure_mode=after_rule.failure_mode,
                        message=after_rule.message,
                    )
                    diff.new_failures.append(rule_diff)
                continue

            # Rule exists in both states
            was_passing = before_rule.passed
            now_passing = after_rule.passed
            before_count = before_rule.failed_count
            after_count = after_rule.failed_count
            delta = after_count - before_count

            rule_diff = RuleDiff(
                rule_id=rule_id,
                change_type="unchanged",
                before_count=before_count,
                after_count=after_count,
                delta=delta,
                was_passing=was_passing,
                now_passing=now_passing,
                severity=after_rule.severity,
                failure_mode=after_rule.failure_mode,
                message=after_rule.message,
            )

            if was_passing and not now_passing:
                # Was passing, now failing
                rule_diff.change_type = "new_failure"
                diff.new_failures.append(rule_diff)
            elif not was_passing and now_passing:
                # Was failing, now passing
                rule_diff.change_type = "resolved"
                diff.resolved.append(rule_diff)
            elif delta > 0:
                # Count increased (regression)
                rule_diff.change_type = "regression"
                diff.regressions.append(rule_diff)
            elif delta < 0:
                # Count decreased (improvement)
                rule_diff.change_type = "improvement"
                diff.improvements.append(rule_diff)
            else:
                # No change
                diff.unchanged.append(rule_diff)

        # Check for rules removed (in before but not in after)
        for rule_id, before_rule in before_rules.items():
            if rule_id not in after_rules:
                # Rule was removed - if it was failing, that's a "resolution"
                if not before_rule.passed:
                    rule_diff = RuleDiff(
                        rule_id=rule_id,
                        change_type="resolved",
                        before_count=before_rule.failed_count,
                        after_count=0,
                        delta=-before_rule.failed_count,
                        was_passing=False,
                        now_passing=True,
                    )
                    diff.resolved.append(rule_diff)

        # Set summary flags
        diff.has_regressions = len(diff.new_failures) > 0 or len(diff.regressions) > 0
        diff.has_improvements = len(diff.resolved) > 0 or len(diff.improvements) > 0

        return diff

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "before_run_at": self.before.run_at.isoformat(),
            "after_run_at": self.after.run_at.isoformat(),
            "contract_name": self.after.contract_name,
            "contract_fingerprint": self.after.contract_fingerprint,
            "status_changed": self.status_changed,
            "has_regressions": self.has_regressions,
            "has_improvements": self.has_improvements,
            "summary": {
                "before": self.before.summary.to_dict(),
                "after": self.after.summary.to_dict(),
            },
            "new_failures": [r.to_dict() for r in self.new_failures],
            "resolved": [r.to_dict() for r in self.resolved],
            "regressions": [r.to_dict() for r in self.regressions],
            "improvements": [r.to_dict() for r in self.improvements],
        }

    def to_json(self, indent: int = 2) -> str:
        """Serialize to JSON string."""
        return json.dumps(self.to_dict(), indent=indent, default=str)

    def to_llm(self) -> str:
        """
        Render diff in token-optimized format for LLM context.

        Focus on changes, skip unchanged rules entirely.
        """
        lines = []

        # Header
        before_ts = self.before.run_at.strftime("%Y-%m-%d")
        after_ts = self.after.run_at.strftime("%Y-%m-%d %H:%M")

        if self.has_regressions:
            status = "REGRESSION"
        elif self.has_improvements:
            status = "IMPROVED"
        else:
            status = "NO_CHANGE"

        lines.append(f"# Diff: {self.after.contract_name}")
        lines.append(f"comparing: {before_ts} → {after_ts}")
        lines.append(f"status: {status}")

        # Summary change
        if self.status_changed:
            before_status = "PASS" if self.before.summary.passed else "FAIL"
            after_status = "PASS" if self.after.summary.passed else "FAIL"
            lines.append(f"overall: {before_status} → {after_status}")

        # New failures (most important) - group by severity
        if self.new_failures:
            # Separate by severity
            blocking = [rd for rd in self.new_failures if rd.severity == "blocking"]
            warnings = [rd for rd in self.new_failures if rd.severity == "warning"]
            infos = [rd for rd in self.new_failures if rd.severity == "info"]

            if blocking:
                lines.append("")
                lines.append(f"## New Blocking Failures ({len(blocking)})")
                for rd in blocking[:5]:
                    parts = [rd.rule_id]
                    if rd.after_count > 0:
                        count_str = f"{rd.after_count:,}" if rd.after_count < 1000000 else f"{rd.after_count/1000000:.1f}M"
                        parts.append(f"+{count_str}")
                    if rd.failure_mode:
                        parts.append(rd.failure_mode)
                    lines.append(f"- {' | '.join(parts)}")
                if len(blocking) > 5:
                    lines.append(f"  ... and {len(blocking) - 5} more")

            if warnings:
                lines.append("")
                lines.append(f"## New Warnings ({len(warnings)})")
                for rd in warnings[:5]:
                    parts = [rd.rule_id]
                    if rd.after_count > 0:
                        count_str = f"{rd.after_count:,}" if rd.after_count < 1000000 else f"{rd.after_count/1000000:.1f}M"
                        parts.append(f"+{count_str}")
                    if rd.failure_mode:
                        parts.append(rd.failure_mode)
                    lines.append(f"- {' | '.join(parts)}")
                if len(warnings) > 5:
                    lines.append(f"  ... and {len(warnings) - 5} more")

            if infos:
                lines.append("")
                lines.append(f"## New Info Issues ({len(infos)})")
                for rd in infos[:5]:
                    parts = [rd.rule_id]
                    if rd.after_count > 0:
                        count_str = f"{rd.after_count:,}" if rd.after_count < 1000000 else f"{rd.after_count/1000000:.1f}M"
                        parts.append(f"+{count_str}")
                    if rd.failure_mode:
                        parts.append(rd.failure_mode)
                    lines.append(f"- {' | '.join(parts)}")
                if len(infos) > 5:
                    lines.append(f"  ... and {len(infos) - 5} more")

        # Regressions (count increased) - group by severity
        if self.regressions:
            blocking_reg = [rd for rd in self.regressions if rd.severity == "blocking"]
            warning_reg = [rd for rd in self.regressions if rd.severity == "warning"]
            info_reg = [rd for rd in self.regressions if rd.severity == "info"]

            def fmt_regression(rd):
                before_str = f"{rd.before_count:,}" if rd.before_count < 1000000 else f"{rd.before_count/1000000:.1f}M"
                after_str = f"{rd.after_count:,}" if rd.after_count < 1000000 else f"{rd.after_count/1000000:.1f}M"
                mode = f" | {rd.failure_mode}" if rd.failure_mode else ""
                return f"- {rd.rule_id}: {before_str} → {after_str} (+{rd.delta:,}){mode}"

            if blocking_reg:
                lines.append("")
                lines.append(f"## Blocking Regressions ({len(blocking_reg)})")
                for rd in blocking_reg[:5]:
                    lines.append(fmt_regression(rd))
                if len(blocking_reg) > 5:
                    lines.append(f"  ... and {len(blocking_reg) - 5} more")

            if warning_reg:
                lines.append("")
                lines.append(f"## Warning Regressions ({len(warning_reg)})")
                for rd in warning_reg[:5]:
                    lines.append(fmt_regression(rd))
                if len(warning_reg) > 5:
                    lines.append(f"  ... and {len(warning_reg) - 5} more")

            if info_reg:
                lines.append("")
                lines.append(f"## Info Regressions ({len(info_reg)})")
                for rd in info_reg[:5]:
                    lines.append(fmt_regression(rd))
                if len(info_reg) > 5:
                    lines.append(f"  ... and {len(info_reg) - 5} more")

        # Resolved
        if self.resolved:
            lines.append("")
            lines.append(f"## Resolved ({len(self.resolved)})")
            for rd in self.resolved[:5]:
                lines.append(f"- {rd.rule_id}")
            if len(self.resolved) > 5:
                lines.append(f"  ... and {len(self.resolved) - 5} more")

        # Improvements (count decreased)
        if self.improvements:
            lines.append("")
            lines.append(f"## Improvements ({len(self.improvements)})")
            for rd in self.improvements[:5]:
                lines.append(f"- {rd.rule_id}: {rd.before_count:,} → {rd.after_count:,} ({rd.delta:,})")
            if len(self.improvements) > 5:
                lines.append(f"  ... and {len(self.improvements) - 5} more")

        # No changes
        if not self.new_failures and not self.regressions and not self.resolved and not self.improvements:
            lines.append("")
            lines.append("No changes detected.")

        # Footer
        lines.append("")
        lines.append(f"fingerprint: {self.after.contract_fingerprint}")

        return "\n".join(lines)
