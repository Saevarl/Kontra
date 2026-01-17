# src/kontra/api/results.py
"""
Public API result types for Kontra.

These classes wrap the internal state/result types with a cleaner interface
for the public Python API.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Union

import yaml


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
            [0] _row_index=1, id=2, email=None, status=active
            [1] _row_index=3, id=4, email=None, status=active
        """
        if not self._samples:
            return f"SAMPLES: {self.rule_id} (0 rows)"

        lines = [f"SAMPLES: {self.rule_id} ({len(self._samples)} rows)"]

        for i, row in enumerate(self._samples[:10]):  # Limit to 10 for token efficiency
            # Format row as compact key=value pairs
            parts = []
            for k, v in row.items():
                if v is None:
                    parts.append(f"{k}=None")
                elif isinstance(v, str) and len(v) > 20:
                    parts.append(f"{k}={v[:20]}...")
                else:
                    parts.append(f"{k}={v}")
            lines.append(f"[{i}] " + ", ".join(parts))

        if len(self._samples) > 10:
            lines.append(f"... +{len(self._samples) - 10} more rows")

        return "\n".join(lines)


@dataclass
class RuleResult:
    """
    Result for a single validation rule.

    Properties:
        rule_id: Unique identifier (e.g., "COL:user_id:not_null")
        name: Rule type name (e.g., "not_null")
        passed: Whether the rule passed
        failed_count: Number of failing rows
        message: Human-readable result message
        severity: "blocking" | "warning" | "info"
        source: Execution source ("metadata", "sql", "polars")
        column: Column name if applicable
    """

    rule_id: str
    name: str
    passed: bool
    failed_count: int
    message: str
    severity: str = "blocking"
    source: str = "polars"
    column: Optional[str] = None
    details: Optional[Dict[str, Any]] = None

    def __repr__(self) -> str:
        status = "PASS" if self.passed else "FAIL"
        if self.failed_count > 0:
            return f"RuleResult({self.rule_id}) {status} - {self.failed_count:,} failures"
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
            column=column,
            details=d.get("details"),
        )

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        d = {
            "rule_id": self.rule_id,
            "name": self.name,
            "passed": self.passed,
            "failed_count": self.failed_count,
            "message": self.message,
            "severity": self.severity,
            "source": self.source,
        }
        if self.column:
            d["column"] = self.column
        if self.details:
            d["details"] = self.details
        return d


@dataclass
class ValidationResult:
    """
    Result of a validation run.

    Properties:
        passed: True if all blocking rules passed
        dataset: Dataset name/path
        total_rules: Total number of rules evaluated
        passed_count: Number of rules that passed
        failed_count: Number of blocking rules that failed
        warning_count: Number of warning rules that failed
        rules: List of RuleResult objects
        blocking_failures: List of failed blocking rules
        warnings: List of failed warning rules
        stats: Optional statistics dict

    Methods:
        sample_failures(rule_id, n=5): Get sample of failing rows for a rule
    """

    passed: bool
    dataset: str
    total_rules: int
    passed_count: int
    failed_count: int
    warning_count: int
    rules: List[RuleResult]
    stats: Optional[Dict[str, Any]] = None
    _raw: Optional[Dict[str, Any]] = field(default=None, repr=False)
    # For sample_failures() - lazy evaluation
    _data_source: Optional[Any] = field(default=None, repr=False)
    _rule_objects: Optional[List[Any]] = field(default=None, repr=False)

    def __repr__(self) -> str:
        status = "PASSED" if self.passed else "FAILED"
        parts = [f"ValidationResult({self.dataset}) {status}"]
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

    @property
    def blocking_failures(self) -> List[RuleResult]:
        """Get all failed blocking rules."""
        return [r for r in self.rules if not r.passed and r.severity == "blocking"]

    @property
    def warnings(self) -> List[RuleResult]:
        """Get all failed warning rules."""
        return [r for r in self.rules if not r.passed and r.severity == "warning"]

    @classmethod
    def from_engine_result(
        cls,
        result: Dict[str, Any],
        dataset: str = "unknown",
        data_source: Optional[Any] = None,
        rule_objects: Optional[List[Any]] = None,
    ) -> "ValidationResult":
        """Create from ValidationEngine.run() result dict.

        Args:
            result: Engine result dict
            dataset: Dataset name (fallback)
            data_source: Original data source for lazy sample_failures()
            rule_objects: Rule objects for sample_failures() predicates
        """
        summary = result.get("summary", {})
        results_list = result.get("results", [])

        # Convert raw results to RuleResult objects
        rules = [RuleResult.from_dict(r) for r in results_list]

        # Calculate counts
        total = summary.get("total_rules", len(rules))
        passed_count = summary.get("rules_passed", sum(1 for r in rules if r.passed))

        # Count by severity
        blocking_failed = sum(1 for r in rules if not r.passed and r.severity == "blocking")
        warning_failed = sum(1 for r in rules if not r.passed and r.severity == "warning")

        return cls(
            passed=summary.get("passed", blocking_failed == 0),
            dataset=summary.get("dataset_name", dataset),
            total_rules=total,
            passed_count=passed_count,
            failed_count=blocking_failed,
            warning_count=warning_failed,
            rules=rules,
            stats=result.get("stats"),
            _raw=result,
            _data_source=data_source,
            _rule_objects=rule_objects,
        )

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "passed": self.passed,
            "dataset": self.dataset,
            "total_rules": self.total_rules,
            "passed_count": self.passed_count,
            "failed_count": self.failed_count,
            "warning_count": self.warning_count,
            "rules": [r.to_dict() for r in self.rules],
            "stats": self.stats,
        }

    def to_json(self, indent: Optional[int] = None) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict(), indent=indent, default=str)

    def to_llm(self) -> str:
        """
        Token-optimized format for LLM context.

        Example output:
            VALIDATION: my_contract FAILED
            BLOCKING: COL:email:not_null (523 nulls), COL:status:allowed_values (12 invalid)
            WARNING: COL:age:range (3 out of bounds)
            PASSED: 15 rules
        """
        lines = []

        status = "PASSED" if self.passed else "FAILED"
        lines.append(f"VALIDATION: {self.dataset} {status}")

        # Blocking failures
        blocking = self.blocking_failures
        if blocking:
            parts = []
            for r in blocking[:5]:
                count = f"({r.failed_count:,})" if r.failed_count > 0 else ""
                parts.append(f"{r.rule_id} {count}".strip())
            line = "BLOCKING: " + ", ".join(parts)
            if len(blocking) > 5:
                line += f" ... +{len(blocking) - 5} more"
            lines.append(line)

        # Warnings
        warnings = self.warnings
        if warnings:
            parts = []
            for r in warnings[:5]:
                count = f"({r.failed_count:,})" if r.failed_count > 0 else ""
                parts.append(f"{r.rule_id} {count}".strip())
            line = "WARNING: " + ", ".join(parts)
            if len(warnings) > 5:
                line += f" ... +{len(warnings) - 5} more"
            lines.append(line)

        # Passed summary
        lines.append(f"PASSED: {self.passed_count} rules")

        return "\n".join(lines)

    def sample_failures(
        self,
        rule_id: str,
        n: int = 5,
    ) -> FailureSamples:
        """
        Get a sample of rows that failed a specific rule.

        This method lazily re-queries the data source to find failing rows.
        For BYOC (database connections), the connection must still be open.

        Args:
            rule_id: The rule ID to get failures for (e.g., "COL:email:not_null")
            n: Number of sample rows to return (default: 5, max: 100)

        Returns:
            FailureSamples: Collection of failing rows with "_row_index" field.
            Supports to_dict(), to_json(), to_llm() methods.
            Empty if the rule passed (no failures).

        Raises:
            ValueError: If rule_id not found or rule doesn't support row-level samples
            RuntimeError: If data source is unavailable for re-query

        Example:
            result = kontra.validate("data.parquet", contract)
            if not result.passed:
                samples = result.sample_failures("COL:email:not_null", n=5)
                for row in samples:
                    print(f"Row {row['_row_index']}: {row}")
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
        predicate = None
        if hasattr(rule_obj, "compile_predicate"):
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
        df = self._load_data_for_sampling(rule_obj, n)

        # For non-database sources (or if SQL filter wasn't available),
        # we need to filter with Polars
        if "_row_index" not in df.columns:
            # Filter to failing rows, add index, limit
            try:
                failing = (
                    df.with_row_index("_row_index")
                    .filter(predicate)
                    .head(n)
                    .to_dicts()
                )
            except Exception as e:
                raise RuntimeError(f"Failed to query failing rows: {e}") from e
        else:
            # SQL pushdown already applied filter and added row index
            failing = df.head(n).to_dicts()

        return FailureSamples(failing, rule_id)

    def _load_data_for_sampling(
        self, rule: Any = None, n: int = 5
    ) -> "pl.DataFrame":
        """
        Load data from the stored data source for sample_failures().

        For database sources with rules that support SQL filters,
        pushes the filter to SQL for performance.
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
                return pl.read_csv(source)
            else:
                # Try parquet first, then CSV
                try:
                    return self._load_parquet_with_filter(source, rule, n)
                except Exception:
                    try:
                        return pl.read_csv(source)
                    except Exception:
                        raise RuntimeError(f"Cannot load data from: {source}")

        # Polars DataFrame (was passed directly)
        if isinstance(source, pl.DataFrame):
            return source

        # DatasetHandle (BYOC or parsed URI)
        if hasattr(source, "scheme") and hasattr(source, "uri"):
            # It's a DatasetHandle
            handle = source

            # Check for BYOC (external connection)
            if handle.scheme == "byoc" or hasattr(handle, "external_conn"):
                conn = getattr(handle, "external_conn", None)
                if conn is None:
                    raise RuntimeError(
                        "Database connection is closed. "
                        "For BYOC, keep the connection open until done with sample_failures()."
                    )
                table = getattr(handle, "table_ref", None) or handle.path
                return self._query_db_with_filter(conn, table, rule, n, "postgres")

            elif handle.scheme in ("postgres", "postgresql"):
                # PostgreSQL via URI
                if hasattr(handle, "external_conn") and handle.external_conn:
                    conn = handle.external_conn
                else:
                    raise RuntimeError(
                        "Database connection is not available. "
                        "For URI-based connections, sample_failures() requires re-connection."
                    )
                table = getattr(handle, "table_ref", None) or handle.path
                return self._query_db_with_filter(conn, table, rule, n, "postgres")

            elif handle.scheme == "mssql":
                # SQL Server
                if hasattr(handle, "external_conn") and handle.external_conn:
                    conn = handle.external_conn
                else:
                    raise RuntimeError(
                        "Database connection is not available."
                    )
                table = getattr(handle, "table_ref", None) or handle.path
                return self._query_db_with_filter(conn, table, rule, n, "mssql")

            elif handle.scheme in ("file", None) or (handle.uri and not handle.scheme):
                # File-based
                uri = handle.uri
                if uri.lower().endswith(".parquet"):
                    return self._load_parquet_with_filter(uri, rule, n)
                elif uri.lower().endswith(".csv"):
                    return pl.read_csv(uri)
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
        if rule is not None and getattr(rule, "name", None) == "unique":
            column = rule.params.get("column")
            if column:
                col = f'"{column}"'
                if dialect == "mssql":
                    query = f"""
                        SELECT *, ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1 AS _row_index
                        FROM {table}
                        WHERE {col} IN (
                            SELECT {col} FROM {table}
                            GROUP BY {col}
                            HAVING COUNT(*) > 1
                        )
                        ORDER BY (SELECT NULL)
                        OFFSET 0 ROWS FETCH FIRST {n} ROWS ONLY
                    """
                else:
                    query = f"""
                        SELECT *, ROW_NUMBER() OVER () - 1 AS _row_index
                        FROM {table}
                        WHERE {col} IN (
                            SELECT {col} FROM {table}
                            GROUP BY {col}
                            HAVING COUNT(*) > 1
                        )
                        LIMIT {n}
                    """
                return pl.read_database(query, conn)

        if rule is not None and hasattr(rule, "to_sql_filter"):
            sql_filter = rule.to_sql_filter(dialect)

        if sql_filter:
            # Build query with filter and row number
            # ROW_NUMBER() gives us the original row index
            if dialect == "mssql":
                # SQL Server syntax
                query = f"""
                    SELECT *, ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1 AS _row_index
                    FROM {table}
                    WHERE {sql_filter}
                    ORDER BY (SELECT NULL)
                    OFFSET 0 ROWS FETCH FIRST {n} ROWS ONLY
                """
            else:
                # PostgreSQL / DuckDB syntax
                query = f"""
                    SELECT *, ROW_NUMBER() OVER () - 1 AS _row_index
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
    ) -> "pl.DataFrame":
        """
        Load Parquet file with predicate pushdown for performance.

        Uses scan_parquet + filter + head to push predicates to row groups,
        avoiding loading the entire file.
        """
        import polars as pl

        predicate = None
        if rule is not None and hasattr(rule, "compile_predicate"):
            pred_obj = rule.compile_predicate()
            if pred_obj is not None:
                predicate = pred_obj.expr

        if predicate is not None:
            # Use lazy scanning with predicate pushdown
            # The filter pushes down to Parquet row groups
            return (
                pl.scan_parquet(path)
                .with_row_index("_row_index")
                .filter(predicate)
                .head(n)
                .collect()
            )
        else:
            # No predicate available, load all
            return pl.read_parquet(path)


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
