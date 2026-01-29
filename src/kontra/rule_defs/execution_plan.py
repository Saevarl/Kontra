# src/kontra/rules/execution_plan.py
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Iterable, List, Dict, Any, Optional, Set

if TYPE_CHECKING:
    import polars as pl

from kontra.rule_defs.base import BaseRule
from kontra.rule_defs.predicates import Predicate
from kontra.logging import get_logger, log_exception

_logger = get_logger(__name__)


def _extract_rule_kind(rule_id: str) -> Optional[str]:
    """
    Extract rule kind from rule_id.

    Rule ID formats:
      - COL:column:rule_kind -> rule_kind
      - DATASET:rule_kind -> rule_kind
      - Custom IDs -> None (fall back to predicate message)
    """
    if rule_id.startswith("COL:"):
        parts = rule_id.split(":")
        if len(parts) >= 3:
            return parts[2]
    elif rule_id.startswith("DATASET:"):
        parts = rule_id.split(":")
        if len(parts) >= 2:
            return parts[1]
    return None


def _generate_polars_message(
    rule_id: str,
    failed_count: int,
    is_tally: bool,
    predicate_message: str,
) -> str:
    """
    Generate a tally-aware message for Polars execution.

    For consistency with SQL execution, uses the same message format.
    Falls back to predicate message for unknown rule kinds.
    """
    if failed_count == 0:
        return "Passed"

    rule_kind = _extract_rule_kind(rule_id)

    # Extract column name from rule_id
    column = None
    if rule_id.startswith("COL:"):
        parts = rule_id.split(":")
        if len(parts) >= 2:
            column = parts[1]

    # Count prefix: "At least 1" for early termination, exact count for tally
    if is_tally:
        count_str = str(failed_count)
        row_str = "row" if failed_count == 1 else "rows"
    else:
        count_str = "At least 1"
        row_str = "row"

    # Generate rule-specific messages (same format as SQL path)
    if rule_kind == "not_null":
        col_part = f" in {column}" if column else ""
        return f"{count_str} null value{'' if not is_tally or failed_count == 1 else 's'} found{col_part}"

    elif rule_kind == "unique":
        col_part = f" in {column}" if column else ""
        return f"{count_str} duplicate {row_str}{col_part}"

    elif rule_kind in ("allowed_values", "disallowed_values"):
        # Use predicate message which includes the allowed values list
        return f"{count_str} {row_str}: {predicate_message}"

    elif rule_kind == "range":
        # Use predicate message which includes the constraint values [min, max]
        # e.g., "age values outside range [0, 150]"
        return f"{count_str} {row_str}: {predicate_message}"

    elif rule_kind == "length":
        # Use predicate message which includes the constraint values [min, max]
        return f"{count_str} {row_str}: {predicate_message}"

    elif rule_kind == "regex":
        # Use predicate message which includes the pattern
        return f"{count_str} {row_str}: {predicate_message}"

    elif rule_kind == "contains":
        col_part = f" in {column}" if column else ""
        return f"{count_str} {row_str} missing required substring{col_part}"

    elif rule_kind == "starts_with":
        col_part = f" in {column}" if column else ""
        return f"{count_str} {row_str} with invalid prefix{col_part}"

    elif rule_kind == "ends_with":
        col_part = f" in {column}" if column else ""
        return f"{count_str} {row_str} with invalid suffix{col_part}"

    elif rule_kind == "compare":
        # Use predicate message which includes column names (e.g., "end_date is not greater than start_date")
        return f"{count_str} {row_str}: {predicate_message}"

    elif rule_kind == "conditional_not_null":
        # Use predicate message which includes column and condition (e.g., "shipping_date is null when status == 'shipped'")
        return f"{count_str} {row_str}: {predicate_message}"

    elif rule_kind == "conditional_range":
        # Use predicate message which includes column, range, and condition
        return f"{count_str} {row_str}: {predicate_message}"

    else:
        # For unknown rule kinds or custom IDs, use predicate message with count prefix
        if is_tally:
            return predicate_message
        else:
            return f"At least 1 violation: {predicate_message}"


# --------------------------------------------------------------------------- #
# Planning Artifact
# --------------------------------------------------------------------------- #

@dataclass
class CompiledPlan:
    """
    Output of planning/compilation.

    Attributes
    ----------
    predicates
        Vectorizable rule predicates (Polars expressions). These can be run in
        a single, columnar pass (df.select([...])) and summarized cheaply.

    fallback_rules
        Rules that couldn't be vectorized. They will be executed individually
        via rule.validate(df). We still include their required columns in
        `required_cols` to enable projection.

    required_cols
        Union of all columns required by `predicates` and `fallback_rules`.
        The engine can hand this list to the materializer for true projection.

    sql_rules
        Tiny, backend-agnostic specs for rules that can be evaluated as
        single-row SQL aggregates (e.g., DuckDB). Polars ignores these; they
        are consumed by a SQL executor if present.
    """
    predicates: List[Predicate]
    fallback_rules: List[BaseRule]
    required_cols: List[str]
    sql_rules: List[Dict[str, Any]]


# --------------------------------------------------------------------------- #
# Planner
# --------------------------------------------------------------------------- #

class RuleExecutionPlan:
    """
    Builds and executes a plan for the given rules.

    Design goals
    ------------
    - Deterministic: same inputs → same outputs
    - Lean: compilation discovers vectorizable work + required columns
    - Extensible: optional `sql_rules` for SQL backends (Polars behavior unchanged)
    """

    def __init__(self, rules: List[BaseRule]):
        self.rules = rules

    def __str__(self) -> str:
        if not self.rules:
            return "RuleExecutionPlan(rules=[])"
        rules_list = [repr(r) for r in self.rules]
        rules_str = ",\n    ".join(rules_list)
        return f"RuleExecutionPlan(rules=[\n    {rules_str}\n])"

    def __repr__(self) -> str:
        return f"RuleExecutionPlan(rules={self.rules})"

    # --------------------------- Public API -----------------------------------

    def compile(self) -> CompiledPlan:
        """
        Compile rules into:
          - vectorizable predicates (Polars)
          - fallback rule list
          - required column set (for projection)
          - sql_rules (for optional SQL executor consumption)
        """
        predicates: List[Predicate] = []
        fallbacks: List[BaseRule] = []
        sql_rules: List[Dict[str, Any]] = []

        for rule in self.rules:
            # 1) Try vectorization (Polars)
            pred = _try_compile_predicate(rule)
            if pred is None:
                fallbacks.append(rule)
            else:
                _validate_predicate(pred)
                if pred.rule_id != rule.rule_id:
                    raise ValueError(
                        f"Predicate.rule_id '{pred.rule_id}' does not match "
                        f"rule.rule_id '{rule.rule_id}'."
                    )
                predicates.append(pred)

            # 2) Optionally generate a SQL spec (non-fatal if inapplicable)
            spec = _maybe_rule_sql_spec(rule)
            if spec:
                sql_rules.append(spec)

        # 3) Derive required columns for projection (predicates + fallbacks)
        cols_pred = _collect_required_columns(predicates)
        cols_fb = _extract_columns_from_rules(fallbacks)
        required_cols = sorted(cols_pred | cols_fb)

        return CompiledPlan(
            predicates=predicates,
            fallback_rules=fallbacks,
            required_cols=required_cols,
            sql_rules=sql_rules,
        )

    def execute_compiled(
        self,
        df: "pl.DataFrame",
        compiled: CompiledPlan,
        rule_tally_map: Optional[Dict[str, bool]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Execute the compiled plan using Polars:
          - vectorized pass for predicates
          - individual validation for fallback rules

        Parameters
        ----------
        df : pl.DataFrame
            The DataFrame to validate.
        compiled : CompiledPlan
            The compiled execution plan.
        rule_tally_map : dict, optional
            Mapping of rule_id -> bool indicating whether to use exact counts (True)
            or early termination (False). If not provided, defaults to exact counts.
        """
        import polars as pl

        # Build rule_id -> severity mapping for predicates
        rule_severity_map = self._build_severity_map()
        available_cols = set(df.columns)
        rule_tally_map = rule_tally_map or {}

        vec_results: List[Dict[str, Any]] = []
        if compiled.predicates:
            # Separate predicates into those with all columns present vs missing columns
            valid_predicates: List[Predicate] = []
            missing_col_results: List[Dict[str, Any]] = []

            for p in compiled.predicates:
                missing = p.columns - available_cols
                if missing:
                    # Column(s) not found - generate failure result
                    missing_list = sorted(missing)
                    if len(missing_list) == 1:
                        msg = f"Column '{missing_list[0]}' not found"
                    else:
                        msg = f"Columns not found: {', '.join(missing_list)}"

                    # Hint if data might be nested (single column available, multiple expected)
                    if len(available_cols) == 1:
                        msg += ". Data may be nested - Kontra requires flat tabular data"

                    from kontra.state.types import FailureMode
                    missing_col_results.append({
                        "rule_id": p.rule_id,
                        "passed": False,
                        "failed_count": df.height,
                        "message": msg,
                        "execution_source": "polars",
                        "severity": rule_severity_map.get(p.rule_id, "blocking"),
                        "failure_mode": str(FailureMode.CONFIG_ERROR),
                        "details": {
                            "missing_columns": missing_list,
                            "available_columns": sorted(available_cols)[:20],
                        },
                    })
                else:
                    valid_predicates.append(p)

            # Execute valid predicates in vectorized pass
            # Split into tally=True (exact counts) and tally=False (early termination)
            if valid_predicates:
                tally_predicates = [p for p in valid_predicates if rule_tally_map.get(p.rule_id, True)]
                fast_predicates = [p for p in valid_predicates if not rule_tally_map.get(p.rule_id, True)]

                # Execute tally=True predicates with .sum() for exact counts
                if tally_predicates:
                    counts_df = df.select([p.expr.sum().alias(p.rule_id) for p in tally_predicates])
                    counts = counts_df.row(0, named=True)
                    for p in tally_predicates:
                        failed_count = int(counts[p.rule_id])
                        passed = failed_count == 0
                        message = _generate_polars_message(
                            p.rule_id, failed_count, is_tally=True, predicate_message=p.message
                        )
                        vec_results.append(
                            {
                                "rule_id": p.rule_id,
                                "passed": passed,
                                "failed_count": failed_count,
                                "message": message,
                                "execution_source": "polars",
                                "severity": rule_severity_map.get(p.rule_id, "blocking"),
                            }
                        )

                # Execute tally=False predicates with .any() for early termination
                if fast_predicates:
                    any_df = df.select([p.expr.any().alias(p.rule_id) for p in fast_predicates])
                    any_results = any_df.row(0, named=True)
                    for p in fast_predicates:
                        has_violation = bool(any_results[p.rule_id])
                        passed = not has_violation
                        failed_count = 1 if has_violation else 0
                        message = _generate_polars_message(
                            p.rule_id, failed_count, is_tally=False, predicate_message=p.message
                        )
                        vec_results.append(
                            {
                                "rule_id": p.rule_id,
                                "passed": passed,
                                "failed_count": failed_count,
                                "message": message,
                                "execution_source": "polars",
                                "severity": rule_severity_map.get(p.rule_id, "blocking"),
                            }
                        )

            # Add missing column results
            vec_results.extend(missing_col_results)

        fb_results: List[Dict[str, Any]] = []
        for r in compiled.fallback_rules:
            try:
                result = r.validate(df)
                result["execution_source"] = "polars"
                result["severity"] = getattr(r, "severity", "blocking")
                fb_results.append(result)
            except Exception as e:
                fb_results.append(
                    {
                        "rule_id": getattr(r, "rule_id", r.name),
                        "passed": False,
                        "failed_count": int(df.height),
                        "message": f"Rule execution failed: {e}",
                        "execution_source": "polars",
                        "severity": getattr(r, "severity", "blocking"),
                    }
                )

        # Deterministic order: predicates first, then fallbacks
        return vec_results + fb_results

    def _build_severity_map(self) -> Dict[str, str]:
        """Build a mapping from rule_id to severity for all rules."""
        return {
            getattr(r, "rule_id", r.name): getattr(r, "severity", "blocking")
            for r in self.rules
        }

    def execute(self, df: "pl.DataFrame") -> List[Dict[str, Any]]:
        """Compile and execute in one step (Polars-only path)."""
        compiled = self.compile()
        return self.execute_compiled(df, compiled)

    def summary(self, results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Aggregate pass/fail counts for reporters."""
        total = len(results)
        failed = sum(1 for r in results if not r.get("passed", False))

        # Count failures by severity
        blocking_failures = 0
        warning_failures = 0
        info_failures = 0

        for r in results:
            if not r.get("passed", False):
                severity = r.get("severity", "blocking")
                if severity == "blocking":
                    blocking_failures += 1
                elif severity == "warning":
                    warning_failures += 1
                elif severity == "info":
                    info_failures += 1

        # Validation passes if no blocking failures
        # (warnings and info are reported but don't fail the pipeline)
        passed = blocking_failures == 0

        return {
            "total_rules": total,
            "rules_failed": failed,
            "rules_passed": total - failed,
            "passed": passed,
            "blocking_failures": blocking_failures,
            "warning_failures": warning_failures,
            "info_failures": info_failures,
        }

    # ------------------------ Hybrid/Residual Helpers -------------------------

    def without_ids(self, compiled: CompiledPlan, handled_ids: Set[str]) -> CompiledPlan:
        """
        Return a new CompiledPlan with any rules whose rule_id is in `handled_ids` removed.

        Used by the hybrid path: a SQL executor handles a subset of rules; the
        remainder (residual) still needs accurate `required_cols` so projection
        works for Polars.
        """
        resid_preds = [p for p in compiled.predicates if p.rule_id not in handled_ids]
        resid_fallbacks = [
            r for r in compiled.fallback_rules
            if getattr(r, "rule_id", r.name) not in handled_ids
        ]

        cols_pred = _collect_required_columns(resid_preds)
        cols_fb = _extract_columns_from_rules(resid_fallbacks)
        required_cols = sorted(cols_pred | cols_fb)

        # sql_rules are irrelevant for the residual Polars pass
        return CompiledPlan(
            predicates=resid_preds,
            fallback_rules=resid_fallbacks,
            required_cols=required_cols,
            sql_rules=[],
        )

    def required_cols_for(self, compiled: CompiledPlan) -> List[str]:
        """Expose the computed required columns for a given compiled plan."""
        return list(compiled.required_cols)


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #

def _try_compile_predicate(rule: BaseRule) -> Optional[Predicate]:
    """
    Ask a rule for its vectorizable Predicate, if any.

    Rules that don't implement `compile_predicate()` or cannot be compiled
    (raise an error) return None and are treated as fallbacks.
    """
    fn = getattr(rule, "compile_predicate", None)
    if fn is None:
        return None
    try:
        return fn() or None
    except Exception as e:
        log_exception(_logger, f"compile_predicate failed for {getattr(rule, 'name', '?')}", e)
        return None


def _collect_required_columns(preds: Iterable[Predicate]) -> Set[str]:
    """Union the required columns declared by each predicate."""
    cols: Set[str] = set()
    for p in preds:
        cols.update(p.columns)
    return cols


def _extract_columns_from_rules(rules: Iterable[BaseRule]) -> Set[str]:
    """
    Extract required columns from fallback rules.

    First tries rule.required_columns(), then falls back to inferring
    from common param names ('column', 'columns').
    """
    cols: Set[str] = set()
    for r in rules:
        try:
            # Prefer explicit declaration from the rule
            rule_cols = r.required_columns() or set()
            if not rule_cols:
                # Heuristic: infer from common param names when not declared
                p = getattr(r, "params", {}) or {}
                col = p.get("column")
                cols_list = p.get("columns")
                if isinstance(col, str) and col:
                    rule_cols.add(col)
                if isinstance(cols_list, (list, tuple)):
                    rule_cols.update(c for c in cols_list if isinstance(c, str))
            cols.update(rule_cols)
        except Exception as e:
            # Be conservative: ignore here; rule will raise during validate() if broken.
            log_exception(_logger, f"Could not extract columns for rule {getattr(r, 'name', '?')}", e)
    return cols


def _validate_predicate(pred: Predicate) -> None:
    """
    Type/shape checks for a Predicate returned by a rule.

    Raises:
        TypeError: If predicate structure is invalid.
        ValueError: If predicate values are invalid.
        ImportError: If polars is not installed.
    """
    try:
        import polars as pl  # Lazy import - only needed when validating predicates
    except ImportError as e:
        raise ImportError(
            "Polars is required to compile validation rules but is not installed. "
            "Install with: pip install polars"
        ) from e

    if not isinstance(pred, Predicate):
        raise TypeError("compile_predicate() must return a Predicate instance")
    if not isinstance(pred.expr, pl.Expr):
        raise TypeError("Predicate.expr must be a Polars Expr")
    if not pred.rule_id or not isinstance(pred.rule_id, str):
        raise ValueError("Predicate.rule_id must be a non-empty string")
    if not isinstance(pred.columns, set):
        raise TypeError("Predicate.columns must be a set[str]")


def _maybe_rule_sql_spec(rule: BaseRule) -> Optional[Dict[str, Any]]:
    """
    Return a tiny, backend-agnostic spec for SQL-capable rules.

    Supported rules:
      - not_null(column)
      - unique(column)
      - min_rows(threshold)
      - max_rows(threshold)
      - allowed_values(column, values)
      - Any custom rule implementing to_sql_agg()

    Notes
    -----
    - If a rule provides `to_sql_spec()`, that takes precedence.
    - If a rule provides `to_sql_agg()`, use it for custom SQL pushdown.
    - We normalize namespaced rule names, e.g. "DATASET:not_null" → "not_null".
    - For min/max rows, accept both `value` and `threshold` to match existing contracts.
    - Not all executors support all rules (DuckDB: 3, PostgreSQL: 5).
    - The `tally` flag is included in specs to control EXISTS vs COUNT execution.
    """
    rid = getattr(rule, "rule_id", None)
    if not isinstance(rid, str):
        return None

    # Get the rule's tally setting (may be None, True, or False)
    rule_tally = getattr(rule, "tally", None)

    # Priority 1: Rule-provided spec (full control)
    to_sql = getattr(rule, "to_sql_spec", None)
    if callable(to_sql):
        try:
            spec = to_sql()
            if spec:
                # Inject tally into rule-provided spec if not already set
                if "tally" not in spec:
                    spec["tally"] = rule_tally
                return spec
        except Exception as e:
            log_exception(_logger, f"to_sql_spec failed for {getattr(rule, 'name', '?')}", e)

    # Priority 2: Rule-provided SQL aggregate (custom rules)
    # This allows custom rules to have SQL pushdown without modifying executors
    to_sql_agg = getattr(rule, "to_sql_agg", None)
    if callable(to_sql_agg):
        try:
            # Try each dialect - executors will use the one they need
            # We include all dialects in the spec so any executor can use it
            agg_duckdb = to_sql_agg("duckdb")
            agg_postgres = to_sql_agg("postgres")
            agg_mssql = to_sql_agg("mssql")

            # If any dialect is supported, include the spec
            if agg_duckdb or agg_postgres or agg_mssql:
                spec = {
                    "kind": "custom_agg",
                    "rule_id": rid,
                    "tally": rule_tally,
                    "sql_agg": {
                        "duckdb": agg_duckdb,
                        "postgres": agg_postgres,
                        "mssql": agg_mssql,
                    },
                }

                # Check for optional to_sql_exists() for early termination (tally=False)
                to_sql_exists = getattr(rule, "to_sql_exists", None)
                if callable(to_sql_exists):
                    try:
                        exists_duckdb = to_sql_exists("duckdb")
                        exists_postgres = to_sql_exists("postgres")
                        exists_mssql = to_sql_exists("mssql")

                        if exists_duckdb or exists_postgres or exists_mssql:
                            spec["sql_exists"] = {
                                "duckdb": exists_duckdb,
                                "postgres": exists_postgres,
                                "mssql": exists_mssql,
                            }
                    except Exception as e:
                        log_exception(_logger, f"to_sql_exists failed for {getattr(rule, 'name', '?')}", e)

                return spec
        except Exception as e:
            log_exception(_logger, f"to_sql_agg failed for {getattr(rule, 'name', '?')}", e)

    # Priority 3: Built-in rule detection (fallback)
    raw_name = getattr(rule, "name", None)
    name = raw_name.split(":")[-1] if isinstance(raw_name, str) else raw_name
    params: Dict[str, Any] = getattr(rule, "params", {}) or {}

    if not (name and isinstance(params, dict)):
        return None

    if name == "not_null":
        col = params.get("column")
        if isinstance(col, str) and col:
            return {"kind": "not_null", "rule_id": rid, "column": col, "tally": rule_tally}

    if name == "unique":
        col = params.get("column")
        if isinstance(col, str) and col:
            return {"kind": "unique", "rule_id": rid, "column": col, "tally": rule_tally}

    if name == "min_rows":
        thr = params.get("value", params.get("threshold"))
        if isinstance(thr, int):
            # Dataset rules don't support tally
            return {"kind": "min_rows", "rule_id": rid, "threshold": int(thr)}

    if name == "max_rows":
        thr = params.get("value", params.get("threshold"))
        if isinstance(thr, int):
            # Dataset rules don't support tally
            return {"kind": "max_rows", "rule_id": rid, "threshold": int(thr)}

    if name == "allowed_values":
        col = params.get("column")
        values = params.get("values", [])
        if isinstance(col, str) and col and values:
            return {"kind": "allowed_values", "rule_id": rid, "column": col, "values": list(values), "tally": rule_tally}

    return None
