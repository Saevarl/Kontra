# src/kontra/rules/execution_plan.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, List, Dict, Any, Optional, Set

import polars as pl

from kontra.rules.base import BaseRule
from kontra.rules.predicates import Predicate
from kontra.logging import get_logger, log_exception

_logger = get_logger(__name__)


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

    def execute_compiled(self, df: pl.DataFrame, compiled: CompiledPlan) -> List[Dict[str, Any]]:
        """
        Execute the compiled plan using Polars:
          - vectorized pass for predicates
          - individual validation for fallback rules
        """
        # Build rule_id -> severity mapping for predicates
        rule_severity_map = self._build_severity_map()
        available_cols = set(df.columns)

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
            if valid_predicates:
                counts_df = df.select([p.expr.sum().alias(p.rule_id) for p in valid_predicates])
                counts = counts_df.row(0, named=True)
                for p in valid_predicates:
                    failed_count = int(counts[p.rule_id])
                    passed = failed_count == 0
                    vec_results.append(
                        {
                            "rule_id": p.rule_id,
                            "passed": passed,
                            "failed_count": failed_count,
                            "message": "Passed" if passed else p.message,
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

    def execute(self, df: pl.DataFrame) -> List[Dict[str, Any]]:
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
    """Type/shape checks for a Predicate returned by a rule."""
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
    """
    rid = getattr(rule, "rule_id", None)
    if not isinstance(rid, str):
        return None

    # Priority 1: Rule-provided spec (full control)
    to_sql = getattr(rule, "to_sql_spec", None)
    if callable(to_sql):
        try:
            spec = to_sql()
            if spec:
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
                return {
                    "kind": "custom_agg",
                    "rule_id": rid,
                    "sql_agg": {
                        "duckdb": agg_duckdb,
                        "postgres": agg_postgres,
                        "mssql": agg_mssql,
                    },
                }
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
            return {"kind": "not_null", "rule_id": rid, "column": col}

    if name == "unique":
        col = params.get("column")
        if isinstance(col, str) and col:
            return {"kind": "unique", "rule_id": rid, "column": col}

    if name == "min_rows":
        thr = params.get("value", params.get("threshold"))
        if isinstance(thr, int):
            return {"kind": "min_rows", "rule_id": rid, "threshold": int(thr)}

    if name == "max_rows":
        thr = params.get("value", params.get("threshold"))
        if isinstance(thr, int):
            return {"kind": "max_rows", "rule_id": rid, "threshold": int(thr)}

    if name == "allowed_values":
        col = params.get("column")
        values = params.get("values", [])
        if isinstance(col, str) and col and values:
            return {"kind": "allowed_values", "rule_id": rid, "column": col, "values": list(values)}

    return None
