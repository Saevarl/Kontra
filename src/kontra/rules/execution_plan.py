# src/kontra/rules/execution_plan.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, List, Dict, Any, Optional, Set

import polars as pl

from kontra.rules.base import BaseRule
from kontra.rules.predicates import Predicate


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
        cols_fb: Set[str] = set()

        for r in fallbacks:
            try:
                # Prefer explicit declaration from the rule
                cols = r.required_columns() or set()
                if not cols:
                    # Heuristic: infer from common param names when not declared
                    p = getattr(r, "params", {}) or {}
                    col = p.get("column")
                    cols_list = p.get("columns")
                    if isinstance(col, str) and col:
                        cols.add(col)
                    if isinstance(cols_list, (list, tuple)):
                        cols.update(c for c in cols_list if isinstance(c, str))
                cols_fb.update(cols)
            except Exception:
                # Be conservative: ignore here; rule will raise during validate() if broken.
                pass

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
        vec_results: List[Dict[str, Any]] = []
        if compiled.predicates:
            # Sum boolean violations per predicate in a single select pass
            counts_df = df.select([p.expr.sum().alias(p.rule_id) for p in compiled.predicates])
            counts = counts_df.row(0, named=True)
            for p in compiled.predicates:
                failed_count = int(counts[p.rule_id])
                vec_results.append(
                    {
                        "rule_id": p.rule_id,
                        "passed": failed_count == 0,
                        "failed_count": failed_count,
                        "message": p.message,
                    }
                )

        fb_results: List[Dict[str, Any]] = []
        for r in compiled.fallback_rules:
            try:
                fb_results.append(r.validate(df))
            except Exception as e:
                fb_results.append(
                    {
                        "rule_id": getattr(r, "rule_id", r.name),
                        "passed": False,
                        "failed_count": int(df.height),
                        "message": f"Rule execution failed: {e}",
                    }
                )

        # Deterministic order: predicates first, then fallbacks
        return vec_results + fb_results

    def execute(self, df: pl.DataFrame) -> List[Dict[str, Any]]:
        """Compile and execute in one step (Polars-only path)."""
        compiled = self.compile()
        return self.execute_compiled(df, compiled)

    def summary(self, results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Aggregate pass/fail counts for reporters."""
        total = len(results)
        failed = sum(1 for r in results if not r.get("passed", False))
        return {
            "total_rules": total,
            "rules_failed": failed,
            "rules_passed": total - failed,
            "passed": failed == 0,
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
        cols_fb: Set[str] = set()
        for r in resid_fallbacks:
            try:
                cols = r.required_columns() or set()
                if not cols:
                    p = getattr(r, "params", {}) or {}
                    col = p.get("column")
                    cols_list = p.get("columns")
                    if isinstance(col, str) and col:
                        cols.add(col)
                    if isinstance(cols_list, (list, tuple)):
                        cols.update(c for c in cols_list if isinstance(c, str))
                cols_fb.update(cols)
            except Exception:
                pass

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
    except Exception:
        return None


def _collect_required_columns(preds: Iterable[Predicate]) -> Set[str]:
    """Union the required columns declared by each predicate."""
    cols: Set[str] = set()
    for p in preds:
        cols.update(p.columns)
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

    v1 scope (keep it very small):
      - not_null(column)
      - min_rows(threshold)
      - max_rows(threshold)

    Notes
    -----
    - If a rule provides `to_sql_spec()`, that takes precedence.
    - We normalize namespaced rule names, e.g. "DATASET:not_null" → "not_null".
    - For min/max rows, accept both `value` and `threshold` to match existing contracts.
    """
    # Prefer a rule-provided spec
    to_sql = getattr(rule, "to_sql_spec", None)
    if callable(to_sql):
        try:
            spec = to_sql()
            if spec:
                return spec
        except Exception:
            return None

    # Normalize and extract context
    raw_name = getattr(rule, "name", None)
    name = raw_name.split(":")[-1] if isinstance(raw_name, str) else raw_name
    params: Dict[str, Any] = getattr(rule, "params", {}) or {}
    rid = getattr(rule, "rule_id", None)

    if not (name and isinstance(params, dict) and isinstance(rid, str)):
        return None

    if name == "not_null":
        col = params.get("column")
        if isinstance(col, str) and col:
            return {"kind": "not_null", "rule_id": rid, "column": col}

    if name == "min_rows":
        thr = params.get("value", params.get("threshold"))
        if isinstance(thr, int):
            return {"kind": "min_rows", "rule_id": rid, "threshold": int(thr)}

    if name == "max_rows":
        thr = params.get("value", params.get("threshold"))
        if isinstance(thr, int):
            return {"kind": "max_rows", "rule_id": rid, "threshold": int(thr)}

    return None
