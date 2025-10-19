# src/contra/rules/execution_plan.py
from __future__ import annotations
from dataclasses import dataclass
from typing import Iterable, List, Dict, Any, Optional, Set

import polars as pl

from kontra.rules.base import BaseRule
from kontra.rules.predicates import Predicate


@dataclass
class CompiledPlan:
    """
    Output of planning/compilation.

    - predicates:      vectorizable rule predicates (Polars expr) we can run in a single pass
    - fallback_rules:  rules that couldn't be vectorized (handled individually)
    - required_cols:   union of all columns needed by predicates + fallbacks (for projection)
    - sql_rules:       specs for rules that can be evaluated as single-row SQL aggregates
                       by a SQL backend (e.g., DuckDB). Polars ignores this.
    """
    predicates: List[Predicate]
    fallback_rules: List[BaseRule]
    required_cols: List[str]
    sql_rules: List[Dict[str, Any]]


class RuleExecutionPlan:
    """
    Builds and executes a plan for the given rules.

    Design goals:
    - Deterministic: same inputs → same outputs
    - Lean: compilation discovers vectorizable work + required columns
    - Extensible: optional sql_rules for SQL backends (no behavior change for Polars)
    """

    def __init__(self, rules: List[BaseRule]):
        self.rules = rules

    def __str__(self) -> str:
            if not self.rules:
                return "RuleExecutionPlan(rules=[])"
            
            # Use repr(r) for each rule, which will call BaseRule.__repr__
            # and format it as 'rule_name(params)'
            rules_list = [repr(r) for r in self.rules]
            
            # Join them with a newline and indentation
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
            # Try vectorization (Polars)
            pred = _try_compile_predicate(rule)
            if pred is None:
                fallbacks.append(rule)
            else:
                _validate_predicate(pred)
                if pred.rule_id != rule.rule_id:
                    raise ValueError(
                        f"Predicate.rule_id '{pred.rule_id}' does not match rule.rule_id '{rule.rule_id}'."
                    )
                predicates.append(pred)

            # Optional SQL spec (minimal subset, non-fatal if not applicable)
            spec = _maybe_rule_sql_spec(rule)
            if spec:
                sql_rules.append(spec)

        # Derive required columns for projection
        cols_pred = _collect_required_columns(predicates)
        cols_fb: Set[str] = set()
        for r in fallbacks:
            try:
                cols_fb.update(r.required_columns() or set())
            except Exception:
                # Be conservative: ignore errors here; rule will error during validate() if broken.
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
        return vec_results + fb_results

    def execute(self, df: pl.DataFrame) -> List[Dict[str, Any]]:
        compiled = self.compile()
        return self.execute_compiled(df, compiled)

    def summary(self, results: List[Dict[str, Any]]) -> Dict[str, Any]:
        total = len(results)
        failed = sum(1 for r in results if not r.get("passed", False))
        return {"total_rules": total, "rules_failed": failed, "rules_passed": total - failed, "passed": failed == 0}

    # ------------------------ New helper APIs (PR-2) --------------------------

    def without_ids(self, compiled: CompiledPlan, handled_ids: Set[str]) -> CompiledPlan:
        """
        Return a new CompiledPlan with any rules whose rule_id is in `handled_ids` removed.

        Used by the hybrid path: SQL executor handles a subset; the remainder (residual)
        must still compute required_cols so projection works for Polars.
        """
        resid_preds = [p for p in compiled.predicates if p.rule_id not in handled_ids]
        resid_fallbacks = [r for r in compiled.fallback_rules if getattr(r, "rule_id", r.name) not in handled_ids]

        cols_pred = _collect_required_columns(resid_preds)
        cols_fb: Set[str] = set()
        for r in resid_fallbacks:
            try:
                cols_fb.update(r.required_columns() or set())
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
        """
        Compute required columns for *this* compiled plan (predicates + fallbacks).
        """
        return list(compiled.required_cols)


# ------------------------------ Helpers --------------------------------------

def _try_compile_predicate(rule: BaseRule) -> Optional[Predicate]:
    fn = getattr(rule, "compile_predicate", None)
    if fn is None:
        return None
    try:
        return fn() or None
    except Exception:
        return None


def _collect_required_columns(preds: Iterable[Predicate]) -> Set[str]:
    cols: Set[str] = set()
    for p in preds:
        cols.update(p.columns)
    return cols


def _validate_predicate(pred: Predicate) -> None:
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
    """
    # If the rule offers its own spec, prefer that.
    to_sql = getattr(rule, "to_sql_spec", None)
    if callable(to_sql):
        try:
            spec = to_sql()
            if spec:
                return spec
        except Exception:
            return None

    name = getattr(rule, "name", None)
    params: Dict[str, Any] = getattr(rule, "params", {}) or {}
    rid = getattr(rule, "rule_id", None)

    if not (name and isinstance(params, dict) and isinstance(rid, str)):
        return None

    if name == "not_null":
        col = params.get("column")
        if isinstance(col, str) and col:
            return {"kind": "not_null", "rule_id": rid, "column": col}

    if name == "min_rows":
        thr = params.get("threshold")
        if isinstance(thr, int):
            return {"kind": "min_rows", "rule_id": rid, "threshold": thr}

    if name == "max_rows":
        thr = params.get("threshold")
        if isinstance(thr, int):
            return {"kind": "max_rows", "rule_id": rid, "threshold": thr}

    return None


