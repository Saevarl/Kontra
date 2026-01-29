# src/kontra/rules/static_predicates.py
from __future__ import annotations

from typing import Any, Dict, Iterable, List, Tuple

from kontra.rule_defs.base import BaseRule

# (rule_id, column, op, value)  -- op ∈ ALLOWED_OPS
PredicateT = Tuple[str, str, str, Any]
ALLOWED_OPS = {"==", "!=", ">=", ">", "<=", "<", "^=", "not_null"}


def _normalize(pairs: Iterable[PredicateT]) -> List[PredicateT]:
    """Validate and normalize a stream of preplan predicates."""
    out: List[PredicateT] = []
    seen: set[Tuple[str, str, str, Any]] = set()
    for rid, col, op, val in pairs:
        if not isinstance(rid, str) or not rid:
            continue
        if not isinstance(col, str) or not col:
            continue
        if op not in ALLOWED_OPS:
            continue
        key = (rid, col, op, val)
        if key in seen:
            continue
        seen.add(key)
        out.append((rid, col, op, val))
    return out


def _from_rule_hook(rule: BaseRule) -> List[PredicateT]:
    """Ask the rule itself (if it implements the optional hook)."""
    fn = getattr(rule, "to_preplan_predicates", None)
    if callable(fn):
        try:
            preds = fn() or []
        except (ValueError, TypeError, AttributeError):
            # Hook implementation failed - skip this rule's predicates
            preds = []
        # Ensure each tuple starts with this rule's rule_id
        fixed: List[PredicateT] = []
        for item in preds:
            if not isinstance(item, tuple) or len(item) != 4:
                continue
            rid, col, op, val = item
            # Allow rule to omit rid; fill it in
            if not isinstance(rid, str) or not rid:
                rid = getattr(rule, "rule_id", getattr(rule, "name", ""))
            fixed.append((rid, col, op, val))
        return fixed
    return []


def _conservative_builtin_mapping(rule: BaseRule) -> List[PredicateT]:
    """
    Optional mapping for known built-ins, so you don't have to add hooks yet.
    Keep this conservative and obvious (no regex engines etc.).
    """
    name = getattr(rule, "name", "")
    params: Dict[str, Any] = getattr(rule, "params", {}) or {}
    rid = getattr(rule, "rule_id", name)

    out: List[PredicateT] = []

    # not_null(column)
    if name.endswith("not_null"):
        col = params.get("column")
        if isinstance(col, str) and col:
            out.append((rid, col, "not_null", True))

    # equals / allowed_values (single value)
    if name in {"equals", "allowed_values"}:
        col = params.get("column")
        val = params.get("value", None)
        if val is None:
            vals = params.get("values")
            if isinstance(vals, (list, tuple)) and len(vals) == 1:
                val = vals[0]
        if isinstance(col, str) and col and isinstance(val, (str, int, float)):
            out.append((rid, col, "==", val))

    # min / max / range style (very conservative)
    if name in {"gte", "min_value", "min"}:
        col = params.get("column"); v = params.get("value")
        if isinstance(col, str) and col and v is not None:
            out.append((rid, col, ">=", v))
    if name in {"lte", "max_value", "max"}:
        col = params.get("column"); v = params.get("value")
        if isinstance(col, str) and col and v is not None:
            out.append((rid, col, "<=", v))
    if name == "range":
        col = params.get("column")
        min_v = params.get("min")
        max_v = params.get("max")
        if isinstance(col, str) and col:
            if min_v is not None:
                out.append((rid, col, ">=", min_v))
            if max_v is not None:
                out.append((rid, col, "<=", max_v))

    # regex("^prefix") → prefix
    if name == "regex":
        col = params.get("column")
        pat = params.get("pattern", "")
        if isinstance(col, str) and col and isinstance(pat, str) and pat.startswith("^"):
            # only allow pure-prefix (no special chars beyond anchors)
            body = pat[1:]
            if body and all(ch.isalnum() or ch in {"_", "-", ".", "@"} for ch in body):
                out.append((rid, col, "^=", body))

    return out


def extract_static_predicates_from_rules(rules: List[BaseRule]) -> List[PredicateT]:
    """
    Preferred entry point: pass the ORIGINAL rule instances (from RuleFactory).
    We ask each rule for an optional hook, then apply a conservative builtin mapping.
    """
    pairs: List[PredicateT] = []
    for r in rules:
        pairs.extend(_from_rule_hook(r))
        pairs.extend(_conservative_builtin_mapping(r))
    return _normalize(pairs)


# Backward-compatible shim if you still want a function named extract_static_predicates
# and you have access to the original rules alongside the compiled plan.
def extract_static_predicates(*, rules: List[BaseRule]) -> List[PredicateT]:
    return extract_static_predicates_from_rules(rules)
