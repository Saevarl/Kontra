from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pyarrow.fs as pafs  # <-- Added
import pyarrow.parquet as pq

from .types import PrePlan, Decision

# NOTE: The preplan consumes simple, metadata-usable predicates only.
# Shape: (rule_id, column, op, value)
#   op ∈ {"==","!=",">=",">","<=","<","^=","not_null"}
#   "^=" means "string prefix"
Predicate = Tuple[str, str, str, Any]  # (rule_id, column, op, value)


# ---------- small helpers ----------

def _iso(v: Any) -> Any:
    if isinstance(v, (date, datetime)):
        return v.isoformat()
    return v


def _schema_names(md_schema) -> List[str]:
    # Compatible with various pyarrow versions
    try:
        return list(md_schema.names)
    except (AttributeError, TypeError):
        try:
            return [f.name for f in md_schema.to_arrow_schema()]
        except (AttributeError, TypeError):
            return []


def _rg_col_stats(rg, j) -> Optional[Dict[str, Any]]:
    """Return a safe dict of min/max/null_count for a row-group column j."""
    col = rg.column(j)
    stats = col.statistics
    if stats is None:
        return None
    out: Dict[str, Any] = {
        "min": _iso(getattr(stats, "min", None)) if getattr(stats, "has_min_max", True) else None,
        "max": _iso(getattr(stats, "max", None)) if getattr(stats, "has_min_max", True) else None,
    }
    if getattr(stats, "has_null_count", True):
        out["null_count"] = getattr(stats, "null_count", None)
    return out


def _name_for_rg_col(rg, j, fallback: str) -> str:
    try:
        # path_in_schema handles nested names properly
        return str(rg.column(j).path_in_schema)
    except (AttributeError, IndexError, TypeError):
        return fallback


# ---------- metadata reasoning (per predicate, per row group) ----------

def _verdict_overlaps(op: str, val: Any, stats: Optional[Dict[str, Any]]) -> Optional[bool]:
    """
    Return:
      - True  -> group MAY satisfy the predicate (cannot be ruled out by min/max)
      - False -> group CANNOT satisfy predicate (disjoint by min/max)
      - None  -> unknown (no stats)
    """
    if not stats or (stats.get("min") is None and stats.get("max") is None):
        return None
    mn, mx = stats.get("min"), stats.get("max")

    # Normalize type for string columns
    if isinstance(mn, str) and not isinstance(val, str):
        val = str(val)

    if op == "==":
        if mn is not None and mx is not None and (val < mn or val > mx):
            return False
        return True
    if op == "!=":
        return True  # min/max alone cannot rule "!=" out
    if op == ">=":
        return False if (mx is not None and mx < val) else True
    if op == "<=":
        return False if (mn is not None and mn > val) else True
    if op == ">":
        return False if (mx is not None and mx <= val) else True
    if op == "<":
        return False if (mn is not None and mn >= val) else True
    if op == "^=":  # string prefix: keep if ranges overlap the prefix window
        if not isinstance(mn, str) or not isinstance(mx, str):
            return None
        upper = str(val) + "\uffff"
        return not (upper < mn or str(val) > mx)
    if op == "not_null":
        # Overlap sense isn't meaningful; we handle not_null via _decide_fail/_decide_pass
        return None
    return None


def _decide_pass(op: str, val: Any, rg_stats_iter: Iterable[Optional[Dict[str, Any]]]) -> bool:
    """
    Can we *prove* that EVERY row in the file satisfies the predicate using only RG stats?
    (dataset-level "PASS" for that rule)
    """
    # For >= c: if for all rgs min >= c → pass
    # For <= c: if for all rgs max <= c → pass
    # For == c: if for all rgs (min==max==c) → pass
    # For not_null: if for all rgs null_count == 0 → pass
    ok_all = True
    for s in rg_stats_iter:
        if s is None:
            return False
        mn, mx = s.get("min"), s.get("max")
        if op == ">=":
            if mn is None or mn < val:
                ok_all = False; break
        elif op == "<=":
            if mx is None or mx > val:
                ok_all = False; break
        elif op == "==":
            if mn is None or mx is None or not (mn == val and mx == val):
                ok_all = False; break
        elif op == "not_null":
            # Can only prove PASS if null_count is exactly 0 for all row groups
            # null_count > 0 means violations exist; None means unknown (can't prove)
            if s.get("null_count") != 0:
                ok_all = False; break
        else:
            # For >, <, !=, ^= we don't try to prove dataset-level PASS via min/max
            ok_all = False; break
    return ok_all


def _decide_fail(op: str, val: Any, rg_stats_iter: Iterable[Optional[Dict[str, Any]]]) -> bool:
    """
    Can we *prove* that AT LEAST ONE row violates the predicate using RG stats?
    (dataset-level "FAIL" for that rule)
    """
    for s in rg_stats_iter:
        if s is None:
            continue
        mn, mx = s.get("min"), s.get("max")
        if op == ">=":
            # If an RG has mx < val ⇒ all rows in that RG violate ⇒ dataset FAIL
            if mx is not None and mx < val:
                return True
        elif op == "<=":
            if mn is not None and mn > val:
                return True
        elif op == "==":
            # If an RG has range entirely not equal to val ⇒ all rows in that RG violate
            if mn is not None and mx is not None and (mx < val or mn > val or (mn == mx and mn != val)):
                return True
        elif op == "not_null":
            # Any rg with null_count > 0 proves at least one violation
            nulls = s.get("null_count")
            if isinstance(nulls, int) and nulls > 0:
                return True
        # For >, <, !=, ^= we typically cannot prove dataset-level FAIL with min/max alone.
    return False


# ---------- public API ----------

def preplan_single_parquet(
    path: str,
    required_columns: List[str],
    predicates: List[Predicate],
    filesystem: pafs.FileSystem | None = None,  # <-- Updated
) -> PrePlan:
    """
    Metadata-only pre-planner for a SINGLE Parquet file.

    Inputs:
      - path:             Parquet file path/URI
      - required_columns: union of columns needed for *all* rules (from your CompiledPlan)
      - predicates:       metadata-usable predicates -> List[(rule_id, column, op, value)]
      - filesystem:       PyArrow filesystem object (e.g., for S3)

    Outputs (PrePlan):
      - manifest_row_groups: RG indices that STILL MATTER for remaining rules
      - manifest_columns:    columns still needed (you can pass through required_columns)
      - rule_decisions:      rule_id -> "pass_meta" | "fail_meta" | "unknown"
      - stats:               {"rg_total": N, "rg_kept": K}
    """
    pf = pq.ParquetFile(path, filesystem=filesystem)  # <-- Updated
    md = pf.metadata
    schema_names = _schema_names(md.schema)

    # Pre-extract per-RG per-column stats into a simple map:
    # rg_stats[i][col_name] -> {"min":..., "max":..., "null_count":...}
    rg_stats: List[Dict[str, Dict[str, Any]]] = []
    for i in range(md.num_row_groups):
        rg = md.row_group(i)
        per_col: Dict[str, Dict[str, Any]] = {}
        for j in range(rg.num_columns):
            name = _name_for_rg_col(rg, j, schema_names[j] if j < len(schema_names) else f"col_{j}")
            s = _rg_col_stats(rg, j)
            if s is not None:
                per_col[name] = s
        rg_stats.append(per_col)

    # Decide each rule at dataset-level (PASS/FAIL/UNKNOWN by metadata)
    rule_decisions: Dict[str, Decision] = {}
    for rule_id, col, op, val in predicates:
        stats_iter = (rgc.get(col) for rgc in rg_stats)
        if _decide_fail(op, val, stats_iter):
            rule_decisions[rule_id] = "fail_meta"
            continue
        # need a fresh iterator
        stats_iter = (rgc.get(col) for rgc in rg_stats)
        if _decide_pass(op, val, stats_iter):
            rule_decisions[rule_id] = "pass_meta"
        else:
            rule_decisions[rule_id] = "unknown"

    # Determine which RGs we still need to scan (conservative):
    # - If no predicates at all -> keep ALL RGs.
    # - Else keep any RG that *might* be relevant for at least one UNKNOWN rule.
    keep_rg: List[int] = list(range(md.num_row_groups))
    unknown_preds = [(rid, col, op, val) for (rid, col, op, val) in predicates if rule_decisions.get(rid) == "unknown"]

    if unknown_preds:
        keep_rg = []
        for i, per_col in enumerate(rg_stats):
            # Keep if ANY unknown predicate "may overlap"
            keep = False
            for _, col, op, val in unknown_preds:
                verdict = _verdict_overlaps(op, val, per_col.get(col))
                # Verdict True  -> overlaps; Verdict None -> unknown -> keep to be safe
                if verdict is True or verdict is None:
                    keep = True
                    break
            if keep:
                keep_rg.append(i)
        if not keep_rg:
            # Safety: if overlap logic ended up too strict, default to ALL
            keep_rg = list(range(md.num_row_groups))

    preplan = PrePlan(
        manifest_columns=list(required_columns) if required_columns else [],
        manifest_row_groups=keep_rg,
        rule_decisions=rule_decisions,
        stats={
            "rg_total": md.num_row_groups,
            "rg_kept": len(keep_rg),
            "total_rows": md.num_rows,
        },
    )
    return preplan