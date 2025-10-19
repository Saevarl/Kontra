from __future__ import annotations
from dataclasses import dataclass
from typing import Iterable, Dict, Any, List, Tuple
import time, uuid
import polars as pl

@dataclass
class RunTimers:
    contract_load_ms: int = 0
    data_load_ms: int = 0
    compile_ms: int = 0
    execute_ms: int = 0
    report_ms: int = 0

def now_ms() -> int:
    return int(time.time() * 1000)

def basic_summary(df: pl.DataFrame) -> Dict[str, int]:
    return {"nrows": int(df.height), "ncols": int(len(df.columns))}

def columns_touched(rule_specs: Iterable[Dict[str, Any]]) -> List[str]:
    cols = []
    for r in rule_specs:
        p = r.get("params", {})
        c = p.get("column")
        if isinstance(c, str):
            cols.append(c)
    # preserve order but unique
    seen = set(); ordered = []
    for c in cols:
        if c not in seen:
            seen.add(c); ordered.append(c)
    return ordered

def profile_for(df: pl.DataFrame, cols: List[str]) -> Dict[str, Dict[str, Any]]:
    if not cols:
        return {}
    exprs = []
    for c in cols:
        s = df.get_column(c)
        # common stats by dtype family
        # NOTE: these are fast vectorized reductions; single pass select
        e = [
            pl.col(c).is_null().sum().alias(f"__nulls__{c}"),
            pl.col(c).n_unique().alias(f"__distinct__{c}"),
        ]
        if pl.datatypes.is_numeric(s.dtype):
            e += [
                pl.col(c).min().alias(f"__min__{c}"),
                pl.col(c).max().alias(f"__max__{c}"),
                pl.col(c).mean().alias(f"__mean__{c}"),
            ]
        exprs.extend(e)

    out = df.select(exprs)
    row = out.row(0)
    # rebuild structured dict
    stats: Dict[str, Dict[str, Any]] = {}
    for c in cols:
        d = {
            "nulls": int(row[out.find_idx_by_name(f"__nulls__{c}")]),
            "distinct": int(row[out.find_idx_by_name(f"__distinct__{c}")]),
        }
        if f"__min__{c}" in out.columns:
            d["min"]  = row[out.find_idx_by_name(f"__min__{c}")]
            d["max"]  = row[out.find_idx_by_name(f"__max__{c}")]
            d["mean"] = float(row[out.find_idx_by_name(f"__mean__{c}")])
        stats[c] = d
    return stats
