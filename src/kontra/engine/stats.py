# src/kontra/engine/stats.py
from __future__ import annotations

"""
Stats helpers — minimal, fast, and CLI-friendly.

Design goals
------------
- Keep helpers tiny and zero-alloc heavy; these run on every validation.
- Avoid coupling to engine internals or reporters; return plain dicts.
- Backwards compatible: existing callers keep working as-is.
"""

from dataclasses import dataclass
from typing import TYPE_CHECKING, Iterable, Dict, Any, List, Optional
import time

if TYPE_CHECKING:
    import polars as pl


# ----------------------------- Timers -----------------------------------------


@dataclass
class RunTimers:
    contract_load_ms: int = 0
    data_load_ms: int = 0
    compile_ms: int = 0
    execute_ms: int = 0
    report_ms: int = 0
    polars_ms: int = 0
    preplan_ms: int = 0
    sql_ms: int = 0

    def total_ms(self) -> int:
        """Total time across all phases."""
        return (
            self.contract_load_ms
            + self.data_load_ms
            + self.compile_ms
            + self.execute_ms
            + self.report_ms
            + self.polars_ms
            + self.preplan_ms
            + self.sql_ms
        )


def now_ms() -> int:
    return int(time.time() * 1000)


# ---------------------------- Summaries ---------------------------------------


def basic_summary(
    df: Optional["pl.DataFrame"],
    *,
    available_cols: Optional[List[str]] = None,
    nrows_override: Optional[int] = None,
) -> Dict[str, int]:
    """
    Return a tiny dataset summary.

    Args
    ----
    df:
        The (possibly pruned) Polars DataFrame. May be None when we
        skipped materialization (e.g., all rules pushed down).
    available_cols:
        Full schema columns if known (e.g., via SQL introspection or a cheap scan).
        When provided, we report ncols = len(available_cols) instead of df width,
        so the CLI consistently shows *total* columns, not just loaded.
    nrows_override:
        Authoritative row count (e.g., from SQL COUNT(*)) to avoid collecting df.height.

    Returns
    -------
    {"nrows": int, "ncols": int}
    """
    if df is None:
        nrows = int(nrows_override or 0)
        ncols = int(len(available_cols or []))
        return {"nrows": nrows, "ncols": ncols}

    nrows = int(nrows_override if nrows_override is not None else df.height)
    ncols = int(len(available_cols)) if available_cols is not None else int(len(df.columns))
    return {"nrows": nrows, "ncols": ncols}


def columns_touched(rule_specs: Iterable[Dict[str, Any]]) -> List[str]:
    """
    Ordered de-duplicated list of columns referenced by rules.
    """
    cols: List[str] = []
    seen: set[str] = set()
    for r in rule_specs:
        col = r.get("params", {}).get("column")
        if isinstance(col, str) and col and col not in seen:
            seen.add(col)
            cols.append(col)
    return cols


def build_coverage(
    *,
    total_rules: int,
    sql_results: Dict[str, Dict[str, Any]] | List[Dict[str, Any]],
    polars_results: List[Dict[str, Any]],
    validated_columns: List[str],
) -> Dict[str, Any]:
    """
    Compact, renderer-friendly coverage block.

    Returns
    -------
    {
      "rules_total": int,
      "rules_sql": int, "rules_failed_sql": int,
      "rules_polars": int, "rules_failed_polars": int,
      "validated_columns": [...],
    }
    """
    # Allow either a dict-by-id or a flat list for sql_results
    if isinstance(sql_results, dict):
        sql_vals = list(sql_results.values())
    else:
        sql_vals = list(sql_results or [])

    rules_sql = len(sql_vals)
    rules_failed_sql = sum(1 for r in sql_vals if not r.get("passed", False))

    rules_polars = len(polars_results or [])
    rules_failed_polars = sum(1 for r in polars_results or [] if not r.get("passed", False))

    return {
        "rules_total": int(total_rules),
        "rules_sql": int(rules_sql),
        "rules_failed_sql": int(rules_failed_sql),
        "rules_polars": int(rules_polars),
        "rules_failed_polars": int(rules_failed_polars),
        "validated_columns": list(validated_columns or []),
    }


# ------------------------------ Profiling -------------------------------------


def profile_for(df: "pl.DataFrame", cols: List[str]) -> Dict[str, Dict[str, Any]]:
    """
    Lightweight, single-pass column profile for touched columns only.

    Raises:
        ImportError: If polars is not installed.
    """
    if not cols:
        return {}

    try:
        import polars as pl  # Lazy import - profiling is optional
    except ImportError as e:
        raise ImportError(
            "Polars is required for profiling but is not installed. "
            "Install with: pip install polars"
        ) from e

    exprs: List[pl.Expr] = []
    for c in cols:
        # common stats by dtype family
        e = [
            pl.col(c).is_null().sum().alias(f"__nulls__{c}"),
            pl.col(c).n_unique().alias(f"__distinct__{c}"),
        ]
        # numeric extras
        try:
            s = df.get_column(c)
            if pl.datatypes.is_numeric(s.dtype):
                e += [
                    pl.col(c).min().alias(f"__min__{c}"),
                    pl.col(c).max().alias(f"__max__{c}"),
                    pl.col(c).mean().alias(f"__mean__{c}"),
                ]
        except (pl.exceptions.ColumnNotFoundError, KeyError):
            # column missing (shouldn't happen if projection is correct) — skip extras
            pass
        exprs.extend(e)

    out = df.select(exprs)
    if out.height == 0:
        return {}

    # Use named=True to get row as dict for direct column access
    row = out.row(0, named=True)
    stats: Dict[str, Dict[str, Any]] = {}
    for c in cols:
        d: Dict[str, Any] = {
            "nulls": int(row[f"__nulls__{c}"]),
            "distinct": int(row[f"__distinct__{c}"]),
        }
        # Only attach numeric extras if these columns exist in the projection
        if f"__min__{c}" in out.columns:
            d["min"] = row[f"__min__{c}"]
            d["max"] = row[f"__max__{c}"]
            d["mean"] = float(row[f"__mean__{c}"])
        stats[c] = d
    return stats
