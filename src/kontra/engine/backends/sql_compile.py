# src/contra/engine/backends/sql_compile.py

from __future__ import annotations
from typing import Dict, List
"""
DuckDB SQL compilation helpers.

This module converts a *subset* of Contra rules into a single DuckDB SQL query
that returns exactly one row, where each column name == rule_id and the value
is the failed_count for that rule.

Scope (v1): not_null, allowed_values, regex, dtype(strict), unique,
            dataset-level min_rows / max_rows.

Anything not representable is skipped here and should be handled by the
Polars backend (hybrid mode). Keep semantics aligned with Polars:
- NULLs count as failures for not_null and regex
- dtype(strict) asserts the physical/storage type (no casts)
- unique counts all duplicate rows (not just existence)
"""


def _esc(s: str) -> str:
    return s.replace("'", "''")

# Aggregate builders (subset v1)
def agg_not_null(col: str, rule_id: str) -> str:
    return f"SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END) AS \"{rule_id}\""

def agg_min_rows(n: int, rule_id: str) -> str:
    return f"GREATEST(0, {int(n)} - COUNT(*)) AS \"{rule_id}\""

def agg_max_rows(n: int, rule_id: str) -> str:
    return f"GREATEST(0, COUNT(*) - {int(n)}) AS \"{rule_id}\""

def assemble_single_row(selects: List[str]) -> str:
    if not selects:
        return "SELECT 0 AS __no_sql_rules__ LIMIT 1;"
    ctes, aliases = [], []
    for i, sel in enumerate(selects):
        name = f"a{i}"
        ctes.append(f"{name} AS (SELECT {sel} FROM _data)")
        aliases.append(name)
    with_clause = "WITH " + ", ".join(ctes)
    cross = " CROSS JOIN ".join(aliases)
    return f"{with_clause} SELECT * FROM {cross};"

def sql_for_plan(compiled_plan) -> str:
    """
    Expect compiled_plan.sql_rules (list[dict]) with items like:
      {"kind":"not_null","rule_id":"COL:email:not_null","column":"email"}
      {"kind":"min_rows","rule_id":"DATASET:min_rows","threshold":100}
    """
    selects: List[str] = []
    for r in getattr(compiled_plan, "sql_rules", []) or []:
        k = r.get("kind"); rid = r.get("rule_id")
        if not k or not rid:
            continue
        if k == "not_null":
            selects.append(agg_not_null(r["column"], rid))
        elif k == "min_rows":
            selects.append(agg_min_rows(int(r["threshold"]), rid))
        elif k == "max_rows":
            selects.append(agg_max_rows(int(r["threshold"]), rid))
        else:
            # unsupported in v1 of DuckDB wiring
            continue
    return assemble_single_row(selects)

def results_from_single_row(df) -> list[dict]:
    if df.shape[0] != 1:
        raise ValueError(f"Expected a single-row result, got {df.shape}")
    row = df.iloc[0]
    out = []
    for rule_id, failed in row.items():
        if rule_id == "__no_sql_rules__":
            continue
        failed_count = int(failed) if failed is not None else 0
        out.append({
            "rule_id": rule_id,
            "passed": failed_count == 0,
            "failed_count": failed_count,
            "message": "Passed" if failed_count == 0 else "Failed",
            "severity": "ERROR",
            "actions_executed": [],
        })
    return out
