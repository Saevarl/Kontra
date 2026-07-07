# src/kontra/engine/phases/merge.py
"""
Result merging phase.

Combines results from preplan, SQL pushdown, and Polars execution
into a single deterministic result list.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from kontra.config.models import Contract
    from kontra.engine.types import (
        CompilationContext,
        PreplanResult,
        PushdownResult,
        ResidualResult,
    )
    from kontra.rule_defs.execution_plan import RuleExecutionPlan


def merge_results(
    preplan: "PreplanResult",
    pushdown: "PushdownResult",
    residual: "ResidualResult",
    ctx: "CompilationContext",
) -> List[Dict[str, Any]]:
    """
    Merge results from all execution tiers.

    Order is deterministic: preplan → SQL → Polars.
    Each rule appears only once (first tier that handles it wins).

    Args:
        preplan: Preplan phase results
        pushdown: SQL pushdown phase results
        residual: Polars execution phase results
        ctx: Compilation context with severity/tally/context maps

    Returns:
        Merged list of rule results
    """
    # Start with preplan results
    results: List[Dict[str, Any]] = list(preplan.results_by_id.values())

    # Add SQL results (skip if already in preplan)
    for r in pushdown.results_by_id.values():
        if r["rule_id"] not in preplan.results_by_id:
            results.append(r)

    # Add Polars results (skip if already handled)
    for r in residual.results:
        if r["rule_id"] not in preplan.results_by_id and r["rule_id"] not in pushdown.results_by_id:
            r["severity"] = ctx.severity_map.get(r["rule_id"], "blocking")
            r["tally"] = ctx.tally_map.get(r["rule_id"], False)
            results.append(r)

    # Inject context into all results
    for r in results:
        context = ctx.context_map.get(r["rule_id"])
        if context:
            r["context"] = context

    return results


def build_summary(
    results: List[Dict[str, Any]],
    plan: "RuleExecutionPlan",
    contract: Optional["Contract"],
    row_count: Optional[int],
    df_height: Optional[int],
    preplan_total_rows: Optional[int],
) -> Dict[str, Any]:
    """
    Build validation summary from merged results.

    Args:
        results: Merged rule results
        plan: Execution plan (has summary() method)
        contract: Contract object (for dataset name)
        row_count: Row count from SQL executor
        df_height: Row count from loaded DataFrame
        preplan_total_rows: Row count from preplan metadata

    Returns:
        Summary dict with pass/fail counts and dataset info
    """
    summary = plan.summary(results)

    # Dataset name
    if contract is None:
        summary["dataset_name"] = "dataframe"
    elif contract.name:
        summary["dataset_name"] = contract.name
    else:
        summary["dataset_name"] = contract.datasource

    # Row count priority: SQL executor > DataFrame > preplan metadata > 0
    if row_count is not None:
        summary["total_rows"] = int(row_count)
    elif df_height is not None:
        summary["total_rows"] = int(df_height)
    elif preplan_total_rows is not None:
        summary["total_rows"] = int(preplan_total_rows)
    else:
        summary["total_rows"] = 0

    return summary
