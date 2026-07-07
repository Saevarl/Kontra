# src/kontra/engine/phases/pushdown.py
"""
SQL pushdown phase.

Executes eligible rules via SQL (DuckDB, PostgreSQL, SQL Server)
to avoid loading data into memory.
"""

from __future__ import annotations

from typing import Any, Literal, Optional, Set, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from kontra.connectors.handle import DatasetHandle
    from kontra.engine.types import CompilationContext

from kontra.engine.types import PushdownResult
from kontra.engine.stats import now_ms
from kontra.logging import get_logger

_logger = get_logger(__name__)


def _empty_pushdown_result() -> PushdownResult:
    """Create an empty pushdown result (pushdown disabled or not applicable)."""
    return PushdownResult(
        effective=False,
        handled_ids=set(),
        results_by_id={},
    )


def execute_pushdown(
    handle: "DatasetHandle",
    ctx: "CompilationContext",
    handled_ids_meta: Set[str],
    pushdown_mode: str,
    csv_mode: Literal["auto", "duckdb", "parquet"],
    show_plan: bool = False,
) -> Tuple[PushdownResult, "DatasetHandle", Optional[Any]]:
    """
    Execute the SQL pushdown phase.

    Args:
        handle: Dataset handle with URI and connection info
        ctx: Compilation context with rules and tally settings
        handled_ids_meta: Rule IDs already handled by preplan
        pushdown_mode: "on" or "off"
        csv_mode: CSV handling mode
        show_plan: Print SQL plan

    Returns:
        Tuple of (PushdownResult, updated_handle, staging_tmpdir)
        - updated_handle may be different if CSV was staged to Parquet
        - staging_tmpdir needs cleanup after run
    """
    from kontra.engine.executors.registry import pick_executor

    if pushdown_mode != "on":
        return _empty_pushdown_result(), handle, None

    # Exclude rules already decided by preplan
    sql_rules_remaining = [
        s for s in ctx.compiled_full.sql_rules
        if s.get("rule_id") not in handled_ids_meta
    ]
    executor = pick_executor(handle, sql_rules_remaining)

    if executor is None:
        return _empty_pushdown_result(), handle, None

    staging_tmpdir = None
    try:
        # Inject effective tally into SQL specs (global override takes precedence)
        sql_specs_for_compile = []
        for s in ctx.compiled_full.sql_rules:
            if s.get("rule_id") not in handled_ids_meta:
                spec = dict(s)  # Copy to avoid mutating original
                rid = spec.get("rule_id")
                spec["tally"] = ctx.tally_map.get(rid, False)
                sql_specs_for_compile.append(spec)

        # Compile
        t0 = now_ms()
        executor_name = getattr(executor, "name", "sql")
        sql_plan_str = executor.compile(sql_specs_for_compile)
        compile_ms = now_ms() - t0
        if show_plan and sql_plan_str:
            print(f"\n-- {executor_name.upper()} SQL PLAN --\n{sql_plan_str}\n")

        # Execute
        t0 = now_ms()
        duck_out = executor.execute(handle, sql_plan_str, csv_mode=csv_mode)
        execute_ms = now_ms() - t0

        # Inject severity and tally into SQL results
        sql_results_raw = duck_out.get("results", [])
        for r in sql_results_raw:
            r["severity"] = ctx.severity_map.get(r.get("rule_id"), "blocking")
            r["tally"] = ctx.tally_map.get(r.get("rule_id"), False)
        results_by_id = {r["rule_id"]: r for r in sql_results_raw}
        handled_ids = set(results_by_id.keys())

        # Get row count and cols from execute result
        t0 = now_ms()
        row_count = duck_out.get("row_count")
        available_cols = duck_out.get("available_cols") or []

        # Fallback to introspect if execute didn't return these
        introspect_ms = 0
        if row_count is None or not available_cols:
            info = executor.introspect(handle, csv_mode=csv_mode)
            introspect_ms = now_ms() - t0
            row_count = info.get("row_count") if row_count is None else row_count
            available_cols = info.get("available_cols") or available_cols
            staging = info.get("staging") or duck_out.get("staging")
        else:
            introspect_ms = now_ms() - t0
            staging = duck_out.get("staging")

        # Handle staged Parquet (CSV -> Parquet staging)
        staging = staging or duck_out.get("staging")
        staged_path = None
        if staging and staging.get("path"):
            staged_path = staging["path"]
            staging_tmpdir = staging.get("tmpdir")
            # Update handle to point to staged file
            from kontra.connectors.handle import DatasetHandle
            handle = DatasetHandle.from_uri(staged_path)

        result = PushdownResult(
            effective=True,
            handled_ids=handled_ids,
            results_by_id=results_by_id,
            row_count=row_count,
            available_cols=available_cols,
            executor_name=executor_name,
            compile_ms=compile_ms,
            execute_ms=execute_ms,
            introspect_ms=introspect_ms,
            staged_path=staged_path,
            staging_tmpdir=staging_tmpdir,
        )
        return result, handle, staging_tmpdir

    except Exception as e:
        # Graceful fallback: residual Polars execution picks these rules up
        _logger.info("SQL pushdown failed (%s): %s", type(e).__name__, e)
        if show_plan:
            print(f"[WARN] SQL pushdown failed ({type(e).__name__}): {e}")
        return _empty_pushdown_result(), handle, staging_tmpdir
