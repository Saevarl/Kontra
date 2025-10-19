# src/kontra/engine/engine.py
from __future__ import annotations

"""
Validation Engine — hybrid-aware, executor- and materializer-friendly.

Flow
----
  1) Load contract (local path or s3://)
  2) Build rules and compile an execution plan (derives required columns)
  3) Pick a materializer from the registry (e.g., DuckDB for remote)
  4) Materialize a Polars DataFrame (with optional column pruning)
  5) If enabled, ask the SQL executor registry for an executor (e.g., DuckDB)
     to run a SQL pushdown query for a subset of rules.
  6) Run the full validation plan in Polars for all remaining rules.
  7) Merge results deterministically (SQL wins on overlaps), summarize,
     (optionally) print a human report, and attach a stats block.

Design goals
------------
- Deterministic runs (identical inputs → identical outputs)
- Extensibility by design (materializers + SQL executors are pluggable)
- Performance aware: compile → prune → load → execute
- Minimal branching: skip SQL leg when nothing to push
"""

import os
import uuid
from typing import Any, Dict, List, Literal, Optional

import polars as pl

from kontra.config.loader import ContractLoader
from kontra.config.models import Contract
from kontra.engine.backends.polars_backend import PolarsBackend
# New: connectors/handles + materializers + SQL executors
from kontra.connectors.handle import DatasetHandle
from kontra.engine.executors.registry import (
    pick_executor,
    register_default_executors,
)
from kontra.engine.materializers.registry import (
    pick_materializer,
    register_default_materializers,
)
from kontra.engine.stats import (
    RunTimers,
    basic_summary,
    columns_touched,
    now_ms,
    profile_for,
)
from kontra.reporters.rich_reporter import report_failure, report_success
from kontra.rules.execution_plan import RuleExecutionPlan
from kontra.rules.factory import RuleFactory

# Ensure built-in rules are registered (import side effects)
import kontra.rules.builtin.allowed_values  # noqa: F401
import kontra.rules.builtin.custom_sql_check  # noqa: F401
import kontra.rules.builtin.dtype  # noqa: F401
import kontra.rules.builtin.max_rows  # noqa: F401
import kontra.rules.builtin.min_rows  # noqa: F401
import kontra.rules.builtin.not_null  # noqa: F401
import kontra.rules.builtin.regex  # noqa: F401
import kontra.rules.builtin.unique  # noqa: F401


def _is_s3_uri(val: str | None) -> bool:
    return isinstance(val, str) and val.lower().startswith("s3://")


class ValidationEngine:
    """
    Thin orchestration wrapper. All business logic lives in rules, planners,
    materializers, connectors, and executors.
    """

    def __init__(
        self,
        contract_path: str,
        data_path: str | None = None,
        emit_report: bool = True,
        stats_mode: Literal["none", "summary", "profile"] = "none",
        enable_projection: bool = True,
        sql_engine: Literal["auto", "none"] = "auto",
        show_plan: bool = False,
    ):
        """
        Args:
            contract_path: Path or s3:// URI to the contract YAML.
            data_path: Optional dataset override; defaults to contract.dataset.
            emit_report: If True, print a Rich banner and failures.
            stats_mode: "none" | "summary" | "profile" (profile = lightweight column stats).
            enable_projection: If True, request only columns required by rules.
            sql_engine: "auto" (default) picks best SQL executor from registry.
                        "none" forces pure Polars execution, skipping SQL pushdown.
            show_plan: If True, print the SQL plan (debug/observability).
        """
        self.contract_path: str = str(contract_path)
        self.data_path = data_path
        self.emit_report = emit_report
        self.stats_mode = stats_mode
        self.enable_projection = enable_projection
        self.sql_engine_choice = sql_engine
        self.show_plan = show_plan

        self.contract: Contract | None = None
        self.df: pl.DataFrame | None = None

        # One-time registry defaults (idempotent)
        register_default_materializers()
        register_default_executors()

    # -------------------------------------------------------------------------

    def run(self) -> Dict[str, Any]:
        timers = RunTimers()

        # 1) Load contract
        t = now_ms()
        if _is_s3_uri(self.contract_path):
            self.contract = ContractLoader.from_s3(self.contract_path)
        else:
            self.contract = ContractLoader.from_path(self.contract_path)
        timers.contract_load_ms = now_ms() - t

        # 2) Build rules & compile plan (derives required columns + sql_rules)
        t = now_ms()
        factory = RuleFactory(self.contract.rules)
        rules = factory.build_rules()
        plan = RuleExecutionPlan(rules)
        compiled = plan.compile()
        timers.compile_ms = now_ms() - t

        # 3) Prepare source handle + pick materializer
        source_uri = self.data_path or self.contract.dataset
        handle = DatasetHandle.from_uri(source_uri)

        # Pick the best materializer for the handle (e.g., DuckDB for S3).
        # This call is no longer dependent on the projection flag.
        mat = pick_materializer(handle)

        # Data materialization respects (optional) projection
        required_cols_polars: Optional[List[str]] = (
            compiled.required_cols if self.enable_projection else None
        )

        # 4) Execute: optional SQL pushdown leg (hybrid mode) + Polars full plan
        sql_results_by_id: Dict[str, Dict[str, Any]] = {}
        available_cols: List[str] = []
        sql_row_count: Optional[int] = None
        engine_name = "polars"  # Default engine name

        # Ask the registry for an executor if enabled
        exec_sql = None
        if self.sql_engine_choice == "auto":
            exec_sql = pick_executor(handle, compiled.sql_rules)

        use_sql = exec_sql is not None

        if use_sql:
            # Compile + run SQL-optimized aggregate query
            sql_plan_str = exec_sql.compile(compiled.sql_rules)
            if self.show_plan and sql_plan_str:
                print(f"\n-- {exec_sql.name} SQL plan --\n{sql_plan_str}\n")

            # --- PHASE 1 FIX ---
            # Pass the whole handle. The executor now knows what to do.
            duck_out = exec_sql.execute(handle, sql_plan_str)
            sql_results_by_id = {
                r["rule_id"]: r for r in duck_out.get("results", [])
            }

            # Honest width/height without materializing data
            info = exec_sql.introspect(handle)
            sql_row_count = info.get("row_count")
            available_cols = info.get("available_cols") or []
            engine_name = f"{exec_sql.name}_hybrid"

        # Materialize Polars DataFrame (column-pruned if enabled and supported)
        t = now_ms()
        self.df = mat.to_polars(required_cols_polars)
        timers.data_load_ms = now_ms() - t

        # Polars pass for coverage parity (vectorized + fallbacks)
        t = now_ms()
        polars_exec = PolarsBackend(executor=plan.execute_compiled)
        polars_art = polars_exec.compile(compiled)
        polars_out = polars_exec.execute(self.df, polars_art)
        timers.execute_ms = now_ms() - t

        # Merge deterministically: SQL wins on overlaps
        merged: List[Dict[str, Any]] = []
        for r in polars_out["results"]:
            rid = r.get("rule_id")
            merged.append(sql_results_by_id.get(rid, r))
        results = merged

        # 5) Summarize
        summary = plan.summary(results)
        summary["dataset_name"] = self.contract.dataset

        # 6) Optional rich report
        t = now_ms()
        if self.emit_report:
            self._report(summary, results)
        timers.report_ms = now_ms() - t

        # 7) Optional stats block
        stats_block: Dict[str, Any] | None = None
        if self.stats_mode != "none":
            # Discover file width if we didn't already (cheap schema scan)
            if not available_cols:
                available_cols = self._peek_available_columns(handle.uri)

            stats_block = self._build_stats_block(
                mode=self.stats_mode,
                timers=timers,
                rules_specs=[
                    {"name": r.name, "params": r.params}
                    for r in self.contract.rules
                ],
                required_cols=(
                    compiled.required_cols if self.enable_projection else None
                ),
                available_cols=available_cols,
            )

            # Engine label + dataset rows/cols if known from SQL leg
            if stats_block and "run_meta" in stats_block:
                stats_block["run_meta"]["engine"] = engine_name
            if use_sql and sql_row_count is not None and "dataset" in stats_block:
                stats_block["dataset"]["nrows"] = int(sql_row_count)
                stats_block["dataset"]["ncols"] = len(available_cols or [])

            # Optional I/O diagnostics from materializer (KONTRA_IO_DEBUG=1)
            if os.getenv("KONTRA_IO_DEBUG"):
                io_dbg = getattr(mat, "io_debug", None)
                if callable(io_dbg):
                    io = io_dbg()  # may be None
                    if io:
                        stats_block["io"] = io

        # Stable structured return for reporters/SDK
        out: Dict[str, Any] = {
            "dataset": self.contract.dataset,
            "results": results,
            "summary": summary,
        }
        if stats_block is not None:
            out["stats"] = stats_block
        return out

    # ----------------------------- Reporting ----------------------------------

    def _report(
        self, summary: Dict[str, Any], results: List[Dict[str, Any]]
    ) -> None:
        """Human-readable console report (Rich). Keep minimal; reporters own structure."""
        if summary["passed"]:
            report_success(
                f"{summary['dataset_name']} — PASSED "
                f"({summary['rules_passed']} of {summary['total_rules']} rules)"
            )
        else:
            report_failure(
                f"{summary['dataset_name']} — FAILED "
                f"({summary['rules_failed']} of {summary['total_rules']} rules)"
            )
        for result in results:
            if not result["passed"]:
                print(f"  ❌ {result['rule_id']}: {result['message']}")

    # ------------------------------ Internals ---------------------------------

    def _peek_available_columns(self, source: str) -> List[str]:
        """
        Best-effort list of all columns present in the source without full materialization.

        Only used for observability; failure here should not affect validation.
        """
        # TODO: This should use the materializer's .schema() method
        try:
            lower = source.lower()
            if lower.endswith(".parquet"):
                # Resolve schema via collect_schema() (cheap)
                return list(pl.scan_parquet(source).collect_schema().names())
            if lower.endswith(".csv"):
                return list(pl.scan_csv(source).collect_schema().names())
        except Exception:
            pass
        return []

    def _build_stats_block(
        self,
        mode: Literal["summary", "profile"],
        timers: RunTimers,
        rules_specs: List[Dict[str, Any]],
        required_cols: Optional[List[str]],
        available_cols: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Construct a stats block with:
          - run metadata (timings, paths, run_id)
          - dataset basics (rows, cols) — filled from current df unless SQL leg provided
          - which columns were touched by rules
          - projection summary (required vs loaded vs available)
          - optional lightweight profile for touched columns
        """
        duration_total = (
            timers.contract_load_ms
            + timers.data_load_ms
            + timers.compile_ms
            + timers.execute_ms
            + timers.report_ms
        )
        run_meta = {
            "run_id": str(uuid.uuid4()),
            "phases_ms": {
                "contract_load": timers.contract_load_ms,
                "data_load": timers.data_load_ms,
                "compile": timers.compile_ms,
                "execute": timers.execute_ms,
                "report": timers.report_ms,
            },
            "duration_ms_total": duration_total,
            "dataset_path": getattr(self, "data_path", None)
            or (self.contract.dataset if self.contract else None),
            "contract_path": self.contract_path,
        }

        # Post-(optional) pruning column count
        loaded_count = len(self.df.columns) if self.df is not None else 0
        required_count = len(required_cols or [])
        available_count = len(available_cols or []) or loaded_count

        projection = {
            "required_columns": required_cols or [],
            "required_count": required_count,
            "loaded_count": loaded_count,
            "available_count": available_count,
            "enabled": bool(self.enable_projection),
            "effective": (
                self.enable_projection
                and required_count > 0
                and required_count < available_count
            ),
        }

        block: Dict[str, Any] = {
            "stats_version": "1",
            "run_meta": run_meta,
            "dataset": basic_summary(self.df),
            "columns_touched": columns_touched(rules_specs),
            "projection": projection,
        }

        if mode == "profile" and block["columns_touched"]:
            block["profile"] = profile_for(self.df, block["columns_touched"])

        return block