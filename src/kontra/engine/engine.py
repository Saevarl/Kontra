from __future__ import annotations

"""
Validation Engine — hybrid-aware, projection-efficient, and deterministic.

Flow
----
  1) Load contract
  2) Build rules → compile plan (required columns + pushdown candidates)
  3) Pick materializer (e.g., DuckDB for S3)
  4) (Optional) SQL pushdown for eligible rules
  5) Materialize residual columns for Polars (projection)
  6) Execute remaining rules in Polars
  7) Merge results, summarize, attach small stats dict

Principles
----------
- Deterministic: identical inputs → identical outputs
- Hybrid-aware: push down SQL-capable checks, do the rest locally
- Performance-first: compile → prune → load → execute
- Clear separation: engine orchestrates; reporters format/print
"""

import os
from typing import Any, Dict, List, Literal, Optional

import polars as pl

from kontra.config.loader import ContractLoader
from kontra.config.models import Contract
from kontra.connectors.handle import DatasetHandle
from kontra.engine.backends.polars_backend import PolarsBackend
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

# Ensure built-in rules are registered via import side effects
import kontra.rules.builtin.allowed_values  # noqa: F401
import kontra.rules.builtin.custom_sql_check  # noqa: F401
import kontra.rules.builtin.dtype  # noqa: F401
import kontra.rules.builtin.max_rows  # noqa: F401
import kontra.rules.builtin.min_rows  # noqa: F401
import kontra.rules.builtin.not_null  # noqa: F401
import kontra.rules.builtin.regex  # noqa: F401
import kontra.rules.builtin.unique  # noqa: F401


# --------------------------------- Helpers ---------------------------------- #

def _is_s3_uri(val: str | None) -> bool:
    return isinstance(val, str) and val.lower().startswith("s3://")


# --------------------------------- Engine ----------------------------------- #

class ValidationEngine:
    """
    Orchestrates:
      - Rule planning
      - Materialization (e.g., DuckDB)
      - SQL pushdown (optional)
      - Residual Polars execution
      - Reporting + small stats block
    """

    def __init__(
        self,
        contract_path: str,
        data_path: Optional[str] = None,
        emit_report: bool = True,
        stats_mode: Literal["none", "summary", "profile"] = "none",
        enable_projection: bool = True,
        # Preferred toggle: SQL pushdown on/off
        pushdown: Literal["auto", "off"] = "auto",
        # Back-compat: if "none", treat as pushdown="off"
        sql_engine: Literal["auto", "none"] = "auto",
        show_plan: bool = False,
    ):
        self.contract_path = str(contract_path)
        self.data_path = data_path
        self.emit_report = emit_report
        self.stats_mode = stats_mode
        self.enable_projection = enable_projection
        self.pushdown = "off" if sql_engine == "none" else pushdown
        self.show_plan = show_plan

        self.contract: Optional[Contract] = None
        self.df: Optional[pl.DataFrame] = None

        register_default_materializers()
        register_default_executors()

    # --------------------------------------------------------------------- #

    def run(self) -> Dict[str, Any]:
        timers = RunTimers()

        # 1) Contract
        t = now_ms()
        self.contract = (
            ContractLoader.from_s3(self.contract_path)
            if _is_s3_uri(self.contract_path)
            else ContractLoader.from_path(self.contract_path)
        )
        timers.contract_load_ms = now_ms() - t

        # 2) Rules & plan
        t = now_ms()
        rules = RuleFactory(self.contract.rules).build_rules()
        plan = RuleExecutionPlan(rules)
        compiled_full = plan.compile()
        timers.compile_ms = now_ms() - t

        # 3) Materializer
        source_uri = self.data_path or self.contract.dataset
        handle = DatasetHandle.from_uri(source_uri)
        materializer = pick_materializer(handle)
        materializer_name = getattr(materializer, "name", "duckdb")

        # 4) SQL pushdown (optional)
        sql_results_by_id: Dict[str, Dict[str, Any]] = {}
        handled_ids_sql: set[str] = set()
        available_cols: List[str] = []
        sql_row_count: Optional[int] = None
        executor_name = "none"

        executor = pick_executor(handle, compiled_full.sql_rules) if self.pushdown == "auto" else None
        if executor:
            executor_name = getattr(executor, "name", "sql")
            sql_plan_str = executor.compile(compiled_full.sql_rules)
            if self.show_plan and sql_plan_str:
                print(f"\n-- {executor_name.upper()} SQL PLAN --\n{sql_plan_str}\n")

            duck_out = executor.execute(handle, sql_plan_str)
            sql_results_by_id = {r["rule_id"]: r for r in duck_out.get("results", [])}
            handled_ids_sql = set(sql_results_by_id.keys())

            info = executor.introspect(handle)  # cheap width/height
            sql_row_count = info.get("row_count")
            available_cols = info.get("available_cols") or []

        # 5) Residual plan → Polars
        compiled_residual = plan.without_ids(compiled_full, handled_ids_sql)
        required_cols = compiled_residual.required_cols if self.enable_projection else None

        if not compiled_residual.predicates and not compiled_residual.fallback_rules:
            self.df = None
            polars_out = {"results": []}
            timers.data_load_ms = 0
            timers.execute_ms = 0
        else:
            t = now_ms()
            self.df = materializer.to_polars(required_cols)
            timers.data_load_ms = now_ms() - t

            t = now_ms()
            polars_exec = PolarsBackend(executor=plan.execute_compiled)
            polars_art = polars_exec.compile(compiled_residual)
            polars_out = polars_exec.execute(self.df, polars_art)
            timers.execute_ms = now_ms() - t

        # 6) Merge results — SQL wins on overlaps
        results: List[Dict[str, Any]] = list(sql_results_by_id.values())
        for r in polars_out["results"]:
            if r.get("rule_id") not in sql_results_by_id:
                results.append(r)

        # 7) Summary + minimal report
        summary = plan.summary(results)
        summary["dataset_name"] = self.contract.dataset

        engine_label = f"{materializer_name}+polars (pushdown: {'on' if executor else 'off'})"
        if self.emit_report:
            t = now_ms()
            self._report(summary, results)
            timers.report_ms = now_ms() - t

        # 8) Stats (small dict; reporters format/print)
        stats: Optional[Dict[str, Any]] = None
        if self.stats_mode != "none":
            if not available_cols:
                available_cols = self._peek_available_columns(handle.uri)

            # dataset: always show total columns + authoritative rows if we have them
            ds_summary = basic_summary(
                self.df,
                available_cols=available_cols or None,
                nrows_override=sql_row_count,
            )

            # projection block
            proj_required = compiled_full.required_cols if self.enable_projection else []
            proj = {
                "required_columns": proj_required or [],
                "required_count": len(proj_required or []),
                "loaded_count": len(self.df.columns) if self.df is not None else 0,
                "available_count": len(available_cols or []) or (len(self.df.columns) if self.df is not None else 0),
                "enabled": bool(self.enable_projection),
                "effective": (
                    self.enable_projection
                    and bool(proj_required)
                    and len(proj_required) < (len(available_cols or []) or 0)
                ),
            }

            touched = columns_touched([{"name": r.name, "params": r.params} for r in self.contract.rules])
            loaded_cols = list(self.df.columns) if self.df is not None else []

            stats = {
                "stats_version": "1",
                "run_meta": {
                    "phases_ms": {
                        "contract_load": timers.contract_load_ms,
                        "data_load": timers.data_load_ms,
                        "compile": timers.compile_ms,
                        "execute": timers.execute_ms,
                        "report": timers.report_ms,
                    },
                    "duration_ms_total": (
                        timers.contract_load_ms
                        + timers.data_load_ms
                        + timers.compile_ms
                        + timers.execute_ms
                        + timers.report_ms
                    ),
                    "dataset_path": self.data_path or self.contract.dataset,
                    "contract_path": self.contract_path,
                    "materializer": materializer_name,
                    "validator": "polars",
                    "pushdown": "on" if executor else "off",
                    "executor": executor_name,
                    "engine": engine_label,
                },
                "dataset": ds_summary,
                "columns_touched": touched,
                "projection": proj,
                "columns_validated": touched,
                "columns_loaded": loaded_cols,
            }

            if self.stats_mode == "profile" and touched and self.df is not None:
                stats["profile"] = profile_for(self.df, touched)

            if os.getenv("KONTRA_IO_DEBUG"):
                io_dbg = getattr(materializer, "io_debug", None)
                if callable(io_dbg):
                    io = io_dbg()
                    if io:
                        stats["io"] = io

        out: Dict[str, Any] = {
            "dataset": self.contract.dataset,
            "results": results,
            "summary": summary,
        }
        if stats is not None:
            out["stats"] = stats
        out.setdefault("run_meta", {})["engine_label"] = engine_label
        return out

    # ------------------------------- Reporting -------------------------------- #

    def _report(self, summary: Dict[str, Any], results: List[Dict[str, Any]]) -> None:
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
        for r in results:
            if not r.get("passed", False):
                print(f"  ❌ {r.get('rule_id', '<unknown>')}: {r.get('message', '')}")

    # ------------------------------- Internals -------------------------------- #

    def _peek_available_columns(self, source: str) -> List[str]:
        """Cheap schema peek; used only for observability."""
        try:
            s = source.lower()
            if s.endswith(".parquet"):
                return list(pl.scan_parquet(source).collect_schema().names())
            if s.endswith(".csv"):
                return list(pl.scan_csv(source).collect_schema().names())
        except Exception:
            pass
        return []
