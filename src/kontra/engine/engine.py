from __future__ import annotations

"""
Validation Engine — preplan-aware, hybrid, projection-efficient, deterministic.

Flow
----
  1) Load contract
  2) Build rules → compile plan (required columns + SQL-capable candidates)
  3) (Optional) Preplan (metadata-only, Parquet): prove PASS/FAIL, build scan manifest
  4) Pick materializer (e.g., DuckDB for S3 / staged CSV)
  5) (Optional) SQL pushdown for eligible *remaining* rules (may stage CSV → Parquet)
  6) Materialize residual slice for Polars (row-groups + projection)
  7) Execute residual rules in Polars
  8) Merge results (preplan → SQL → Polars), summarize, attach small stats dict

Principles
----------
- Deterministic: identical inputs → identical outputs
- Layered & independent toggles:
    * Preplan (metadata) — independent of pushdown/projection
    * Pushdown (SQL execution) — independent of preplan/projection
    * Projection (contract-driven columns) — independent of preplan/pushdown
- Performance-first: plan → prune → load minimal slice → execute
- Clear separation: engine orchestrates; preplan is a leaf; reporters format/print
"""

import os
from typing import Any, Dict, List, Literal, Optional, Set

import polars as pl
import pyarrow as pa
import pyarrow.fs as pafs  # <-- Added
import pyarrow.parquet as pq

from kontra.config.loader import ContractLoader
from kontra.config.models import Contract
from kontra.connectors.handle import DatasetHandle
from kontra.engine.backends.polars_backend import PolarsBackend
from kontra.engine.executors.registry import pick_executor, register_default_executors
from kontra.engine.materializers.registry import pick_materializer, register_default_materializers
from kontra.engine.stats import RunTimers, basic_summary, columns_touched, now_ms, profile_for
from kontra.reporters.rich_reporter import report_failure, report_success
from kontra.rules.execution_plan import RuleExecutionPlan
from kontra.rules.factory import RuleFactory

# Preplan (metadata-only) + static predicate extraction
from kontra.preplan.planner import preplan_single_parquet
from kontra.preplan.types import PrePlan
from kontra.rules.static_predicates import extract_static_predicates

# Built-ins (side-effect registration)
import kontra.rules.builtin.allowed_values  # noqa: F401
import kontra.rules.builtin.custom_sql_check  # noqa: F401
import kontra.rules.builtin.dtype  # noqa: F401
import kontra.rules.builtin.max_rows  # noqa: F401
import kontra.rules.builtin.min_rows  # noqa: F401
import kontra.rules.builtin.not_null  # noqa: F401
import kontra.rules.builtin.regex  # noqa: F401
import kontra.rules.builtin.unique  # noqa: F401


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #

def _is_s3_uri(val: str | None) -> bool:
    return isinstance(val, str) and val.lower().startswith("s3://")


def _s3_uri_to_path(uri: str) -> str:
    """Convert s3://bucket/key to bucket/key (PyArrow S3FileSystem format)."""
    if uri.lower().startswith("s3://"):
        return uri[5:]  # Strip 's3://'
    return uri


def _create_s3_filesystem(handle: DatasetHandle) -> pafs.S3FileSystem:
    """
    Create a PyArrow S3FileSystem from handle's fs_opts (populated from env vars).
    Supports MinIO and other S3-compatible storage via custom endpoints.
    """
    opts = handle.fs_opts or {}

    # Map our fs_opts keys to PyArrow S3FileSystem kwargs
    kwargs: Dict[str, Any] = {}
    if opts.get("s3_access_key_id") and opts.get("s3_secret_access_key"):
        kwargs["access_key"] = opts["s3_access_key_id"]
        kwargs["secret_key"] = opts["s3_secret_access_key"]
    if opts.get("s3_session_token"):
        kwargs["session_token"] = opts["s3_session_token"]
    if opts.get("s3_region"):
        kwargs["region"] = opts["s3_region"]
    if opts.get("s3_endpoint"):
        # PyArrow expects endpoint_override without the scheme
        endpoint = opts["s3_endpoint"]
        # Strip scheme if present and set scheme kwarg
        if endpoint.startswith("http://"):
            endpoint = endpoint[7:]
            kwargs["scheme"] = "http"
        elif endpoint.startswith("https://"):
            endpoint = endpoint[8:]
            kwargs["scheme"] = "https"
        kwargs["endpoint_override"] = endpoint

    # MinIO and some S3-compatible storage require path-style URLs (not virtual-hosted)
    # DUCKDB_S3_URL_STYLE=path -> force_virtual_addressing=False
    url_style = opts.get("s3_url_style", "").lower()
    if url_style == "path":
        kwargs["force_virtual_addressing"] = False
    elif url_style == "host":
        kwargs["force_virtual_addressing"] = True
    # If endpoint is set but no url_style, default to path-style (common for MinIO)
    elif opts.get("s3_endpoint"):
        kwargs["force_virtual_addressing"] = False

    return pafs.S3FileSystem(**kwargs)


def _is_parquet(path: str | None) -> bool:
    return isinstance(path, str) and path.lower().endswith(".parquet")


# --------------------------------------------------------------------------- #
# Engine
# --------------------------------------------------------------------------- #

class ValidationEngine:
    """
    Orchestrates:
      - Rule planning
      - Preplan (metadata-only; Parquet)  [independent]
      - SQL pushdown (optional)           [independent]
      - Residual Polars execution
      - Reporting + stats
    """

    def __init__(
        self,
        contract_path: str,
        data_path: Optional[str] = None,
        emit_report: bool = True,
        stats_mode: Literal["none", "summary", "profile"] = "none",
        # Independent toggles
        preplan: Literal["on", "off", "auto"] = "auto",
        pushdown: Literal["on", "off", "auto"] = "auto",
        enable_projection: bool = True,
        csv_mode: Literal["auto", "duckdb", "parquet"] = "auto",
        # Diagnostics
        show_plan: bool = False,
        explain_preplan: bool = False,
    ):
        self.contract_path = str(contract_path)
        self.data_path = data_path
        self.emit_report = emit_report
        self.stats_mode = stats_mode

        self.preplan = preplan
        self.pushdown = pushdown
        self.enable_projection = bool(enable_projection)
        self.csv_mode = csv_mode
        self.show_plan = show_plan
        self.explain_preplan = explain_preplan

        self.contract: Optional[Contract] = None
        self.df: Optional[pl.DataFrame] = None

        register_default_materializers()
        register_default_executors()

    # --------------------------------------------------------------------- #

    def run(self) -> Dict[str, Any]:
        timers = RunTimers()
        self._staging_tmpdir = None  # Track for cleanup in finally block

        try:
            return self._run_impl(timers)
        finally:
            # Cleanup staged temp directory (CSV -> Parquet staging)
            if self._staging_tmpdir is not None:
                try:
                    self._staging_tmpdir.cleanup()
                except Exception:
                    pass
                self._staging_tmpdir = None

    def _run_impl(self, timers: RunTimers) -> Dict[str, Any]:
        # 1) Contract
        t0 = now_ms()
        self.contract = (
            ContractLoader.from_s3(self.contract_path)
            if _is_s3_uri(self.contract_path)
            else ContractLoader.from_path(self.contract_path)
        )
        timers.contract_load_ms = now_ms() - t0

        # 2) Rules & plan
        t0 = now_ms()
        rules = RuleFactory(self.contract.rules).build_rules()
        plan = RuleExecutionPlan(rules)
        compiled_full = plan.compile()
        timers.compile_ms = now_ms() - t0

        # Dataset handle (used across phases)
        source_uri = self.data_path or self.contract.dataset
        handle = DatasetHandle.from_uri(source_uri)

        # ------------------------------------------------------------------ #
        # 3) Preplan (metadata-only; independent of pushdown/projection)
        preplan_effective = False
        handled_ids_meta: Set[str] = set()
        meta_results_by_id: Dict[str, Dict[str, Any]] = {}
        preplan_row_groups: Optional[List[int]] = None
        preplan_columns: Optional[List[str]] = None
        preplan_analyze_ms = 0
        preplan_summary: Dict[str, Any] = {
            "enabled": self.preplan in {"on", "auto"},
            "effective": False,
            "rules_pass_meta": 0,
            "rules_fail_meta": 0,
            "rules_unknown": len(compiled_full.required_cols or []),
            "row_groups_kept": None,
            "row_groups_total": None,
            "row_groups_pruned": None,
        }

        # Get filesystem from handle; preplan needs this for S3/remote access.
        preplan_fs: pafs.FileSystem | None = None
        if _is_s3_uri(handle.uri):
            try:
                preplan_fs = _create_s3_filesystem(handle)
            except Exception:
                # If S3 libs aren't installed, this will fail.
                # We'll let the ParquetFile call fail below and be caught.
                pass

        if self.preplan in {"on", "auto"} and _is_parquet(handle.uri):
            try:
                t0 = now_ms()
                static_preds = extract_static_predicates(rules=rules)
                # PyArrow S3FileSystem expects 'bucket/key' format, not 's3://bucket/key'
                preplan_path = _s3_uri_to_path(handle.uri) if preplan_fs else handle.uri
                pre: PrePlan = preplan_single_parquet(
                    path=preplan_path,
                    required_columns=compiled_full.required_cols,  # DC-driven columns
                    predicates=static_preds,
                    filesystem=preplan_fs,
                )
                preplan_analyze_ms = now_ms() - t0

                # Register metadata-based rule decisions (pass/fail), unknowns remain
                pass_meta = fail_meta = unknown = 0
                for rid, decision in pre.rule_decisions.items():
                    if decision == "pass_meta":
                        meta_results_by_id[rid] = {
                            "rule_id": rid,
                            "passed": True,
                            "failed_count": 0,
                            "message": "Proven by metadata (Parquet stats)",
                        }
                        handled_ids_meta.add(rid)
                        pass_meta += 1
                    elif decision == "fail_meta":
                        meta_results_by_id[rid] = {
                            "rule_id": rid,
                            "passed": False,
                            "failed_count": 1,
                            "message": "Failed: violation proven by Parquet metadata (null values detected)",
                        }
                        handled_ids_meta.add(rid)
                        fail_meta += 1
                    else:
                        unknown += 1

                preplan_row_groups = list(pre.manifest_row_groups or [])
                preplan_columns = list(pre.manifest_columns or [])
                preplan_effective = True

                rg_total = pre.stats.get("rg_total", None)
                rg_kept = len(preplan_row_groups)
                preplan_summary.update({
                    "effective": True,
                    "rules_pass_meta": pass_meta,
                    "rules_fail_meta": fail_meta,
                    "rules_unknown": unknown,
                    "row_groups_kept": rg_kept if rg_total is not None else None,
                    "row_groups_total": rg_total,
                    "row_groups_pruned": (rg_total - rg_kept) if (rg_total is not None) else None,
                })

                if self.explain_preplan:
                    print(
                        "\n-- PREPLAN (metadata) --"
                        f"\n  Row-groups kept: {preplan_summary.get('row_groups_kept')}/{preplan_summary.get('row_groups_total')}"
                        f"\n  Rules: {pass_meta} pass, {fail_meta} fail, {unknown} unknown\n"
                    )

            except Exception as e:
                # Distinguish between "preplan not available" vs "real errors"
                err_str = str(e).lower()
                err_type = type(e).__name__

                # Re-raise errors that indicate real problems (auth, file not found, etc.)
                is_auth_error = (
                    "access denied" in err_str
                    or "forbidden" in err_str
                    or "unauthorized" in err_str
                    or "credentials" in err_str
                    or "authentication" in err_str
                )
                is_not_found = (
                    isinstance(e, FileNotFoundError)
                    or "not found" in err_str
                    or "no such file" in err_str
                    or "does not exist" in err_str
                )
                is_permission = isinstance(e, PermissionError)

                if is_auth_error or is_not_found or is_permission:
                    # These are real errors - don't silently skip
                    raise RuntimeError(
                        f"Preplan failed due to {err_type}: {e}. "
                        "Check file path and credentials."
                    ) from e

                # Otherwise, preplan optimization just isn't available (e.g., no stats)
                if os.getenv("KONTRA_VERBOSE"):
                    print(f"[INFO] Preplan skipped ({err_type}): {e}")
                preplan_effective = False  # leave summary with effective=False

        # ------------------------------------------------------------------ #
        # 4) Materializer setup (orthogonal)
        materializer = pick_materializer(handle)
        materializer_name = getattr(materializer, "name", "duckdb")
        _staged_override_uri: Optional[str] = None

        # ------------------------------------------------------------------ #
        # 5) SQL pushdown (independent of preplan/projection)
        sql_results_by_id: Dict[str, Dict[str, Any]] = {}
        handled_ids_sql: Set[str] = set()
        available_cols: List[str] = []
        sql_row_count: Optional[int] = None
        executor_name = "none"
        pushdown_effective = False
        push_compile_ms = push_execute_ms = push_introspect_ms = 0

        executor = None
        if self.pushdown in {"on", "auto"}:
            # Exclude rules already decided by preplan
            sql_rules_remaining = [s for s in compiled_full.sql_rules if s.get("rule_id") not in handled_ids_meta]
            executor = pick_executor(handle, sql_rules_remaining)

        if executor:
            try:
                # Compile
                t0 = now_ms()
                executor_name = getattr(executor, "name", "sql")
                sql_plan_str = executor.compile([s for s in compiled_full.sql_rules if s.get("rule_id") not in handled_ids_meta])
                push_compile_ms = now_ms() - t0
                if self.show_plan and sql_plan_str:
                    print(f"\n-- {executor_name.upper()} SQL PLAN --\n{sql_plan_str}\n")

                # Execute
                t0 = now_ms()
                duck_out = executor.execute(handle, sql_plan_str, csv_mode=self.csv_mode)
                push_execute_ms = now_ms() - t0
                sql_results_by_id = {r["rule_id"]: r for r in duck_out.get("results", [])}
                handled_ids_sql = set(sql_results_by_id.keys())

                # Introspect
                t0 = now_ms()
                info = executor.introspect(handle, csv_mode=self.csv_mode)
                push_introspect_ms = now_ms() - t0
                sql_row_count = info.get("row_count")
                available_cols = info.get("available_cols") or []

                # Reuse staged Parquet (if the executor staged CSV → Parquet)
                staging = info.get("staging") or duck_out.get("staging")
                if staging and staging.get("path"):
                    _staged_override_uri = staging["path"]
                    self._staging_tmpdir = staging.get("tmpdir")
                    handle = DatasetHandle.from_uri(_staged_override_uri)
                    materializer = pick_materializer(handle)
                    materializer_name = getattr(materializer, "name", materializer_name)

                pushdown_effective = True
            except Exception as e:
                if os.getenv("KONTRA_VERBOSE") or self.show_plan:
                    print(f"[WARN] SQL pushdown failed ({type(e).__name__}): {e}")
                executor = None  # fall back silently

        # ------------------------------------------------------------------ #
        # 6) Residual Polars execution (projection independent; manifest optional)
        handled_all = handled_ids_meta | handled_ids_sql
        compiled_residual = plan.without_ids(compiled_full, handled_all)

        # Projection is DC-driven; independent of preplan/pushdown
        required_cols_full = compiled_full.required_cols if self.enable_projection else []
        required_cols_residual = compiled_residual.required_cols if self.enable_projection else []

        if not compiled_residual.predicates and not compiled_residual.fallback_rules:
            self.df = None
            polars_out = {"results": []}
            timers.data_load_ms = timers.execute_ms = 0
        else:
            # Materialize minimal slice:
            # If preplan produced a row-group manifest, honor it — otherwise let the materializer decide.
            t0 = now_ms()
            if preplan_effective and _is_parquet(handle.uri) and preplan_row_groups:
                cols = (required_cols_residual or None) if self.enable_projection else None
                
                # Reuse preplan filesystem if available, otherwise create from handle
                residual_fs = preplan_fs
                if residual_fs is None and _is_s3_uri(handle.uri):
                    try:
                        residual_fs = _create_s3_filesystem(handle)
                    except Exception:
                        pass  # Let ParquetFile try default credentials

                # PyArrow S3FileSystem expects 'bucket/key' format, not 's3://bucket/key'
                residual_path = _s3_uri_to_path(handle.uri) if residual_fs else handle.uri
                pf = pq.ParquetFile(residual_path, filesystem=residual_fs)

                pa_cols = cols if cols else None
                rg_tables = [pf.read_row_group(i, columns=pa_cols) for i in preplan_row_groups]
                pa_tbl = pa.concat_tables(rg_tables) if len(rg_tables) > 1 else rg_tables[0]
                self.df = pl.from_arrow(pa_tbl)
            else:
                # Materializer respects projection (engine passes residual required cols)
                self.df = materializer.to_polars(required_cols_residual or None)
            timers.data_load_ms = now_ms() - t0

            # Execute residual rules in Polars
            t0 = now_ms()
            polars_exec = PolarsBackend(executor=plan.execute_compiled)
            polars_art = polars_exec.compile(compiled_residual)
            polars_out = polars_exec.execute(self.df, polars_art)
            timers.execute_ms = now_ms() - t0

        # ------------------------------------------------------------------ #
        # 7) Merge results — deterministic order: preplan → SQL → Polars
        results: List[Dict[str, Any]] = list(meta_results_by_id.values())
        results += [r for r in sql_results_by_id.values() if r["rule_id"] not in meta_results_by_id]
        results += [r for r in polars_out["results"] if r["rule_id"] not in meta_results_by_id and r["rule_id"] not in sql_results_by_id]

        # 8) Summary
        summary = plan.summary(results)
        summary["dataset_name"] = self.contract.dataset
        engine_label = (
            f"{materializer_name}+polars "
            f"(preplan:{'on' if preplan_effective else 'off'}, "
            f"pushdown:{'on' if pushdown_effective else 'off'}, "
            f"projection:{'on' if self.enable_projection else 'off'})"
        )

        if self.emit_report:
            t0 = now_ms()
            self._report(summary, results)
            timers.report_ms = now_ms() - t0

        # ------------------------------------------------------------------ #
        # 9) Stats (feature-attributed)
        stats: Optional[Dict[str, Any]] = None
        if self.stats_mode != "none":
            if not available_cols:
                available_cols = self._peek_available_columns(handle.uri)

            ds_summary = basic_summary(self.df, available_cols=available_cols, nrows_override=sql_row_count)

            loaded_cols = list(self.df.columns) if self.df is not None else []
            proj = {
                "enabled": self.enable_projection,
                "available_count": len(available_cols or []) if available_cols is not None else len(loaded_cols),
                "full": {
                    "required_columns": required_cols_full or [],
                    "required_count": len(required_cols_full or []),
                },
                "residual": {
                    "required_columns": required_cols_residual or [],
                    "required_count": len(required_cols_residual or []),
                    "loaded_count": len(loaded_cols),
                    "effective": self.enable_projection and bool(required_cols_residual)
                                   and len(loaded_cols) <= len(required_cols_residual),
                },
            }

            push = {
                "enabled": self.pushdown in {"on", "auto"},
                "effective": bool(pushdown_effective),
                "executor": executor_name,
                "rules_pushed": len(sql_results_by_id),
                "breakdown_ms": {
                    "compile": push_compile_ms,
                    "execute": push_execute_ms,
                    "introspect": push_introspect_ms,
                },
            }

            res = {
                "rules_local": len(polars_out["results"]) if "polars_out" in locals() else 0,
            }

            phases_ms = {
                "contract_load": int(timers.contract_load_ms or 0),
                "compile": int(timers.compile_ms or 0),
                "preplan": int(preplan_analyze_ms or 0),
                "pushdown": int(push_compile_ms + push_execute_ms + push_introspect_ms),
                "data_load": int(timers.data_load_ms or 0),
                "execute": int(timers.execute_ms or 0),
                "report": int(timers.report_ms or 0),
            }

            stats = {
                "stats_version": "2",
                "run_meta": {
                    "phases_ms": phases_ms,
                    "duration_ms_total": sum(phases_ms.values()),
                    "dataset_path": self.data_path or self.contract.dataset,
                    "contract_path": self.contract_path,
                    "engine": engine_label,
                    "materializer": materializer_name,
                    "preplan_requested": self.preplan,
                    "preplan": "on" if preplan_effective else "off",
                    "pushdown_requested": self.pushdown,
                    "pushdown": "on" if pushdown_effective else "off",
                    "csv_mode": self.csv_mode,
                    "staged_override": bool(_staged_override_uri),
                },
                "dataset": ds_summary,
                "preplan": preplan_summary,
                "pushdown": push,
                "projection": proj,
                "residual": res,
                "columns_touched": columns_touched([{"name": r.name, "params": r.params} for r in self.contract.rules]),
                "columns_validated": columns_touched([{"name": r.name, "params": r.params} for r in self.contract.rules]),
                "columns_loaded": loaded_cols,
            }

            if self.stats_mode == "profile" and self.df is not None:
                stats["profile"] = profile_for(self.df, proj["residual"]["required_columns"])

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

        # Ensure staged tempdir (if any) is cleaned after the whole run
        return out

    # --------------------------------------------------------------------- #

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

    # --------------------------------------------------------------------- #

    def _peek_available_columns(self, source: str) -> List[str]:
        """Cheap schema peek; used only for observability."""
        try:
            s = source.lower()
            # We can't easily peek S3 without a filesystem object,
            # so we'll just handle local files for now.
            if _is_s3_uri(s):
                return []
            if s.endswith(".parquet"):
                return list(pl.scan_parquet(source).collect_schema().names())
            if s.endswith(".csv"):
                return list(pl.scan_csv(source).collect_schema().names())
        except Exception:
            pass
        return []