# src/kontra/engine/phases/preplan.py
"""
Preplan (metadata-only) phase.

Executes metadata analysis to prove rules pass/fail without scanning data.
Supports three backends:
- Parquet: Row-group statistics (min/max/null_count)
- PostgreSQL: pg_stats catalog
- SQL Server: sys.columns catalog
"""

from __future__ import annotations

from typing import Any, Dict, Optional, Set, TYPE_CHECKING

if TYPE_CHECKING:
    import pyarrow.fs as pafs
    from kontra.connectors.handle import DatasetHandle
    from kontra.engine.types import CompilationContext

from kontra.engine.types import PreplanResult
from kontra.engine.stats import now_ms
from kontra.logging import get_logger

_logger = get_logger(__name__)


def _build_preplan_summary(
    enabled: bool,
    effective: bool,
    pass_meta: int,
    fail_meta: int,
    unknown: int,
    rg_kept: Optional[int] = None,
    rg_total: Optional[int] = None,
) -> Dict[str, Any]:
    """Build preplan summary dict for stats."""
    summary: Dict[str, Any] = {
        "enabled": enabled,
        "effective": effective,
        "rules_pass_meta": pass_meta,
        "rules_fail_meta": fail_meta,
        "rules_unknown": unknown,
        "row_groups_kept": rg_kept,
        "row_groups_total": rg_total,
        "row_groups_pruned": (rg_total - rg_kept) if (rg_total is not None and rg_kept is not None) else None,
    }
    return summary


def _process_preplan_decisions(
    pre: Any,  # PrePlan
    tally_map: Dict[str, bool],
    severity_map: Dict[str, str],
    execution_source_msg: str,
) -> tuple[Dict[str, Dict[str, Any]], Set[str], int, int, int]:
    """
    Process preplan rule decisions into results.

    Shared logic for all preplan backends (Parquet, PostgreSQL, SQL Server).

    Returns:
        Tuple of (results_by_id, handled_ids, pass_count, fail_count, unknown_count)
    """
    results_by_id: Dict[str, Dict[str, Any]] = {}
    handled_ids: Set[str] = set()
    pass_meta = fail_meta = unknown = 0

    for rid, decision in pre.rule_decisions.items():
        # If rule needs exact counts (tally=True), skip preplan for this rule
        if tally_map.get(rid, False):
            unknown += 1
            continue

        if decision == "pass_meta":
            results_by_id[rid] = {
                "rule_id": rid,
                "passed": True,
                "failed_count": 0,
                "message": f"Proven by metadata ({execution_source_msg})",
                "execution_source": "metadata",
                "severity": severity_map.get(rid, "blocking"),
                "tally": tally_map.get(rid, False),
            }
            handled_ids.add(rid)
            pass_meta += 1
        elif decision == "fail_meta":
            # Build appropriate message based on rule type
            details = pre.fail_details.get(rid, {})
            if details.get("expected") and details.get("actual"):
                # dtype mismatch - compact format
                msg = f"{details['expected']} ≠ {details['actual']}"
            else:
                msg = f"Failed: violation proven by {execution_source_msg} metadata"
            results_by_id[rid] = {
                "rule_id": rid,
                "passed": False,
                "failed_count": 1,
                "message": msg,
                "execution_source": "metadata",
                "severity": severity_map.get(rid, "blocking"),
                "tally": tally_map.get(rid, False),
            }
            handled_ids.add(rid)
            fail_meta += 1
        else:
            unknown += 1

    return results_by_id, handled_ids, pass_meta, fail_meta, unknown


def _execute_parquet_preplan(
    handle: "DatasetHandle",
    ctx: "CompilationContext",
    preplan_fs: Optional["pafs.FileSystem"],
    explain_preplan: bool,
) -> PreplanResult:
    """Execute preplan for Parquet files."""
    from kontra.preplan.planner import preplan_single_parquet, preplan_parquet_glob, is_glob_pattern
    from kontra.preplan.types import PrePlan
    from kontra.rule_defs.static_predicates import extract_static_predicates

    from kontra.connectors.uri_utils import (
        is_s3_uri as _is_s3_uri,
        is_azure_uri as _is_azure_uri,
        s3_uri_to_path as _s3_uri_to_path,
        azure_uri_to_path as _azure_uri_to_path,
    )

    t0 = now_ms()
    static_preds = extract_static_predicates(rules=ctx.rules)

    # Check for glob patterns - use different preplan strategy
    if is_glob_pattern(handle.uri):
        # Glob patterns: use DuckDB-based expansion, schema-only preplan
        pre: PrePlan = preplan_parquet_glob(
            glob_path=handle.uri,
            required_columns=ctx.compiled_full.required_cols,
            predicates=static_preds,
            fs_opts=handle.fs_opts,
        )
    else:
        # Single file: use pyarrow with full metadata preplan
        if _is_s3_uri(handle.uri) and preplan_fs:
            preplan_path = _s3_uri_to_path(handle.uri)
        elif _is_azure_uri(handle.uri) and preplan_fs:
            preplan_path = _azure_uri_to_path(handle.uri)
        else:
            preplan_path = handle.uri
        pre: PrePlan = preplan_single_parquet(
            path=preplan_path,
            required_columns=ctx.compiled_full.required_cols,
            predicates=static_preds,
            filesystem=preplan_fs,
        )
    analyze_ms = now_ms() - t0

    # Process decisions
    results_by_id, handled_ids, pass_meta, fail_meta, unknown = _process_preplan_decisions(
        pre=pre,
        tally_map=ctx.tally_map,
        severity_map=ctx.severity_map,
        execution_source_msg="Parquet stats",
    )

    row_groups = list(pre.manifest_row_groups or [])
    columns = list(pre.manifest_columns or [])
    total_rows = pre.stats.get("total_rows")

    rg_total = pre.stats.get("rg_total")
    rg_kept = len(row_groups)

    if explain_preplan:
        print(
            "\n-- PREPLAN (metadata) --"
            f"\n  Row-groups kept: {rg_kept}/{rg_total}"
            f"\n  Rules: {pass_meta} pass, {fail_meta} fail, {unknown} unknown\n"
        )

    return PreplanResult(
        effective=True,
        handled_ids=handled_ids,
        results_by_id=results_by_id,
        row_groups=row_groups,
        columns=columns,
        total_rows=total_rows,
        analyze_ms=analyze_ms,
        summary=_build_preplan_summary(
            enabled=True,
            effective=True,
            pass_meta=pass_meta,
            fail_meta=fail_meta,
            unknown=unknown,
            rg_kept=rg_kept,
            rg_total=rg_total,
        ),
    )


def _execute_postgres_preplan(
    handle: "DatasetHandle",
    ctx: "CompilationContext",
) -> PreplanResult:
    """Execute preplan for PostgreSQL tables."""
    from kontra.preplan.postgres import preplan_postgres, can_preplan_postgres
    from kontra.rule_defs.static_predicates import extract_static_predicates

    if not can_preplan_postgres(handle):
        return _empty_preplan_result(enabled=True)

    t0 = now_ms()
    static_preds = extract_static_predicates(rules=ctx.rules)
    pre = preplan_postgres(
        handle=handle,
        required_columns=ctx.compiled_full.required_cols,
        predicates=static_preds,
    )
    analyze_ms = now_ms() - t0

    # Process decisions
    results_by_id, handled_ids, pass_meta, fail_meta, unknown = _process_preplan_decisions(
        pre=pre,
        tally_map=ctx.tally_map,
        severity_map=ctx.severity_map,
        execution_source_msg="PostgreSQL catalog",
    )

    return PreplanResult(
        effective=True,
        handled_ids=handled_ids,
        results_by_id=results_by_id,
        analyze_ms=analyze_ms,
        summary=_build_preplan_summary(
            enabled=True,
            effective=True,
            pass_meta=pass_meta,
            fail_meta=fail_meta,
            unknown=unknown,
        ),
    )


def _execute_sqlserver_preplan(
    handle: "DatasetHandle",
    ctx: "CompilationContext",
) -> PreplanResult:
    """Execute preplan for SQL Server tables."""
    from kontra.preplan.sqlserver import preplan_sqlserver, can_preplan_sqlserver
    from kontra.rule_defs.static_predicates import extract_static_predicates

    if not can_preplan_sqlserver(handle):
        return _empty_preplan_result(enabled=True)

    t0 = now_ms()
    static_preds = extract_static_predicates(rules=ctx.rules)
    pre = preplan_sqlserver(
        handle=handle,
        required_columns=ctx.compiled_full.required_cols,
        predicates=static_preds,
    )
    analyze_ms = now_ms() - t0

    # Process decisions
    results_by_id, handled_ids, pass_meta, fail_meta, unknown = _process_preplan_decisions(
        pre=pre,
        tally_map=ctx.tally_map,
        severity_map=ctx.severity_map,
        execution_source_msg="SQL Server catalog",
    )

    return PreplanResult(
        effective=True,
        handled_ids=handled_ids,
        results_by_id=results_by_id,
        analyze_ms=analyze_ms,
        summary=_build_preplan_summary(
            enabled=True,
            effective=True,
            pass_meta=pass_meta,
            fail_meta=fail_meta,
            unknown=unknown,
        ),
    )


def _empty_preplan_result(enabled: bool) -> PreplanResult:
    """Create an empty preplan result (preplan disabled or not applicable)."""
    return PreplanResult(
        effective=False,
        handled_ids=set(),
        results_by_id={},
        summary=_build_preplan_summary(
            enabled=enabled,
            effective=False,
            pass_meta=0,
            fail_meta=0,
            unknown=0,
        ),
    )


def _is_parquet(path: str | None) -> bool:
    return isinstance(path, str) and path.lower().endswith(".parquet")


def execute_preplan(
    handle: "DatasetHandle",
    ctx: "CompilationContext",
    preplan_mode: str,
    preplan_fs: Optional["pafs.FileSystem"],
    explain_preplan: bool = False,
) -> PreplanResult:
    """
    Execute the preplan (metadata-only) phase.

    Args:
        handle: Dataset handle with URI and connection info
        ctx: Compilation context with rules and tally settings
        preplan_mode: "on" or "off"
        preplan_fs: PyArrow filesystem for cloud storage (S3/Azure)
        explain_preplan: Print preplan diagnostics

    Returns:
        PreplanResult with handled rules and metadata
    """
    if preplan_mode != "on":
        return _empty_preplan_result(enabled=False)

    # Parquet files
    if _is_parquet(handle.uri):
        try:
            return _execute_parquet_preplan(handle, ctx, preplan_fs, explain_preplan)
        except Exception as e:
            # Distinguish between "preplan not available" vs "real errors"
            err_str = str(e).lower()

            # Re-raise errors that indicate real problems
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
                # Distinguish local vs cloud paths for error message clarity
                uri = handle.uri
                _is_cloud = uri.lower().startswith(("s3://", "abfs://", "abfss://", "az://", "http://", "https://"))
                if _is_cloud:
                    raise RuntimeError(
                        f"Unable to access file: {e}. "
                        "Check file path and credentials."
                    ) from e
                raise FileNotFoundError(
                    f"File not found: {uri}"
                ) from e

            # Otherwise, preplan optimization just isn't available
            _logger.info("Preplan skipped (%s): %s", type(e).__name__, e)
            return _empty_preplan_result(enabled=True)

    # PostgreSQL
    if handle.scheme in ("postgres", "postgresql"):
        try:
            return _execute_postgres_preplan(handle, ctx)
        except Exception as e:
            _logger.info("PostgreSQL preplan skipped: %s", e)
            return _empty_preplan_result(enabled=True)

    # SQL Server
    if handle.scheme in ("mssql", "sqlserver"):
        try:
            return _execute_sqlserver_preplan(handle, ctx)
        except Exception as e:
            _logger.info("SQL Server preplan skipped: %s", e)
            return _empty_preplan_result(enabled=True)

    # No preplan available for this data source type
    return _empty_preplan_result(enabled=True)
