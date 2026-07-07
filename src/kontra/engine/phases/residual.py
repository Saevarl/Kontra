# src/kontra/engine/phases/residual.py
"""
Residual Polars execution phase.

Executes remaining rules that couldn't be resolved by preplan or SQL pushdown.
"""

from __future__ import annotations

from typing import Any, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    import polars as pl
    import pyarrow.fs as pafs
    from kontra.connectors.handle import DatasetHandle
    from kontra.engine.types import CompilationContext, PreplanResult, PushdownResult

from kontra.engine.types import ResidualResult
from kontra.engine.stats import now_ms
from kontra.logging import get_logger, log_exception

_logger = get_logger(__name__)


# Lazy loading cache
_lazy_polars = None
_lazy_polars_backend = None


def _get_polars():
    """Lazy load polars module."""
    global _lazy_polars
    if _lazy_polars is None:
        import polars
        _lazy_polars = polars
    return _lazy_polars


def _get_polars_backend():
    """Lazy load PolarsBackend class."""
    global _lazy_polars_backend
    if _lazy_polars_backend is None:
        from kontra.engine.backends.polars_backend import PolarsBackend
        _lazy_polars_backend = PolarsBackend
    return _lazy_polars_backend


from kontra.connectors.uri_utils import (
    is_parquet as _is_parquet,
    is_s3_uri as _is_s3_uri,
    is_azure_uri as _is_azure_uri,
    s3_uri_to_path as _s3_uri_to_path,
    azure_uri_to_path as _azure_uri_to_path,
    create_s3_filesystem as _create_s3_filesystem,
    create_azure_filesystem as _create_azure_filesystem,
)


def execute_residual(
    handle: "DatasetHandle",
    ctx: "CompilationContext",
    preplan: "PreplanResult",
    pushdown: "PushdownResult",
    materializer: Any,
    preplan_fs: Optional["pafs.FileSystem"],
    enable_projection: bool,
) -> ResidualResult:
    """
    Execute residual rules via Polars.

    Args:
        handle: Dataset handle (may be updated if CSV was staged)
        ctx: Compilation context with rules and tally settings
        preplan: Preplan result with handled rules and row-group manifest
        pushdown: Pushdown result with handled rules
        materializer: Data materializer for loading data
        preplan_fs: PyArrow filesystem for cloud storage
        enable_projection: Whether to use column projection

    Returns:
        ResidualResult with Polars execution results and loaded DataFrame
    """
    # Compute residual rules
    handled_all = preplan.handled_ids | pushdown.handled_ids
    compiled_residual = ctx.plan.without_ids(ctx.compiled_full, handled_all)

    # Projection is DC-driven; independent of preplan/pushdown
    required_cols_residual = compiled_residual.required_cols if enable_projection else []

    # If no residual rules, skip data loading entirely
    if not compiled_residual.predicates and not compiled_residual.fallback_rules:
        return ResidualResult(results=[], df=None)

    # Lazy load polars
    pl = _get_polars()

    # Materialize minimal slice
    t0 = now_ms()

    # If preplan produced a row-group manifest, honor it
    if preplan.effective and _is_parquet(handle.uri) and preplan.row_groups:
        import pyarrow as pa
        import pyarrow.parquet as pq

        cols = (required_cols_residual or None) if enable_projection else None

        # Reuse preplan filesystem if available, otherwise create from handle
        residual_fs = preplan_fs
        if residual_fs is None and _is_s3_uri(handle.uri):
            try:
                residual_fs = _create_s3_filesystem(handle)
            except Exception as e:
                log_exception(_logger, "Could not create S3 filesystem for residual load", e)
        elif residual_fs is None and _is_azure_uri(handle.uri):
            try:
                residual_fs = _create_azure_filesystem(handle)
            except Exception as e:
                log_exception(_logger, "Could not create Azure filesystem for residual load", e)

        # PyArrow filesystems expect specific path formats
        if _is_s3_uri(handle.uri) and residual_fs:
            residual_path = _s3_uri_to_path(handle.uri)
        elif _is_azure_uri(handle.uri) and residual_fs:
            residual_path = _azure_uri_to_path(handle.uri)
        else:
            residual_path = handle.uri
        pf = pq.ParquetFile(residual_path, filesystem=residual_fs)

        pa_cols = cols if cols else None
        rg_tables = [pf.read_row_group(i, columns=pa_cols) for i in preplan.row_groups]
        pa_tbl = pa.concat_tables(rg_tables) if len(rg_tables) > 1 else rg_tables[0]
        df = pl.from_arrow(pa_tbl)
    else:
        # Materializer respects projection (engine passes residual required cols)
        df = materializer.to_polars(required_cols_residual or None)
    load_ms = now_ms() - t0

    # Execute residual rules in Polars
    t0 = now_ms()
    PolarsBackend = _get_polars_backend()
    polars_exec = PolarsBackend(executor=ctx.plan.execute_compiled)
    polars_art = polars_exec.compile(compiled_residual)
    polars_out = polars_exec.execute(df, polars_art, ctx.tally_map)
    execute_ms = now_ms() - t0

    return ResidualResult(
        results=polars_out.get("results", []),
        df=df,
        load_ms=load_ms,
        execute_ms=execute_ms,
    )
