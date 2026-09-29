"""Conservative local routing for bounded, exact-count scalar contracts.

SQL remains preferable for unbounded sources and early-stop contracts. For a
small Parquet file whose rules all have equivalent scalar Polars implementations,
one projected materialization avoids SQL setup and repeated scans. CSV retains
its existing SQL route: parser costs dominate and changing readers is unsafe.
"""

from __future__ import annotations

import math
import os
import stat
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from kontra.connectors.handle import DatasetHandle
    from kontra.engine.types import CompilationContext, PreplanResult

# These are routing budgets, not limits on what validate() accepts. Larger and
# unsupported inputs continue through the existing SQL/residual pipeline.
_MAX_FILE_BYTES = 32 * 1024 * 1024
_MAX_PARQUET_BYTES = 64 * 1024 * 1024
_MAX_PARQUET_ROWS = 250_000


def _eligible_rules(ctx, handled):
    from kontra.rule_defs.builtin.allowed_values import AllowedValuesRule
    from kontra.rule_defs.builtin.not_null import NotNullRule
    from kontra.rule_defs.builtin.range import RangeRule
    from kontra.rule_defs.builtin.unique import UniqueRule

    rules = [r for r in ctx.rules if r.rule_id not in handled]
    # Explicitly whitelist implementations, not extensible rule names. Keep
    # early-stop and mixed tally contracts on their existing execution path.
    if not rules or any(
        type(r) not in (NotNullRule, UniqueRule, RangeRule, AllowedValuesRule)
        or not ctx.tally_map.get(r.rule_id, False)
        for r in rules
    ):
        return None
    for rule in rules:
        literals = (
            [rule.params.get(k) for k in ("min", "max")]
            if rule.name == "range"
            else rule.params.get("values", [])
        )
        # Polars cannot represent arbitrary Python integer literals. Let SQL
        # handle oversized values instead of turning a predicate error into a
        # failed validation or overflowing an is_in() series.
        if any(type(v) is int and not -(2**63) <= v < 2**63 for v in literals):
            return None
        if rule.name == "range":
            bounds = [rule.params.get(k) for k in ("min", "max")]
            if any(
                v is not None
                and (type(v) not in (int, float) or (type(v) is float and not math.isfinite(v)))
                for v in bounds
            ):
                return None
        if rule.name == "allowed_values":
            values = rule.params["values"]
            if any(v is not None and type(v) not in (str, int, float, bool) for v in values):
                return None
            if any(type(v) is float and not math.isfinite(v) for v in values):
                return None
    return rules


def _compatible_schema(rules, schema):
    """Avoid coercions and nested/decimal/temporal backend differences."""
    for rule in rules:
        kind = schema.get(rule.params["column"])
        if kind not in ("string", "integer", "unsigned", "float", "float32", "boolean"):
            return False
        # Polars and DuckDB order NaN differently: DuckDB sorts it above every
        # number, so a min-only bound passes NaN in SQL but fails it in Polars.
        if rule.name == "range" and kind != "integer":
            return False
        # Comparing BIGINT with DOUBLE in Polars can round values beyond
        # 2**53; DuckDB may compare to the SQL literal as an exact decimal.
        if (
            rule.name == "range"
            and kind == "integer"
            and any(type(rule.params.get(k)) is float for k in ("min", "max"))
        ):
            return False
        if rule.name == "allowed_values":
            if kind in ("unsigned", "float32"):
                return False
            types = {
                "string": (str,),
                "integer": (int,),
                "float": (int, float),
                "boolean": (bool,),
            }[kind]
            if any(v is not None and type(v) not in types for v in rule.params["values"]):
                return False
    return True


class _MaterializedParquet:
    """A projected frame owned by one validation, never cached by pathname."""

    name = "polars-connector"
    prefer_native_parquet = True

    def __init__(self, df, columns):
        self.df = df
        self.columns = columns

    def schema(self):
        return self.columns

    def to_polars(self, columns):
        return self.df if not columns or columns == self.df.columns else self.df.select(columns)

    def io_debug(self):
        return None


def select_local_materializer(
    handle: DatasetHandle,
    ctx: CompilationContext,
    preplan: PreplanResult,
    *,
    pushdown_mode: str,
    csv_mode: str,
    enable_projection: bool,
):
    """Return a per-call materializer, or None to retain SQL execution.

    Filesystem/stat/reader failures decline the optimization; the existing
    pipeline remains responsible for fallback and user-facing errors.
    """
    if (
        not preplan.effective
        or pushdown_mode != "on"
        or csv_mode != "auto"
        or handle.scheme not in ("", "file")
        or handle.sql is not None
        or handle.format != "parquet"
        or preplan.summary.get("row_groups_pruned", 0) not in (None, 0)
        or preplan.total_rows is None
        or not 0 <= preplan.total_rows <= _MAX_PARQUET_ROWS
        or preplan.parquet_byte_size is None
        or not 0 <= preplan.parquet_byte_size <= _MAX_PARQUET_BYTES
    ):
        return None
    rules = _eligible_rules(ctx, preplan.handled_ids)
    if rules is None:
        return None
    try:
        info = os.stat(handle.uri)
        if not stat.S_ISREG(info.st_mode) or info.st_size > _MAX_FILE_BYTES:
            return None
        import polars as pl

        # Reuse the preplan's size budget and ask the actual reader for types.
        # Importing Arrow solely for routing would retain its native libraries
        # (and NumPy) even though the data is read and validated in Polars.
        kinds = {
            pl.String: "string",
            **dict.fromkeys((pl.Int8, pl.Int16, pl.Int32, pl.Int64), "integer"),
            **dict.fromkeys((pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64), "unsigned"),
            pl.Float64: "float",
            pl.Float32: "float32",
            pl.Boolean: "boolean",
        }
        schema = {name: kinds.get(t, "other") for name, t in pl.read_parquet_schema(handle.uri).items()}
        if not _compatible_schema(rules, schema):
            return None
        from kontra.engine.materializers.polars_connector import PolarsConnectorMaterializer

        columns = sorted({r.params["column"] for r in rules}) if enable_projection else None
        # Load inside this optional phase so an unsupported native reader still
        # falls back to SQL. A complete manifest permits the parallel native
        # reader; pruned manifests were excluded above to preserve their proofs.
        df = PolarsConnectorMaterializer(handle).to_polars(columns)
        return _MaterializedParquet(df, list(schema))
    except Exception as exc:  # noqa: BLE001 - optional route retains SQL fallback
        from kontra.logging import get_logger

        get_logger(__name__).debug("Local routing unavailable: %s", exc)
        # Match the existing SQL optimization's graceful fallback policy.
        return None
