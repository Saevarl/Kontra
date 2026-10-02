# src/kontra/engine/materializers/trino.py
"""
Trino Materializer - loads Trino tables into Polars DataFrames.

Only runs for rules the Trino executor leaves to the Polars tier. Projection
is applied in the SELECT, so only the columns those rules need are read.
"""

from __future__ import annotations

import os
import time
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import polars as pl

from kontra.connectors.detection import parse_table_reference
from kontra.connectors.handle import DatasetHandle
from kontra.connectors.trino import TrinoConnectionParams
from kontra.connectors.trino_types import polars_dtype
from kontra.engine.sql_ir import esc_ident, lit_str

from .base import BaseMaterializer
from .registry import register_materializer


def _ident(name: str) -> str:
    return esc_ident(name, "trino")


@register_materializer("trino")
class TrinoMaterializer(BaseMaterializer):
    """Materialize Trino tables as Polars DataFrames."""

    materializer_name = "trino"

    def __init__(self, handle: DatasetHandle):
        super().__init__(handle)
        self._is_byoc = handle.external_conn is not None and handle.scheme == "byoc"

        if self._is_byoc:
            if not handle.table_ref:
                raise ValueError("BYOC handle missing table_ref")
            catalog, schema, table = parse_table_reference(handle.table_ref)
            if not schema:
                raise ValueError(
                    f"Trino table reference needs a schema: 'schema.table' or "
                    f"'catalog.schema.table' (got: {handle.table_ref!r})"
                )
        elif handle.db_params:
            self.params: TrinoConnectionParams = handle.db_params
            catalog, schema, table = self.params.catalog, self.params.schema, self.params.table
        else:
            raise ValueError("Trino handle missing db_params or external_conn")

        self._catalog: str | None = catalog
        self._schema_name = schema
        self._table_name = table
        parts = [catalog, schema, table] if catalog else [schema, table]
        self._qualified_table = ".".join(_ident(p) for p in parts)
        self._io_debug_enabled = bool(os.getenv("KONTRA_IO_DEBUG"))
        self._last_io_debug: dict[str, Any] | None = None

    def _connection_ctx(self):
        from kontra.connectors.db_utils import get_connection_ctx

        return get_connection_ctx(self.handle, "trino")

    def schema(self) -> list[str]:
        """Return column names without loading data."""
        from kontra.connectors import trino_read

        declared = trino_read.declared_columns(self.handle)
        if declared is not None:
            return [name for name, _, _ in declared]
        source = (
            f"{_ident(self._catalog)}.information_schema.columns"
            if self._catalog
            else "information_schema.columns"
        )
        with self._connection_ctx() as conn:
            cur = conn.cursor()
            try:
                cur.execute(
                    f"SELECT column_name FROM {source} "
                    f"WHERE table_schema = {lit_str(self._schema_name, 'trino')} "
                    f"AND table_name = {lit_str(self._table_name, 'trino')} "
                    "ORDER BY ordinal_position"
                )
                return [row[0] for row in cur.fetchall()]
            finally:
                cur.close()

    def to_polars(self, columns: list[str] | None) -> pl.DataFrame:
        """Load table data as a Polars DataFrame with optional projection."""
        import polars as pl

        from kontra.connectors import trino_read

        cols_sql = ", ".join(_ident(c) for c in columns) if columns else "*"
        query = (
            f"SELECT {cols_sql} FROM {self._qualified_table}{trino_read.pin_suffix(self.handle)}"
        )

        t0 = time.perf_counter()
        with self._connection_ctx() as conn:
            cur = conn.cursor()
            try:
                cur.execute(query)
                rows = cur.fetchall()
                description = list(cur.description or [])
            finally:
                cur.close()
        t1 = time.perf_counter()

        col_names = [desc[0] for desc in description]
        # The declared types, read once per validation, are the ones preplan's
        # dtype decisions used; outside a held state, the cursor's own types.
        types = {
            name: data_type for name, data_type, _ in trino_read.declared_columns(self.handle) or []
        }
        declared = {desc[0]: polars_dtype(types.get(desc[0], desc[1])) for desc in description}
        if rows:
            # Scan every row for dtype inference, as the SQL Server materializer
            # does: a column NULL for its first 100 rows would otherwise infer as
            # Null and fail on its first real value. Then cast to the declared
            # type: inference makes every integer Int64 and every float Float64.
            df = pl.DataFrame(rows, schema=col_names, orient="row", infer_schema_length=None)
            df = df.with_columns(
                pl.col(name).cast(declared[name])
                for name, dtype in df.schema.items()
                if declared.get(name) is not None and dtype != declared[name]
            )
        else:
            df = pl.DataFrame(schema={name: declared[name] or pl.Utf8 for name in col_names})

        if self._io_debug_enabled:
            self._last_io_debug = {
                "materializer": "trino",
                "mode": "byoc_fetch" if self._is_byoc else "trino_fetch",
                "table": self._qualified_table,
                "columns_requested": list(columns or []),
                "column_count": len(columns or col_names),
                "row_count": len(rows),
                "elapsed_ms": int((t1 - t0) * 1000),
            }
        else:
            self._last_io_debug = None

        return df

    def io_debug(self) -> dict[str, Any] | None:
        return self._last_io_debug
