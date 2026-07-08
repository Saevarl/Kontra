# src/kontra/engine/executors/clickhouse_sql.py
"""
ClickHouse SQL Executor - pushes validation rules down to ClickHouse.

ClickHouse is a columnar OLAP engine: validation aggregates (countIf, uniqExact,
SUM(CASE ...)) run at native speed on compressed columns. The design goal is to
push *everything* down and transfer only the small result rows back — the Polars
residual tier should almost never fire. Unlike SQL Server, ClickHouse has native
regex via match(), so regex pushes down too.
"""

from __future__ import annotations

import re
from contextlib import contextmanager
from typing import Any, Dict, Tuple

from kontra.connectors.handle import DatasetHandle
from kontra.connectors.clickhouse import ClickHouseConnectionParams, get_connection
from kontra.connectors.detection import parse_table_reference

from .database_base import DatabaseSqlExecutor
from .registry import register_executor


@register_executor("clickhouse")
class ClickHouseSqlExecutor(DatabaseSqlExecutor):
    """
    ClickHouse SQL pushdown executor.

    Inherits compile()/execute() from DatabaseSqlExecutor; provides the
    ClickHouse connection, table reference, and dialect. Regex IS supported
    (native match()).
    """

    DIALECT = "clickhouse"
    SUPPORTED_RULES = {
        "not_null", "unique", "min_rows", "max_rows",
        "allowed_values", "disallowed_values",
        "freshness", "range", "length", "regex",
        "contains", "starts_with", "ends_with",
        "compare", "conditional_not_null", "conditional_range",
        "custom_sql_check", "custom_agg",
    }

    # \w \W \d \D \s \S \b \B mean ASCII in ClickHouse's RE2 but Unicode in
    # Polars' regex crate, so a pattern using them can flip pass/fail between
    # tiers. Detect them (backslash + one of those letters) and defer to Polars.
    _AMBIGUOUS_REGEX = re.compile(r"\\[wWdDsSbB]")

    @property
    def name(self) -> str:
        return "clickhouse"

    def _is_pushable(self, kind: str, spec: Dict[str, Any]) -> bool:
        if kind not in self.SUPPORTED_RULES:
            return False
        if kind == "regex":
            pattern = spec.get("pattern") or spec.get("value") or ""
            if self._AMBIGUOUS_REGEX.search(pattern):
                return False  # RE2 ASCII vs Polars Unicode — defer to Polars
        return True

    def _supports_scheme(self, scheme: str, handle: DatasetHandle) -> bool:
        if scheme == "byoc" and handle.dialect == "clickhouse":
            return handle.external_conn is not None
        return scheme in {"clickhouse", "clickhouses"}

    @contextmanager
    def _get_connection_ctx(self, handle: DatasetHandle):
        if handle.scheme == "byoc" and handle.external_conn is not None:
            yield handle.external_conn
        elif handle.db_params:
            with get_connection(handle.db_params) as conn:
                yield conn
        else:
            raise ValueError("Handle has neither external_conn nor db_params")

    def _db_and_table(self, handle: DatasetHandle) -> Tuple[str, str]:
        """ClickHouse has no schema layer: return (database, table)."""
        if handle.scheme == "byoc" and handle.table_ref:
            db, _schema, table = parse_table_reference(handle.table_ref)
            return db or "", table
        if handle.db_params:
            params: ClickHouseConnectionParams = handle.db_params
            return params.database, params.table
        raise ValueError("Handle has neither table_ref nor db_params")

    def _get_table_reference(self, handle: DatasetHandle) -> str:
        db, table = self._db_and_table(handle)
        return f"{self._esc(db)}.{self._esc(table)}" if db else self._esc(table)

    def _get_schema_and_table(self, handle: DatasetHandle) -> Tuple[str, str]:
        # Used for custom_sql_check {table} placeholder; ClickHouse's "schema"
        # is the database.
        return self._db_and_table(handle)

    def introspect(self, handle: DatasetHandle, **kwargs) -> Dict[str, Any]:
        from kontra.engine.sql_ir import lit_str

        db, table_name = self._db_and_table(handle)
        table_ref = self._get_table_reference(handle)

        with self._get_connection_ctx(handle) as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table_ref}")
                row = cursor.fetchone()
                n = int(row[0]) if row else 0

                cursor.execute(
                    "SELECT name FROM system.columns "
                    f"WHERE database = {lit_str(db, 'clickhouse')} "
                    f"AND table = {lit_str(table_name, 'clickhouse')} "
                    "ORDER BY position"
                )
                cols = [r[0] for r in cursor.fetchall()]
            finally:
                cursor.close()

        return {"row_count": n, "available_cols": cols, "staging": None}
