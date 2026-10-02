# src/kontra/engine/executors/trino_sql.py
"""
Trino SQL Executor - pushes validation rules down to Trino.

Trino is strictly typed and its semantics differ from Polars in a few places
(NaN ordering, CHAR padding, Java regex, no implicit string-to-date casts).
Before running, the executor reads the table's column types once from
``information_schema.columns`` and pushes only the rules whose SQL gives the
same answer as the Polars tier for those types. Every other rule is left out
of the SQL results, so the residual Polars tier measures it and the rule
reports ``execution_source`` ``polars``. Pushdown is exact or it falls back.
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any

from kontra.connectors import trino_read
from kontra.connectors.db_utils import get_connection_ctx
from kontra.connectors.detection import parse_table_reference
from kontra.connectors.handle import DatasetHandle
from kontra.connectors.trino import TrinoConnectionParams
from kontra.engine.sql_ir import lit_str, trino_regex
from kontra.logging import get_logger

from .database_base import DatabaseSqlExecutor
from .registry import register_executor

_logger = get_logger(__name__)

_INTEGER = {"tinyint", "smallint", "integer", "bigint"}

# Type families whose comparisons, equality and ordering agree with Polars.
# Floating columns are excluded (Polars orders NaN above every number, Trino
# compares NaN as false), as is CHAR(n) (Trino pads it for comparison).
_ORDERED = {"integer", "decimal", "varchar", "date", "timestamp", "timestamptz", "boolean"}


def _family(data_type: str | None) -> str | None:
    """Map an ``information_schema`` data_type to the family used for gating."""
    if not data_type:
        return None
    t = data_type.lower().strip()
    if t in _INTEGER:
        return "integer"
    if t.startswith("decimal"):
        return "decimal"
    if t.startswith("varchar"):
        return "varchar"
    if t.startswith("timestamp"):
        # The materializer reads timestamps at microsecond precision, so finer
        # values the SQL tier tells apart can collapse into equal ones.
        precision = t[len("timestamp(") : t.find(")")] if t.startswith("timestamp(") else "3"
        if not precision.isdigit() or int(precision) > 6:
            return "other"
        return "timestamptz" if t.endswith("with time zone") else "timestamp"
    if t in ("date", "boolean", "real", "double"):
        return {"real": "float", "double": "float"}.get(t, t)
    return "other"


def _literal_fits(family: str | None, value: Any) -> bool:
    """Whether a rule literal compares to a column of this family without a cast."""
    if family == "varchar":
        return isinstance(value, str)
    if family == "boolean":
        return isinstance(value, bool)
    if family in ("integer", "decimal"):
        # Float literals stay in Polars: Trino compares them exactly as decimals,
        # Polars through a float or decimal cast, and the two can disagree.
        return isinstance(value, int) and not isinstance(value, bool)
    return False


def _bounds_fit(family: str | None, *bounds: Any) -> bool:
    if family not in ("integer", "decimal"):
        return False
    return all(b is None or _literal_fits(family, b) for b in bounds)


@register_executor("trino")
class TrinoSqlExecutor(DatabaseSqlExecutor):
    """
    Trino SQL pushdown executor.

    Inherits compile()/execute() from DatabaseSqlExecutor; adds the per-type
    exactness gate, the Trino connection and the catalog.schema.table reference.
    """

    DIALECT = "trino"
    INCLUDE_ROW_COUNT_IN_AGGREGATE = True
    SUPPORTED_RULES = frozenset(
        {
            "not_null",
            "unique",
            "min_rows",
            "max_rows",
            "allowed_values",
            "disallowed_values",
            "freshness",
            "range",
            "length",
            "regex",
            "contains",
            "starts_with",
            "ends_with",
            "compare",
            "conditional_not_null",
            "conditional_range",
            "custom_sql_check",
            "custom_agg",
        }
    )

    @property
    def name(self) -> str:
        return "trino"

    def _is_pushable(self, kind: str, spec: dict[str, Any]) -> bool:
        if kind not in self.SUPPORTED_RULES:
            return False
        if kind == "regex":
            pattern = spec.get("pattern")
            return isinstance(pattern, str) and trino_regex(pattern) is not None
        return True

    def _supports_scheme(self, scheme: str, handle: DatasetHandle) -> bool:
        if scheme == "byoc" and handle.dialect == "trino":
            return handle.external_conn is not None
        return scheme in {"trino", "trinos"}

    @contextmanager
    def _get_connection_ctx(self, handle: DatasetHandle):
        with get_connection_ctx(handle, "trino") as conn:
            yield conn

    def _parts(self, handle: DatasetHandle) -> tuple[str | None, str, str]:
        """(catalog, schema, table). BYOC may omit the catalog (session default)."""
        if handle.scheme == "byoc" and handle.table_ref:
            catalog, schema, table = parse_table_reference(handle.table_ref)
            if not schema:
                raise ValueError(
                    f"Trino table reference needs a schema: 'schema.table' or "
                    f"'catalog.schema.table' (got: {handle.table_ref!r})"
                )
            return catalog, schema, table
        if handle.db_params:
            params: TrinoConnectionParams = handle.db_params
            return params.catalog, params.schema, params.table
        raise ValueError("Handle has neither table_ref nor db_params")

    def _get_table_reference(self, handle: DatasetHandle) -> str:
        catalog, schema, table = self._parts(handle)
        parts = [catalog, schema, table] if catalog else [schema, table]
        # Pinned to one snapshot when the validation reads a caller's autocommit
        # connection (trino_read); empty otherwise.
        return ".".join(self._esc(p) for p in parts) + trino_read.pin_suffix(handle)

    def _execute_custom_sql_queries(self, cursor, handle, custom_sql_specs):
        # A pinned validation pins custom SQL too: {table} becomes the pinned relation.
        if trino_read.pin_suffix(handle):
            table = self._get_table_reference(handle)
            custom_sql_specs = [
                {**spec, "sql": spec.get("sql", "").replace("{table}", table)}
                for spec in custom_sql_specs
            ]
        return super()._execute_custom_sql_queries(cursor, handle, custom_sql_specs)

    def _get_schema_and_table(self, handle: DatasetHandle) -> tuple[str, str]:
        # custom_sql_check {table}: the catalog travels with the schema part.
        catalog, schema, table = self._parts(handle)
        return (f"{catalog}.{schema}" if catalog else schema), table

    def _close_cursor(self, cursor):
        cursor.close()

    def _assemble_single_row(self, selects: list[str], table: str) -> str:
        # Trino rejects a trailing semicolon.
        return super()._assemble_single_row(selects, table).rstrip(";")

    def _assemble_exists_query(self, exists_exprs: list[str]) -> str:
        return super()._assemble_exists_query(exists_exprs).rstrip(";")

    # ------------------------------------------------------------------ #
    # Exactness gate
    # ------------------------------------------------------------------ #

    def _column_types(self, cursor, handle: DatasetHandle) -> list[tuple[str, str]]:
        """[(column_name, data_type)] in ordinal order, from information_schema."""
        declared = trino_read.declared_columns(handle)
        if declared is not None:
            return [(name, data_type) for name, data_type, _ in declared]
        catalog, schema, table = self._parts(handle)
        source = (
            f"{self._esc(catalog)}.information_schema.columns"
            if catalog
            else ("information_schema.columns")
        )
        cursor.execute(
            f"SELECT column_name, data_type FROM {source} "
            f"WHERE table_schema = {lit_str(schema, 'trino')} "
            f"AND table_name = {lit_str(table, 'trino')} "
            "ORDER BY ordinal_position"
        )
        return [(row[0], row[1]) for row in cursor.fetchall()]

    def _is_exact(self, spec: dict[str, Any], families: dict[str, str], byoc: bool) -> bool:
        """Whether Trino's answer for this spec matches the Polars tier."""
        kind = spec.get("kind")

        def fam(key: str) -> str | None:
            col = spec.get(key)
            return families.get(col.lower()) if isinstance(col, str) else None

        if kind in ("min_rows", "max_rows", "custom_sql_check", "custom_agg"):
            return True
        if kind == "not_null":
            return fam("column") is not None
        if kind == "unique":
            return fam("column") in _ORDERED
        if kind in ("allowed_values", "disallowed_values"):
            f = fam("column")
            values = [v for v in spec.get("values") or [] if v is not None]
            return f in _ORDERED and all(_literal_fits(f, v) for v in values)
        if kind == "range":
            return _bounds_fit(fam("column"), spec.get("min"), spec.get("max"))
        if kind in ("length", "regex", "contains", "starts_with", "ends_with"):
            return fam("column") == "varchar"
        if kind == "compare":
            left, right = fam("left"), fam("right")
            numeric = {"integer", "decimal"}
            return left in _ORDERED and (left == right or {left, right} <= numeric)
        if kind in ("conditional_not_null", "conditional_range"):
            when = fam("when_column")
            value = spec.get("when_value")
            if when not in _ORDERED:
                return False
            if value is not None and not _literal_fits(when, value):
                return False
            if kind == "conditional_not_null":
                return fam("column") is not None
            return _bounds_fit(fam("column"), spec.get("min"), spec.get("max"))
        if kind == "freshness":
            # Polars reads naive timestamps as UTC. Owned connections run in
            # UTC; a caller's connection may not, so only zoned columns push.
            f = fam("column")
            return f == "timestamptz" or (not byoc and f in ("timestamp", "date"))
        return False

    def execute(
        self,
        handle: DatasetHandle,
        compiled_plan: dict[str, Any],
        **kwargs,
    ) -> dict[str, Any]:
        """
        Read column types, drop specs Trino can't answer exactly, then run the
        rest through the shared three-phase plan.

        Returns:
            {"results": [...], "row_count": ..., "available_cols": [...], "staging": None}
        """
        with self._get_connection_ctx(handle) as conn:
            cursor = conn.cursor()
            try:
                columns = self._column_types(cursor, handle)
            finally:
                cursor.close()

        families = {name.lower(): _family(data_type) for name, data_type in columns}
        byoc = handle.scheme == "byoc"
        supported = compiled_plan.get("supported_specs", [])
        exact = [s for s in supported if self._is_exact(s, families, byoc)]
        if len(exact) != len(supported):
            deferred = [s.get("rule_id") for s in supported if s not in exact]
            _logger.info("Trino pushdown left %d rule(s) to Polars: %s", len(deferred), deferred)
            compiled_plan = self.compile(exact)

        out = super().execute(handle, compiled_plan, **kwargs)
        out["available_cols"] = [name for name, _ in columns]
        return out

    def introspect(self, handle: DatasetHandle, **kwargs) -> dict[str, Any]:
        table_ref = self._get_table_reference(handle)
        with self._get_connection_ctx(handle) as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table_ref}")
                row = cursor.fetchone()
                n = int(row[0]) if row else 0
                cols = [name for name, _ in self._column_types(cursor, handle)]
            finally:
                cursor.close()
        return {"row_count": n, "available_cols": cols, "staging": None}
