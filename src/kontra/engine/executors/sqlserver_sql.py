# src/kontra/engine/executors/sqlserver_sql.py
"""
SQL Server SQL Executor - executes validation rules via SQL pushdown.

Uses DatabaseSqlExecutor base class for shared compile/execute logic.

Supports rules:
  - not_null(column) - uses EXISTS (fast early termination)
  - unique(column) - uses COUNT DISTINCT
  - min_rows(threshold) - uses COUNT
  - max_rows(threshold) - uses COUNT
  - allowed_values(column, values) - uses SUM CASE
  - freshness(column, max_age) - uses MAX
  - range(column, min, max) - uses SUM CASE
  - compare(left, right, op) - uses SUM CASE
  - conditional_not_null(column, when) - uses SUM CASE
  - conditional_range(column, when, min, max) - uses SUM CASE

NOT supported (falls back to Polars):
  - regex(column, pattern) - PATINDEX uses LIKE wildcards, not regex

Text needs typed SQL. SQL Server compares text under a collation, and the
default ones ignore case. ``=`` and ``IN`` also ignore trailing spaces, even
under a binary collation, and ``LEN`` ignores them too and counts an emoji as
two. Polars compares the exact text. When a rule compares the values of a
text column (char, varchar, nchar, nvarchar, text, ntext), execute() reads the
column types and writes the rule out on the exact text:

* equality, value lists, unique and distinct_count compare a binary key, the
  UTF-16 bytes of the text plus a 0x01 byte. VARBINARY comparison pads the
  shorter side with 0x00, so without the last byte 'a' would equal 'a' + NUL;
* contains, starts_with and ends_with run LIKE under a binary collation, on
  NVARCHAR so trailing spaces count;
* length counts characters under a supplementary-character collation.

CHAR(n) and NCHAR(n) keep their padding through the cast to NVARCHAR, which is
the value the materializer hands Polars. Ordering a text column (``<``, ``>``,
range bounds) is left to Polars: no collation orders text by code point and
keeps trailing spaces. So is a value list with non-string values on a text
column, which Polars rejects. Rules on other columns, and a text column
compared with a number, keep the SQL compile() built.
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Dict, Tuple

from kontra.connectors.handle import DatasetHandle
from kontra.connectors.sqlserver import SqlServerConnectionParams, get_connection
from kontra.connectors.detection import parse_table_reference, get_default_schema, SQLSERVER
from kontra.connectors.db_utils import execute_with_params
from kontra.engine.sql_ir import (
    SQL_OP_MAP,
    And,
    Col,
    Group,
    IsNotNull,
    IsNull,
    Node,
    Or,
    RawExpr,
    bounds,
    esc_ident,
    escape_like_pattern,
    renderer_for,
)
from kontra.engine.sql_utils import results_from_row
from kontra.logging import get_logger

from .database_base import DatabaseSqlExecutor
from .registry import register_executor

_logger = get_logger(__name__)

_TEXT_TYPES = {"char", "varchar", "nchar", "nvarchar", "text", "ntext"}
# Rules whose SQL depends on whether a column they compare is text.
_TEXT_RULES = {
    "unique", "allowed_values", "disallowed_values", "range", "length",
    "contains", "starts_with", "ends_with", "compare",
    "conditional_not_null", "conditional_range", "metric",
}
# Compares code points and keeps trailing spaces in LIKE on NVARCHAR.
_BIN2 = "Latin1_General_100_BIN2"
# LEN counts a surrogate pair as one character under an _SC collation.
_SC = "Latin1_General_100_CI_AS_SC"
_LIKE_PATTERNS = {"contains": "%{}%", "starts_with": "{}%", "ends_with": "%{}"}
_LIKE_PARAM = {"contains": "substring", "starts_with": "prefix", "ends_with": "suffix"}
# _text_sql's answer for a spec SQL Server can't answer as Polars does.
_DEFER = object()


def _as_text(col_sql: str) -> str:
    """The column's text as NVARCHAR, CHAR(n) padding included."""
    return f"CAST({col_sql} AS NVARCHAR(MAX))"


def _key(col_sql: str) -> str:
    """A binary key equal for two values exactly when their text is."""
    return f"(CAST({_as_text(col_sql)} AS VARBINARY(MAX)) + 0x01)"


def _key_literal(value: str) -> str:
    """_key of a string literal, as a binary constant."""
    return "0x" + value.encode("utf-16-le", "surrogatepass").hex().upper() + "01"


def _text_in(col_sql: str, values: list, negate: bool) -> Node:
    keyword = "NOT IN" if negate else "IN"
    return RawExpr(f"{_key(col_sql)} {keyword} ({', '.join(_key_literal(v) for v in values)})")


def _text_when(spec: dict[str, Any], texts: set) -> Node | object | None:
    """A conditional rule's when clause on a text column; None if the column isn't one."""
    column, op, value = spec["when_column"], spec["when_op"], spec.get("when_value")
    if column not in texts or not isinstance(value, str):
        return None  # IS [NOT] NULL, or a number the server converts the text to
    if op not in ("==", "!="):
        return _DEFER
    return RawExpr(f"{_key(esc_ident(column, 'sqlserver'))} {SQL_OP_MAP[op]} {_key_literal(value)}")


def _text_sql(spec: dict[str, Any], texts: set) -> Node | object | None:
    """
    The exact-text SQL for a spec: its violation condition, or for unique and
    distinct_count the key to count. None when no column the rule compares is
    text (compile()'s SQL stands), _DEFER when SQL Server can't give Polars'
    answer.
    """
    kind = spec.get("kind")

    def is_text(key: str) -> bool:
        col = spec.get(key)
        return isinstance(col, str) and col in texts

    def esc(key: str) -> str:
        return esc_ident(spec[key], "sqlserver")

    def orders_text() -> bool:
        """A range on a text column with a string bound orders text."""
        bounds_ = (spec.get("min"), spec.get("max"))
        return is_text("column") and any(isinstance(b, str) for b in bounds_)

    if kind == "metric":
        if spec.get("metric") != "distinct_count" or not is_text("column"):
            return None
        return RawExpr(_key(esc("column")))
    if kind == "compare":
        if not (is_text("left") and is_text("right")):
            return None  # text against a number converts the text
        if spec.get("op") not in ("==", "!="):
            return _DEFER
        holds = f"{_key(esc('left'))} {SQL_OP_MAP[spec['op']]} {_key(esc('right'))}"
        return Or(IsNull(Col(spec["left"])), IsNull(Col(spec["right"])), RawExpr(f"NOT ({holds})"))
    if kind in ("conditional_not_null", "conditional_range"):
        when = _text_when(spec, texts)
        if when is _DEFER or (kind == "conditional_range" and orders_text()):
            return _DEFER
        if when is None:
            return None
        if kind == "conditional_not_null":
            return And(Group(when), IsNull(Col(spec["column"])))
        column = Col(spec["column"])
        target = Or(IsNull(column), *bounds(column, spec.get("min"), spec.get("max")))
        return And(Group(when), Group(target))
    if not is_text("column"):
        return None
    c = esc("column")
    if kind == "unique":
        return RawExpr(_key(c))
    if kind == "range":
        return _DEFER if orders_text() else None
    if kind == "length":
        length = RawExpr(f"(LEN(({_as_text(c)} + N'.') COLLATE {_SC}) - 1)")
        lo, hi = spec.get("min"), spec.get("max")
        limits = (int(lo) if lo is not None else None, int(hi) if hi is not None else None)
        return Or(IsNull(Col(spec["column"])), *bounds(length, *limits))
    if kind in _LIKE_PATTERNS:
        pattern = _LIKE_PATTERNS[kind].format(escape_like_pattern(spec[_LIKE_PARAM[kind]]))
        quoted = "N'" + pattern.replace("'", "''") + "'"
        like = RawExpr(f"{_as_text(c)} COLLATE {_BIN2} NOT LIKE {quoted} ESCAPE '\\'")
        return Or(IsNull(Col(spec["column"])), like)
    if kind in ("allowed_values", "disallowed_values"):
        values = spec.get("values") or []
        listed = [v for v in values if v is not None]
        if not listed:
            return None  # only NULL listed: compile()'s SQL reads no text
        if not all(isinstance(v, str) for v in listed):
            return _DEFER
        column = Col(spec["column"])
        if kind == "disallowed_values":
            return And(IsNotNull(column), _text_in(c, listed, negate=False))
        outside = _text_in(c, listed, negate=True)
        return And(IsNotNull(column), outside) if None in values else Or(IsNull(column), outside)
    return None


@register_executor("sqlserver")
class SqlServerSqlExecutor(DatabaseSqlExecutor):
    """
    SQL Server SQL pushdown executor.

    Inherits compile() and execute() from DatabaseSqlExecutor.
    Provides SQL Server-specific connection and table handling.

    Note: regex is NOT supported because PATINDEX uses LIKE-style wildcards.
    Regex rules fall back to Polars execution.
    """

    DIALECT = "sqlserver"
    # Note: regex is NOT supported - PATINDEX uses LIKE wildcards, not regex.
    # But contains/starts_with/ends_with use LIKE, so they work!
    SUPPORTED_RULES = {
        "not_null", "unique", "min_rows", "max_rows",
        "allowed_values", "disallowed_values",
        "freshness", "range", "length",
        "contains", "starts_with", "ends_with",  # LIKE-based, works on SQL Server
        "compare", "conditional_not_null", "conditional_range",
        "custom_sql_check", "custom_agg", "metric"
    }

    @property
    def name(self) -> str:
        return "sqlserver"

    def _supports_scheme(self, scheme: str, handle: DatasetHandle) -> bool:
        """Check if this executor supports the given URI scheme."""
        # BYOC: check dialect for external connections
        if scheme == "byoc" and handle.dialect == "sqlserver":
            return handle.external_conn is not None

        # URI-based: handle mssql:// or sqlserver:// URIs
        return scheme in {"mssql", "sqlserver"}

    @contextmanager
    def _get_connection_ctx(self, handle: DatasetHandle):
        """
        Get a SQL Server connection context.

        For BYOC, yields the external connection directly (not owned by us).
        For URI-based, yields a new connection (owned by context manager).
        """
        if handle.scheme == "byoc" and handle.external_conn is not None:
            yield handle.external_conn
        elif handle.db_params:
            with get_connection(handle.db_params) as conn:
                yield conn
        else:
            raise ValueError("Handle has neither external_conn nor db_params")

    def _get_table_reference(self, handle: DatasetHandle) -> str:
        """
        Get the fully-qualified table reference for SQL Server.

        Returns: [schema].[table] format.
        """
        if handle.scheme == "byoc" and handle.table_ref:
            _db, schema, table = parse_table_reference(handle.table_ref)
            schema = schema or get_default_schema(SQLSERVER)
            return f"{self._esc(schema)}.{self._esc(table)}"
        elif handle.db_params:
            params: SqlServerConnectionParams = handle.db_params
            return f"{self._esc(params.schema)}.{self._esc(params.table)}"
        raise ValueError("Handle has neither table_ref nor db_params")

    def _get_schema_and_table(self, handle: DatasetHandle) -> Tuple[str, str]:
        """
        Get schema and table name separately for custom SQL placeholder replacement.

        Returns: Tuple of (schema, table_name)
        """
        if handle.scheme == "byoc" and handle.table_ref:
            _db, schema, table = parse_table_reference(handle.table_ref)
            schema = schema or get_default_schema(SQLSERVER)
            return schema, table
        elif handle.db_params:
            params: SqlServerConnectionParams = handle.db_params
            return params.schema, params.table
        raise ValueError("Handle has neither table_ref nor db_params")

    def _text_columns(self, cursor, handle: DatasetHandle) -> set:
        """Names of the table's text columns, from INFORMATION_SCHEMA.

        Upper-case identifiers and exact names, so a case-sensitive database
        resolves the view and keeps columns ``s`` and ``S`` apart.
        """
        schema, table_name = self._get_schema_and_table(handle)
        execute_with_params(
            cursor,
            """
            SELECT COLUMN_NAME, DATA_TYPE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
            """,
            (schema, table_name),
        )
        rows = cursor.fetchall()
        return {name for name, data_type in rows if data_type.lower() in _TEXT_TYPES}

    def execute(
        self,
        handle: DatasetHandle,
        compiled_plan: dict[str, Any],
        **kwargs,
    ) -> dict[str, Any]:
        """
        Run the compiled plan with text columns compared as exact text.

        When a rule that compares values may name a text column, the column
        types are read first and the plan is rewritten (see the module
        docstring): typed exact counts replace their aggregate, typed
        early-stop probes run in one EXISTS query beside the plan, and specs
        that can't be written exactly are left out, so Polars measures them.
        """
        specs = compiled_plan.get("exists_specs", []) + compiled_plan.get("aggregate_specs", [])
        if not any(s.get("kind") in _TEXT_RULES for s in specs):
            return super().execute(handle, compiled_plan, **kwargs)

        probe_results: list = []
        with self._get_connection_ctx(handle) as conn:
            cursor = conn.cursor()
            try:
                texts = self._text_columns(cursor, handle)
                plan, probes = self._text_plan(handle, compiled_plan, texts)
                if probes:
                    cursor.execute(self._assemble_exists_query([sql for _, sql in probes]))
                    row = cursor.fetchone()
                    kinds = {spec["rule_id"]: spec.get("kind") for spec, _ in probes}
                    ids = [spec["rule_id"] for spec, _ in probes]
                    probe_results = results_from_row(ids, row, is_exists=True, rule_kinds=kinds)
            finally:
                cursor.close()

        out = super().execute(handle, plan, **kwargs)
        out["results"] = out.get("results", []) + probe_results
        return out

    def _text_plan(
        self,
        handle: DatasetHandle,
        compiled_plan: dict[str, Any],
        texts: set,
    ) -> tuple[dict[str, Any], list]:
        """
        (plan, probes): the plan without deferred specs and with typed counts,
        and the (spec, EXISTS expression) of each typed early-stop spec.
        """
        typed: dict[str, Node] = {}
        deferred = set()
        specs = compiled_plan.get("exists_specs", []) + compiled_plan.get("aggregate_specs", [])
        for spec in specs:
            sql = _text_sql(spec, texts)
            if sql is _DEFER:
                deferred.add(spec["rule_id"])
            elif sql is not None:
                typed[spec["rule_id"]] = sql
        if deferred:
            _logger.info(
                "SQL Server pushdown left %d text rule(s) to Polars: %s",
                len(deferred),
                sorted(deferred),
            )
            supported = compiled_plan.get("supported_specs", [])
            compiled_plan = self.compile([s for s in supported if s.get("rule_id") not in deferred])
        if not typed:
            return compiled_plan, []

        table = self._get_table_reference(handle)
        plan = dict(compiled_plan)
        probes = [
            (spec, self._text_probe(spec, typed[spec["rule_id"]], table))
            for spec in plan["exists_specs"]
            if spec["rule_id"] in typed
        ]
        plan["exists_specs"] = [s for s in plan["exists_specs"] if s["rule_id"] not in typed]
        plan["aggregate_selects"] = [
            self._text_count(spec, typed[spec["rule_id"]]) if spec["rule_id"] in typed else select
            for spec, select in zip(plan["aggregate_specs"], plan["aggregate_selects"])
        ]
        return plan, probes

    def _text_count(self, spec: dict[str, Any], sql: Node) -> str:
        """A typed spec's exact count (or distinct_count), AS its rule id."""
        r = renderer_for(self.DIALECT)
        if spec["kind"] == "metric":
            return r.metric("distinct_count", sql.sql(r), spec["rule_id"])
        if spec["kind"] == "unique":
            c = self._esc(spec["column"])
            return f"(COUNT({c}) - COUNT(DISTINCT {sql.sql(r)})) AS {self._esc(spec['rule_id'])}"
        return r.sum_case([sql.sql(r)], spec["rule_id"])

    def _text_probe(self, spec: dict[str, Any], sql: Node, table: str) -> str:
        """A typed spec's EXISTS expression: 1 when a violation exists."""
        r = renderer_for(self.DIALECT)
        if spec["kind"] == "unique":
            c = self._esc(spec["column"])
            inner = (
                f"SELECT 1 FROM {table} WHERE {c} IS NOT NULL "
                f"GROUP BY {sql.sql(r)} HAVING COUNT(*) > 1"
            )
        else:
            inner = f"SELECT 1 FROM {table} WHERE {sql.sql(r)}"
        return r.exists_wrap(inner, spec["rule_id"])

    def introspect(self, handle: DatasetHandle, **kwargs) -> Dict[str, Any]:
        """
        Introspect the SQL Server table for metadata.

        Returns:
            {"row_count": int, "available_cols": [...], "staging": None}
        """
        table = self._get_table_reference(handle)

        # Get schema and table name for information_schema query
        if handle.scheme == "byoc" and handle.table_ref:
            _db, schema, table_name = parse_table_reference(handle.table_ref)
            schema = schema or get_default_schema(SQLSERVER)
        elif handle.db_params:
            params: SqlServerConnectionParams = handle.db_params
            schema = params.schema
            table_name = params.table
        else:
            raise ValueError("Handle has neither table_ref nor db_params")

        with self._get_connection_ctx(handle) as conn:
            cursor = conn.cursor()
            try:
                # Get row count
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                row_count = cursor.fetchone()
                n = int(row_count[0]) if row_count else 0

                # Get column names (%s placeholders; adapted to ? for pyodbc)
                execute_with_params(
                    cursor,
                    """
                    SELECT COLUMN_NAME
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
                    ORDER BY ORDINAL_POSITION
                    """,
                    (schema, table_name),
                )
                cols = [row[0] for row in cursor.fetchall()]
            finally:
                cursor.close()

        return {"row_count": n, "available_cols": cols, "staging": None}
