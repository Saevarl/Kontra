# src/kontra/engine/executors/clickhouse_sql.py
"""
ClickHouse SQL Executor - pushes validation rules down to ClickHouse.

ClickHouse is a columnar OLAP engine: validation aggregates (countIf, uniqExact,
SUM(CASE ...)) run at native speed on compressed columns. The design goal is to
push *everything* down and transfer only the small result rows back — the Polars
residual tier should almost never fire. Unlike SQL Server, ClickHouse has native
regex via match(), so regex pushes down too.

Floats need typed SQL. ClickHouse follows IEEE: every comparison with NaN is
false, and DISTINCT/GROUP BY keep -0.0 apart from 0.0 and NaNs with different
bits apart from each other. Polars uses a total order instead: NaN equals NaN
and is above every number, and -0.0 equals 0.0. It also casts a literal to the
column's type, so on a Float32 column ``f > 0.1`` compares with Float32(0.1).
When a rule touches a float column, execute() reads the column types from
system.columns and writes that order out for compare, range, the when clause
and target range of the conditional rules, and the unique key. A spec that
can't be written exactly (a float compared with a Decimal, a NaN literal, an
integer bound past 2^53) is left to Polars. Rules on other columns keep the
SQL compile() built.
"""

from __future__ import annotations

import math
import re
from contextlib import contextmanager
from typing import Any, Dict, Tuple

from kontra.connectors.handle import DatasetHandle
from kontra.connectors.clickhouse import ClickHouseConnectionParams, get_connection
from kontra.connectors.detection import parse_table_reference
from kontra.engine.sql_ir import (
    FLOAT_ORDER,
    SQL_OP_MAP,
    And,
    Col,
    Group,
    IsNull,
    Node,
    Or,
    RawExpr,
    bounds,
    esc_ident,
    lit_str,
    renderer_for,
    when_condition,
)
from kontra.engine.sql_utils import results_from_row
from kontra.logging import get_logger
from kontra.preplan.clickhouse import _normalize_ch_type

from .database_base import DatabaseSqlExecutor
from .registry import register_executor

_logger = get_logger(__name__)

_FLOATS = {"float32", "float64"}
# Integers Polars compares with a float in Float64. (Int8/Int16 against Float32
# compare in Float32, which holds them exactly, so Float64 agrees there too.)
_INTEGERS = {"int8", "int16", "int32", "int64", "uint8", "uint16", "uint32", "uint64"}
# _normalize_ch_type family -> value_lists column kind (integers: _INTEGERS).
_CH_KINDS = {
    "float32": "float",
    "float64": "float",
    "decimal": "decimal",
    "string": "text",
    "boolean": "boolean",
    "date": "date",
    "datetime": "datetime",
}
# Rules whose SQL depends on whether a column they compare is a float.
_FLOAT_RULES = {"compare", "unique", "range", "conditional_not_null", "conditional_range"}
# Integers up to 2^53 convert to Float64 exactly, so a cast to Float32 rounds
# once, as Polars' does.
_EXACT_DOUBLE_INT = 2**53
# `a op b` is `b flipped-op a`.
_FLIP = {"<": ">", "<=": ">=", ">": "<", ">=": "<=", "==": "==", "!=": "!="}
# _float_sql's answer for a spec ClickHouse can't answer as Polars does.
_DEFER = object()


def _float_literal(value: Any, family: str) -> str | None:
    """A rule literal as a value of the float column's type, as Polars casts it; None if not exact."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    if isinstance(value, int) and abs(value) > _EXACT_DOUBLE_INT:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    # repr round-trips the double; inf and -inf are ClickHouse literals too.
    text = repr(float(value))
    return f"toFloat32({text})" if family == "float32" else text


def _float_holds(op: str, left: str, right: str, left_nan: bool, right_nan: bool) -> str:
    """
    ``left op right`` in Polars' float order, for non-NULL operands.

    ``left_nan``/``right_nan`` say whether a side can be NaN (it is a float).
    The result is not parenthesized.
    """
    if left_nan and right_nan:
        ln, rn = f"isNaN({left})", f"isNaN({right})"
        return FLOAT_ORDER[op].format(l=left, r=right, ln=ln, rn=rn)
    if right_nan:
        return _float_holds(_FLIP[op], right, left, True, False)
    cmp = f"{left} {SQL_OP_MAP[op]} {right}"
    if not left_nan:
        return cmp
    # NaN is above every number, so `NaN op x` holds exactly for >, >= and !=.
    if op in (">", ">=", "!="):
        return f"isNaN({left}) OR {cmp}"
    return f"NOT isNaN({left}) AND {cmp}"


def _float_range(spec: dict[str, Any], family: str) -> Node | object:
    """NULL, NaN or outside [min, max] on a float column, in Polars' order."""
    c = esc_ident(spec["column"], "clickhouse")
    out: list[Node] = [IsNull(RawExpr(c)), RawExpr(f"isNaN({c})")]
    for op, key in (("<", "min"), (">", "max")):
        if spec.get(key) is not None:
            bound = _float_literal(spec[key], family)
            if bound is None:
                return _DEFER
            out.append(RawExpr(f"({_float_holds(op, c, bound, True, False)})"))
    return Or(*out)


def _float_when(spec: dict[str, Any], family: str | None) -> Node | object | None:
    """A conditional rule's when clause on a float column; None if the column isn't one."""
    value = spec.get("when_value")
    if family not in _FLOATS or value is None:
        return None  # IS [NOT] NULL reads no float order
    literal = _float_literal(value, family)
    if literal is None or spec.get("when_op") not in FLOAT_ORDER:
        return _DEFER
    column = esc_ident(spec["when_column"], "clickhouse")
    return RawExpr(_float_holds(spec["when_op"], column, literal, True, False))


def _float_sql(spec: dict[str, Any], families: dict[str, str]) -> Node | object | None:
    """
    The float-aware SQL for a spec: its violation condition, or for unique the
    grouping key. None when no column the rule compares is a float (compile()'s
    SQL is exact), _DEFER when ClickHouse can't give Polars' answer.
    """
    kind = spec.get("kind")

    def fam(key: str) -> str | None:
        col = spec.get(key)
        return families.get(col) if isinstance(col, str) else None

    def esc(key: str) -> str:
        return esc_ident(spec[key], "clickhouse")

    if kind == "unique":
        if fam("column") not in _FLOATS:
            return None
        c = esc("column")
        # One key per NaN and for both zeros, as Polars counts them.
        return RawExpr(f"multiIf(isNaN({c}), nan, {c} = 0, 0, {c})")
    if kind == "range":
        family = fam("column")
        # Polars' range rule counts NaN as out of range whatever the bounds.
        return _float_range(spec, family) if family in _FLOATS else None
    if kind == "compare":
        left, right = fam("left"), fam("right")
        if left not in _FLOATS and right not in _FLOATS:
            return None
        if not {left, right} <= _FLOATS | _INTEGERS or spec.get("op") not in FLOAT_ORDER:
            return _DEFER
        # Polars compares a float with an integer or another float in Float64.
        left_sql, right_sql = (
            esc(key) if family == "float64" else f"toFloat64({esc(key)})"
            for key, family in (("left", left), ("right", right))
        )
        holds = _float_holds(spec["op"], left_sql, right_sql, left in _FLOATS, right in _FLOATS)
        return Or(
            IsNull(RawExpr(esc("left"))), IsNull(RawExpr(esc("right"))), RawExpr(f"NOT ({holds})")
        )
    if kind in ("conditional_not_null", "conditional_range"):
        when = _float_when(spec, fam("when_column"))
        target = None
        if kind == "conditional_range" and fam("column") in _FLOATS:
            # NaN is out of range whatever the bounds, as in range.
            target = _float_range(spec, fam("column"))
        if when is _DEFER or target is _DEFER:
            return _DEFER
        if when is None and target is None:
            return None
        if when is None:
            when = when_condition(spec["when_column"], spec["when_op"], spec.get("when_value"))
        if kind == "conditional_not_null":
            return And(Group(when), IsNull(Col(spec["column"])))
        if target is None:
            column = Col(spec["column"])
            target = Or(IsNull(column), *bounds(column, spec.get("min"), spec.get("max")))
        return And(Group(when), Group(target))
    return None


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
        "custom_sql_check", "custom_agg", "metric",
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

    def _column_types(self, cursor, handle: DatasetHandle) -> list[tuple[str, str]]:
        """[(name, type)] in table order, from system.columns."""
        db, table_name = self._db_and_table(handle)
        # A BYOC reference without a database names a table in the session's.
        database = lit_str(db, "clickhouse") if db else "currentDatabase()"
        cursor.execute(
            "SELECT name, type FROM system.columns "
            f"WHERE database = {database} "
            f"AND table = {lit_str(table_name, 'clickhouse')} "
            "ORDER BY position"
        )
        return [(r[0], r[1]) for r in cursor.fetchall()]

    def _column_kinds(self, conn, cursor, handle: DatasetHandle) -> dict[str, str | None]:
        """From the column types, named as the materializer loads them."""
        kinds: dict[str, str | None] = {}
        for name, ch_type in self._column_types(cursor, handle):
            family = _normalize_ch_type(ch_type)
            kinds[name] = "integer" if family in _INTEGERS else _CH_KINDS.get(family)
        return kinds

    def execute(
        self,
        handle: DatasetHandle,
        compiled_plan: dict[str, Any],
        **kwargs,
    ) -> dict[str, Any]:
        """
        Run the compiled plan with float columns answered in Polars' order.

        When a rule that compares values names a float column, the column types
        are read first and the plan is rewritten (see the module docstring):
        typed exact counts replace their aggregate, typed early-stop probes run
        in one EXISTS query beside the plan, and specs that can't be written
        exactly are left out, so Polars measures them.
        """
        specs = compiled_plan.get("exists_specs", []) + compiled_plan.get("aggregate_specs", [])
        if not any(s.get("kind") in _FLOAT_RULES for s in specs):
            return super().execute(handle, compiled_plan, **kwargs)

        probe_results: list[dict[str, Any]] = []
        with self._get_connection_ctx(handle) as conn:
            cursor = conn.cursor()
            try:
                columns = self._column_types(cursor, handle)
                families = {name: _normalize_ch_type(t) for name, t in columns}
                plan, probes = self._float_plan(handle, compiled_plan, families)
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
        out["available_cols"] = [name for name, _ in columns]
        return out

    def _float_plan(
        self,
        handle: DatasetHandle,
        compiled_plan: dict[str, Any],
        families: dict[str, str],
    ) -> tuple[dict[str, Any], list[tuple[dict[str, Any], str]]]:
        """
        (plan, probes): the plan without deferred specs and with typed counts,
        and the (spec, EXISTS expression) of each typed early-stop spec.
        """
        typed: dict[str, Node] = {}
        deferred = set()
        for spec in compiled_plan.get("exists_specs", []) + compiled_plan.get("aggregate_specs", []):
            sql = _float_sql(spec, families)
            if sql is _DEFER:
                deferred.add(spec["rule_id"])
            elif sql is not None:
                typed[spec["rule_id"]] = sql
        if deferred:
            _logger.info(
                "ClickHouse pushdown left %d float rule(s) to Polars: %s",
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
            (spec, self._float_probe(spec, typed[spec["rule_id"]], table))
            for spec in plan["exists_specs"]
            if spec["rule_id"] in typed
        ]
        plan["exists_specs"] = [s for s in plan["exists_specs"] if s["rule_id"] not in typed]
        plan["aggregate_selects"] = [
            self._float_count(spec, typed[spec["rule_id"]]) if spec["rule_id"] in typed else select
            for spec, select in zip(plan["aggregate_specs"], plan["aggregate_selects"])
        ]
        return plan, probes

    def _float_count(self, spec: dict[str, Any], sql: Node) -> str:
        """A typed spec's exact count, AS its rule id."""
        r = renderer_for(self.DIALECT)
        if spec["kind"] == "unique":
            c = self._esc(spec["column"])
            return f"(COUNT({c}) - COUNT(DISTINCT {sql.sql(r)})) AS {self._esc(spec['rule_id'])}"
        return r.sum_case([sql.sql(r)], spec["rule_id"])

    def _float_probe(self, spec: dict[str, Any], sql: Node, table: str) -> str:
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
