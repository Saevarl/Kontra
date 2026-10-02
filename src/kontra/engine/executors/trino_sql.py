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

Three of those differences are written out in SQL instead of left to Polars:

* floats: ``range`` counts NaN as out of range and compares with the bounds
  cast to the column's type; ``compare`` between float columns uses Polars'
  order (NaN equals NaN and is above every number); ``unique`` already
  matches (NaN equals NaN, -0.0 equals 0.0);
* ``char(n)``: the string rules read ``rpad(CAST(c AS varchar), n, ' ')``,
  the padded value the client hands Polars. Trino's own ``char`` drops the
  padding in casts, ``regexp_like`` and comparisons, but not in ``LIKE``;
* ``freshness`` on a naive timestamp or a date compares with the current
  time in UTC, as Polars does, whatever the session's time zone.

The pushed rules are answered by one scan plan in both modes:

* one fused aggregate over the table: ``count_if(<violation>)`` per rule, the
  dataset rules and ``COUNT(*)``. Fail-fast reports a violation as at least 1,
  as an ``EXISTS`` probe would; on Trino every passing probe scans the whole
  table, so one shared scan is cheaper than a probe per rule;
* ``unique`` inside that scan when ``$files`` says the table has fewer than
  1M rows (with ``mark_distinct`` on Kontra's own connection), else one
  ``GROUP BY ... HAVING count(*) > 1`` query per column, which needs little
  memory. A table of unknown size counts as large;
* the ``GROUP BY`` queries and custom SQL run concurrently with the fused
  scan, at most four at a time, on cursors of the validation's connection.
"""

from __future__ import annotations

import math
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from typing import Any

from kontra.connectors import trino_read
from kontra.connectors.db_utils import get_connection_ctx
from kontra.connectors.detection import parse_table_reference
from kontra.connectors.handle import DatasetHandle
from kontra.connectors.trino import TrinoConnectionParams
from kontra.engine.sql_ir import (
    And,
    In,
    IsNotNull,
    IsNull,
    LenOf,
    Like,
    Node,
    Or,
    RawExpr,
    RegexNoMatch,
    bounds,
    esc_ident,
    escape_like_pattern,
    lit_str,
    renderer_for,
    trino_regex,
)
from kontra.engine.sql_utils import results_from_row
from kontra.logging import get_logger

from .database_base import DatabaseSqlExecutor
from .registry import register_executor

_logger = get_logger(__name__)

_INTEGER = {"tinyint", "smallint", "integer", "bigint"}

# From this many rows, a unique rule gets its own GROUP BY query: inside the
# fused scan, COUNT(DISTINCT) holds every distinct value in memory at once.
_UNIQUE_IN_SCAN_BELOW = 1_000_000
# Queries of one validation in flight at once.
_MAX_CONCURRENT = 4
# A freshness cell's value when the column holds no timestamps (MAX is NULL).
_NO_TIMESTAMPS = -1

# Type families whose comparisons, equality and ordering agree with Polars.
# Floating columns are excluded (Polars orders NaN above every number, Trino
# compares NaN as false), as is CHAR(n) (Trino pads it for comparison); the
# rules that write those differences out are gated one by one.
_ORDERED = {"integer", "decimal", "varchar", "date", "timestamp", "timestamptz", "boolean"}

# String rules that read a char(n) column through its padded varchar value.
_CHAR_RULES = {
    "allowed_values",
    "disallowed_values",
    "length",
    "regex",
    "contains",
    "starts_with",
    "ends_with",
}

# Integers up to 2^53 convert to double exactly, so both sides round alike.
_EXACT_DOUBLE_INT = 2**53

# Polars' order for floats as SQL over a non-NULL pair: NaN equals NaN and is
# above every number. {l}/{r} are the columns, {ln}/{rn} whether each is NaN.
_FLOAT_ORDER = {
    "==": "({ln} AND {rn}) OR (NOT {ln} AND NOT {rn} AND {l} = {r})",
    "!=": "NOT (({ln} AND {rn}) OR (NOT {ln} AND NOT {rn} AND {l} = {r}))",
    "<": "(NOT {ln} AND {rn}) OR (NOT {ln} AND NOT {rn} AND {l} < {r})",
    "<=": "{rn} OR (NOT {ln} AND {l} <= {r})",
    ">": "({ln} AND NOT {rn}) OR (NOT {ln} AND NOT {rn} AND {l} > {r})",
    ">=": "{ln} OR (NOT {rn} AND {l} >= {r})",
}


def _no_timestamps(rule_id: str, column: str, row_count: int) -> dict[str, Any]:
    """Freshness with no non-NULL value: a failure counting every row, as in Polars."""
    return {
        "rule_id": rule_id,
        "passed": False,
        "failed_count": row_count,
        "tally": False,
        "message": f"Column '{column}' has no non-null timestamps",
        "severity": "ERROR",
        "actions_executed": [],
        "execution_source": "sql",
    }


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
    if _char_length(t) is not None:
        return "char"
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


def _char_length(data_type: str) -> int | None:
    """n for ``char(n)`` (``char`` alone is ``char(1)``), else None."""
    t = data_type.lower().strip()
    if t == "char":
        return 1
    if t.startswith("char(") and t.endswith(")") and t[5:-1].strip().isdigit():
        return int(t[5:-1])
    return None


def _float_bound_fits(value: Any) -> bool:
    """A range bound that Trino and Polars turn into the same float."""
    if isinstance(value, bool):
        return False
    if isinstance(value, int):
        return abs(value) <= _EXACT_DOUBLE_INT
    return isinstance(value, float) and math.isfinite(value)


def _float_literal(value: Any, data_type: str) -> str:
    """A bound as a float of the column's type, as Polars casts it."""
    # repr round-trips the double; double -> real rounds as Polars' cast does.
    double = f"DOUBLE '{float(value)!r}'"
    return f"CAST({double} AS real)" if data_type.lower().strip() == "real" else double


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


def _float_range_violation(column: str, data_type: str, spec: dict[str, Any]) -> Node:
    """NULL, NaN or out of bounds, as the Polars range rule counts a float."""
    col = RawExpr(esc_ident(column, "trino"))
    out: list[Node] = [IsNull(col), RawExpr(f"is_nan({col.text})")]
    for op, key in (("<", "min"), (">", "max")):
        if spec.get(key) is not None:
            out.append(RawExpr(f"{col.text} {op} {_float_literal(spec[key], data_type)}"))
    return Or(*out)


def _float_compare_violation(spec: dict[str, Any]) -> Node:
    """NULL on either side, or the comparison false in Polars' float order."""
    left, right = (esc_ident(spec[k], "trino") for k in ("left", "right"))
    holds = _FLOAT_ORDER[spec["op"]].format(
        l=left, r=right, ln=f"is_nan({left})", rn=f"is_nan({right})"
    )
    return Or(IsNull(RawExpr(left)), IsNull(RawExpr(right)), RawExpr(f"NOT ({holds})"))


def _char_violation(spec: dict[str, Any], length: int) -> Node:
    """A string rule's violation over a char(n) column's padded value."""
    kind = spec["kind"]
    padded = RawExpr(f"rpad(CAST({esc_ident(spec['column'], 'trino')} AS varchar), {length}, ' ')")
    if kind == "allowed_values":
        values = spec.get("values") or []
        allowed = [v for v in values if v is not None]
        if not allowed:
            return IsNotNull(padded)
        outside = In(padded, allowed, negate=True)
        return And(IsNotNull(padded), outside) if None in values else Or(IsNull(padded), outside)
    if kind == "disallowed_values":
        listed = [v for v in spec.get("values") or [] if v is not None]
        return And(IsNotNull(padded), In(padded, listed)) if listed else RawExpr("false")
    if kind == "length":
        lo, hi = spec.get("min"), spec.get("max")
        limits = (int(lo) if lo is not None else None, int(hi) if hi is not None else None)
        return Or(IsNull(padded), *bounds(LenOf(padded), *limits))
    if kind == "regex":
        return RegexNoMatch(padded, spec["pattern"])
    affix = escape_like_pattern(
        spec[{"contains": "substring", "starts_with": "prefix", "ends_with": "suffix"}[kind]]
    )
    pattern = {
        "contains": f"%{affix}%",
        "starts_with": f"{affix}%",
        "ends_with": f"%{affix}",
    }[kind]
    return Or(IsNull(padded), Like(padded, pattern, negate=True))


@register_executor("trino")
class TrinoSqlExecutor(DatabaseSqlExecutor):
    """
    Trino SQL pushdown executor.

    Inherits compile() from DatabaseSqlExecutor; adds the per-type exactness
    gate, the scan plan, the Trino connection and the catalog.schema.table
    reference.
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

    def _is_exact(self, spec: dict[str, Any], families: dict[str, str]) -> bool:
        """Whether Trino's answer for this spec matches the Polars tier."""
        kind = spec.get("kind")

        def fam(key: str) -> str | None:
            col = spec.get(key)
            return families.get(col.lower()) if isinstance(col, str) else None

        if kind in ("min_rows", "max_rows", "custom_sql_check", "custom_agg"):
            return True
        if kind == "not_null":
            return fam("column") is not None
        if kind in _CHAR_RULES and fam("column") == "char":
            # Read through the padded value; only string values compare to it.
            values = [v for v in spec.get("values") or [] if v is not None]
            return all(isinstance(v, str) for v in values)
        if kind == "unique":
            # COUNT(DISTINCT) and GROUP BY treat NaN as one value and -0.0 as 0.0.
            return fam("column") in _ORDERED | {"float"}
        if kind in ("allowed_values", "disallowed_values"):
            f = fam("column")
            values = [v for v in spec.get("values") or [] if v is not None]
            return f in _ORDERED and all(_literal_fits(f, v) for v in values)
        if kind == "range":
            if fam("column") == "float":
                bounds_ = (spec.get("min"), spec.get("max"))
                return all(b is None or _float_bound_fits(b) for b in bounds_)
            return _bounds_fit(fam("column"), spec.get("min"), spec.get("max"))
        if kind in ("length", "regex", "contains", "starts_with", "ends_with"):
            return fam("column") == "varchar"
        if kind == "compare":
            left, right = fam("left"), fam("right")
            numeric = {"integer", "decimal"}
            if left == right == "float":
                return spec.get("op") in _FLOAT_ORDER
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
            # Naive values compare with UTC now, as in Polars (_freshness_select).
            return fam("column") in ("timestamptz", "timestamp", "date")
        return False

    def execute(
        self,
        handle: DatasetHandle,
        compiled_plan: dict[str, Any],
        **kwargs,
    ) -> dict[str, Any]:
        """
        Read column types, drop specs Trino can't answer exactly, then answer
        the rest with the scan plan (see the module docstring).

        Returns:
            {"results": [...], "row_count": ..., "available_cols": [...], "staging": None}
        """
        with self._get_connection_ctx(handle) as conn:
            cursor = conn.cursor()
            try:
                columns = self._column_types(cursor, handle)
            finally:
                cursor.close()

        types = {name.lower(): data_type for name, data_type in columns}
        families = {name: _family(data_type) for name, data_type in types.items()}
        supported = compiled_plan.get("supported_specs", [])
        exact = [s for s in supported if self._is_exact(s, families)]
        if len(exact) != len(supported):
            deferred = [s.get("rule_id") for s in supported if s not in exact]
            _logger.info("Trino pushdown left %d rule(s) to Polars: %s", len(deferred), deferred)
            compiled_plan = self.compile(exact)

        if any(s.get("kind") in ("custom_sql_check", "custom_agg") for s in exact):
            trino_read.mark_user_sql(handle)
        out = self._scan(handle, compiled_plan, types)
        out["available_cols"] = [name for name, _ in columns]
        return out

    # ------------------------------------------------------------------ #
    # Scan plan
    # ------------------------------------------------------------------ #

    def _scan(
        self,
        handle: DatasetHandle,
        compiled_plan: dict[str, Any],
        types: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """
        Run the fused scan, the unique GROUP BYs and custom SQL; collect the results.

        ``types`` maps lowercase column names to their declared Trino types.
        """
        types = types or {}
        table = self._get_table_reference(handle)
        # (spec, fail-fast): fail-fast specs report a violation as at least 1.
        specs = [(s, True) for s in compiled_plan.get("exists_specs", [])]
        specs += [(s, False) for s in compiled_plan.get("aggregate_specs", [])]
        custom_sql_specs = compiled_plan.get("custom_sql_specs", [])
        kinds = {s["rule_id"]: s.get("kind") for s, _ in specs}
        fail_fast = {s["rule_id"] for s, ff in specs if ff}

        with self._get_connection_ctx(handle) as conn:
            uniques = [s for s, _ in specs if s.get("kind") == "unique"]
            rows = self._data_rows(conn, handle) if uniques else None
            in_scan = rows is not None and rows < _UNIQUE_IN_SCAN_BELOW
            selects, grouped = [], []
            for spec, ff in specs:
                if spec.get("kind") == "unique" and not in_scan:
                    grouped.append(spec)
                else:
                    selects.append(self._count_select(spec, ff, types))
            if uniques and in_scan and self._owned_transaction(handle):
                # Faster and leaner than the default strategy for a few distinct
                # aggregates on a table this size.
                # Only on Kontra's own connection: a caller's session is theirs.
                self._run(conn, "SET SESSION distinct_aggregations_strategy = 'mark_distinct'")

            # COUNT(*) last, by position, so a rule id "__row_count" can't collide.
            selects.append('COUNT(*) AS "__row_count"')
            fused_sql = f"SELECT {', '.join(selects)} FROM {table}"
            scan = [lambda: self._run(conn, fused_sql)]
            scan += [
                lambda s=s: self._run(conn, self._grouped_unique_sql(s, table)) for s in grouped
            ]
            custom = [lambda s=s: self._custom_sql(conn, handle, s) for s in custom_sql_specs]
            mode = getattr(trino_read.state_of(handle), "mode", None)
            if trino_read.caller_transaction(handle):
                # A failed query aborts a Trino transaction, and Kontra can't
                # roll back the caller's: user SQL runs after the scan, one by one.
                outputs = self._concurrently(conn, scan, recover=False)
                outputs += [task() for task in custom]
            else:
                outputs = self._concurrently(
                    conn, scan + custom, recover=mode == trino_read.TRANSACTION
                )

        fused_columns, fused_row = outputs[0]
        row_count = int(fused_row[-1] or 0)
        cells = list(zip(fused_columns[:-1], fused_row[:-1]))
        for spec, (_, row) in zip(grouped, outputs[1 : 1 + len(grouped)]):
            cells.append((spec["rule_id"], row[0]))
        results: list[dict[str, Any]] = []
        columns_of = {s["rule_id"]: s.get("column") for s, _ in specs}
        for rule_id, value in cells:
            if kinds.get(rule_id) == "freshness" and value == _NO_TIMESTAMPS:
                results.append(_no_timestamps(rule_id, columns_of[rule_id], row_count))
                continue
            results += results_from_row(
                [rule_id], (value,), is_exists=rule_id in fail_fast, rule_kinds=kinds
            )
        for custom_results in outputs[1 + len(grouped) :]:
            results += custom_results
        return {"results": results, "row_count": row_count, "staging": None}

    def _concurrently(self, conn, tasks: list, recover: bool) -> list:
        """
        Run the tasks, at most four at a time, and return their outputs in order.

        ``recover`` is for Kontra's own transaction. A query that fails there
        aborts the whole transaction, and the queries running beside it fail
        too. So every task that failed runs again alone, each in a new
        transaction: a query that fails alone fails for its own reason, and
        the others get their answers. The new transactions read the table
        afresh; ``trino_read.finish`` proves it was the same state.
        """
        if len(tasks) == 1 and not recover:
            return [tasks[0]()]
        with ThreadPoolExecutor(min(_MAX_CONCURRENT, len(tasks))) as pool:
            futures = [pool.submit(self._attempt, task) for task in tasks]
            attempts = [f.result() for f in futures]
        outputs = []
        for task, (output, error) in zip(tasks, attempts):
            if error is not None and recover:
                output, error = self._retry_alone(conn, task)
            if isinstance(error, Exception) and not isinstance(error, _UserSqlFailed):
                raise error
            outputs.append(output)
        if recover and any(error is not None for _, error in attempts):
            # The last retry may have failed too; start the next phase in a live transaction.
            self._new_transaction(conn)
        return outputs

    @staticmethod
    def _attempt(task) -> tuple[Any, BaseException | None]:
        """(output, None), or (output, failure) for user SQL, or (None, exception)."""
        try:
            output = task()
        except Exception as e:  # noqa: BLE001 - raised or retried by the caller
            return None, e
        if isinstance(output, _CustomOutput) and output.failure is not None:
            return output, _UserSqlFailed(output.failure)
        return output, None

    def _retry_alone(self, conn, task) -> tuple[Any, BaseException | None]:
        self._new_transaction(conn)
        return self._attempt(task)

    @staticmethod
    def _new_transaction(conn) -> None:
        # Kontra's own connection is in autocommit outside its explicit transaction.
        if conn.transaction is not None:
            conn.rollback()
        conn.start_transaction()

    def _count_select(
        self, spec: dict[str, Any], fail_fast: bool, types: dict[str, str] | None = None
    ) -> str:
        """The fused scan's column for one rule: its violation count, AS its rule id."""
        rule_id = spec["rule_id"]
        kind = spec.get("kind")
        if kind == "custom_agg" and fail_fast:
            # A custom rule's to_sql_exists() condition, the one fail-fast probes.
            condition = spec["sql_exists"][self.DIALECT]
            return f"count_if({condition}) AS {self._esc(rule_id)}"
        violation = self._typed_violation(spec, types or {})
        if violation is not None:
            return f"count_if({violation.sql(renderer_for('trino'))}) AS {self._esc(rule_id)}"
        if kind == "freshness":
            return self._freshness_select(spec, types or {})
        # The tally aggregate counts the same violation condition as the probe.
        return self.compile([{**spec, "tally": True}])["aggregate_selects"][0]

    @staticmethod
    def _typed_violation(spec: dict[str, Any], types: dict[str, str]) -> Node | None:
        """The violation for a rule whose SQL depends on its column's type, else None."""
        kind = spec.get("kind")

        def declared(key: str) -> str:
            col = spec.get(key)
            return types.get(col.lower(), "") if isinstance(col, str) else ""

        if kind == "range" and _family(declared("column")) == "float":
            return _float_range_violation(spec["column"], declared("column"), spec)
        if kind == "compare" and _family(declared("left")) == _family(declared("right")) == "float":
            return _float_compare_violation(spec)
        length = _char_length(declared("column")) if kind in _CHAR_RULES else None
        if length is not None:
            return _char_violation(spec, length)
        return None

    def _freshness_select(self, spec: dict[str, Any], types: dict[str, str]) -> str:
        """0 when the newest value is recent enough, else 1, against UTC now."""
        column = spec["column"]
        now = "current_timestamp"
        if _family(types.get(column.lower(), "")) in ("timestamp", "date"):
            # Polars reads a naive value as UTC; the session's zone may differ.
            now = "CAST(current_timestamp AT TIME ZONE 'UTC' AS timestamp(6))"
        threshold = f"date_add('second', -{int(spec['max_age_seconds'])}, {now})"
        c = self._esc(column)
        return (
            f"CASE WHEN MAX({c}) IS NULL THEN {_NO_TIMESTAMPS} "
            f"WHEN MAX({c}) >= {threshold} THEN 0 ELSE 1 END "
            f"AS {self._esc(spec['rule_id'])}"
        )

    def _grouped_unique_sql(self, spec: dict[str, Any], table: str) -> str:
        """Duplicates (non-NULL rows beyond one per value), as COUNT(c) - COUNT(DISTINCT c)."""
        c = self._esc(spec["column"])
        return (
            f"SELECT coalesce(sum(n - 1), 0) AS {self._esc(spec['rule_id'])} FROM "
            f"(SELECT count(*) AS n FROM {table} WHERE {c} IS NOT NULL "
            f"GROUP BY {c} HAVING count(*) > 1) AS _dup"
        )

    def _data_rows(self, conn, handle: DatasetHandle) -> int | None:
        """The table's data-file records from $files, or None when no table state is held."""
        state = trino_read.state_of(handle)
        if state is None:
            return None
        if state.data_rows is None:
            # Trino can't pin $files; a pinned run then keeps its guard.
            trino_read.mark_files_read(handle)
            relation = trino_read.metadata_relation(handle, "$files")
            _, row = self._run(
                conn, f"SELECT coalesce(sum(record_count), 0) FROM {relation} WHERE content = 0"
            )
            state.data_rows = int(row[0])
        return state.data_rows

    @staticmethod
    def _owned_transaction(handle: DatasetHandle) -> bool:
        state = trino_read.state_of(handle)
        return state is not None and state.mode == trino_read.TRANSACTION

    @staticmethod
    def _run(conn, sql: str) -> tuple[list[str], tuple]:
        """Execute one query on its own cursor: (column names, first row)."""
        cursor = conn.cursor()
        try:
            cursor.execute(sql)
            rows = cursor.fetchall()
            columns = [d[0] for d in cursor.description or []]
        finally:
            cursor.close()
        return columns, tuple(rows[0]) if rows else ()

    def _custom_sql(self, conn, handle: DatasetHandle, spec: dict[str, Any]) -> _CustomOutput:
        """One custom SQL query; its failure is the rule's result, and noted for recovery."""
        cursor = _WatchedCursor(conn.cursor())
        try:
            results = self._execute_custom_sql_queries(cursor, handle, [spec])
        finally:
            cursor.close()
        return _CustomOutput(results, cursor.failure)


class _CustomOutput(list):
    """A custom SQL query's results, and the error its query raised, if any."""

    def __init__(self, results: list[dict], failure: BaseException | None):
        super().__init__(results)
        self.failure = failure


class _UserSqlFailed(Exception):
    """User SQL failed; its result already says so (only a retry may change it)."""


class _WatchedCursor:
    """A cursor that remembers the error its query raised (the caller catches it)."""

    def __init__(self, cursor):
        self._cursor = cursor
        self.failure: BaseException | None = None

    def _watch(self, method, *args):
        try:
            return method(*args)
        except Exception as e:
            self.failure = e
            raise

    def execute(self, sql):
        return self._watch(self._cursor.execute, sql)

    def fetchone(self):
        # Trino streams results: a query can fail while its rows are fetched.
        return self._watch(self._cursor.fetchone)

    def fetchall(self):
        return self._watch(self._cursor.fetchall)

    def __getattr__(self, name):
        return getattr(self._cursor, name)

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
