# src/kontra/preplan/trino.py
"""
Trino preplan — resolve rules from catalog metadata, no data scan.

``information_schema.columns.is_nullable`` reports the column's declared
nullability, which the connector enforces (Iceberg required fields, NOT NULL
columns in the memory and JDBC connectors). A column declared NOT NULL cannot
hold a NULL, so ``not_null`` is provably PASS with zero rows read — the same
schema guarantee the ClickHouse preplan uses for non-Nullable columns.

``dtype`` is decided from the declared type, through the same Trino-to-Polars
map the Trino materializer casts to, so metadata and the Polars tier give one
answer. A type without a fixed Polars dtype, or a column the rule names in a
different case, is left to the Polars tier.

On an Iceberg table read in one state (``trino_read``), one ``$files``
aggregate adds per-file counts and bounds. Counts are exact per data file;
bounds are only bounds (strings are truncated, floats lose them to NaN). Row
deletes don't touch data-file statistics, so with delete files present the
counts include deleted rows: a PASS that only needs "no NULL anywhere" or
"every value inside the bounds" still holds (deletes only remove rows), but a
FAIL or a row count needs a table without delete files.

* ``not_null`` and ``conditional_not_null`` PASS when every data file has a
  null count and they sum to 0. ``not_null`` FAILs when there are no delete
  files and the sum is above 0. ``conditional_not_null`` never FAILs from
  metadata: its NULLs may all sit outside the condition.
* ``range`` on an integer, decimal or date column, with integer (or date)
  bounds, PASSes when the column has no NULLs and every data file holding a
  value has bounds inside [min, max]. It FAILs, without delete files, on a
  NULL or on a file whose values all lie outside.
* ``min_rows`` and ``max_rows`` PASS from the data files' row counts when
  there are no delete files. A FAIL reports the shortfall, which the scan
  counts, so it is left to the scan.

Anything else (floats from bounds, strings, files without metrics) is left
to the scan. Table statistics (``SHOW STATS``) are estimates and are not used
to decide rules.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field
from typing import Any

from kontra.connectors.handle import DatasetHandle
from kontra.preplan.types import Decision, PrePlan

Predicate = tuple[str, str, str, Any]  # (rule_id, column, op, value)


def can_preplan_trino(handle: DatasetHandle) -> bool:
    """Trino preplan applies to URI handles and BYOC Trino connections."""
    if handle.scheme in ("trino", "trinos") and handle.db_params is not None:
        return True
    return handle.scheme == "byoc" and handle.dialect == "trino" and bool(handle.table_ref)


def _parts(handle: DatasetHandle) -> tuple[str | None, str, str]:
    from kontra.connectors.detection import parse_table_reference

    if handle.scheme == "byoc":
        catalog, schema, table = parse_table_reference(handle.table_ref)
        if not schema:
            raise ValueError(f"Trino table reference needs a schema: {handle.table_ref!r}")
        return catalog, schema, table
    params = handle.db_params
    return params.catalog, params.schema, params.table


def _fetch_columns(handle: DatasetHandle) -> list[tuple[str, str, bool]]:
    """(column_name, data_type, nullable) in ordinal order, from information_schema."""
    from kontra.connectors import trino_read

    declared = trino_read.declared_columns(handle)
    if declared is not None:
        return declared
    from kontra.connectors.db_utils import get_connection_ctx
    from kontra.engine.sql_ir import esc_ident, lit_str

    catalog, schema, table = _parts(handle)
    source = (
        f"{esc_ident(catalog, 'trino')}.information_schema.columns"
        if catalog
        else "information_schema.columns"
    )
    with get_connection_ctx(handle, "trino") as conn:
        cur = conn.cursor()
        try:
            cur.execute(
                f"SELECT column_name, data_type, is_nullable FROM {source} "
                f"WHERE table_schema = {lit_str(schema, 'trino')} "
                f"AND table_name = {lit_str(table, 'trino')} "
                "ORDER BY ordinal_position"
            )
            return [(row[0], row[1], row[2] != "NO") for row in cur.fetchall()]
        finally:
            cur.close()


def _dtype_decision(data_type: str, expected: str) -> tuple[Decision, dict[str, Any]]:
    """The dtype rule's answer on the Polars frame the materializer would build."""
    from kontra.connectors.trino_types import polars_dtype
    from kontra.rule_defs.builtin.dtype import DtypeRule, expected_dtypes

    actual = polars_dtype(data_type)
    _label, allowed = expected_dtypes(str(expected))
    if actual is None or allowed is None:
        return "unknown", {}
    if any(actual == a for a in allowed):
        return "pass_meta", {}
    return "fail_meta", {"expected": expected, "actual": DtypeRule._dtype_label(actual)}


def _strict_dtype_rules(rules: list[Any] | None) -> set[str]:
    """dtype rules in strict mode; other modes always fail on the Polars tier."""
    return {
        r.rule_id
        for r in rules or []
        if r.name == "dtype" and str(r.params.get("mode") or "strict").lower() == "strict"
    }


# --------------------------------------------------------------------------- #
# Iceberg $files
# --------------------------------------------------------------------------- #

_INTEGER_TYPES = ("tinyint", "smallint", "integer", "bigint")

# Per-file metrics by column name, from readable_metrics (keyed by the current
# schema's names). Malformed metrics read as NULL, which is "unknown".
_METRICS = (
    "try(CAST(readable_metrics AS map(varchar, "
    "row(null_value_count bigint, lower_bound varchar, upper_bound varchar))))"
)


@dataclass
class _ColumnFiles:
    unknown_nulls: int  # data files with no null count for the column
    nulls: int  # sum of the known null counts
    unbounded: int = 0  # data files holding a value but no usable bounds
    min_lower: Any = None
    max_upper: Any = None
    max_lower: Any = None
    min_upper: Any = None


@dataclass
class _Files:
    data_files: int
    delete_files: int
    records: int
    columns: dict[str, _ColumnFiles] = field(default_factory=dict)


def _bound_type(data_type: str) -> str | None:
    """The type a range bound is compared in, for column types range can use."""
    from kontra.connectors.trino_types import normalize_type

    t = normalize_type(data_type)
    if t in _INTEGER_TYPES or t == "date" or t.startswith("decimal("):
        return t
    return None


def _files_sql(relation: str, columns: dict[str, str | None]) -> str:
    """One aggregate over $files: data and delete files, rows, and per-column metrics."""
    from kontra.engine.sql_ir import lit_str

    selects = [
        "count_if(content = 0)",
        "count_if(content <> 0)",
        "coalesce(sum(record_count) FILTER (WHERE content = 0), 0)",
    ]
    for name, bound_type in columns.items():
        col = f"element_at(m, {lit_str(name, 'trino')})"
        nulls = f"{col}.null_value_count"
        selects += [
            f"count_if(content = 0 AND {nulls} IS NULL)",
            f"coalesce(sum({nulls}) FILTER (WHERE content = 0), 0)",
        ]
        if bound_type is None:
            continue
        lo = f"try_cast({col}.lower_bound AS {bound_type})"
        hi = f"try_cast({col}.upper_bound AS {bound_type})"
        valued = f"content = 0 AND record_count > {nulls}"
        selects += [
            f"count_if({valued} AND ({lo} IS NULL OR {hi} IS NULL))",
            f"min({lo}) FILTER (WHERE {valued})",
            f"max({hi}) FILTER (WHERE {valued})",
            f"max({lo}) FILTER (WHERE {valued})",
            f"min({hi}) FILTER (WHERE {valued})",
        ]
    return (
        f"SELECT {', '.join(selects)} FROM "
        f"(SELECT content, record_count, {_METRICS} AS m FROM {relation})"
    )


def _read_files(handle: DatasetHandle, columns: dict[str, str | None]) -> _Files:
    from kontra.connectors import trino_read
    from kontra.connectors.db_utils import get_connection_ctx

    # Trino can't read $files at a snapshot; a pinned run checks its guard instead.
    trino_read.mark_files_read(handle)
    sql = _files_sql(trino_read.metadata_relation(handle, "$files"), columns)
    with get_connection_ctx(handle, "trino") as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql)
            row = list(cur.fetchall()[0])
        finally:
            cur.close()
    files = _Files(int(row[0]), int(row[1]), int(row[2]))
    i = 3
    for name, bound_type in columns.items():
        stats = _ColumnFiles(int(row[i]), int(row[i + 1]))
        i += 2
        if bound_type is not None:
            stats.unbounded = int(row[i])
            stats.min_lower, stats.max_upper, stats.max_lower, stats.min_upper = row[i + 1 : i + 5]
            i += 5
        files.columns[name] = stats
    return files


def _no_nulls(stats: _ColumnFiles) -> bool:
    """Every data file counts its NULLs, and there are none (holds under deletes)."""
    return stats.unknown_nulls == 0 and stats.nulls == 0


def _not_null_decision(files: _Files, stats: _ColumnFiles) -> Decision:
    if _no_nulls(stats):
        return "pass_meta"
    if files.delete_files == 0 and stats.nulls > 0:
        return "fail_meta"
    return "unknown"


def _range_literal(value: Any, bound_type: str) -> bool:
    """Whether a range bound compares with the column exactly as the Polars tier does."""
    if value is None:
        return True
    if bound_type == "date":
        if isinstance(value, dt.datetime):
            return False
        if isinstance(value, dt.date):
            return True
        if isinstance(value, str):
            try:
                dt.date.fromisoformat(value)
            except ValueError:
                return False
            return True
        return False
    # Integer and decimal columns: integer bounds only. A decimal compared with
    # a fractional float is a fallback in Kontra today (study Q5).
    return isinstance(value, int) and not isinstance(value, bool)


def _range_decision(files: _Files, stats: _ColumnFiles, low: Any, high: Any) -> Decision:
    if isinstance(low, str):
        low = dt.date.fromisoformat(low)
    if isinstance(high, str):
        high = dt.date.fromisoformat(high)
    if (
        _no_nulls(stats)
        and stats.unbounded == 0
        and (low is None or stats.min_lower is None or stats.min_lower >= low)
        and (high is None or stats.max_upper is None or stats.max_upper <= high)
    ):
        return "pass_meta"
    if files.delete_files == 0 and (
        stats.nulls > 0
        # A file whose lowest value is above max, or highest below min, holds
        # at least one value and every one of them is out of range.
        or (high is not None and stats.max_lower is not None and stats.max_lower > high)
        or (low is not None and stats.min_upper is not None and stats.min_upper < low)
    ):
        return "fail_meta"
    return "unknown"


def _row_count_decision(files: _Files, rule: Any) -> Decision:
    """PASS only: a FAIL reports how far off the count is, which the scan counts."""
    if files.delete_files:
        return "unknown"
    threshold = int(rule.params.get("value", rule.params.get("threshold", 0)))
    if rule.name == "min_rows":
        return "pass_meta" if files.records >= threshold else "unknown"
    return "pass_meta" if files.records <= threshold else "unknown"


def _files_rules(
    rules: list[Any], predicates: list[Predicate], types: dict[str, str], decided: set[str]
) -> tuple[list[tuple[Any, ...]], dict[str, str | None]]:
    """The rules $files can settle, and the columns (with bound types) they need."""
    wanted: list[tuple[Any, ...]] = []
    columns: dict[str, str | None] = {}
    for rule_id, column, op, _value in predicates:
        if op == "not_null" and rule_id not in decided and column in types:
            wanted.append(("not_null", rule_id, column))
            columns.setdefault(column, None)
    for rule in rules:
        if rule.rule_id in decided:
            continue
        params = rule.params or {}
        column = params.get("column")
        if rule.name == "range" and column in types:
            bound_type = _bound_type(types[column])
            low, high = params.get("min"), params.get("max")
            if bound_type and _range_literal(low, bound_type) and _range_literal(high, bound_type):
                wanted.append(("range", rule.rule_id, column, low, high))
                columns[column] = bound_type
        elif rule.name == "conditional_not_null" and column in types:
            when_column = getattr(rule, "_when_column", None)
            if when_column in types:
                wanted.append(("conditional_not_null", rule.rule_id, column))
                columns.setdefault(column, None)
        elif rule.name in ("min_rows", "max_rows"):
            wanted.append((rule.name, rule.rule_id, rule))
    return wanted, columns


def preplan_trino(
    handle: DatasetHandle,
    required_columns: list[str],
    predicates: list[Predicate],
    rules: list[Any] | None = None,
) -> PrePlan:
    """Resolve rules from declared columns and, on a held Iceberg table, from $files."""
    from kontra.connectors import trino_read

    rules = rules or []
    held = trino_read.state_of(handle) is not None
    columns: list[tuple[str, str, bool]] = []
    if any(op in ("not_null", "dtype") for _rid, _col, op, _val in predicates) or (held and rules):
        columns = _fetch_columns(handle)
    nullable = {name.lower(): null for name, _type, null in columns}
    # The Polars frame keeps Trino's column names; a rule naming a column in
    # another case finds no column there, so only an exact name is decided.
    types = {name: data_type for name, data_type, _null in columns}
    strict_dtype = _strict_dtype_rules(rules)

    rule_decisions: dict[str, Decision] = {}
    fail_details: dict[str, dict[str, Any]] = {}
    for rule_id, column, op, value in predicates:
        if op == "not_null" and nullable.get(column.lower()) is False:
            # Declared NOT NULL: the column cannot contain NULL — proven pass.
            rule_decisions[rule_id] = "pass_meta"
        elif op == "dtype" and rule_id in strict_dtype and column in types:
            decision, details = _dtype_decision(types[column], value)
            rule_decisions[rule_id] = decision
            if details:
                fail_details[rule_id] = details
        else:
            rule_decisions.setdefault(rule_id, "unknown")

    # $files only describes the state the scans read when that state is held.
    decided = {rid for rid, d in rule_decisions.items() if d != "unknown"}
    wanted, file_columns = _files_rules(rules, predicates, types, decided) if held else ([], {})
    if wanted:
        files = _read_files(handle, file_columns)
        for kind, rule_id, *args in wanted:
            if kind == "not_null":
                decision = _not_null_decision(files, files.columns[args[0]])
            elif kind == "conditional_not_null":
                decision = "pass_meta" if _no_nulls(files.columns[args[0]]) else "unknown"
            elif kind == "range":
                decision = _range_decision(files, files.columns[args[0]], args[1], args[2])
            else:
                decision = _row_count_decision(files, args[0])
            rule_decisions[rule_id] = decision

    return PrePlan(
        manifest_columns=list(required_columns) if required_columns else [],
        manifest_row_groups=[],
        rule_decisions=rule_decisions,
        stats={},
        fail_details=fail_details,
    )
