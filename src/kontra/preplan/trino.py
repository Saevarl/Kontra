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

Table statistics (``SHOW STATS``) are estimates and are not used to decide
rules. Everything else defers to pushdown.
"""

from __future__ import annotations

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


def preplan_trino(
    handle: DatasetHandle,
    required_columns: list[str],
    predicates: list[Predicate],
    rules: list[Any] | None = None,
) -> PrePlan:
    """Resolve not_null rules from declared nullability and dtype rules from declared types."""
    columns: list[tuple[str, str, bool]] = []
    if any(op in ("not_null", "dtype") for _rid, _col, op, _val in predicates):
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
            rule_decisions[rule_id] = "unknown"

    return PrePlan(
        manifest_columns=list(required_columns) if required_columns else [],
        manifest_row_groups=[],
        rule_decisions=rule_decisions,
        stats={},
        fail_details=fail_details,
    )
