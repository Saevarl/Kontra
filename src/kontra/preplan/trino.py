# src/kontra/preplan/trino.py
"""
Trino preplan — resolve rules from catalog metadata, no data scan.

``information_schema.columns.is_nullable`` reports the column's declared
nullability, which the connector enforces (Iceberg required fields, NOT NULL
columns in the memory and JDBC connectors). A column declared NOT NULL cannot
hold a NULL, so ``not_null`` is provably PASS with zero rows read — the same
schema guarantee the ClickHouse preplan uses for non-Nullable columns.

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


def _fetch_nullability(handle: DatasetHandle) -> dict[str, bool]:
    """lowercased column name -> declared nullable, from information_schema."""
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
                f"SELECT column_name, is_nullable FROM {source} "
                f"WHERE table_schema = {lit_str(schema, 'trino')} "
                f"AND table_name = {lit_str(table, 'trino')}"
            )
            return {row[0].lower(): row[1] != "NO" for row in cur.fetchall()}
        finally:
            cur.close()


def preplan_trino(
    handle: DatasetHandle,
    required_columns: list[str],
    predicates: list[Predicate],
) -> PrePlan:
    """Resolve not_null rules from declared column nullability."""
    nullable: dict[str, bool] = {}
    if any(op == "not_null" for _rid, _col, op, _val in predicates):
        nullable = _fetch_nullability(handle)

    rule_decisions: dict[str, Decision] = {}
    for rule_id, column, op, _value in predicates:
        if op == "not_null" and nullable.get(column.lower()) is False:
            # Declared NOT NULL: the column cannot contain NULL — proven pass.
            rule_decisions[rule_id] = "pass_meta"
        else:
            rule_decisions[rule_id] = "unknown"

    return PrePlan(
        manifest_columns=list(required_columns) if required_columns else [],
        manifest_row_groups=[],
        rule_decisions=rule_decisions,
        stats={},
        fail_details={},
    )
