# src/kontra/preplan/clickhouse.py
"""
ClickHouse preplan — resolve rules from system-table metadata, no data scan.

This is ClickHouse's analog of Parquet row-group metadata. Two schema facts are
free and exact:

  - ``system.columns.type`` — a column that is NOT ``Nullable(T)`` cannot hold a
    NULL, so ``not_null`` is provably PASS with zero rows read. This is a hard
    schema guarantee (stronger than Postgres's sampled ``null_frac``).
  - ``system.parts`` — the exact live row count without scanning data.

``dtype`` rules are also resolved from ``system.columns``.
"""

from __future__ import annotations

from typing import Any, Dict, List, Tuple

from kontra.connectors.handle import DatasetHandle
from kontra.preplan.types import PrePlan, Decision

Predicate = Tuple[str, str, str, Any]  # (rule_id, column, op, value)


def can_preplan_clickhouse(handle: DatasetHandle) -> bool:
    """ClickHouse preplan applies to URI handles and BYOC clickhouse connections."""
    if handle.scheme in ("clickhouse", "clickhouses") and handle.db_params is not None:
        return True
    return handle.scheme == "byoc" and handle.dialect == "clickhouse"


def _db_and_table(handle: DatasetHandle) -> Tuple[str, str]:
    from kontra.connectors.detection import parse_table_reference

    if handle.scheme == "byoc" and handle.table_ref:
        db, _schema, table = parse_table_reference(handle.table_ref)
        return db or "", table
    params = handle.db_params
    return params.database, params.table


def _fetch_column_types(handle: DatasetHandle, database: str, table: str) -> Dict[str, str]:
    """name -> ClickHouse type string, from system.columns (no data scan)."""
    from kontra.connectors.db_utils import get_connection_ctx
    from kontra.engine.sql_ir import lit_str

    with get_connection_ctx(handle, "clickhouse") as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT name, type FROM system.columns "
            f"WHERE database = {lit_str(database, 'clickhouse')} "
            f"AND table = {lit_str(table, 'clickhouse')}"
        )
        return {row[0]: row[1] for row in cur.fetchall()}


def _fetch_row_count(handle: DatasetHandle, database: str, table: str) -> int:
    from kontra.connectors.db_utils import get_connection_ctx
    from kontra.engine.sql_ir import lit_str

    with get_connection_ctx(handle, "clickhouse") as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT sum(rows) FROM system.parts "
            f"WHERE database = {lit_str(database, 'clickhouse')} "
            f"AND table = {lit_str(table, 'clickhouse')} AND active"
        )
        row = cur.fetchone()
        return int(row[0]) if row and row[0] is not None else 0


_SIGNED = {"int8", "int16", "int32", "int64"}
_UNSIGNED = {"uint8", "uint16", "uint32", "uint64"}
_FLOATS = {"float32", "float64"}


def _normalize_ch_type(ch_type: str) -> str:
    """
    ClickHouse type string -> canonical name matching how clickhouse-connect
    materializes it into Polars (so preplan dtype decisions agree with the
    residual tier).
    """
    t = ch_type.lower().strip()
    # Unwrap nested wrappers (LowCardinality(Nullable(...)) etc.)
    while True:
        for wrapper in ("nullable(", "lowcardinality("):
            if t.startswith(wrapper) and t.endswith(")"):
                t = t[len(wrapper):-1].strip()
                break
        else:
            break
    # FixedString / raw String bytes materialize as Polars Binary, not Utf8 —
    # map to "binary" so dtype("string") does not wrongly match (it defers).
    if t.startswith("fixedstring"):
        return "binary"
    if t == "string":
        return "string"
    # Enum8/Enum16 materialize as Polars Int8/Int16.
    if t.startswith("enum8"):
        return "int8"
    if t.startswith("enum16"):
        return "int16"
    if t.startswith("datetime"):
        return "datetime"
    if t.startswith("date"):
        return "date"
    if t.startswith("decimal"):
        return "decimal"
    if t in ("bool", "boolean"):
        return "boolean"
    if t.startswith("float32"):
        return "float32"
    if t.startswith("float64"):
        return "float64"
    return t  # int8..int64, uint8..uint64, or an unmapped exotic type


def _ch_dtype_matches(ch_type: str, expected: str):
    """
    True/False if we can decide the dtype from the ClickHouse type, else None
    (defer to the exact tier). Mirrors the Polars dtype rule EXACTLY — notably
    the "int" family is signed-only (UInt is not "int"), so a wrong pass_meta
    that residual would contradict is impossible.
    """
    norm = _normalize_ch_type(ch_type)
    exp = expected.lower()

    # Exact physical types
    if exp in _SIGNED | _UNSIGNED | _FLOATS | {"string", "boolean", "date", "datetime"}:
        return norm == exp
    # Families (match Polars _FAMILY_MAP)
    if exp in ("int", "integer"):
        return norm in _SIGNED
    if exp == "float":
        return norm in _FLOATS
    if exp == "numeric":
        return norm in (_SIGNED | _FLOATS)
    if exp in ("str", "text", "utf8"):
        return norm == "string"
    if exp in ("bool",):
        return norm == "boolean"
    # Unknown expected dtype spec — let the exact tier decide.
    return None


def _is_nullable_type(ch_type: str) -> bool:
    """
    True if a ClickHouse column type can hold NULL.

    Nullability can hide inside a LowCardinality wrapper:
    ``LowCardinality(Nullable(String))`` IS nullable even though the outer type
    is LowCardinality, not Nullable. Unwrap LowCardinality first, then check.
    Only a definitively non-nullable type is safe to prove not_null PASS.
    """
    t = ch_type.strip()
    low = t.lower()
    if low.startswith("lowcardinality(") and t.endswith(")"):
        t = t[len("lowcardinality("):-1].strip()
        low = t.lower()
    return low.startswith("nullable(")


def preplan_clickhouse(
    handle: DatasetHandle,
    required_columns: List[str],
    predicates: List[Predicate],
) -> PrePlan:
    """Resolve not_null and dtype rules from ClickHouse system tables."""
    database, table = _db_and_table(handle)

    # Only touch metadata for the rule kinds we can resolve.
    relevant = [p for p in predicates if p[2] in ("not_null", "dtype")]
    col_types: Dict[str, str] = {}
    if relevant:
        col_types = _fetch_column_types(handle, database, table)

    row_count = _fetch_row_count(handle, database, table)

    rule_decisions: Dict[str, Decision] = {}
    fail_details: Dict[str, Dict[str, Any]] = {}

    for rule_id, column, op, value in predicates:
        ch_type = col_types.get(column)

        if op == "not_null":
            if ch_type is None:
                rule_decisions[rule_id] = "unknown"
            elif not _is_nullable_type(ch_type):
                # Non-Nullable column CANNOT contain NULL — proven pass, no scan.
                rule_decisions[rule_id] = "pass_meta"
            else:
                # Nullable: may or may not have nulls; must check.
                rule_decisions[rule_id] = "unknown"

        elif op == "dtype":
            match = None if ch_type is None else _ch_dtype_matches(ch_type, value)
            if match is None:
                rule_decisions[rule_id] = "unknown"  # defer to exact tier
            elif match:
                rule_decisions[rule_id] = "pass_meta"
            else:
                rule_decisions[rule_id] = "fail_meta"
                fail_details[rule_id] = {"expected": value, "actual": ch_type}

        else:
            # unique/range/etc. — ClickHouse has no unique constraints and we
            # don't keep per-column min/max here; defer to pushdown.
            rule_decisions[rule_id] = "unknown"

    return PrePlan(
        manifest_columns=list(required_columns) if required_columns else [],
        manifest_row_groups=[],
        rule_decisions=rule_decisions,
        stats={"total_rows": row_count},
        fail_details=fail_details,
    )
