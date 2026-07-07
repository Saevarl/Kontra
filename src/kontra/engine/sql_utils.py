# src/kontra/engine/sql_utils.py
"""
Shared SQL utilities for all database executors.

This module exposes the public builder surface used by the DuckDB / PostgreSQL
/ SQL Server executors. Each ``agg_*`` / ``exists_*`` function is a thin
IR-building wrapper: it assembles a small SQL condition IR (see
``kontra.engine.sql_ir``) and renders it with the dialect's ``Renderer``.

Two families of builders, one per execution strategy:

- ``agg_*``    — SUM(CASE ...) aggregates that count every violation (tally).
- ``exists_*`` — EXISTS subqueries that stop at the first violation (fast,
                 failed_count is a lower bound of 1).

Both are built from the same violation conditions, so the tiers agree on
whether violations exist (the tier equivalence invariant).

Dialect differences (identifier quoting, string escaping, CAST-to-text, string
length, regex support, the EXISTS wrapper, dataset aggregates) live in exactly
one place: the ``Renderer`` hierarchy in ``sql_ir``. Adding a rule means adding
one builder here; adding a dialect means adding one ``Renderer`` subclass there.
"""

from __future__ import annotations

from typing import Any, List, Optional

from kontra.engine.sql_ir import (
    # Dialect type + primitives re-exported for backwards compatibility
    Dialect,
    SQL_OP_MAP,
    esc_ident,
    lit_str,
    lit_value,
    escape_like_pattern,
    # IR nodes
    Col,
    CastText,
    LenOf,
    IsNull,
    IsNotNull,
    Cmp,
    Not,
    In,
    Like,
    RegexNoMatch,
    Group,
    Raw,
    And,
    Or,
    Node,
    # IR helpers
    bounds,
    when_condition,
    allowed_values_violation,
    val_list,
    renderer_for,
)

__all__ = [
    "Dialect",
    "SQL_OP_MAP",
    "RULE_KIND_TO_FAILURE_MODE",
    "esc_ident",
    "lit_str",
    "lit_value",
    "escape_like_pattern",
    "agg_not_null",
    "agg_unique",
    "agg_min_rows",
    "agg_max_rows",
    "agg_allowed_values",
    "agg_disallowed_values",
    "agg_freshness",
    "agg_range",
    "agg_length",
    "agg_regex",
    "agg_contains",
    "agg_starts_with",
    "agg_ends_with",
    "agg_compare",
    "agg_conditional_not_null",
    "agg_conditional_range",
    "exists_not_null",
    "exists_unique",
    "exists_allowed_values",
    "exists_disallowed_values",
    "exists_range",
    "exists_length",
    "exists_regex",
    "exists_contains",
    "exists_starts_with",
    "exists_ends_with",
    "exists_compare",
    "exists_conditional_not_null",
    "exists_conditional_range",
    "exists_custom",
    "results_from_row",
]

# Mapping from rule kind to failure_mode
RULE_KIND_TO_FAILURE_MODE = {
    "not_null": "null_values",
    "unique": "duplicate_values",
    "allowed_values": "novel_category",
    "disallowed_values": "disallowed_value",
    "min_rows": "row_count_low",
    "max_rows": "row_count_high",
    "range": "range_violation",
    "length": "length_violation",
    "freshness": "freshness_lag",
    "regex": "pattern_mismatch",
    "contains": "pattern_mismatch",
    "starts_with": "pattern_mismatch",
    "ends_with": "pattern_mismatch",
    "dtype": "schema_drift",
    "custom_sql_check": "custom_check_failed",
    "compare": "comparison_failed",
    "conditional_not_null": "conditional_null",
    "conditional_range": "conditional_range_violation",
}


# =============================================================================
# Internal condition builders (shared between agg_* and exists_* families)
# =============================================================================

def _like_violation(column: str, pattern: str) -> Node:
    """NULL or NOT LIKE condition (same syntax across all three dialects)."""
    col = Col(column)
    return Or(IsNull(col), Like(col, pattern, negate=True))


def _range_violation(column: str, min_val: Optional[Any], max_val: Optional[Any]) -> Node:
    """NULL-or-out-of-bounds condition on a column (flat, no grouping)."""
    col = Col(column)
    return Or(IsNull(col), *bounds(col, min_val, max_val))


def _length_violation(column: str, min_len: Optional[int], max_len: Optional[int]) -> Node:
    """NULL-or-invalid-length condition (flat, no grouping)."""
    col = Col(column)
    length = LenOf(col)
    min_i = int(min_len) if min_len is not None else None
    max_i = int(max_len) if max_len is not None else None
    return Or(IsNull(col), *bounds(length, min_i, max_i))


def _compare_violation(left: str, right: str, op: str) -> Node:
    """NULL-either-side or failed-comparison condition."""
    l = Col(left)
    r = Col(right)
    return Or(IsNull(l), IsNull(r), Not(Cmp(l, op, r)))


def _disallowed_in_check(column: str, values: List[Any]) -> Node:
    """Non-null value that IS in the disallowed set."""
    col = Col(column)
    return And(IsNotNull(col), In(CastText(col), values, negate=False))


# =============================================================================
# Aggregate Expression Builders (exact counts)
# =============================================================================

def agg_not_null(col: str, rule_id: str, dialect: Dialect = "duckdb") -> str:
    """Count NULL values in a column."""
    r = renderer_for(dialect)
    return r.sum_case([IsNull(Col(col)).sql(r)], rule_id)


def agg_unique(col: str, rule_id: str, dialect: Dialect = "duckdb") -> str:
    """Count duplicate values (extra rows beyond one per unique value).

    Uses COUNT(col) - COUNT(DISTINCT col) to exclude NULLs from the count,
    matching Polars semantics. NULLs are not duplicates (NULL != NULL in SQL).
    """
    c = esc_ident(col, dialect)
    rid = esc_ident(rule_id, dialect)
    return f"(COUNT({c}) - COUNT(DISTINCT {c})) AS {rid}"


def agg_min_rows(threshold: int, rule_id: str, dialect: Dialect = "duckdb") -> str:
    """Check if row count >= threshold. Returns deficit if below."""
    return renderer_for(dialect).min_rows(int(threshold), rule_id)


def agg_max_rows(threshold: int, rule_id: str, dialect: Dialect = "duckdb") -> str:
    """Check if row count <= threshold. Returns excess if above."""
    return renderer_for(dialect).max_rows(int(threshold), rule_id)


def agg_allowed_values(
    col: str, values: List[Any], rule_id: str, dialect: Dialect = "duckdb"
) -> str:
    """Count values not in the allowed set.

    NULLs are counted as violations unless None is explicitly in values,
    matching Polars behavior for tier equivalence.
    """
    r = renderer_for(dialect)
    column = Col(col)
    null_allowed = None in values
    non_null_values = [v for v in values if v is not None]

    if not non_null_values:
        return r.sum_case([IsNotNull(column).sql(r)], rule_id)

    in_check = In(CastText(column), non_null_values, negate=True)
    if null_allowed:
        return r.sum_case([And(IsNotNull(column), in_check).sql(r)], rule_id)
    # NULL is not allowed — count NULLs AND non-null values not in the list
    return r.sum_case([IsNull(column).sql(r), in_check.sql(r)], rule_id)


def agg_disallowed_values(
    col: str, values: List[Any], rule_id: str, dialect: Dialect = "duckdb"
) -> str:
    """
    Count values that ARE in the disallowed set.

    Inverse of allowed_values: fails if value IS in the list.
    NULL values are NOT failures (NULL is not in any list).
    """
    if not values:
        return f"0 AS {esc_ident(rule_id, dialect)}"
    r = renderer_for(dialect)
    return r.sum_case([_disallowed_in_check(col, values).sql(r)], rule_id)


def agg_freshness(
    col: str, max_age_seconds: int, rule_id: str, dialect: Dialect = "duckdb"
) -> str:
    """Check if MAX(column) is within max_age_seconds of now."""
    r = renderer_for(dialect)
    return r.freshness(r.ident(col), int(max_age_seconds), rule_id)


def agg_range(
    col: str,
    min_val: Optional[Any],
    max_val: Optional[Any],
    rule_id: str,
    dialect: Dialect = "duckdb",
) -> str:
    """Count values outside [min, max] range. NULLs are failures."""
    r = renderer_for(dialect)
    column = Col(col)
    out_of_range = bounds(column, min_val, max_val)
    inner = Or(*out_of_range) if out_of_range else Raw("0=1")
    condition = Or(IsNull(column), Group(inner))
    return r.sum_case([condition.sql(r)], rule_id)


def agg_length(
    col: str,
    min_len: Optional[int],
    max_len: Optional[int],
    rule_id: str,
    dialect: Dialect = "duckdb",
) -> str:
    """
    Count values where string length is outside [min_len, max_len].

    NULL values are failures (can't measure length of NULL).
    """
    r = renderer_for(dialect)
    return r.sum_case([_length_violation(col, min_len, max_len).sql(r)], rule_id)


def agg_regex(
    col: str, pattern: str, rule_id: str, dialect: Dialect = "duckdb"
) -> str:
    """Count values that don't match the regex pattern. NULLs are failures."""
    r = renderer_for(dialect)
    return r.sum_case([RegexNoMatch(Col(col), pattern).sql(r)], rule_id)


def agg_contains(
    col: str, substring: str, rule_id: str, dialect: Dialect = "duckdb"
) -> str:
    """
    Count values that do NOT contain the substring.

    Uses LIKE for efficiency (3-6x faster than regex). NULLs are failures.
    """
    r = renderer_for(dialect)
    pattern = f"%{escape_like_pattern(substring)}%"
    return r.sum_case([_like_violation(col, pattern).sql(r)], rule_id)


def agg_starts_with(
    col: str, prefix: str, rule_id: str, dialect: Dialect = "duckdb"
) -> str:
    """Count values that do NOT start with the prefix. NULLs are failures."""
    r = renderer_for(dialect)
    pattern = f"{escape_like_pattern(prefix)}%"
    return r.sum_case([_like_violation(col, pattern).sql(r)], rule_id)


def agg_ends_with(
    col: str, suffix: str, rule_id: str, dialect: Dialect = "duckdb"
) -> str:
    """Count values that do NOT end with the suffix. NULLs are failures."""
    r = renderer_for(dialect)
    pattern = f"%{escape_like_pattern(suffix)}"
    return r.sum_case([_like_violation(col, pattern).sql(r)], rule_id)


def agg_compare(
    left: str,
    right: str,
    op: str,
    rule_id: str,
    dialect: Dialect = "duckdb",
) -> str:
    """Count rows where the comparison fails or either column is NULL."""
    r = renderer_for(dialect)
    return r.sum_case([_compare_violation(left, right, op).sql(r)], rule_id)


def agg_conditional_not_null(
    column: str,
    when_column: str,
    when_op: str,
    when_value: Any,
    rule_id: str,
    dialect: Dialect = "duckdb",
) -> str:
    """Count rows where column is NULL when the condition is met."""
    r = renderer_for(dialect)
    cond = when_condition(when_column, when_op, when_value)
    violation = And(Group(cond), IsNull(Col(column)))
    return r.sum_case([violation.sql(r)], rule_id)


def agg_conditional_range(
    column: str,
    when_column: str,
    when_op: str,
    when_value: Any,
    min_val: Any,
    max_val: Any,
    rule_id: str,
    dialect: Dialect = "duckdb",
) -> str:
    """Count rows outside [min, max] (or NULL) when the condition is met."""
    r = renderer_for(dialect)
    cond = when_condition(when_column, when_op, when_value)
    range_violation = _range_violation(column, min_val, max_val)
    violation = And(Group(cond), Group(range_violation))
    return r.sum_case([violation.sql(r)], rule_id)


# =============================================================================
# EXISTS Expression Builders (early termination, failed_count is lower bound)
# =============================================================================

def _exists_where(where: Node, table: str, rule_id: str, dialect: Dialect) -> str:
    """Wrap ``SELECT 1 FROM table WHERE <where>`` as an EXISTS expression."""
    r = renderer_for(dialect)
    return r.exists_wrap(f"SELECT 1 FROM {table} WHERE {where.sql(r)}", rule_id)


def exists_not_null(
    col: str, rule_id: str, table: str, dialect: Dialect = "duckdb"
) -> str:
    """EXISTS expression for not_null — stops at first NULL found."""
    return _exists_where(IsNull(Col(col)), table, rule_id, dialect)


def exists_unique(
    col: str, rule_id: str, table: str, dialect: Dialect = "duckdb"
) -> str:
    """EXISTS expression for unique — stops at first duplicated key group."""
    r = renderer_for(dialect)
    c = r.ident(col)
    inner = (
        f"SELECT 1 FROM {table} WHERE {c} IS NOT NULL "
        f"GROUP BY {c} HAVING COUNT(*) > 1"
    )
    return r.exists_wrap(inner, rule_id)


def exists_allowed_values(
    col: str, values: List[Any], table: str, rule_id: str, dialect: Dialect = "duckdb"
) -> str:
    """
    EXISTS expression for allowed_values — stops at first disallowed value.

    NULLs are violations unless None is explicitly in values, matching
    Polars behavior for tier equivalence.
    """
    return _exists_where(
        allowed_values_violation(col, values), table, rule_id, dialect
    )


def exists_disallowed_values(
    col: str, values: List[Any], table: str, rule_id: str, dialect: Dialect = "duckdb"
) -> str:
    """EXISTS expression for disallowed_values — stops at first hit."""
    if not values:
        return f"0 AS {esc_ident(rule_id, dialect)}"  # nothing can fail
    return _exists_where(
        _disallowed_in_check(col, values), table, rule_id, dialect
    )


def exists_range(
    col: str,
    min_val: Optional[Any],
    max_val: Optional[Any],
    table: str,
    rule_id: str,
    dialect: Dialect = "duckdb",
) -> str:
    """EXISTS expression for range — stops at first out-of-range or NULL value."""
    return _exists_where(
        _range_violation(col, min_val, max_val), table, rule_id, dialect
    )


def exists_length(
    col: str,
    min_len: Optional[int],
    max_len: Optional[int],
    table: str,
    rule_id: str,
    dialect: Dialect = "duckdb",
) -> str:
    """EXISTS expression for length — stops at first invalid-length or NULL value."""
    return _exists_where(
        _length_violation(col, min_len, max_len), table, rule_id, dialect
    )


def exists_regex(
    col: str, pattern: str, table: str, rule_id: str, dialect: Dialect = "duckdb"
) -> str:
    """EXISTS expression for regex — stops at first non-matching or NULL value."""
    return _exists_where(RegexNoMatch(Col(col), pattern), table, rule_id, dialect)


def exists_contains(
    col: str, substring: str, table: str, rule_id: str, dialect: Dialect = "duckdb"
) -> str:
    """EXISTS expression for contains — stops at first non-containing value."""
    pattern = f"%{escape_like_pattern(substring)}%"
    return _exists_where(_like_violation(col, pattern), table, rule_id, dialect)


def exists_starts_with(
    col: str, prefix: str, table: str, rule_id: str, dialect: Dialect = "duckdb"
) -> str:
    """EXISTS expression for starts_with — stops at first non-matching value."""
    pattern = f"{escape_like_pattern(prefix)}%"
    return _exists_where(_like_violation(col, pattern), table, rule_id, dialect)


def exists_ends_with(
    col: str, suffix: str, table: str, rule_id: str, dialect: Dialect = "duckdb"
) -> str:
    """EXISTS expression for ends_with — stops at first non-matching value."""
    pattern = f"%{escape_like_pattern(suffix)}"
    return _exists_where(_like_violation(col, pattern), table, rule_id, dialect)


def exists_compare(
    left: str,
    right: str,
    op: str,
    table: str,
    rule_id: str,
    dialect: Dialect = "duckdb",
) -> str:
    """EXISTS expression for compare — stops at first comparison failure."""
    return _exists_where(_compare_violation(left, right, op), table, rule_id, dialect)


def exists_conditional_not_null(
    column: str,
    when_column: str,
    when_op: str,
    when_value: Any,
    table: str,
    rule_id: str,
    dialect: Dialect = "duckdb",
) -> str:
    """EXISTS expression for conditional_not_null — stops at first violation."""
    cond = when_condition(when_column, when_op, when_value)
    violation = And(Group(cond), IsNull(Col(column)))
    return _exists_where(violation, table, rule_id, dialect)


def exists_conditional_range(
    column: str,
    when_column: str,
    when_op: str,
    when_value: Any,
    min_val: Any,
    max_val: Any,
    table: str,
    rule_id: str,
    dialect: Dialect = "duckdb",
) -> str:
    """EXISTS expression for conditional_range — stops at first violation."""
    cond = when_condition(when_column, when_op, when_value)
    range_violation = _range_violation(column, min_val, max_val)
    violation = And(Group(cond), Group(range_violation))
    return _exists_where(violation, table, rule_id, dialect)


def exists_custom(
    where_condition: str,
    table: str,
    rule_id: str,
    dialect: Dialect = "duckdb",
) -> str:
    """
    EXISTS expression for custom rules — the caller provides the WHERE
    condition that identifies violations (e.g. '"score" <= 0').
    """
    return _exists_where(Raw(where_condition), table, rule_id, dialect)


# =============================================================================
# Result Parsing
# =============================================================================

# Message templates per rule kind. Placeholders: {count} = "N" or "At least 1",
# {rows} = "row"/"rows", {s} = plural suffix, {col} = " in <column>".
_RULE_MESSAGES = {
    "not_null": "{count} null value{s} found{col}",
    "unique": "{count} duplicate {rows}{col}",
    "allowed_values": "{count} {rows} with disallowed value{col}",
    "disallowed_values": "{count} {rows} with disallowed value{col}",
    "range": "{count} {rows} out of range{col}",
    "length": "{count} {rows} with invalid length{col}",
    "regex": "{count} {rows} failed regex match{col}",
    "contains": "{count} {rows} missing required substring{col}",
    "starts_with": "{count} {rows} with invalid prefix{col}",
    "ends_with": "{count} {rows} with invalid suffix{col}",
    "compare": "{count} {rows} failed comparison",
    "conditional_not_null": "{count} {rows} with null value when condition met",
    "conditional_range": "{count} {rows} out of range when condition met",
}


def _generate_rule_message(
    rule_kind: Optional[str],
    failed_count: int,
    is_tally: bool,
    rule_id: str,
) -> str:
    """
    Generate a descriptive message for a rule result.

    Args:
        rule_kind: The type of rule (e.g., "not_null", "unique", "range")
        failed_count: Number of violations (1 for EXISTS mode)
        is_tally: True if exact count (COUNT mode), False if lower bound (EXISTS mode)
        rule_id: The rule ID (used to extract column name if needed)
    """
    if failed_count == 0:
        return "Passed"

    if rule_kind == "min_rows":
        return f"Dataset has {failed_count} fewer rows than required minimum"
    if rule_kind == "max_rows":
        return f"Dataset has {failed_count} more rows than allowed maximum"
    if rule_kind == "freshness":
        return "Data is stale"

    # Extract column name from rule_id (format: COL:column:rule_kind)
    column = None
    if rule_id.startswith("COL:"):
        parts = rule_id.split(":")
        if len(parts) >= 2:
            column = parts[1]

    if is_tally:
        count_str = str(failed_count)
        row_str = "row" if failed_count == 1 else "rows"
    else:
        # EXISTS mode: failed_count is a lower bound
        count_str = "At least 1"
        row_str = "row"

    template = _RULE_MESSAGES.get(rule_kind, "{count} {rows} failed validation")
    return template.format(
        count=count_str,
        rows=row_str,
        s="" if not is_tally or failed_count == 1 else "s",
        col=f" in {column}" if column else "",
    )


def results_from_row(
    columns: List[str],
    values: tuple,
    is_exists: bool = False,
    rule_kinds: Optional[dict] = None,
) -> List[dict]:
    """
    Convert a single-row SQL result to Kontra result format.

    Args:
        columns: Column names (rule IDs)
        values: Result values
        is_exists: If True, values are booleans (True=violation, False=pass)
                   If False, values are counts (0=pass, >0=violation count)
        rule_kinds: Optional dict mapping rule_id -> rule_kind for failure_mode
    """
    rule_kinds = rule_kinds or {}
    out = []
    for rule_id, val in zip(columns, values):
        if rule_id == "__no_sql_rules__":
            continue

        if is_exists:
            failed_count = 1 if val else 0
        else:
            failed_count = int(val) if val is not None else 0

        rule_kind = rule_kinds.get(rule_id)
        result = {
            "rule_id": rule_id,
            "passed": failed_count == 0,
            "failed_count": failed_count,
            "tally": not is_exists,
            "message": _generate_rule_message(
                rule_kind, failed_count, is_tally=not is_exists, rule_id=rule_id
            ),
            "severity": "ERROR",
            "actions_executed": [],
            "execution_source": "sql",
        }
        failure_mode = RULE_KIND_TO_FAILURE_MODE.get(rule_kind) if rule_kind else None
        if failed_count > 0 and failure_mode:
            result["failure_mode"] = failure_mode
        out.append(result)

    return out
