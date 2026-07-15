# src/kontra/probes/compare.py
"""
Compare probe: Measure transformation effects between before/after datasets.

This probe answers: "Did my transformation preserve rows and keys as expected?"

It does NOT answer: whether the transformation is "correct".
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Union

import polars as pl

from kontra.api.compare import CompareResult
from kontra.probes.compare_facts import (
    CompareFacts,
    CompareSamples,
    CompareSchema,
    finalize_compare_result,
)
from kontra.probes.utils import load_data


def _changed_expr(frame: pl.DataFrame, before_col: str, after_col: str) -> pl.Expr:
    """Compare a joined column pair without assuming identical driver dtypes."""
    before = pl.col(before_col)
    after = pl.col(after_col)
    before_type = frame.schema[before_col]
    after_type = frame.schema[after_col]

    if isinstance(before_type, pl.Datetime) and isinstance(after_type, pl.Datetime):
        before = before.dt.timestamp("us")
        after = after.dt.timestamp("us")
    elif (
        before_type != after_type
        and not (before_type.is_numeric() and after_type.is_numeric())
    ):
        before = before.cast(pl.String, strict=False)
        after = after.cast(pl.String, strict=False)

    return before.ne_missing(after)


def compare(
    before: Any,
    after: Any,
    key: Optional[Union[str, List[str]]] = None,
    *,
    before_key: Optional[Union[str, List[str]]] = None,
    after_key: Optional[Union[str, List[str]]] = None,
    before_table: Optional[str] = None,
    after_table: Optional[str] = None,
    sample_limit: int = 0,
    save: bool = False,
    storage_options: Optional[Dict[str, Any]] = None,
) -> CompareResult:
    """
    Compare two datasets to measure transformation effects.

    Answers: "Did my transformation preserve rows and keys as expected?"

    Does NOT answer: whether the transformation is "correct".

    This probe provides deterministic, structured measurements that allow
    agents (and humans) to reason about transformation effects with confidence.

    ``before`` and ``after`` may each be any Kontra source, mixed freely:
    a Polars/pandas DataFrame, a file or cloud path, a database URI
    (``postgres://.../public.users``), a named datasource (``prod_db.users``),
    or a live database connection (pass ``before_table``/``after_table``).

    Keys may be the same name on both sides (``key=``) or differently named
    (``before_key=`` / ``after_key=``). The latter is the common FK→PK case,
    e.g. ``compare(tickets, orgs, before_key="organization_id", after_key="id")``.
    Provide exactly one of ``key`` or the ``before_key``/``after_key`` pair.

    Args:
        before: Dataset before transformation (any supported source).
        after: Dataset after transformation (any supported source).
        key: Same-named key column(s) present on both sides.
        before_key: Key column(s) on the ``before`` side (use with ``after_key``).
        after_key: Key column(s) on the ``after`` side (use with ``before_key``).
            ``before_key``/``after_key`` are paired positionally for composite
            keys and must have the same number of columns.
        before_table: Table reference when ``before`` is a DB connection object.
        after_table: Table reference when ``after`` is a DB connection object.
        sample_limit: Max samples per category. Defaults to 0 (aggregate-only,
            no row-level values). Opt in by passing a positive limit, like validate.
        save: Persist result to state backend (not yet implemented)

    Returns:
        CompareResult with row_stats, key_stats, change_stats,
        column_stats, and bounded samples.

    Example:
        # Basic comparison
        result = compare(raw_df, transformed_df, key="order_id")

        # With composite key
        result = compare(before, after, key=["customer_id", "date"])

        # Different-named keys (FK → PK)
        result = compare(tickets, orgs, before_key="organization_id", after_key="id")

        # Check for issues
        if result.duplicated_after > 0:
            print(f"Warning: {result.duplicated_after} keys are duplicated")
            print(f"Sample: {result.samples_duplicated_keys}")

        # Get structured output for LLM
        print(result.to_llm())
    """
    # Resolve the key specification. The ``before`` side's key names are used
    # in the result; computation itself uses collision-safe internal aliases.
    before_key_list, after_key_list = _resolve_keys(
        key, "key",
        before_key, after_key, "before_key", "after_key",
    )

    # Mode A: when both sides are the same DB engine, compute the counts with
    # set-based SQL (zero rows moved). It fires only inside a provably-safe
    # envelope and otherwise falls back to the Polars path below.
    from kontra.logging import get_logger
    from kontra.probes.compare_sql import _FallbackToPolars, compare_sql, plan_compare

    _log = get_logger(__name__)
    _plan = plan_compare(
        before, after, before_key_list, after_key_list,
        before_table, after_table, sample_limit,
    )
    if _plan is not None:
        try:
            return compare_sql(_plan, sample_limit)
        except _FallbackToPolars as e:
            _log.debug("compare: SQL pushdown not applicable (%s); using Polars", e)
        except Exception as e:  # noqa: BLE001 - graceful pushdown fallback, like validate
            _log.warning("compare: SQL pushdown failed (%s); using Polars", e)

    # Materialize both sides from any supported source (file, db table, named
    # datasource, DataFrame, or BYOC connection). before_table/after_table
    # apply only when the corresponding side is a live database connection.
    before_df = load_data(before, storage_options=storage_options, table=before_table)
    after_df = load_data(after, storage_options=storage_options, table=after_table)

    # Align both sides onto neutral aliases. This preserves non-key columns
    # when either dataset already has a column named like the other side's key.
    before_df, after_df, internal_key = _align_side_keys(
        before_df,
        after_df,
        before_key_list,
        after_key_list,
        "before",
        "after",
    )

    # Compute the comparison
    result = _compute_compare(before_df, after_df, internal_key, sample_limit)
    _restore_compare_output_key_names(result, internal_key, before_key_list)

    if save:
        raise NotImplementedError("Probe save not yet implemented")

    return result


def _resolve_keys(
    symmetric: Optional[Union[str, List[str]]],
    symmetric_name: str,
    left_value: Optional[Union[str, List[str]]],
    right_value: Optional[Union[str, List[str]]],
    left_name: str,
    right_name: str,
) -> tuple[List[str], List[str]]:
    """
    Resolve a symmetric-or-asymmetric key specification into two aligned lists.

    Returns ``(left_list, right_list)`` where the two lists are paired
    positionally. For the same-named shortcut, both lists are identical.

    Raises ValueError if both the symmetric and asymmetric forms are provided,
    if only one side of the asymmetric pair is given, if the asymmetric sides
    have mismatched arity, or if no key is provided at all.
    """
    asymmetric = left_value is not None or right_value is not None

    if symmetric is not None and asymmetric:
        raise ValueError(
            f"Provide either {symmetric_name}= (same-named keys) or "
            f"{left_name}=/{right_name}= (different-named keys), not both."
        )

    if asymmetric:
        if left_value is None or right_value is None:
            raise ValueError(
                f"Both {left_name}= and {right_name}= are required for "
                f"different-named keys (only one was given)."
            )
        left_list = [left_value] if isinstance(left_value, str) else list(left_value)
        right_list = [right_value] if isinstance(right_value, str) else list(right_value)
        if len(left_list) != len(right_list):
            raise ValueError(
                f"{left_name}= and {right_name}= must reference the same number "
                f"of columns (got {len(left_list)} and {len(right_list)})."
            )
        return left_list, right_list

    if symmetric is None:
        raise ValueError(
            f"A key is required: pass {symmetric_name}= for same-named keys, "
            f"or {left_name}=/{right_name}= for different-named keys."
        )

    sym_list = [symmetric] if isinstance(symmetric, str) else list(symmetric)
    return sym_list, sym_list


def _align_side_keys(
    left: pl.DataFrame,
    right: pl.DataFrame,
    left_key: List[str],
    right_key: List[str],
    left_label: str,
    right_label: str,
) -> tuple[pl.DataFrame, pl.DataFrame, List[str]]:
    """
    Rename both sides' key columns onto collision-safe internal aliases.

    This lets the core join/compare logic operate on a single set of key names
    even when the two sides use differently-named keys. Aliases are unique
    across both input schemas, so non-key columns are never renamed or lost.

    Raises ValueError if a declared key column is missing.
    """
    for col in left_key:
        if col not in left.columns:
            raise ValueError(f"Key column '{col}' not found in {left_label} dataset")
    for col in right_key:
        if col not in right.columns:
            raise ValueError(f"Key column '{col}' not found in {right_label} dataset")

    occupied_names = set(left.columns) | set(right.columns)
    internal_key = []
    alias_index = 0
    while len(internal_key) < len(left_key):
        candidate = f"__kontra_key_{alias_index}"
        alias_index += 1
        if candidate not in occupied_names:
            internal_key.append(candidate)
            occupied_names.add(candidate)

    left_rename_map = dict(zip(left_key, internal_key))
    right_rename_map = dict(zip(right_key, internal_key))
    return left.rename(left_rename_map), right.rename(right_rename_map), internal_key


def _restore_key_sample_names(
    samples: List[Any],
    internal_key: List[str],
    output_key: List[str],
) -> List[Any]:
    """Replace internal alias names in composite-key samples."""
    aliases_to_output = dict(zip(internal_key, output_key))
    return [
        {aliases_to_output.get(name, name): value for name, value in sample.items()}
        if isinstance(sample, dict)
        else sample
        for sample in samples
    ]


def _restore_compare_output_key_names(
    result: CompareResult,
    internal_key: List[str],
    output_key: List[str],
) -> None:
    """Restore caller-facing key names after internal alias computation."""
    result.key = output_key
    result.samples_duplicated_keys = _restore_key_sample_names(
        result.samples_duplicated_keys, internal_key, output_key
    )
    result.samples_dropped_keys = _restore_key_sample_names(
        result.samples_dropped_keys, internal_key, output_key
    )
    for sample in result.samples_changed_rows:
        key_value = sample["key"]
        if isinstance(key_value, dict):
            sample["key"] = {
                dict(zip(internal_key, output_key)).get(name, name): value
                for name, value in key_value.items()
            }


def _compute_compare(
    before: pl.DataFrame,
    after: pl.DataFrame,
    key: List[str],
    sample_limit: int,
) -> CompareResult:
    """
    Compute all comparison metrics between before and after datasets.

    This is the core algorithm implementing the MVP schema.
    """
    # Validate key columns exist
    for k in key:
        if k not in before.columns:
            raise ValueError(f"Key column '{k}' not found in before dataset")
        if k not in after.columns:
            raise ValueError(f"Key column '{k}' not found in after dataset")

    # Handle empty DataFrames: cast columns to match schema
    # Empty DataFrames have Null dtype columns which break joins
    if len(after) == 0 and len(before) > 0:
        # Cast after columns to match before schema for common columns
        cast_exprs = []
        for col in after.columns:
            if col in before.columns:
                cast_exprs.append(pl.col(col).cast(before[col].dtype))
            else:
                cast_exprs.append(pl.col(col))
        if cast_exprs:
            after = after.select(cast_exprs)

    if len(before) == 0 and len(after) > 0:
        # Cast before columns to match after schema for common columns
        cast_exprs = []
        for col in before.columns:
            if col in after.columns:
                cast_exprs.append(pl.col(col).cast(after[col].dtype))
            else:
                cast_exprs.append(pl.col(col))
        if cast_exprs:
            before = before.select(cast_exprs)

    # Primitive measurements only — every derived field lives in the finalizer,
    # shared with the set-based SQL path so the two cannot interpret facts differently.
    before_rows = len(before)
    after_rows = len(after)

    before_keys = before.select(key).unique()
    after_keys = after.select(key).unique()
    unique_before = len(before_keys)
    unique_after = len(after_keys)
    preserved = len(before_keys.join(after_keys, on=key, how="inner"))

    after_key_counts = after.group_by(key).agg(pl.len().alias("_count"))
    duplicated_keys_df = after_key_counts.filter(pl.col("_count") > 1)
    duplicated_after = len(duplicated_keys_df)

    non_key_before = [c for c in before.columns if c not in key]
    non_key_after = [c for c in after.columns if c not in key]
    common = sorted(set(non_key_before) & set(non_key_after))
    columns_added = sorted(set(non_key_after) - set(non_key_before))
    columns_removed = sorted(set(non_key_before) - set(non_key_after))

    # Change stats over the row-level inner join (duplicate keys cross-multiply).
    matched_rows = 0
    changed_rows = 0
    changed_by_column: Dict[str, int] = {}
    if preserved > 0 and common:
        merged = before.join(after, on=key, how="inner", suffix="_after")
        matched_rows = len(merged)
        change_exprs = []
        for col in common:
            after_col = f"{col}_after"
            if after_col in merged.columns:
                expr = _changed_expr(merged, col, after_col)
                changed_by_column[col] = len(merged.filter(expr))
                change_exprs.append(expr)
        if change_exprs:
            combined = change_exprs[0]
            for e in change_exprs[1:]:
                combined = combined | e
            changed_rows = len(merged.filter(combined))

    changed_cols = [c for c in common if changed_by_column.get(c, 0) > 0]
    nulls_before = {c: before[c].null_count() for c in changed_cols}
    nulls_after = {c: after[c].null_count() for c in changed_cols + columns_added}

    samples = CompareSamples(
        duplicated_keys=_extract_key_samples(
            duplicated_keys_df.select(key), key, sample_limit
        ),
        dropped_keys=_extract_key_samples(
            before_keys.join(after_keys, on=key, how="anti"), key, sample_limit
        ),
        changed_rows=_extract_changed_row_samples(
            before, after, key, set(common), sample_limit
        ),
    )

    schema = CompareSchema(
        key=list(key), common=common, added=columns_added, removed=columns_removed
    )
    facts = CompareFacts(
        before_rows=before_rows,
        after_rows=after_rows,
        unique_before=unique_before,
        unique_after=unique_after,
        preserved=preserved,
        duplicated_after=duplicated_after,
        matched_rows=matched_rows,
        changed_rows=changed_rows,
        changed_by_column=changed_by_column,
        nulls_before=nulls_before,
        nulls_after=nulls_after,
    )
    return finalize_compare_result(
        facts, schema, execution_tier="polars", sample_limit=sample_limit, samples=samples
    )


def _extract_key_samples(
    keys_df: pl.DataFrame,
    key: List[str],
    limit: int,
) -> List[Any]:
    """
    Extract sample key values from a DataFrame.

    Returns list of key values (single value if single key, tuple if composite).
    """
    if len(keys_df) == 0:
        return []

    samples = keys_df.head(limit)

    if len(key) == 1:
        # Single key - return list of values
        return samples[key[0]].to_list()
    else:
        # Composite key - return list of dicts
        return samples.to_dicts()


def _extract_changed_row_samples(
    before: pl.DataFrame,
    after: pl.DataFrame,
    key: List[str],
    common_cols: set,
    limit: int,
) -> List[Dict[str, Any]]:
    """
    Extract sample changed rows with before/after values.

    Returns list of dicts with key, before values, and after values
    for columns that changed.
    """
    if not common_cols:
        return []

    # Join on key
    merged = before.join(after, on=key, how="inner", suffix="_after")

    if len(merged) == 0:
        return []

    samples = []
    for row in merged.head(limit * 2).iter_rows(named=True):
        # Check if any column changed
        changes_before = {}
        changes_after = {}
        has_change = False

        for col in common_cols:
            after_col = f"{col}_after"
            if after_col in row:
                before_val = row[col]
                after_val = row[after_col]

                # Check for change (handle NULL)
                is_changed = (before_val != after_val) or (
                    (before_val is None) != (after_val is None)
                )

                if is_changed:
                    has_change = True
                    changes_before[col] = before_val
                    changes_after[col] = after_val

        if has_change:
            # Extract key value(s)
            if len(key) == 1:
                key_val = row[key[0]]
            else:
                key_val = {k: row[k] for k in key}

            samples.append({
                "key": key_val,
                "before": changes_before,
                "after": changes_after,
            })

            if len(samples) >= limit:
                break

    return samples
