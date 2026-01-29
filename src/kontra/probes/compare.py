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
from kontra.probes.utils import load_data


def compare(
    before: Union[pl.DataFrame, str],
    after: Union[pl.DataFrame, str],
    key: Union[str, List[str]],
    *,
    sample_limit: int = 5,
    save: bool = False,
) -> CompareResult:
    """
    Compare two datasets to measure transformation effects.

    Answers: "Did my transformation preserve rows and keys as expected?"

    Does NOT answer: whether the transformation is "correct".

    This probe provides deterministic, structured measurements that allow
    agents (and humans) to reason about transformation effects with confidence.

    Args:
        before: Dataset before transformation (DataFrame or path/URI)
        after: Dataset after transformation (DataFrame or path/URI)
        key: Column(s) to use as row identifier
        sample_limit: Max samples per category (default 5)
        save: Persist result to state backend (not yet implemented)

    Returns:
        CompareResult with row_stats, key_stats, change_stats,
        column_stats, and bounded samples.

    Example:
        # Basic comparison
        result = compare(raw_df, transformed_df, key="order_id")

        # With composite key
        result = compare(before, after, key=["customer_id", "date"])

        # Check for issues
        if result.duplicated_after > 0:
            print(f"Warning: {result.duplicated_after} keys are duplicated")
            print(f"Sample: {result.samples_duplicated_keys}")

        # Get structured output for LLM
        print(result.to_llm())
    """
    # Normalize key to list
    if isinstance(key, str):
        key = [key]

    # Load data if paths provided
    before_df = load_data(before)
    after_df = load_data(after)

    # Compute the comparison
    result = _compute_compare(before_df, after_df, key, sample_limit)

    # TODO: Implement save functionality
    if save:
        pass

    return result


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

    # ==========================================================================
    # 1. Row stats
    # ==========================================================================
    before_rows = len(before)
    after_rows = len(after)
    row_delta = after_rows - before_rows
    row_ratio = after_rows / before_rows if before_rows > 0 else float('inf')

    # ==========================================================================
    # 2. Key stats
    # ==========================================================================
    before_keys = before.select(key).unique()
    after_keys = after.select(key).unique()

    unique_before = len(before_keys)
    unique_after = len(after_keys)

    # Keys in both (preserved)
    preserved_keys = before_keys.join(after_keys, on=key, how="inner")
    preserved = len(preserved_keys)

    # Keys dropped (in before but not in after)
    dropped = unique_before - preserved

    # Keys added (in after but not in before)
    added = unique_after - preserved

    # Duplicated after: count of keys appearing >1x in after
    # (This is key count, not row count)
    after_key_counts = after.group_by(key).agg(pl.len().alias("_count"))
    duplicated_keys_df = after_key_counts.filter(pl.col("_count") > 1)
    duplicated_after = len(duplicated_keys_df)

    # ==========================================================================
    # 3. Change stats (for preserved keys only)
    # ==========================================================================
    # Join before and after on key to find matching rows
    # Use suffix to disambiguate columns
    non_key_cols_before = [c for c in before.columns if c not in key]
    non_key_cols_after = [c for c in after.columns if c not in key]
    common_non_key_cols = set(non_key_cols_before) & set(non_key_cols_after)

    unchanged_rows = 0
    changed_rows = 0

    if preserved > 0 and common_non_key_cols:
        # For each preserved key, compare values
        # Join on key, suffix the after columns
        merged = before.join(after, on=key, how="inner", suffix="_after")

        # Build a change mask: True if any common non-key column differs
        # Handle NULL comparison: NULL != value should be True
        change_exprs = []
        for col in common_non_key_cols:
            after_col = f"{col}_after"
            if after_col in merged.columns:
                # Use ne_missing to treat NULLs as different from values
                # but NULL == NULL as same
                change_exprs.append(
                    (pl.col(col).ne(pl.col(after_col))) |
                    (pl.col(col).is_null() != pl.col(after_col).is_null())
                )

        if change_exprs:
            # Combine all expressions with OR
            combined_mask = change_exprs[0]
            for expr in change_exprs[1:]:
                combined_mask = combined_mask | expr

            # Count changed and unchanged
            changed_df = merged.filter(combined_mask)
            changed_rows = len(changed_df)
            unchanged_rows = len(merged) - changed_rows
        else:
            # No common columns to compare
            unchanged_rows = len(merged)
            changed_rows = 0
    elif preserved > 0:
        # No common non-key columns, so no changes possible
        unchanged_rows = preserved
        changed_rows = 0

    # ==========================================================================
    # 4. Column stats
    # ==========================================================================
    before_cols = set(before.columns)
    after_cols = set(after.columns)

    columns_added = sorted(after_cols - before_cols)
    columns_removed = sorted(before_cols - after_cols)

    # Modified columns: columns in both where at least one value differs
    # Also compute modified_fraction
    columns_modified = []
    modified_fraction: Dict[str, float] = {}

    if preserved > 0:
        merged = before.join(after, on=key, how="inner", suffix="_after")
        preserved_count = len(merged)

        for col in sorted(common_non_key_cols):
            after_col = f"{col}_after"
            if after_col in merged.columns and preserved_count > 0:
                # Count rows where this column changed
                changed_count = len(merged.filter(
                    (pl.col(col).ne(pl.col(after_col))) |
                    (pl.col(col).is_null() != pl.col(after_col).is_null())
                ))
                if changed_count > 0:
                    columns_modified.append(col)
                    modified_fraction[col] = changed_count / preserved_count

    # Nullability delta
    nullability_delta: Dict[str, Dict[str, Optional[float]]] = {}

    # For modified columns, compute before and after null rates
    for col in columns_modified:
        before_null = before[col].null_count() / before_rows if before_rows > 0 else 0.0
        after_null = after[col].null_count() / after_rows if after_rows > 0 else 0.0
        nullability_delta[col] = {"before": before_null, "after": after_null}

    # For added columns, only after rate
    for col in columns_added:
        after_null = after[col].null_count() / after_rows if after_rows > 0 else 0.0
        nullability_delta[col] = {"before": None, "after": after_null}

    # ==========================================================================
    # 5. Samples
    # ==========================================================================

    # Sample duplicated keys
    samples_duplicated_keys = _extract_key_samples(
        duplicated_keys_df.select(key),
        key,
        sample_limit
    )

    # Sample dropped keys (in before but not in after)
    dropped_keys_df = before_keys.join(after_keys, on=key, how="anti")
    samples_dropped_keys = _extract_key_samples(dropped_keys_df, key, sample_limit)

    # Sample changed rows (with before/after values)
    samples_changed_rows = _extract_changed_row_samples(
        before, after, key, common_non_key_cols, sample_limit
    )

    # ==========================================================================
    # Build result
    # ==========================================================================
    return CompareResult(
        # Meta
        before_rows=before_rows,
        after_rows=after_rows,
        key=key,
        execution_tier="polars",

        # Row stats
        row_delta=row_delta,
        row_ratio=row_ratio,

        # Key stats
        unique_before=unique_before,
        unique_after=unique_after,
        preserved=preserved,
        dropped=dropped,
        added=added,
        duplicated_after=duplicated_after,

        # Change stats
        unchanged_rows=unchanged_rows,
        changed_rows=changed_rows,

        # Column stats
        columns_added=columns_added,
        columns_removed=columns_removed,
        columns_modified=columns_modified,
        modified_fraction=modified_fraction,
        nullability_delta=nullability_delta,

        # Samples
        samples_duplicated_keys=samples_duplicated_keys,
        samples_dropped_keys=samples_dropped_keys,
        samples_changed_rows=samples_changed_rows,

        # Config
        sample_limit=sample_limit,
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
