# src/kontra/probes/relationship.py
"""
Relationship probe: Measure JOIN viability between two datasets.

This probe answers: "What is the shape of this join?"

It does NOT answer: which join type to use, or whether the join is correct.
"""

from __future__ import annotations

from typing import Any, List, Union

import polars as pl

from kontra.api.compare import RelationshipProfile
from kontra.probes.utils import load_data


def profile_relationship(
    left: Union[pl.DataFrame, str],
    right: Union[pl.DataFrame, str],
    on: Union[str, List[str]],
    *,
    sample_limit: int = 5,
    save: bool = False,
) -> RelationshipProfile:
    """
    Profile the structural relationship between two datasets.

    Answers: "What is the shape of this join?"

    Does NOT answer: which join type to use, or whether the join is correct.

    This probe provides deterministic, structured measurements that allow
    agents (and humans) to understand JOIN viability before writing SQL.

    Args:
        left: Left dataset (DataFrame or path/URI)
        right: Right dataset (DataFrame or path/URI)
        on: Column(s) to join on
        sample_limit: Max samples per category (default 5)
        save: Persist result to state backend (not yet implemented)

    Returns:
        RelationshipProfile with key_stats, cardinality, coverage,
        and bounded samples.

    Example:
        # Profile before writing JOIN
        profile = profile_relationship(orders, customers, on="customer_id")

        # Check for issues
        if profile.right_key_multiplicity_max > 1:
            print("Warning: right side has duplicates, JOIN may explode rows")
            print(f"Sample duplicated keys: {profile.samples_right_duplicates}")

        if profile.left_keys_without_match > 0:
            print(f"Warning: {profile.left_keys_without_match} keys won't match")

        # Get structured output for LLM
        print(profile.to_llm())
    """
    # Normalize on to list
    if isinstance(on, str):
        on = [on]

    # Load data if paths provided
    left_df = load_data(left)
    right_df = load_data(right)

    # Compute the profile
    result = _compute_relationship(left_df, right_df, on, sample_limit)

    # TODO: Implement save functionality
    if save:
        pass

    return result


def _compute_relationship(
    left: pl.DataFrame,
    right: pl.DataFrame,
    on: List[str],
    sample_limit: int,
) -> RelationshipProfile:
    """
    Compute all relationship metrics between left and right datasets.

    This is the core algorithm implementing the MVP schema.
    """
    # Validate key columns exist
    for col in on:
        if col not in left.columns:
            raise ValueError(f"Key column '{col}' not found in left dataset")
        if col not in right.columns:
            raise ValueError(f"Key column '{col}' not found in right dataset")

    # ==========================================================================
    # 1. Basic counts
    # ==========================================================================
    left_rows = len(left)
    right_rows = len(right)

    # ==========================================================================
    # 2. Key stats - left
    # ==========================================================================
    # Null rate: fraction of rows with any NULL in join key columns
    if left_rows > 0:
        null_mask = pl.lit(False)
        for col in on:
            null_mask = null_mask | pl.col(col).is_null()
        left_null_count = len(left.filter(null_mask))
        left_null_rate = left_null_count / left_rows
    else:
        left_null_rate = 0.0

    # Unique keys (excluding NULLs)
    left_keys = left.select(on).drop_nulls().unique()
    left_unique_keys = len(left_keys)

    # Duplicate keys: count of keys appearing >1x
    left_key_counts = left.drop_nulls(subset=on).group_by(on).agg(pl.len().alias("_count"))
    left_duplicate_keys = len(left_key_counts.filter(pl.col("_count") > 1))

    # ==========================================================================
    # 3. Key stats - right
    # ==========================================================================
    if right_rows > 0:
        null_mask = pl.lit(False)
        for col in on:
            null_mask = null_mask | pl.col(col).is_null()
        right_null_count = len(right.filter(null_mask))
        right_null_rate = right_null_count / right_rows
    else:
        right_null_rate = 0.0

    # Unique keys (excluding NULLs)
    right_keys = right.select(on).drop_nulls().unique()
    right_unique_keys = len(right_keys)

    # Duplicate keys: count of keys appearing >1x
    right_key_counts = right.drop_nulls(subset=on).group_by(on).agg(pl.len().alias("_count"))
    right_duplicate_keys = len(right_key_counts.filter(pl.col("_count") > 1))

    # ==========================================================================
    # 4. Cardinality (rows per key)
    # ==========================================================================
    # Left multiplicity
    if len(left_key_counts) > 0:
        left_key_multiplicity_min = left_key_counts["_count"].min()
        left_key_multiplicity_max = left_key_counts["_count"].max()
    else:
        left_key_multiplicity_min = 0
        left_key_multiplicity_max = 0

    # Right multiplicity
    if len(right_key_counts) > 0:
        right_key_multiplicity_min = right_key_counts["_count"].min()
        right_key_multiplicity_max = right_key_counts["_count"].max()
    else:
        right_key_multiplicity_min = 0
        right_key_multiplicity_max = 0

    # ==========================================================================
    # 5. Coverage
    # ==========================================================================
    # Left keys with match in right
    left_matched = left_keys.join(right_keys, on=on, how="inner")
    left_keys_with_match = len(left_matched)
    left_keys_without_match = left_unique_keys - left_keys_with_match

    # Right keys with match in left
    right_matched = right_keys.join(left_keys, on=on, how="inner")
    right_keys_with_match = len(right_matched)
    right_keys_without_match = right_unique_keys - right_keys_with_match

    # ==========================================================================
    # 6. Samples
    # ==========================================================================
    # Sample left unmatched keys
    left_unmatched = left_keys.join(right_keys, on=on, how="anti")
    samples_left_unmatched = _extract_key_samples(left_unmatched, on, sample_limit)

    # Sample right unmatched keys
    right_unmatched = right_keys.join(left_keys, on=on, how="anti")
    samples_right_unmatched = _extract_key_samples(right_unmatched, on, sample_limit)

    # Sample right duplicate keys
    right_duplicates = right_key_counts.filter(pl.col("_count") > 1).select(on)
    samples_right_duplicates = _extract_key_samples(right_duplicates, on, sample_limit)

    # ==========================================================================
    # Build result
    # ==========================================================================
    return RelationshipProfile(
        # Meta
        on=on,
        left_rows=left_rows,
        right_rows=right_rows,
        execution_tier="polars",

        # Key stats - left
        left_null_rate=left_null_rate,
        left_unique_keys=left_unique_keys,
        left_duplicate_keys=left_duplicate_keys,

        # Key stats - right
        right_null_rate=right_null_rate,
        right_unique_keys=right_unique_keys,
        right_duplicate_keys=right_duplicate_keys,

        # Cardinality
        left_key_multiplicity_min=left_key_multiplicity_min,
        left_key_multiplicity_max=left_key_multiplicity_max,
        right_key_multiplicity_min=right_key_multiplicity_min,
        right_key_multiplicity_max=right_key_multiplicity_max,

        # Coverage
        left_keys_with_match=left_keys_with_match,
        left_keys_without_match=left_keys_without_match,
        right_keys_with_match=right_keys_with_match,
        right_keys_without_match=right_keys_without_match,

        # Samples
        samples_left_unmatched=samples_left_unmatched,
        samples_right_unmatched=samples_right_unmatched,
        samples_right_duplicates=samples_right_duplicates,

        # Config
        sample_limit=sample_limit,
    )


def _extract_key_samples(
    keys_df: pl.DataFrame,
    on: List[str],
    limit: int,
) -> List[Any]:
    """
    Extract sample key values from a DataFrame.

    Returns list of key values (single value if single key, dict if composite).
    """
    if len(keys_df) == 0:
        return []

    samples = keys_df.head(limit)

    if len(on) == 1:
        # Single key - return list of values
        return samples[on[0]].to_list()
    else:
        # Composite key - return list of dicts
        return samples.to_dicts()
