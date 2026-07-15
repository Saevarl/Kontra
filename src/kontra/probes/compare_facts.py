# src/kontra/probes/compare_facts.py
"""Shared measurement model and result assembly for the compare probe.

Both execution paths — the Polars reference and the set-based SQL path — measure
the same PRIMITIVE facts and hand them to :func:`finalize_compare_result`, which
derives every ``CompareResult`` field once. Only *measurement* differs between
backends; the facts-to-result mapping lives here, so the two paths cannot drift
in how they interpret those facts.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from kontra.api.compare import CompareResult


@dataclass(frozen=True)
class CompareSchema:
    """The column shape of a comparison (all lists in the caller-facing order)."""

    key: List[str]
    common: List[str]   # sorted non-key columns present on both sides
    added: List[str]    # sorted non-key columns only in `after`
    removed: List[str]  # sorted non-key columns only in `before`


@dataclass
class CompareFacts:
    """Primitive, directly-measured counts. Everything else is derived."""

    before_rows: int
    after_rows: int
    unique_before: int
    unique_after: int
    preserved: int
    duplicated_after: int
    # Row-level join cardinality (matched rows); meaningful only when there are
    # common non-key columns to compare.
    matched_rows: int
    changed_rows: int
    changed_by_column: Dict[str, int] = field(default_factory=dict)
    nulls_before: Dict[str, int] = field(default_factory=dict)
    nulls_after: Dict[str, int] = field(default_factory=dict)


@dataclass
class CompareSamples:
    """Bounded, explanatory samples (empty in aggregate-only mode)."""

    duplicated_keys: List[Any] = field(default_factory=list)
    dropped_keys: List[Any] = field(default_factory=list)
    changed_rows: List[Dict[str, Any]] = field(default_factory=list)


def finalize_compare_result(
    facts: CompareFacts,
    schema: CompareSchema,
    *,
    execution_tier: str,
    sample_limit: int,
    samples: Optional[CompareSamples] = None,
) -> CompareResult:
    """Derive the full CompareResult from primitive facts. One derivation, shared
    by every backend, so measurement is the only thing that can differ."""
    samples = samples or CompareSamples()
    before_rows = facts.before_rows
    after_rows = facts.after_rows

    dropped = facts.unique_before - facts.preserved
    added = facts.unique_after - facts.preserved

    if schema.common and facts.preserved > 0:
        unchanged_rows = facts.matched_rows - facts.changed_rows
    elif facts.preserved > 0:
        # No common non-key columns: nothing can change; unchanged == preserved
        # (distinct keys), not the row-level join cardinality.
        unchanged_rows = facts.preserved
    else:
        unchanged_rows = 0

    columns_modified = [c for c in schema.common if facts.changed_by_column.get(c, 0) > 0]
    modified_fraction = {
        c: facts.changed_by_column[c] / facts.matched_rows
        for c in columns_modified
        if facts.matched_rows > 0
    }

    nullability_delta: Dict[str, Dict[str, Optional[float]]] = {}
    for c in columns_modified:
        nullability_delta[c] = {
            "before": (facts.nulls_before.get(c, 0) / before_rows) if before_rows else 0.0,
            "after": (facts.nulls_after.get(c, 0) / after_rows) if after_rows else 0.0,
        }
    for c in schema.added:
        nullability_delta[c] = {
            "before": None,
            "after": (facts.nulls_after.get(c, 0) / after_rows) if after_rows else 0.0,
        }

    return CompareResult(
        before_rows=before_rows,
        after_rows=after_rows,
        key=list(schema.key),
        execution_tier=execution_tier,
        row_delta=after_rows - before_rows,
        row_ratio=after_rows / before_rows if before_rows > 0 else float("inf"),
        unique_before=facts.unique_before,
        unique_after=facts.unique_after,
        preserved=facts.preserved,
        dropped=dropped,
        added=added,
        duplicated_after=facts.duplicated_after,
        unchanged_rows=unchanged_rows,
        changed_rows=facts.changed_rows,
        columns_added=list(schema.added),
        columns_removed=list(schema.removed),
        columns_modified=columns_modified,
        modified_fraction=modified_fraction,
        nullability_delta=nullability_delta,
        samples_duplicated_keys=samples.duplicated_keys,
        samples_dropped_keys=samples.dropped_keys,
        samples_changed_rows=samples.changed_rows,
        sample_limit=sample_limit,
    )
