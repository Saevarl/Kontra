# src/kontra/api/compare.py
"""
Result types for transformation probes.

These are the structured result types returned by compare() and
profile_relationship() probes.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class CompareResult:
    """
    Result of comparing two datasets to measure transformation effects.

    Answers: "Did my transformation preserve rows and keys as expected?"

    Does NOT answer: whether the transformation is "correct".

    All measurements are deterministic and factual. Interpretation
    belongs to the consumer (agent or human).

    Attributes:
        # Meta
        before_rows: Number of rows in before dataset
        after_rows: Number of rows in after dataset
        key: Column(s) used as row identifier
        execution_tier: Which execution tier computed the result ("polars" | "sql")

        # Row stats
        row_delta: Change in row count (after - before)
        row_ratio: Ratio of after/before rows

        # Key stats
        unique_before: Count of unique keys in before
        unique_after: Count of unique keys in after
        preserved: Keys present in both before and after
        dropped: Keys in before but not in after
        added: Keys in after but not in before
        duplicated_after: Keys appearing >1x in after (not row count, key count)

        # Change stats (for preserved keys only)
        unchanged_rows: Rows where no non-key columns changed
        changed_rows: Rows where at least one non-key column changed

        # Column stats
        columns_added: Columns in after but not in before
        columns_removed: Columns in before but not in after
        columns_modified: Columns in both where at least one value differs
        modified_fraction: {col: fraction of preserved rows where col changed}
        nullability_delta: {col: {before: rate, after: rate}}

        # Samples (bounded, explanatory only)
        samples_duplicated_keys: Sample keys appearing >1x in after
        samples_dropped_keys: Sample keys dropped from before
        samples_changed_rows: Sample changed rows with before/after values

        # Config
        sample_limit: Maximum samples per category

    Semantic Definitions:
        - changed_rows: Structural value change. Any non-key column inequality
          counts as a change. NULL → value and value → NULL are changes.
          Computed only for preserved keys.
        - duplicated_after: Count of keys (not rows) that appear more than once
          in the after dataset. A key appearing 3x contributes 1 to this count.
        - modified_fraction: For each modified column, the fraction of preserved
          rows where that column's value differs between before and after.
    """

    # Meta
    before_rows: int
    after_rows: int
    key: List[str]
    execution_tier: str = "polars"

    # Row stats
    row_delta: int = 0
    row_ratio: float = 1.0

    # Key stats
    unique_before: int = 0
    unique_after: int = 0
    preserved: int = 0
    dropped: int = 0
    added: int = 0
    duplicated_after: int = 0

    # Change stats
    unchanged_rows: int = 0
    changed_rows: int = 0

    # Column stats
    columns_added: List[str] = field(default_factory=list)
    columns_removed: List[str] = field(default_factory=list)
    columns_modified: List[str] = field(default_factory=list)
    modified_fraction: Dict[str, float] = field(default_factory=dict)
    nullability_delta: Dict[str, Dict[str, Optional[float]]] = field(default_factory=dict)

    # Samples
    samples_duplicated_keys: List[Any] = field(default_factory=list)
    samples_dropped_keys: List[Any] = field(default_factory=list)
    samples_changed_rows: List[Dict[str, Any]] = field(default_factory=list)

    # Config
    sample_limit: int = 5

    def __repr__(self) -> str:
        delta_sign = "+" if self.row_delta >= 0 else ""
        return (
            f"CompareResult(rows: {self.before_rows:,} → {self.after_rows:,} "
            f"({delta_sign}{self.row_delta:,}), "
            f"keys: preserved={self.preserved:,}, dropped={self.dropped:,}, added={self.added:,}, "
            f"duplicated={self.duplicated_after:,})"
        )

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert to dictionary format matching the MVP schema.

        Returns nested structure with meta, row_stats, key_stats,
        change_stats, column_stats, and samples sections.
        """
        return {
            "meta": {
                "before_rows": self.before_rows,
                "after_rows": self.after_rows,
                "key": self.key,
                "execution_tier": self.execution_tier,
            },
            "row_stats": {
                "delta": self.row_delta,
                "ratio": self.row_ratio,
            },
            "key_stats": {
                "unique_before": self.unique_before,
                "unique_after": self.unique_after,
                "preserved": self.preserved,
                "dropped": self.dropped,
                "added": self.added,
                "duplicated_after": self.duplicated_after,
            },
            "change_stats": {
                "unchanged_rows": self.unchanged_rows,
                "changed_rows": self.changed_rows,
            },
            "column_stats": {
                "added": self.columns_added,
                "removed": self.columns_removed,
                "modified": self.columns_modified,
                "modified_fraction": self.modified_fraction,
                "nullability_delta": self.nullability_delta,
            },
            "samples": {
                "duplicated_keys": self.samples_duplicated_keys,
                "dropped_keys": self.samples_dropped_keys,
                "changed_rows": self.samples_changed_rows,
            },
        }

    def to_json(self, indent: Optional[int] = 2) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict(), indent=indent, default=str)

    def to_llm(self) -> str:
        """
        Token-optimized text format for LLM context.

        Returns human-readable text (not JSON) for consistency with
        other to_llm() methods. Concise but complete.
        """
        lines = []

        # Header
        delta_sign = "+" if self.row_delta >= 0 else ""
        lines.append(f"COMPARE: {self.before_rows:,} → {self.after_rows:,} rows ({delta_sign}{self.row_delta:,})")
        lines.append(f"key: {', '.join(self.key)}")

        # Key stats
        lines.append(f"keys: preserved={self.preserved:,}, dropped={self.dropped:,}, added={self.added:,}")
        if self.duplicated_after > 0:
            lines.append(f"duplicated_keys: {self.duplicated_after:,}")

        # Change stats
        if self.preserved > 0:
            lines.append(f"changes: {self.changed_rows:,} modified, {self.unchanged_rows:,} unchanged")

        # Column changes
        if self.columns_added:
            lines.append(f"columns_added: {', '.join(self.columns_added)}")
        if self.columns_removed:
            lines.append(f"columns_removed: {', '.join(self.columns_removed)}")
        if self.columns_modified:
            mod_parts = []
            for col in self.columns_modified[:5]:
                frac = self.modified_fraction.get(col, 0)
                mod_parts.append(f"{col} ({frac:.1%})")
            if len(self.columns_modified) > 5:
                mod_parts.append(f"... +{len(self.columns_modified) - 5} more")
            lines.append(f"columns_modified: {', '.join(mod_parts)}")

        # Samples (abbreviated)
        if self.samples_dropped_keys:
            sample_str = ", ".join(str(k) for k in self.samples_dropped_keys[:3])
            lines.append(f"sample_dropped_keys: [{sample_str}]")
        if self.samples_duplicated_keys:
            sample_str = ", ".join(str(k) for k in self.samples_duplicated_keys[:3])
            lines.append(f"sample_duplicated_keys: [{sample_str}]")

        return "\n".join(lines)


@dataclass
class RelationshipProfile:
    """
    Result of profiling the structural relationship between two datasets.

    Answers: "What is the shape of this join?"

    Does NOT answer: which join type to use, or whether the join is correct.

    All measurements are deterministic and factual. Interpretation
    belongs to the consumer (agent or human).

    Attributes:
        # Meta
        on: Column(s) used as join key
        left_rows: Number of rows in left dataset
        right_rows: Number of rows in right dataset
        execution_tier: Which execution tier computed the result

        # Key stats - left
        left_null_rate: Fraction of rows with NULL in join key
        left_unique_keys: Count of unique key values
        left_duplicate_keys: Number of keys appearing >1x

        # Key stats - right
        right_null_rate: Fraction of rows with NULL in join key
        right_unique_keys: Count of unique key values
        right_duplicate_keys: Number of keys appearing >1x

        # Cardinality (rows per key)
        # NOTE: min/max hides distribution shape. A single pathological key
        # with max=1000 looks like "many-to-many" even if 99.9% of keys have
        # 1 row. Acceptable for MVP since samples are present and we don't label.
        left_key_multiplicity_min: Minimum rows per key (left)
        left_key_multiplicity_max: Maximum rows per key (left)
        right_key_multiplicity_min: Minimum rows per key (right)
        right_key_multiplicity_max: Maximum rows per key (right)

        # Coverage
        left_keys_with_match: Keys in left that exist in right
        left_keys_without_match: Keys in left that don't exist in right
        right_keys_with_match: Keys in right that exist in left
        right_keys_without_match: Keys in right that don't exist in left

        # Samples (bounded, explanatory only)
        samples_left_unmatched: Sample keys in left without match
        samples_right_unmatched: Sample keys in right without match
        samples_right_duplicates: Sample keys with >1 row in right

        # Config
        sample_limit: Maximum samples per category
    """

    # Meta
    on: List[str]
    left_rows: int
    right_rows: int
    execution_tier: str = "polars"

    # Key stats - left
    left_null_rate: float = 0.0
    left_unique_keys: int = 0
    left_duplicate_keys: int = 0

    # Key stats - right
    right_null_rate: float = 0.0
    right_unique_keys: int = 0
    right_duplicate_keys: int = 0

    # Cardinality
    left_key_multiplicity_min: int = 0
    left_key_multiplicity_max: int = 0
    right_key_multiplicity_min: int = 0
    right_key_multiplicity_max: int = 0

    # Coverage
    left_keys_with_match: int = 0
    left_keys_without_match: int = 0
    right_keys_with_match: int = 0
    right_keys_without_match: int = 0

    # Samples
    samples_left_unmatched: List[Any] = field(default_factory=list)
    samples_right_unmatched: List[Any] = field(default_factory=list)
    samples_right_duplicates: List[Any] = field(default_factory=list)

    # Config
    sample_limit: int = 5

    def __repr__(self) -> str:
        return (
            f"RelationshipProfile(on={self.on}, "
            f"left={self.left_rows:,} rows/{self.left_unique_keys:,} keys, "
            f"right={self.right_rows:,} rows/{self.right_unique_keys:,} keys, "
            f"coverage: left={self.left_keys_with_match:,}/{self.left_unique_keys:,}, "
            f"right={self.right_keys_with_match:,}/{self.right_unique_keys:,})"
        )

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert to dictionary format matching the MVP schema.

        Returns nested structure with meta, key_stats, cardinality,
        coverage, and samples sections.
        """
        return {
            "meta": {
                "on": self.on,
                "left_rows": self.left_rows,
                "right_rows": self.right_rows,
                "execution_tier": self.execution_tier,
            },
            "key_stats": {
                "left": {
                    "null_rate": self.left_null_rate,
                    "unique_keys": self.left_unique_keys,
                    "duplicate_keys": self.left_duplicate_keys,
                    "rows": self.left_rows,
                },
                "right": {
                    "null_rate": self.right_null_rate,
                    "unique_keys": self.right_unique_keys,
                    "duplicate_keys": self.right_duplicate_keys,
                    "rows": self.right_rows,
                },
            },
            "cardinality": {
                "left_key_multiplicity": {
                    "min": self.left_key_multiplicity_min,
                    "max": self.left_key_multiplicity_max,
                },
                "right_key_multiplicity": {
                    "min": self.right_key_multiplicity_min,
                    "max": self.right_key_multiplicity_max,
                },
            },
            "coverage": {
                "left_keys_with_match": self.left_keys_with_match,
                "left_keys_without_match": self.left_keys_without_match,
                "right_keys_with_match": self.right_keys_with_match,
                "right_keys_without_match": self.right_keys_without_match,
            },
            "samples": {
                "left_keys_without_match": self.samples_left_unmatched,
                "right_keys_without_match": self.samples_right_unmatched,
                "right_keys_with_multiple_rows": self.samples_right_duplicates,
            },
        }

    def to_json(self, indent: Optional[int] = 2) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict(), indent=indent, default=str)

    def to_llm(self) -> str:
        """
        Token-optimized text format for LLM context.

        Returns human-readable text (not JSON) for consistency with
        other to_llm() methods. Concise but complete.
        """
        lines = []

        # Header
        lines.append(f"RELATIONSHIP: {', '.join(self.on)}")
        lines.append(f"left: {self.left_rows:,} rows, {self.left_unique_keys:,} unique keys")
        lines.append(f"right: {self.right_rows:,} rows, {self.right_unique_keys:,} unique keys")

        # Coverage
        left_match_pct = (self.left_keys_with_match / self.left_unique_keys * 100) if self.left_unique_keys > 0 else 0
        right_match_pct = (self.right_keys_with_match / self.right_unique_keys * 100) if self.right_unique_keys > 0 else 0
        lines.append(f"coverage: left={left_match_pct:.1f}%, right={right_match_pct:.1f}%")

        # Duplicates
        if self.left_duplicate_keys > 0 or self.right_duplicate_keys > 0:
            lines.append(f"duplicates: left={self.left_duplicate_keys:,}, right={self.right_duplicate_keys:,}")

        # Null rates
        if self.left_null_rate > 0 or self.right_null_rate > 0:
            lines.append(f"null_rates: left={self.left_null_rate:.1%}, right={self.right_null_rate:.1%}")

        # Cardinality hints
        lines.append(
            f"multiplicity: left=[{self.left_key_multiplicity_min}-{self.left_key_multiplicity_max}], "
            f"right=[{self.right_key_multiplicity_min}-{self.right_key_multiplicity_max}]"
        )

        # Samples (abbreviated)
        if self.samples_left_unmatched:
            sample_str = ", ".join(str(k) for k in self.samples_left_unmatched[:3])
            lines.append(f"sample_left_unmatched: [{sample_str}]")
        if self.samples_right_unmatched:
            sample_str = ", ".join(str(k) for k in self.samples_right_unmatched[:3])
            lines.append(f"sample_right_unmatched: [{sample_str}]")

        return "\n".join(lines)
