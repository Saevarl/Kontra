# src/kontra/scout/types.py
"""
Data types for Kontra Scout profiling results.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class NumericStats:
    """Statistics for numeric columns."""

    min: Optional[float] = None
    max: Optional[float] = None
    mean: Optional[float] = None
    median: Optional[float] = None
    std: Optional[float] = None
    percentiles: Dict[str, float] = field(default_factory=dict)  # {"p25": ..., "p50": ..., ...}

    def to_dict(self) -> Dict[str, Any]:
        result = {
            "min": self.min,
            "max": self.max,
            "mean": self.mean,
            "median": self.median,
            "std": self.std,
        }
        # Only include percentiles if computed (interrogate preset)
        if self.percentiles:
            result["percentiles"] = self.percentiles
        return result


@dataclass
class StringStats:
    """Statistics for string columns."""

    min_length: Optional[int] = None
    max_length: Optional[int] = None
    avg_length: Optional[float] = None
    empty_count: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "min_length": self.min_length,
            "max_length": self.max_length,
            "avg_length": self.avg_length,
            "empty_count": self.empty_count,
        }


@dataclass
class TemporalStats:
    """Statistics for date/datetime columns."""

    date_min: Optional[str] = None  # ISO format
    date_max: Optional[str] = None  # ISO format

    def to_dict(self) -> Dict[str, Any]:
        return {
            "date_min": self.date_min,
            "date_max": self.date_max,
        }


@dataclass
class TopValue:
    """A frequently occurring value with its count."""

    value: Any
    count: int
    pct: float  # Percentage of total rows

    def to_dict(self) -> Dict[str, Any]:
        return {
            "value": self.value,
            "count": self.count,
            "pct": round(self.pct, 2),
        }


@dataclass
class ColumnProfile:
    """Complete profile for a single column."""

    name: str
    dtype: str  # Normalized: string/int/float/bool/date/datetime/binary/unknown
    dtype_raw: str  # Original DuckDB/Polars type string

    # Counts
    row_count: int = 0
    null_count: int = 0
    null_rate: float = 0.0  # null_count / row_count
    distinct_count: int = 0
    uniqueness_ratio: float = 0.0  # distinct / non_null_count

    # Cardinality analysis
    is_low_cardinality: bool = False
    values: Optional[List[Any]] = None  # All values if low cardinality
    top_values: List[TopValue] = field(default_factory=list)

    # Type-specific stats
    numeric: Optional[NumericStats] = None
    string: Optional[StringStats] = None
    temporal: Optional[TemporalStats] = None

    # Pattern detection (optional)
    detected_patterns: List[str] = field(default_factory=list)

    # Semantic type inference
    semantic_type: Optional[str] = None  # identifier/category/measure/timestamp

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        d: Dict[str, Any] = {
            "name": self.name,
            "dtype": self.dtype,
            "dtype_raw": self.dtype_raw,
            "counts": {
                "rows": self.row_count,
                "nulls": self.null_count,
                "null_rate": round(self.null_rate, 4),
                "distinct": self.distinct_count,
                "uniqueness_ratio": round(self.uniqueness_ratio, 4),
            },
            "cardinality": {
                "is_low": self.is_low_cardinality,
                "values": self.values,
                "top_values": [tv.to_dict() for tv in self.top_values],
            },
        }

        if self.numeric:
            d["numeric_stats"] = self.numeric.to_dict()
        if self.string:
            d["string_stats"] = self.string.to_dict()
        if self.temporal:
            d["temporal_stats"] = self.temporal.to_dict()
        if self.detected_patterns:
            d["patterns"] = self.detected_patterns
        if self.semantic_type:
            d["semantic_type"] = self.semantic_type

        return d


@dataclass
class DatasetProfile:
    """Complete profile for a dataset."""

    # Metadata
    source_uri: str
    source_format: str  # "parquet", "csv"
    profiled_at: str  # ISO timestamp
    engine_version: str
    preset: str = "scan"  # "scout", "scan", "interrogate"

    # Dataset-level stats
    row_count: int = 0
    column_count: int = 0
    estimated_size_bytes: Optional[int] = None

    # Sampling info
    sampled: bool = False
    sample_size: Optional[int] = None

    # Columns
    columns: List[ColumnProfile] = field(default_factory=list)

    # Timing
    profile_duration_ms: int = 0

    def __repr__(self) -> str:
        """Human-readable representation for notebooks/REPL."""
        from kontra.connectors.handle import mask_credentials

        lines = [f"DatasetProfile({mask_credentials(self.source_uri)})"]
        lines.append(f"  Preset: {self.preset}")
        lines.append(f"  Rows: {self.row_count:,} | Columns: {self.column_count}")
        if self.sampled:
            lines.append(f"  Sampled: {self.sample_size:,} rows")
        lines.append(f"  Duration: {self.profile_duration_ms}ms")
        lines.append(f"  Columns:")
        for col in self.columns[:10]:
            # Build concise column summary
            parts = [col.dtype]
            if col.null_rate > 0:
                parts.append(f"{col.null_rate:.0%} null")
            if col.distinct_count is not None and col.distinct_count > 0:
                parts.append(f"{col.distinct_count:,} distinct")
            if col.semantic_type:
                parts.append(f"[{col.semantic_type}]")
            lines.append(f"    - {col.name}: {', '.join(parts)}")
        if len(self.columns) > 10:
            lines.append(f"    ... and {len(self.columns) - 10} more columns")
        return "\n".join(lines)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "schema_version": "1.0",
            "source_uri": self.source_uri,
            "source_format": self.source_format,
            "profiled_at": self.profiled_at,
            "engine_version": self.engine_version,
            "preset": self.preset,
            "dataset": {
                "row_count": self.row_count,
                "column_count": self.column_count,
                "estimated_size_bytes": self.estimated_size_bytes,
                "sampled": self.sampled,
                "sample_size": self.sample_size,
            },
            "columns": [c.to_dict() for c in self.columns],
            "profile_duration_ms": self.profile_duration_ms,
        }

    def get_column(self, name: str) -> Optional[ColumnProfile]:
        """Get a column profile by name."""
        for col in self.columns:
            if col.name == name:
                return col
        return None

    def to_json(self, indent: int = 2) -> str:
        """Serialize to JSON string."""
        import json
        return json.dumps(self.to_dict(), indent=indent, default=str)

    def to_llm(self) -> str:
        """Token-optimized format for LLM context."""
        from kontra.connectors.handle import mask_credentials

        lines = []
        lines.append(f"PROFILE: {mask_credentials(self.source_uri)}")
        lines.append(f"rows={self.row_count:,} cols={self.column_count}")
        if self.sampled:
            lines.append(f"(sampled: {self.sample_size:,} rows)")

        # Check if this is a metadata-only preset (scout/lite)
        is_metadata_only = self.preset in ("scout", "lite")

        lines.append("")
        lines.append("COLUMNS:")
        for col in self.columns[:20]:  # Limit to 20 columns
            # Include semantic type tag if detected
            type_tag = f" [{col.semantic_type}]" if col.semantic_type else ""
            parts = [f"  {col.name} ({col.dtype}){type_tag}"]
            if col.null_count > 0:
                parts.append(f"nulls={col.null_count:,} ({col.null_rate:.1%})")
            # Only show distinct_count if actually computed (not 0 in metadata-only mode)
            if col.distinct_count > 0 or not is_metadata_only:
                parts.append(f"distinct={col.distinct_count:,}")
            if col.numeric:
                if col.numeric.min is not None and col.numeric.max is not None:
                    parts.append(f"range=[{col.numeric.min}, {col.numeric.max}]")
                # Include percentiles if computed (interrogate preset)
                if col.numeric.percentiles:
                    pct_str = " ".join(f"p{k.replace('p','')}={v}" for k, v in sorted(col.numeric.percentiles.items()))
                    parts.append(pct_str)
            # Skip top values for unique/identifier columns (every value appears once - not useful)
            if col.top_values and col.uniqueness_ratio < 0.99:
                top = col.top_values[0]
                parts.append(f"top='{top.value}'({top.count:,})")
            lines.append(" ".join(parts))

        if len(self.columns) > 20:
            lines.append(f"  ... +{len(self.columns) - 20} more columns")

        return "\n".join(lines)

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "DatasetProfile":
        """Create from dictionary."""
        ds = d.get("dataset", {})
        cols_data = d.get("columns", [])

        columns = []
        for c in cols_data:
            counts = c.get("counts", {})
            card = c.get("cardinality", {})

            # Parse top values
            top_values = []
            for tv in card.get("top_values", []):
                top_values.append(TopValue(
                    value=tv.get("value"),
                    count=tv.get("count", 0),
                    pct=tv.get("pct", 0.0),
                ))

            # Parse type-specific stats
            numeric = None
            if "numeric_stats" in c:
                ns = c["numeric_stats"]
                numeric = NumericStats(
                    min=ns.get("min"),
                    max=ns.get("max"),
                    mean=ns.get("mean"),
                    median=ns.get("median"),
                    std=ns.get("std"),
                    percentiles=ns.get("percentiles", {}),
                )

            string = None
            if "string_stats" in c:
                ss = c["string_stats"]
                string = StringStats(
                    min_length=ss.get("min_length"),
                    max_length=ss.get("max_length"),
                    avg_length=ss.get("avg_length"),
                    empty_count=ss.get("empty_count", 0),
                )

            temporal = None
            if "temporal_stats" in c:
                ts = c["temporal_stats"]
                temporal = TemporalStats(
                    date_min=ts.get("date_min"),
                    date_max=ts.get("date_max"),
                )

            columns.append(ColumnProfile(
                name=c.get("name", ""),
                dtype=c.get("dtype", "unknown"),
                dtype_raw=c.get("dtype_raw", ""),
                row_count=counts.get("rows", 0),
                null_count=counts.get("nulls", 0),
                null_rate=counts.get("null_rate", 0.0),
                distinct_count=counts.get("distinct", 0),
                uniqueness_ratio=counts.get("uniqueness_ratio", 0.0),
                is_low_cardinality=card.get("is_low", False),
                values=card.get("values"),
                top_values=top_values,
                numeric=numeric,
                string=string,
                temporal=temporal,
                detected_patterns=c.get("patterns", []),
                semantic_type=c.get("semantic_type"),
            ))

        return cls(
            source_uri=d.get("source_uri", ""),
            source_format=d.get("source_format", ""),
            profiled_at=d.get("profiled_at", ""),
            engine_version=d.get("engine_version", ""),
            preset=d.get("preset", "scan"),
            row_count=ds.get("row_count", 0),
            column_count=ds.get("column_count", 0),
            estimated_size_bytes=ds.get("estimated_size_bytes"),
            sampled=ds.get("sampled", False),
            sample_size=ds.get("sample_size"),
            columns=columns,
            profile_duration_ms=d.get("profile_duration_ms", 0),
        )


@dataclass
class ProfileState:
    """
    Persistent state for a scout profile.

    Similar to ValidationState, enables tracking profile changes over time.
    """

    # Identity
    source_fingerprint: str  # Hash of source URI
    source_uri: str

    # Timing
    profiled_at: str  # ISO timestamp

    # The actual profile
    profile: DatasetProfile

    # Metadata
    schema_version: str = "1.0"
    engine_version: str = ""

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "schema_version": self.schema_version,
            "engine_version": self.engine_version,
            "source_fingerprint": self.source_fingerprint,
            "source_uri": self.source_uri,
            "profiled_at": self.profiled_at,
            "profile": self.profile.to_dict(),
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "ProfileState":
        """Create from dictionary."""
        return cls(
            schema_version=d.get("schema_version", "1.0"),
            engine_version=d.get("engine_version", ""),
            source_fingerprint=d["source_fingerprint"],
            source_uri=d["source_uri"],
            profiled_at=d["profiled_at"],
            profile=DatasetProfile.from_dict(d["profile"]),
        )

    def to_json(self, indent: int = 2) -> str:
        """Serialize to JSON string."""
        import json
        return json.dumps(self.to_dict(), indent=indent, default=str)

    @classmethod
    def from_json(cls, json_str: str) -> "ProfileState":
        """Deserialize from JSON string."""
        import json
        return cls.from_dict(json.loads(json_str))


@dataclass
class ColumnDiff:
    """Diff for a single column between two profiles."""

    column_name: str
    change_type: str  # "added", "removed", "changed", "unchanged"

    # For changed columns
    null_rate_before: Optional[float] = None
    null_rate_after: Optional[float] = None
    null_rate_delta: Optional[float] = None

    distinct_count_before: Optional[int] = None
    distinct_count_after: Optional[int] = None
    distinct_count_delta: Optional[int] = None

    dtype_before: Optional[str] = None
    dtype_after: Optional[str] = None
    dtype_changed: bool = False

    # Value distribution changes
    new_values: List[Any] = field(default_factory=list)
    removed_values: List[Any] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        d: Dict[str, Any] = {
            "column": self.column_name,
            "change_type": self.change_type,
        }
        if self.change_type == "changed":
            if self.null_rate_delta is not None and abs(self.null_rate_delta) > 0.001:
                d["null_rate"] = {
                    "before": self.null_rate_before,
                    "after": self.null_rate_after,
                    "delta": round(self.null_rate_delta, 4),
                }
            if self.distinct_count_delta is not None and self.distinct_count_delta != 0:
                d["distinct_count"] = {
                    "before": self.distinct_count_before,
                    "after": self.distinct_count_after,
                    "delta": self.distinct_count_delta,
                }
            if self.dtype_changed:
                d["dtype"] = {
                    "before": self.dtype_before,
                    "after": self.dtype_after,
                }
            if self.new_values:
                d["new_values"] = self.new_values[:10]  # Limit
            if self.removed_values:
                d["removed_values"] = self.removed_values[:10]
        return d


@dataclass
class ProfileDiff:
    """Diff between two scout profiles."""

    before: ProfileState
    after: ProfileState

    # Dataset-level changes
    row_count_before: int = 0
    row_count_after: int = 0
    row_count_delta: int = 0
    row_count_pct_change: float = 0.0

    column_count_before: int = 0
    column_count_after: int = 0

    # Column-level changes
    columns_added: List[str] = field(default_factory=list)
    columns_removed: List[str] = field(default_factory=list)
    columns_changed: List[ColumnDiff] = field(default_factory=list)

    # Significant changes summary
    null_rate_increases: List[ColumnDiff] = field(default_factory=list)
    null_rate_decreases: List[ColumnDiff] = field(default_factory=list)
    cardinality_changes: List[ColumnDiff] = field(default_factory=list)
    dtype_changes: List[ColumnDiff] = field(default_factory=list)

    @property
    def has_changes(self) -> bool:
        """Check if there are any meaningful changes."""
        return bool(
            self.columns_added
            or self.columns_removed
            or self.columns_changed
            or abs(self.row_count_delta) > 0
        )

    @property
    def has_schema_changes(self) -> bool:
        """Check if there are schema-level changes."""
        return bool(
            self.columns_added
            or self.columns_removed
            or self.dtype_changes
        )

    @classmethod
    def compute(cls, before: ProfileState, after: ProfileState) -> "ProfileDiff":
        """Compute diff between two profile states."""
        diff = cls(before=before, after=after)

        # Dataset-level
        diff.row_count_before = before.profile.row_count
        diff.row_count_after = after.profile.row_count
        diff.row_count_delta = after.profile.row_count - before.profile.row_count
        if before.profile.row_count > 0:
            diff.row_count_pct_change = (diff.row_count_delta / before.profile.row_count) * 100

        diff.column_count_before = before.profile.column_count
        diff.column_count_after = after.profile.column_count

        # Build column maps
        before_cols = {c.name: c for c in before.profile.columns}
        after_cols = {c.name: c for c in after.profile.columns}

        before_names = set(before_cols.keys())
        after_names = set(after_cols.keys())

        # Added/removed columns
        diff.columns_added = sorted(after_names - before_names)
        diff.columns_removed = sorted(before_names - after_names)

        # Changed columns
        common_cols = before_names & after_names
        for col_name in sorted(common_cols):
            bc = before_cols[col_name]
            ac = after_cols[col_name]

            col_diff = ColumnDiff(
                column_name=col_name,
                change_type="unchanged",
            )

            changed = False

            # Null rate change
            null_delta = ac.null_rate - bc.null_rate
            if abs(null_delta) > 0.001:  # > 0.1% change
                col_diff.null_rate_before = bc.null_rate
                col_diff.null_rate_after = ac.null_rate
                col_diff.null_rate_delta = null_delta
                changed = True

                if null_delta > 0.01:  # > 1% increase
                    diff.null_rate_increases.append(col_diff)
                elif null_delta < -0.01:  # > 1% decrease
                    diff.null_rate_decreases.append(col_diff)

            # Distinct count change
            distinct_delta = ac.distinct_count - bc.distinct_count
            if distinct_delta != 0:
                col_diff.distinct_count_before = bc.distinct_count
                col_diff.distinct_count_after = ac.distinct_count
                col_diff.distinct_count_delta = distinct_delta
                changed = True

                # Significant cardinality change (>10%)
                if bc.distinct_count > 0:
                    pct_change = abs(distinct_delta / bc.distinct_count)
                    if pct_change > 0.1:
                        diff.cardinality_changes.append(col_diff)

            # Dtype change
            if bc.dtype != ac.dtype:
                col_diff.dtype_before = bc.dtype
                col_diff.dtype_after = ac.dtype
                col_diff.dtype_changed = True
                changed = True
                diff.dtype_changes.append(col_diff)

            # Value distribution changes (if low cardinality)
            if bc.values and ac.values:
                before_vals = set(bc.values) if bc.values else set()
                after_vals = set(ac.values) if ac.values else set()
                col_diff.new_values = list(after_vals - before_vals)
                col_diff.removed_values = list(before_vals - after_vals)
                if col_diff.new_values or col_diff.removed_values:
                    changed = True

            if changed:
                col_diff.change_type = "changed"
                diff.columns_changed.append(col_diff)

        return diff

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "before": {
                "source_uri": self.before.source_uri,
                "profiled_at": self.before.profiled_at,
                "row_count": self.row_count_before,
                "column_count": self.column_count_before,
            },
            "after": {
                "source_uri": self.after.source_uri,
                "profiled_at": self.after.profiled_at,
                "row_count": self.row_count_after,
                "column_count": self.column_count_after,
            },
            "changes": {
                "row_count_delta": self.row_count_delta,
                "row_count_pct_change": round(self.row_count_pct_change, 2),
                "columns_added": self.columns_added,
                "columns_removed": self.columns_removed,
                "columns_changed": [c.to_dict() for c in self.columns_changed],
            },
            "significant": {
                "null_rate_increases": [c.column_name for c in self.null_rate_increases],
                "null_rate_decreases": [c.column_name for c in self.null_rate_decreases],
                "cardinality_changes": [c.column_name for c in self.cardinality_changes],
                "dtype_changes": [c.column_name for c in self.dtype_changes],
            },
        }

    def to_json(self, indent: int = 2) -> str:
        """Serialize to JSON."""
        import json
        return json.dumps(self.to_dict(), indent=indent, default=str)

    def to_llm(self) -> str:
        """Render diff in token-optimized format for LLM context."""
        from kontra.connectors.handle import mask_credentials

        lines = []

        # Header
        lines.append(f"# Profile Diff: {mask_credentials(self.after.source_uri)}")
        lines.append(f"comparing: {self.before.profiled_at[:10]} → {self.after.profiled_at[:10]}")

        # Row count
        if self.row_count_delta != 0:
            sign = "+" if self.row_count_delta > 0 else ""
            lines.append(f"rows: {self.row_count_before:,} → {self.row_count_after:,} ({sign}{self.row_count_delta:,}, {self.row_count_pct_change:+.1f}%)")
        else:
            lines.append(f"rows: {self.row_count_after:,} (unchanged)")

        # Schema changes
        if self.columns_added:
            lines.append(f"\n## Columns Added ({len(self.columns_added)})")
            for col in self.columns_added[:10]:
                lines.append(f"- {col}")

        if self.columns_removed:
            lines.append(f"\n## Columns Removed ({len(self.columns_removed)})")
            for col in self.columns_removed[:10]:
                lines.append(f"- {col}")

        # Significant changes
        if self.dtype_changes:
            lines.append(f"\n## Type Changes ({len(self.dtype_changes)})")
            for cd in self.dtype_changes[:10]:
                lines.append(f"- {cd.column_name}: {cd.dtype_before} → {cd.dtype_after}")

        if self.null_rate_increases:
            lines.append(f"\n## Null Rate Increases ({len(self.null_rate_increases)})")
            for cd in self.null_rate_increases[:10]:
                lines.append(f"- {cd.column_name}: {cd.null_rate_before:.1%} → {cd.null_rate_after:.1%}")

        if self.cardinality_changes:
            lines.append(f"\n## Cardinality Changes ({len(self.cardinality_changes)})")
            for cd in self.cardinality_changes[:10]:
                sign = "+" if cd.distinct_count_delta > 0 else ""
                lines.append(f"- {cd.column_name}: {cd.distinct_count_before:,} → {cd.distinct_count_after:,} ({sign}{cd.distinct_count_delta:,})")

        # Other column changes
        other_changes = [c for c in self.columns_changed if c not in self.dtype_changes and c not in self.null_rate_increases and c not in self.cardinality_changes]
        if other_changes:
            lines.append(f"\n## Other Changes ({len(other_changes)})")
            for cd in other_changes[:10]:
                parts = [cd.column_name]
                if cd.new_values:
                    parts.append(f"+{len(cd.new_values)} values")
                if cd.removed_values:
                    parts.append(f"-{len(cd.removed_values)} values")
                lines.append(f"- {' | '.join(parts)}")

        if not self.has_changes:
            lines.append("\n✓ No significant changes detected")

        lines.append(f"\nfingerprint: {self.after.source_fingerprint}")
        return "\n".join(lines)
