# src/kontra/scout/reporters/json_reporter.py
"""
JSON reporter for Kontra Scout - optimized for LLM consumption.
"""

from __future__ import annotations

import json
from typing import Any, Dict

from kontra.scout.types import DatasetProfile


def render_json(profile: DatasetProfile, indent: int = 2) -> str:
    """
    Render a DatasetProfile as JSON.

    Args:
        profile: The DatasetProfile to render
        indent: JSON indentation (default: 2)

    Returns:
        JSON string
    """
    payload = profile.to_dict()
    return json.dumps(payload, indent=indent, default=str, ensure_ascii=False)


def build_compact_json(profile: DatasetProfile) -> Dict[str, Any]:
    """
    Build a compact JSON representation optimized for LLM context.
    Omits null/empty fields for minimal token usage.
    """
    d = profile.to_dict()
    return _strip_nulls(d)


def _strip_nulls(obj: Any) -> Any:
    """Recursively remove None values and empty lists/dicts."""
    if isinstance(obj, dict):
        return {
            k: _strip_nulls(v)
            for k, v in obj.items()
            if v is not None and v != [] and v != {}
        }
    elif isinstance(obj, list):
        return [_strip_nulls(item) for item in obj if item is not None]
    return obj


def render_llm(profile: DatasetProfile) -> str:
    """
    Render a DatasetProfile in token-optimized format for LLM context.

    Design goals:
    - Minimal tokens while preserving signal
    - Easy for LLM to parse and reason about
    - Key info: schema, null rates, cardinality, semantic types
    - Actionable: enough info to infer validation rules

    Format:
    ```
    # Dataset: source_uri
    rows=N cols=N

    ## Columns
    col_name: type | nulls=N% | distinct=N | semantic_type
      values: [val1, val2, ...] or top: val1(N%), val2(N%)
    ```
    """
    lines = []

    # Header
    lines.append(f"# Dataset: {profile.source_uri}")
    lines.append(f"rows={profile.row_count:,} cols={profile.column_count}")
    lines.append("")
    lines.append("## Columns")

    for col in profile.columns:
        # Main column line: name: type | nulls | distinct | semantic
        parts = [col.dtype]

        # Null rate (only if > 0)
        if col.null_rate > 0:
            null_pct = col.null_rate * 100
            if null_pct < 0.1:
                parts.append("nulls=<0.1%")
            else:
                parts.append(f"nulls={null_pct:.1f}%")

        # Distinct count with uniqueness hint
        if col.uniqueness_ratio >= 0.99 and col.distinct_count > 100:
            parts.append(f"distinct={col.distinct_count:,} (unique)")
        elif col.distinct_count <= 20:
            parts.append(f"distinct={col.distinct_count}")
        else:
            parts.append(f"distinct={col.distinct_count:,}")

        # Semantic type
        if col.semantic_type:
            parts.append(col.semantic_type)

        # Pattern detection
        if col.detected_patterns:
            parts.append(f"pattern:{col.detected_patterns[0]}")

        lines.append(f"{col.name}: {' | '.join(parts)}")

        # Values line (if low cardinality or has top values)
        if col.values and col.is_low_cardinality:
            # All values for low cardinality
            vals_str = ", ".join(repr(v) for v in col.values[:10])
            if len(col.values) > 10:
                vals_str += f", ... ({len(col.values)} total)"
            lines.append(f"  values: [{vals_str}]")
        elif col.top_values:
            # Top values with percentages
            top_parts = []
            for tv in col.top_values[:5]:
                val_repr = repr(tv.value) if isinstance(tv.value, str) else str(tv.value)
                top_parts.append(f"{val_repr}({tv.pct:.0f}%)")
            lines.append(f"  top: {', '.join(top_parts)}")

        # Temporal range (useful for freshness rules)
        if col.temporal and (col.temporal.date_min or col.temporal.date_max):
            date_range = f"{col.temporal.date_min or '?'} to {col.temporal.date_max or '?'}"
            lines.append(f"  range: {date_range}")

    # Footer with quick stats
    lines.append("")
    lines.append("## Summary")

    # Count column types
    type_counts: Dict[str, int] = {}
    for col in profile.columns:
        t = col.dtype
        type_counts[t] = type_counts.get(t, 0) + 1
    type_summary = ", ".join(f"{t}:{n}" for t, n in sorted(type_counts.items()))
    lines.append(f"types: {type_summary}")

    # Identify potential issues
    issues = []
    for col in profile.columns:
        if col.null_rate > 0.1:  # >10% nulls
            issues.append(f"{col.name}:{col.null_rate*100:.0f}%null")
    if issues:
        lines.append(f"high_nulls: {', '.join(issues[:5])}")

    # Identify unique columns (likely identifiers)
    unique_cols = [
        col.name for col in profile.columns
        if col.uniqueness_ratio >= 0.99 and col.distinct_count > 100
    ]
    if unique_cols:
        lines.append(f"likely_ids: {', '.join(unique_cols[:5])}")

    # Identify categorical columns
    categorical = [
        col.name for col in profile.columns
        if col.is_low_cardinality or col.semantic_type == "category"
    ]
    if categorical:
        lines.append(f"categorical: {', '.join(categorical[:5])}")

    return "\n".join(lines)
