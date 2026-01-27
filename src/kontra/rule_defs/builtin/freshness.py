from __future__ import annotations

"""
Freshness Rule - validates that a timestamp column has recent data.

Usage in contract:
  - name: freshness
    params:
      column: updated_at
      max_age: "24h"  # or "1d", "30m", "7d", etc.

Supported time units:
  - s, sec, second(s): seconds
  - m, min, minute(s): minutes
  - h, hr, hour(s): hours
  - d, day(s): days
  - w, week(s): weeks

The rule passes if MAX(column) >= NOW() - max_age.
"""

import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, Set

import polars as pl

from kontra.rule_defs.base import BaseRule
from kontra.rule_defs.predicates import Predicate
from kontra.rule_defs.registry import register_rule
from kontra.state.types import FailureMode


def parse_duration(duration_str: str) -> timedelta:
    """
    Parse a human-readable duration string into a timedelta.

    Examples:
        "24h" -> 24 hours
        "1d" -> 1 day
        "30m" -> 30 minutes
        "7 days" -> 7 days
        "2w" -> 2 weeks
        "1h30m" -> 1 hour 30 minutes
        "2d12h" -> 2 days 12 hours
    """
    duration_str = duration_str.strip().lower()

    # Map unit variations to timedelta kwargs
    unit_map = {
        's': 'seconds', 'sec': 'seconds', 'second': 'seconds', 'seconds': 'seconds',
        'm': 'minutes', 'min': 'minutes', 'minute': 'minutes', 'minutes': 'minutes',
        'h': 'hours', 'hr': 'hours', 'hour': 'hours', 'hours': 'hours',
        'd': 'days', 'day': 'days', 'days': 'days',
        'w': 'weeks', 'week': 'weeks', 'weeks': 'weeks',
    }

    # Find all number+unit pairs (e.g., "1h30m" -> [("1", "h"), ("30", "m")])
    pattern = r'(\d+(?:\.\d+)?)\s*([a-z]+)'
    matches = re.findall(pattern, duration_str)

    if not matches:
        raise ValueError(f"Invalid duration format: '{duration_str}'. Expected format like '24h', '1d', '30m', '1h30m'")

    # Verify that the matches cover the entire string (no unmatched parts)
    reconstructed = ''.join(f'{v}{u}' for v, u in matches)
    # Remove spaces from original for comparison
    original_no_spaces = re.sub(r'\s+', '', duration_str)
    if reconstructed != original_no_spaces:
        raise ValueError(f"Invalid duration format: '{duration_str}'. Expected format like '24h', '1d', '30m', '1h30m'")

    # Accumulate timedelta components
    total = timedelta()
    for value_str, unit in matches:
        value = float(value_str)
        if unit not in unit_map:
            raise ValueError(f"Unknown time unit: '{unit}'. Supported: s, m, h, d, w (or full names)")
        total += timedelta(**{unit_map[unit]: value})

    return total


@register_rule("freshness")
class FreshnessRule(BaseRule):
    """
    Validates that a timestamp column contains recent data.

    The rule checks if the maximum value in the timestamp column
    is within the specified max_age from the current time.

    Parameters:
        column: The timestamp column to check
        max_age: Maximum age allowed (e.g., "24h", "1d", "30m")
    """

    rule_scope = "dataset"
    supports_tally = False

    def __init__(self, name: str, params: Dict[str, Any]):
        super().__init__(name, params)
        self._validate_params()

    def _validate_params(self) -> None:
        if "column" not in self.params:
            raise ValueError("freshness rule requires 'column' parameter")
        if "max_age" not in self.params:
            raise ValueError("freshness rule requires 'max_age' parameter")
        # Validate max_age is parseable
        parse_duration(str(self.params["max_age"]))

    def required_columns(self) -> Set[str]:
        return {self.params["column"]}

    def validate(self, df: pl.DataFrame) -> Dict[str, Any]:
        column = self.params["column"]
        max_age = parse_duration(str(self.params["max_age"]))

        # Check column exists before accessing
        col_check = self._check_columns(df, {column})
        if col_check is not None:
            return col_check

        # Check column dtype is datetime-compatible
        col_dtype = df[column].dtype
        datetime_types = (pl.Datetime, pl.Date)
        is_datetime_type = isinstance(col_dtype, datetime_types) or col_dtype in datetime_types

        if not is_datetime_type:
            # Check if it's a string that might be parseable
            if col_dtype not in (pl.Utf8, getattr(pl, "String", pl.Utf8)):
                return {
                    "rule_id": self.rule_id,
                    "passed": False,
                    "failed_count": df.height,
                    "message": f"Column '{column}' must be a datetime type for freshness check (found {col_dtype})",
                }

        # Get the maximum timestamp
        max_ts = df[column].max()

        if max_ts is None:
            return {
                "rule_id": self.rule_id,
                "passed": False,
                "failed_count": df.height,
                "message": f"Column '{column}' has no non-null timestamps",
            }

        # Convert to datetime if needed
        if isinstance(max_ts, datetime):
            max_datetime = max_ts
        else:
            # Try to handle various timestamp types (including strings)
            try:
                max_datetime = datetime.fromisoformat(str(max_ts).replace('Z', '+00:00'))
            except (ValueError, AttributeError):
                return {
                    "rule_id": self.rule_id,
                    "passed": False,
                    "failed_count": df.height,
                    "message": f"Column '{column}' contains values that cannot be parsed as datetime (got: {type(max_ts).__name__})",
                }

        # Get current time (use UTC for consistency)
        now = datetime.now(timezone.utc)

        # Make max_datetime timezone-aware if it isn't
        if hasattr(max_datetime, 'tzinfo') and max_datetime.tzinfo is None:
            max_datetime = max_datetime.replace(tzinfo=timezone.utc)

        threshold = now - max_age

        # Check if the most recent data is fresh enough
        is_fresh = max_datetime >= threshold

        if is_fresh:
            return {
                "rule_id": self.rule_id,
                "passed": True,
                "failed_count": 0,
                "message": "Passed",
            }
        else:
            age = now - max_datetime
            return {
                "rule_id": self.rule_id,
                "passed": False,
                "failed_count": 1,
                "message": f"Data is stale: most recent record is {_format_timedelta(age)} old (max allowed: {self.params['max_age']})",
                "failure_mode": str(FailureMode.FRESHNESS_LAG),
                "details": {
                    "latest_timestamp": max_datetime.isoformat(),
                    "threshold_timestamp": threshold.isoformat(),
                    "actual_age_seconds": int(age.total_seconds()),
                    "max_age_seconds": int(max_age.total_seconds()),
                    "max_age_spec": str(self.params["max_age"]),
                },
            }

    def compile_predicate(self) -> Optional[Predicate]:
        # Freshness is an aggregate check (MAX), not row-level
        # Cannot be vectorized as a per-row predicate
        return None

    def to_sql_spec(self) -> Optional[Dict[str, Any]]:
        """Generate SQL spec for pushdown execution."""
        column = self.params.get("column")
        max_age = self.params.get("max_age")

        if not (column and max_age):
            return None

        try:
            td = parse_duration(str(max_age))
            # Convert to total seconds for SQL
            total_seconds = int(td.total_seconds())
        except ValueError:
            return None

        return {
            "kind": "freshness",
            "rule_id": self.rule_id,
            "column": column,
            "max_age_seconds": total_seconds,
        }


def _format_timedelta(td: timedelta) -> str:
    """Format a timedelta in human-readable form."""
    total_seconds = int(td.total_seconds())

    if total_seconds < 60:
        return f"{total_seconds}s"
    elif total_seconds < 3600:
        return f"{total_seconds // 60}m"
    elif total_seconds < 86400:
        hours = total_seconds // 3600
        mins = (total_seconds % 3600) // 60
        return f"{hours}h {mins}m" if mins else f"{hours}h"
    else:
        days = total_seconds // 86400
        hours = (total_seconds % 86400) // 3600
        return f"{days}d {hours}h" if hours else f"{days}d"
