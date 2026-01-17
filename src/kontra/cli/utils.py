"""CLI utility functions."""

from __future__ import annotations

import re


def parse_duration(duration_str: str) -> int:
    """
    Parse a duration string like '7d', '24h', '30m' into seconds.

    Supported formats:
    - Xd: X days
    - Xh: X hours
    - Xm: X minutes
    - Xs: X seconds
    """
    match = re.match(r"^(\d+)([dhms])$", duration_str.lower())
    if not match:
        raise ValueError(
            f"Invalid duration format: {duration_str}. Use '7d', '24h', '30m', or '60s'."
        )

    value = int(match.group(1))
    unit = match.group(2)

    multipliers = {"d": 86400, "h": 3600, "m": 60, "s": 1}
    return value * multipliers[unit]
