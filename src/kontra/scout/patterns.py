# src/kontra/scout/patterns.py
"""
Pattern detection for common data formats.
"""

from __future__ import annotations

import re
from typing import List


# Common patterns to detect
PATTERNS = {
    "email": re.compile(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"),
    "uuid": re.compile(
        r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
    ),
    "phone_us": re.compile(
        r"^\+?1?[-.\s]?\(?[0-9]{3}\)?[-.\s]?[0-9]{3}[-.\s]?[0-9]{4}$"
    ),
    "phone_intl": re.compile(r"^\+[1-9]\d{6,14}$"),
    "url": re.compile(r"^https?://[^\s]+$"),
    "ipv4": re.compile(r"^(?:[0-9]{1,3}\.){3}[0-9]{1,3}$"),
    "ipv6": re.compile(r"^([0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}$"),
    "iso_date": re.compile(r"^\d{4}-\d{2}-\d{2}$"),
    "iso_datetime": re.compile(r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}"),
    "hex_color": re.compile(r"^#[0-9A-Fa-f]{6}$"),
    "credit_card": re.compile(r"^[0-9]{4}[- ]?[0-9]{4}[- ]?[0-9]{4}[- ]?[0-9]{4}$"),
    "ssn": re.compile(r"^\d{3}-\d{2}-\d{4}$"),
    "zip_us": re.compile(r"^\d{5}(-\d{4})?$"),
    "slug": re.compile(r"^[a-z0-9]+(?:-[a-z0-9]+)*$"),
    "json": re.compile(r"^[\[\{].*[\]\}]$", re.DOTALL),
}


def detect_patterns(sample_values: List[str], threshold: float = 0.8) -> List[str]:
    """
    Detect common patterns in a sample of string values.

    Args:
        sample_values: List of string values to analyze
        threshold: Minimum fraction of values that must match (default: 80%)

    Returns:
        List of pattern names where >= threshold of non-null values match.
    """
    if not sample_values:
        return []

    # Filter out empty strings for pattern matching
    non_empty = [v for v in sample_values if v and v.strip()]
    if not non_empty:
        return []

    matches = []
    for pattern_name, regex in PATTERNS.items():
        match_count = sum(1 for v in non_empty if regex.match(str(v)))
        if match_count / len(non_empty) >= threshold:
            matches.append(pattern_name)

    return matches


def get_pattern_regex(pattern_name: str) -> str:
    """Get the regex pattern string for a pattern name."""
    pattern = PATTERNS.get(pattern_name)
    if pattern:
        return pattern.pattern
    return ""
