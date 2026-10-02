# src/kontra/connectors/trino_types.py
"""
The Polars dtype of each declared Trino type.

One map serves both the preplan ``dtype`` decision and the Trino materializer,
so a ``dtype`` rule gets the same answer from metadata as from the Polars tier
(Architecture: path agreement). It follows what Kontra's Parquet path reads
for the same Iceberg data files: ``integer`` is Int32, ``real`` is Float32,
``decimal(p, s)`` keeps its precision and scale.
"""

from __future__ import annotations

import re
from typing import Any

_INTEGERS = {"tinyint": "Int8", "smallint": "Int16", "integer": "Int32", "bigint": "Int64"}


def normalize_type(data_type: str) -> str:
    """The client describes ``decimal(12, 2)``; information_schema says ``decimal(12,2)``."""
    return re.sub(r",\s+", ",", data_type.strip().lower())


def polars_dtype(trino_type: str | None) -> Any:
    """Polars dtype for a declared Trino type, or None for types without a fixed mapping.

    None means "no claim": preplan leaves the rule to the scan or the Polars
    tier, and the materializer keeps the dtype Polars infers from the values.
    """
    import polars as pl

    t = normalize_type(trino_type or "")
    base = t.split("(", 1)[0].strip()
    if base in _INTEGERS:
        return getattr(pl, _INTEGERS[base])
    if base == "boolean":
        return pl.Boolean
    if base == "real":
        return pl.Float32
    if base == "double":
        return pl.Float64
    if base == "decimal":
        match = re.fullmatch(r"decimal\((\d+)(?:,(\d+))?\)", t)
        if match is None:
            return None
        return pl.Decimal(int(match.group(1)), int(match.group(2) or 0))
    if base in {"varchar", "char", "json"}:
        return pl.Utf8
    if base == "date":
        return pl.Date
    if base == "timestamp":
        # The client returns Python datetimes, which hold microseconds.
        return pl.Datetime("us", "UTC" if t.endswith("with time zone") else None)
    if base == "time" and not t.endswith("with time zone"):
        return pl.Time
    if base == "varbinary":
        return pl.Binary
    return None
