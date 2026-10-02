# src/kontra/engine/materializers/trino_decode.py
"""
Build a Polars frame from a Trino result, column by column.

The Trino client turns every value into a Python object (Decimal, datetime)
before Kontra sees it, and a row-wise DataFrame then takes the objects apart
again. Most of the fetch's time went there. Here the cursor hands back
Trino's raw JSON values (``legacy_primitive_types=True``), and each column is
decoded at once by Polars, straight to the declared Polars dtype.

Exact or fall back: the frame must equal the one built from the client's
Python objects. A column whose type has no fast decode, and any chunk whose
values a fast decode doesn't take exactly as they are, go through the client's
own value mapper, the one a default cursor applies. Rows arrive in chunks, so
Python holds the frame plus one chunk, not every row as tuples.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from datetime import timezone
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import polars as pl

from kontra.connectors.trino_types import normalize_type, polars_dtype

# Four-digit years only. Python's datetime, which the client builds, can't
# hold others, so those values keep the client's error.
_DATE = r"^\d{4}-\d{2}-\d{2}"
_SPECIAL_FLOATS = {"NaN": float("nan"), "Infinity": float("inf"), "-Infinity": float("-inf")}


def _precision(trino_type: str) -> int:
    match = re.search(r"\((\d+)\)", trino_type)
    return int(match.group(1)) if match else 3


def _fast_decoder(trino_type: str, dtype: Any) -> Callable[[list], pl.Series] | None:
    """The column decoder for a type, or None when only the client's mapper is exact."""
    import polars as pl

    t = normalize_type(trino_type)
    base = t.split("(", 1)[0].strip()
    if dtype is None or polars_dtype(t) != dtype:
        return None

    def strict(values: list) -> pl.Series:
        return pl.Series(values, dtype=dtype, strict=True)

    def text(values: list) -> pl.Series:
        return pl.Series(values, dtype=pl.Utf8, strict=True)

    if base in {"tinyint", "smallint", "integer", "bigint", "boolean"}:
        return strict
    if base in {"real", "double"}:

        def floats(values: list) -> pl.Series:
            try:
                s = pl.Series(values, dtype=pl.Float64, strict=True)
            except TypeError:
                # NaN and the infinities arrive as strings, mapped as the
                # client's mapper does. Any other string is refused (KeyError).
                s = pl.Series(
                    [_SPECIAL_FLOATS[v] if v.__class__ is str else v for v in values],
                    dtype=pl.Float64,
                    strict=True,
                )
            return s.cast(dtype)

        return floats
    if base in {"varchar", "char", "json"}:
        return strict
    if base == "decimal":
        return lambda values: text(values).cast(dtype, strict=True)
    if base == "varbinary":
        return lambda values: text(values).str.decode("base64", strict=True)
    if base == "date":

        def date(values: list) -> pl.Series:
            s = text(values)
            if not s.str.contains(f"{_DATE}$").all():
                raise ValueError("date outside 0000-9999")
            return s.str.to_date("%Y-%m-%d", strict=True)

        return date
    # Times and timestamps finer than microseconds are rounded by the client,
    # half to even; only exact microseconds are decoded here.
    if base in {"timestamp", "time"} and _precision(t) > 6:
        return None
    fraction = "%.f" if _precision(t) > 0 else ""
    if base == "time" and not t.endswith("with time zone"):
        return lambda values: text(values).str.to_time(f"%H:%M:%S{fraction}", strict=True)
    if base == "timestamp":
        zoned = t.endswith("with time zone")
        pattern = f"{_DATE} " + (r".* UTC$" if zoned else r"[^ ]*$")

        def timestamp(values: list) -> pl.Series:
            s = text(values)
            # A zone other than UTC (a name or an offset) takes the mapper.
            if not s.str.contains(pattern).all():
                raise ValueError("not a UTC timestamp with a four-digit year")
            if zoned:
                s = s.str.strip_suffix(" UTC")
            s = s.str.to_datetime(f"%Y-%m-%d %H:%M:%S{fraction}", time_unit="us", strict=True)
            return s.dt.replace_time_zone("UTC") if zoned else s

        return timestamp
    return None


class FrameDecoder:
    """Decode chunks of raw rows into one frame with the declared dtypes."""

    def __init__(self, names: list[str], types: list[str], dtypes: list[Any], mappers: list[Any]):
        self._names = names
        self._dtypes = dtypes
        self._mappers = mappers
        self._fast = [_fast_decoder(t, d) for t, d in zip(types, dtypes)]
        self._parts: list[dict[str, pl.Series]] = []
        # Columns without a fast decode keep the client's objects until the
        # end, so their dtype is inferred over every row, as before.
        self._objects: dict[int, list] = {i: [] for i, f in enumerate(self._fast) if f is None}
        self.rows = 0
        self.mapped_chunks = 0

    def add(self, rows: list) -> None:
        import polars as pl

        # What a fast decode raises on values it doesn't take as they are.
        refused = (pl.exceptions.PolarsError, TypeError, ValueError, OverflowError, KeyError)
        columns = list(zip(*rows)) if rows else [() for _ in self._names]
        part: dict[str, pl.Series] = {}
        for i, values in enumerate(columns):
            values = list(values)
            if i in self._objects:
                self._objects[i].extend(self._map(i, values))
                continue
            try:
                part[self._names[i]] = self._fast[i](values).alias(self._names[i])
            except refused:
                self.mapped_chunks += 1
                part[self._names[i]] = self._infer(i, self._map(i, values))
        self._parts.append(part)
        self.rows += len(rows)

    def _map(self, i: int, values: list) -> list:
        from trino.exceptions import TrinoDataError

        mapper = self._mappers[i]
        out = []
        for value in values:
            try:
                out.append(mapper.map(value))
            except ValueError as e:
                # The message a default cursor raises for the same value.
                raise TrinoDataError(
                    f"Could not convert '{value}' into the associated python type"
                ) from e
        return out

    def _infer(self, i: int, objects: list) -> pl.Series:
        """The previous decoder for one column: infer over every row, then cast."""
        import polars as pl

        name = self._names[i]
        dtype = self._dtypes[i]
        if isinstance(dtype, pl.Datetime) and dtype.time_zone == "UTC":
            # Values in several zones have no common Polars dtype; the same
            # instants in UTC do. One chunk's zones mustn't decide whether
            # the fetch raises.
            objects = [v if v is None else v.astimezone(timezone.utc) for v in objects]
        s = pl.DataFrame(
            [(v,) for v in objects], schema=[name], orient="row", infer_schema_length=None
        ).to_series()
        return s.cast(dtype) if dtype is not None and s.dtype != dtype else s

    def frame(self) -> pl.DataFrame:
        import polars as pl

        if self.rows == 0:
            return pl.DataFrame(schema={n: d or pl.Utf8 for n, d in zip(self._names, self._dtypes)})
        columns: list[pl.Series] = []
        for i, name in enumerate(self._names):
            if i in self._objects:
                columns.append(self._infer(i, self._objects[i]))
            else:
                parts = [p[name] for p in self._parts if name in p]
                columns.append(pl.concat(parts, rechunk=True))
        return pl.DataFrame(columns)
