# src/kontra/rule_defs/value_lists.py
"""
Whether a value list can be compared with a column (allowed_values, disallowed_values).

Polars' ``is_in`` refuses a list whose values are of another kind than the
column: text on a number column, numbers or text on a boolean or date
column, a date on a datetime column. SQL databases cast or refuse each of
these in their own way, so the same rule used to pass on one route and fail
with an internal error on another. Such a list is a config error on every
route: the Polars tier reports it, and the SQL and metadata tiers leave the
rule to Polars, as they do for a missing column. Ints and floats compare
with every number column.

Column kinds: "integer", "float", "decimal", "text", "boolean", "date",
"datetime", "time". Other columns (binary, nested, Enum, Null, ...) have no
kind and their lists are not checked.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Iterable
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import polars as pl

# The kind of value each column kind compares with.
_ACCEPTS = {
    "integer": "number",
    "float": "number",
    "decimal": "number",
    "text": "text",
    "boolean": "boolean",
    "date": "date",
    "datetime": "datetime",
    "time": "time",
}

_COLUMN_WORDS = {
    "integer": "an integer",
    "float": "a float",
    "decimal": "a decimal",
    "text": "text",
    "boolean": "a boolean",
    "date": "a date",
    "datetime": "a datetime",
    "time": "a time",
}

# value kind -> (one value, several values)
_VALUE_WORDS = {
    "number": ("a number", "numbers"),
    "text": ("text", "text"),
    "boolean": ("a boolean", "booleans"),
    "date": ("a date", "dates"),
    "datetime": ("a datetime", "datetimes"),
    "time": ("a time", "times"),
}


def _value_kind(value: Any) -> str | None:
    """The kind of a list literal; None for literals that aren't checked."""
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, (int, float)):
        return "number"
    if isinstance(value, str):
        return "text"
    if isinstance(value, dt.datetime):
        return "datetime"
    if isinstance(value, dt.date):
        return "date"
    if isinstance(value, dt.time):
        return "time"
    return None


def unfit_values(values: Iterable[Any], column_kind: str | None) -> list[Any]:
    """The values a column of this kind can't be compared with (none for an unknown kind)."""
    want = _ACCEPTS.get(column_kind or "")
    if want is None:
        return []
    values = list(values)
    kinds = [_value_kind(v) for v in values]
    # Beside a number, Polars builds the list as numbers and True/False as
    # 1/0, so [1, 2, True] compares with a number column.
    fits = {want, "boolean"} if want == "number" and "number" in kinds else {want}
    return [v for v, kind in zip(values, kinds, strict=True) if kind is not None and kind not in fits]


def polars_kind(dtype: pl.DataType) -> str | None:
    """The column kind of a Polars dtype."""
    import polars as pl

    if dtype.is_integer():
        return "integer"
    if dtype.is_float():
        return "float"
    if dtype.is_decimal():
        return "decimal"
    if dtype == pl.String or dtype == pl.Categorical:
        return "text"
    if dtype == pl.Boolean:
        return "boolean"
    if dtype == pl.Date:
        return "date"
    if isinstance(dtype, pl.Datetime):
        return "datetime"
    if dtype == pl.Time:
        return "time"
    return None


def unfit_message(column: str, column_kind: str, unfit: list[Any]) -> str:
    """E.g. "Values '1', '2' are text but column 'i' is an integer"."""
    kinds = sorted({_value_kind(v) for v in unfit})
    one = len(unfit) == 1
    what = " and ".join(_VALUE_WORDS[k][0 if one else 1] for k in kinds)
    shown = ", ".join(repr(v) if isinstance(v, str) else str(v) for v in unfit[:5])
    if len(unfit) > 5:
        shown += f", ... ({len(unfit)} total)"
    subject = f"Value {shown} is" if one else f"Values {shown} are"
    return f"{subject} {what} but column '{column}' is {_COLUMN_WORDS[column_kind]}"


def is_in(series: pl.Series, values: Iterable[Any]) -> pl.Series:
    """``series.is_in(values)``, with the list in the column's type when it is a float.

    Polars compares a Float32 column with a list of Python floats as Float64,
    so a REAL 0.1 is not in [0.1]; SQL casts the literal to the column's type,
    and so does this. Mixed ints and floats build one list (Polars would
    take its type from the first value and refuse the rest).
    """
    import polars as pl

    present = [v for v in values if v is not None]
    if series.dtype.is_float():
        literal = pl.Series(present, dtype=series.dtype, strict=False)
    else:
        literal = pl.Series(present, strict=False)
    return series.is_in(literal.implode())
