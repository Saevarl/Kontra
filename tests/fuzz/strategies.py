"""Hypothesis strategies for random datasets and random rules that fit them.

The frame has a fixed set of columns, one per type family, so every rule can
find a column of the right type. Values lean towards edge cases: NULLs, NaN,
infinities, -0.0, integer limits, empty strings, whitespace, non-ASCII text
and the characters that matter to LIKE and regex (``%``, ``_``, ``\\``, ``.``).
Rule parameters are drawn from the same pools, so bounds and literals often
land exactly on values that are in the data.
"""

from __future__ import annotations

from datetime import date, datetime

import polars as pl
from hypothesis import strategies as st

INTS = [0, 1, -1, 2, 3, 7, 100, 2**31 - 1, -(2**31), 2**53 + 1, 2**63 - 1, -(2**63)]
FLOATS = [0.0, -0.0, 0.5, 1.0, -1.0, 2.5, 0.1 + 0.2, 0.3, 1e-300, 1e300,
          float("inf"), float("-inf"), float("nan")]
# Characters chosen to hit case folding, multi-byte encodings, LIKE wildcards,
# regex metacharacters and SQL quoting.
CHARS = ["a", "b", "A", "é", "ß", "日", "🎉", " ", "\t", "%", "_", "\\", ".", "'", '"', "0"]
# Naive on purpose: the column is a timestamp without time zone.
DATETIMES = [datetime(*parts) for parts in [  # noqa: DTZ001
    (2000, 1, 1), (2000, 1, 1, 0, 0, 1), (1970, 1, 1), (2024, 2, 29, 23, 59, 59), (2999, 12, 31),
]]
DATES = [d.date() for d in DATETIMES] + [date(1900, 1, 1)]

COLUMNS = {
    "i": pl.Int64,
    "j": pl.Int64,
    "f": pl.Float64,
    "s": pl.Utf8,
    "c": pl.Utf8,  # low-cardinality strings, for allowed_values and when clauses
    "b": pl.Boolean,
    "t": pl.Datetime("us"),
    "d": pl.Date,
}
NUMERIC = ["i", "j", "f"]
STRINGS = ["s", "c"]
ORDERED = NUMERIC + STRINGS + ["t", "d"]

ints = st.one_of(st.sampled_from(INTS), st.integers(-5, 5))
floats = st.one_of(st.sampled_from(FLOATS), st.integers(-5, 5).map(float))
strings = st.lists(st.sampled_from(CHARS), max_size=4).map("".join)
categories = st.sampled_from(["x", "y", "z", "", "X", " x"])

VALUES = {
    "i": ints,
    "j": ints,
    "f": floats,
    "s": strings,
    "c": categories,
    "b": st.booleans(),
    "t": st.sampled_from(DATETIMES),
    "d": st.sampled_from(DATES),
}


def nullable(values: st.SearchStrategy) -> st.SearchStrategy:
    return st.one_of(st.none(), values, values)


@st.composite
def frames(draw, max_rows: int = 30) -> pl.DataFrame:
    n = draw(st.integers(0, max_rows))
    data = {}
    for name, dtype in COLUMNS.items():
        # Some columns never hold NULLs, so metadata can prove not_null.
        values = nullable(VALUES[name]) if draw(st.booleans()) else VALUES[name]
        data[name] = pl.Series(name, draw(st.lists(values, min_size=n, max_size=n)), dtype=dtype)
    return pl.DataFrame(data)


def _literal(draw, column: str):
    return draw(VALUES[column])


def _bounds(draw, column: str):
    lo = draw(st.one_of(st.none(), VALUES[column]))
    hi = draw(st.one_of(st.none(), VALUES[column]))
    if lo is None and hi is None:
        lo = draw(VALUES[column])
    if lo is not None and hi is not None and lo > hi:
        lo, hi = hi, lo
    return lo, hi


def _when(draw) -> str:
    column = draw(st.sampled_from(["i", "c", "b", "f"]))
    if column == "b":
        return f"b {draw(st.sampled_from(['==', '!=']))} {draw(st.sampled_from(['true', 'false']))}"
    op = draw(st.sampled_from(["==", "!=", ">", ">=", "<", "<="]))
    if column == "c":
        return f"c {op} '{draw(st.sampled_from(['x', 'y', 'X']))}'"
    if column == "f":
        return f"f {op} {draw(st.sampled_from(['0', '1.0', '-1', '0.5']))}"
    return f"i {op} {draw(st.sampled_from([0, 1, -1, 3]))}"


REGEXES = ["^a", "a$", "^$", ".", "^[a-z]+$", "\\d", "é", "^.{2}$", "^\\s", "%", "\\.",
           "^[^a]*$", "a|b", "^(a|é)+$", "[A-Z]"]


@st.composite
def rules(draw) -> dict:
    """One rule dict, as built by ``kontra.rules``."""
    from kontra import rules as r

    kind = draw(st.sampled_from([
        "not_null", "unique", "range", "allowed_values", "disallowed_values", "regex",
        "length", "contains", "starts_with", "ends_with", "compare", "conditional_not_null",
        "conditional_range", "min_rows", "max_rows", "dtype", "freshness",
    ]))
    if kind == "not_null":
        column = draw(st.sampled_from(list(COLUMNS)))
        return r.not_null(column, include_nan=column == "f" and draw(st.booleans()))
    if kind == "unique":
        return r.unique(draw(st.sampled_from(list(COLUMNS))))
    if kind == "range":
        column = draw(st.sampled_from(NUMERIC))
        lo, hi = _bounds(draw, column)
        return r.range(column, min=lo, max=hi)
    if kind in ("allowed_values", "disallowed_values"):
        column = draw(st.sampled_from(list(COLUMNS)))
        values = draw(st.lists(st.one_of(VALUES[column], st.none()), min_size=1, max_size=3))
        return getattr(r, kind)(column, values)
    if kind == "regex":
        return r.regex(draw(st.sampled_from(STRINGS)), draw(st.sampled_from(REGEXES)))
    if kind == "length":
        lo = draw(st.one_of(st.none(), st.integers(0, 4)))
        hi = draw(st.one_of(st.none(), st.integers(0, 4)))
        if lo is None and hi is None:
            lo = 1
        if lo is not None and hi is not None and lo > hi:
            lo, hi = hi, lo
        return r.length(draw(st.sampled_from(STRINGS)), min=lo, max=hi)
    if kind in ("contains", "starts_with", "ends_with"):
        needle = draw(st.lists(st.sampled_from(CHARS), min_size=1, max_size=2).map("".join))
        return getattr(r, kind)(draw(st.sampled_from(STRINGS)), needle)
    if kind == "compare":
        left, right = draw(st.sampled_from([("i", "j"), ("i", "f"), ("s", "c"), ("t", "d")]))
        return r.compare(left, right, draw(st.sampled_from([">", ">=", "<", "<=", "==", "!="])))
    if kind == "conditional_not_null":
        return r.conditional_not_null(draw(st.sampled_from(list(COLUMNS))), _when(draw))
    if kind == "conditional_range":
        column = draw(st.sampled_from(NUMERIC))
        lo, hi = _bounds(draw, column)
        return r.conditional_range(column, _when(draw), min=lo, max=hi)
    if kind in ("min_rows", "max_rows"):
        return getattr(r, kind)(draw(st.integers(0, 32)))
    if kind == "dtype":
        return r.dtype(draw(st.sampled_from(list(COLUMNS))), draw(st.sampled_from([
            "int64", "int32", "int", "numeric", "float64", "float", "string", "bool",
            "datetime", "date",
        ])))
    if kind == "freshness":
        # The data's timestamps are decades old or centuries ahead, so the
        # verdict doesn't depend on when the test runs.
        return r.freshness(draw(st.sampled_from(["t", "d"])), draw(st.sampled_from(["1d", "30d"])))
    raise AssertionError(kind)


@st.composite
def rule_sets(draw, max_rules: int = 6) -> list[dict]:
    chosen = draw(st.lists(rules(), min_size=1, max_size=max_rules))
    # Give every rule its own id so two rules of one kind on one column don't collide.
    return [{**rule, "id": f"r{k}"} for k, rule in enumerate(chosen)]


# Column names that need quoting in SQL. Only columns that no `when` clause uses
# are renamed: the condition parser takes plain identifiers only.
ODD_NAMES = ["my col", 'we"ird', "it's", "br]ack[et", "select", "a.b", "Ünï", "x`y",
             "back\\slash", "%_", "order by", "-1"]
RENAMEABLE = ["s", "j", "t", "d"]


@st.composite
def renames(draw) -> dict[str, str]:
    """A mapping from some of the plain column names to odd ones (may be empty)."""
    chosen = draw(st.lists(st.sampled_from(RENAMEABLE), unique=True, max_size=len(RENAMEABLE)))
    names = draw(st.lists(st.sampled_from(ODD_NAMES), min_size=len(chosen),
                          max_size=len(chosen), unique=True))
    return dict(zip(chosen, names, strict=True))


def rename(df: pl.DataFrame, rules: list[dict], mapping: dict[str, str]):
    """Apply a column mapping to a frame and to the rules that name its columns."""
    if not mapping:
        return df, rules
    out = []
    for rule in rules:
        params = {k: mapping.get(v, v) if k in ("column", "left", "right") else v
                  for k, v in rule["params"].items()}
        out.append({**rule, "params": params})
    return df.rename(mapping), out
