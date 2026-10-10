"""Regression tests for SQL literal quoting / param coercion in to_sql_filter.

These cover the defect class where temporal or string rule params were
interpolated raw into SQL. An unquoted ISO date such as 2024-01-01 is not a
date literal in SQL — DuckDB evaluates it as integer arithmetic (2022) and
Postgres raises a type error, so range bounds, allowed/disallowed values, and
conditional-range bounds must be quoted via sql_ir.lit_value.
"""

import datetime as dt

import duckdb
import polars as pl
import pytest

from kontra.rule_defs.builtin.allowed_values import AllowedValuesRule
from kontra.rule_defs.builtin.conditional_range import ConditionalRangeRule
from kontra.rule_defs.builtin.disallowed_values import DisallowedValuesRule
from kontra.rule_defs.builtin.length import LengthRule
from kontra.rule_defs.builtin.range import RangeRule


def _rows(con, sql):
    return con.execute(sql).fetchall()


# ---------------------------------------------------------------------------
# Bug 1: range.to_sql_filter must quote temporal bounds
# ---------------------------------------------------------------------------


def test_range_sql_filter_quotes_iso_string_bounds():
    r = RangeRule(name="r", params={"column": "d", "min": "2024-01-01"})
    assert r.to_sql_filter("duckdb") == '"d" < \'2024-01-01\' OR "d" IS NULL'


def test_range_sql_filter_quotes_native_date_and_datetime():
    r = RangeRule(
        name="r",
        params={
            "column": "d",
            "min": dt.date(2024, 1, 1),
            "max": dt.datetime(2024, 12, 31, 10, 0, 0),
        },
    )
    f = r.to_sql_filter("duckdb")
    # yaml.safe_load turns an unquoted `min: 2024-01-01` into a datetime.date,
    # so native date objects must be quoted too, and a datetime must not
    # produce a syntax error like `"d" > 2024-12-31 10:00:00`.
    assert "'2024-01-01'" in f
    assert "'2024-12-31 10:00:00'" in f
    assert f"'{dt.datetime(2024, 12, 31, 10, 0, 0)}'" in f


def test_range_mixed_date_and_datetime_bounds_do_not_crash():
    """min as a date and max as a datetime (a natural YAML combo) must pass
    the min <= max check — Python raises TypeError comparing them directly."""
    r = RangeRule(
        name="r",
        params={"column": "d", "min": dt.date(2024, 1, 1),
                "max": dt.datetime(2024, 12, 31, 23, 59, 59)},
    )
    assert r.params["min"] == dt.date(2024, 1, 1)

    # Reversed mix must also be accepted (datetime min, date max).
    r2 = RangeRule(
        name="r2",
        params={"column": "d", "min": dt.datetime(2024, 1, 1, 0, 0, 0),
                "max": dt.date(2024, 12, 31)},
    )
    assert r2.params["max"] == dt.date(2024, 12, 31)

    # Genuine min > max violations are still rejected.
    from kontra.errors import RuleParameterError
    with pytest.raises(RuleParameterError, match="must be <= max"):
        RangeRule(
            name="r3",
            params={"column": "d", "min": dt.datetime(2025, 1, 1),
                    "max": dt.date(2024, 12, 31)},
        )


def test_range_sql_filter_leaves_numeric_bounds_bare():
    r = RangeRule(name="r", params={"column": "age", "min": 0, "max": 120})
    assert r.to_sql_filter("duckdb") == '"age" < 0 OR "age" > 120 OR "age" IS NULL'


def test_range_sql_filter_executes_correctly_on_duckdb():
    """The quoted filter must select the same rows the rule considers failures:
    out-of-range dates plus NULLs."""
    r = RangeRule(name="r", params={"column": "d", "min": "2024-01-01"})
    f = r.to_sql_filter("duckdb")
    con = duckdb.connect()
    con.execute(
        "CREATE TABLE t AS SELECT * FROM (VALUES "
        "(DATE '2023-06-01'), (DATE '2024-06-01'), (NULL)) v(d)"
    )
    # Failures: the 2023 row (below min) and the NULL row.
    assert len(_rows(con, f"SELECT * FROM t WHERE {f}")) == 2


def test_range_sql_filter_numeric_still_executes_on_duckdb():
    r = RangeRule(name="r", params={"column": "n", "min": 1, "max": 3})
    f = r.to_sql_filter("duckdb")
    con = duckdb.connect()
    con.execute(
        "CREATE TABLE t AS SELECT * FROM (VALUES (0), (2), (4), (NULL)) v(n)"
    )
    # Failures: 0, 4, NULL.
    assert len(_rows(con, f"SELECT * FROM t WHERE {f}")) == 3


# ---------------------------------------------------------------------------
# Bug 1 (same class): allowed_values / disallowed_values temporal members
# ---------------------------------------------------------------------------


def test_allowed_values_sql_filter_quotes_date_values():
    r = AllowedValuesRule(name="av", params={"column": "d", "values": [dt.date(2024, 1, 1)]})
    f = r.to_sql_filter("duckdb")
    assert "'2024-01-01'" in f
    con = duckdb.connect()
    con.execute(
        "CREATE TABLE t AS SELECT * FROM (VALUES "
        "(DATE '2024-01-01'), (DATE '2024-02-02')) v(d)"
    )
    # NULL is not allowed, so the 2024-02-02 row is the only failure.
    assert len(_rows(con, f"SELECT * FROM t WHERE {f}")) == 1


def test_disallowed_values_sql_filter_quotes_date_values():
    r = DisallowedValuesRule(name="dv", params={"column": "d", "values": [dt.date(2024, 1, 1)]})
    f = r.to_sql_filter("duckdb")
    assert "'2024-01-01'" in f
    con = duckdb.connect()
    con.execute(
        "CREATE TABLE t AS SELECT * FROM (VALUES "
        "(DATE '2024-01-01'), (DATE '2024-02-02')) v(d)"
    )
    # Failure = value IS in the disallowed list.
    assert len(_rows(con, f"SELECT * FROM t WHERE {f}")) == 1


# ---------------------------------------------------------------------------
# Bug 2: conditional_range min/max coercion
# ---------------------------------------------------------------------------


def test_conditional_range_temporal_string_bound_on_date_column():
    """A valid config — ISO-string min on a Date column — must validate, not
    raise a raw polars error."""
    r = ConditionalRangeRule(
        name="cr",
        params={"column": "d", "when": "t == 'x'", "min": "2024-01-01"},
    )
    df = pl.DataFrame(
        {"d": [dt.date(2023, 1, 1), dt.date(2025, 1, 1)], "t": ["x", "x"]}
    )
    res = r.validate(df)
    assert res["failed_count"] == 1  # only the 2023 row is below min
    assert "'2024-01-01'" in r.to_sql_filter("duckdb")


def test_conditional_range_numeric_string_bounds_coerced_at_construction():
    """YAML may pass min/max as strings; '5' > '10' lexicographically must not
    reject a valid contract, and the coerced values must compare numerically."""
    r = ConditionalRangeRule(
        name="cr",
        params={"column": "v", "when": "t > 0", "min": "5", "max": "10"},
    )
    assert r._min_val == 5 and isinstance(r._min_val, int)
    assert r._max_val == 10 and isinstance(r._max_val, int)
    df = pl.DataFrame({"v": [1, 7, 20], "t": [1, 1, 1]})
    res = r.validate(df)
    assert res["failed_count"] == 2  # 1 below min, 20 above max


def test_conditional_range_incompatible_bound_is_rule_failure_not_crash():
    """A date-string min on a numeric column is a misconfiguration: it must
    surface as a rule failure with a clear message, not a raw ComputeError."""
    r = ConditionalRangeRule(
        name="cr",
        params={"column": "v", "when": "t > 0", "min": "2024-01-01"},
    )
    df = pl.DataFrame({"v": [1], "t": [2]})
    res = r.validate(df)
    assert res["passed"] is False
    assert "Rule execution failed" in res["message"]


def test_conditional_range_sql_filter_quotes_temporal_bounds():
    r = ConditionalRangeRule(
        name="cr",
        params={"column": "d", "when": "t == 'x'", "min": dt.date(2024, 1, 1)},
    )
    f = r.to_sql_filter("duckdb")
    assert "'2024-01-01'" in f


# ---------------------------------------------------------------------------
# Bug 3: length.to_sql_filter must match the executor's dialect renderers
# ---------------------------------------------------------------------------


def test_length_sql_filter_sqlserver_counts_trailing_spaces():
    r = LengthRule(name="l", params={"column": "s", "min": 4})
    # Plain LEN() ignores trailing spaces ('x  ' -> 1); the executor uses
    # LEN(x + N'.') - 1 so sampling must too.
    assert r.to_sql_filter("mssql") == '"s" IS NULL OR (LEN("s" + N\'.\') - 1) < 4'
    assert r.to_sql_filter("sqlserver") == r.to_sql_filter("mssql")


def test_length_sql_filter_clickhouse_counts_characters():
    r = LengthRule(name="l", params={"column": "s", "min": 4})
    assert "lengthUTF8(" in r.to_sql_filter("clickhouse")


def test_length_sql_filter_default_dialect_uses_length():
    r = LengthRule(name="l", params={"column": "s", "min": 4})
    assert r.to_sql_filter("postgres") == '"s" IS NULL OR LENGTH("s") < 4'


def test_length_sql_filter_executes_same_as_polars_on_trailing_spaces():
    """The SQL Server LEN trick must actually count trailing spaces, matching
    Polars str.len_chars().

    With max=3, 'ab  ' has true length 4 (a failure) but plain LEN() reports 2
    (would wrongly pass). 'abc' (length 3) is a pass and NULL is a failure.
    """
    r = LengthRule(name="l", params={"column": "s", "max": 3})
    con = duckdb.connect()
    con.execute(
        "CREATE TABLE t AS SELECT * FROM (VALUES ('ab  '), ('abc'), (NULL)) v(s)"
    )
    f = r.to_sql_filter("duckdb")
    # Failures: the 4-char row (trailing spaces count) and NULL.
    assert len(_rows(con, f"SELECT * FROM t WHERE {f}")) == 2


# ---------------------------------------------------------------------------
# Same class: length bounds coerced from strings
# ---------------------------------------------------------------------------


def test_length_string_bounds_coerced_and_lexicographic_rejected():
    r = LengthRule(name="l", params={"column": "s", "min": "3", "max": "10"})
    assert r._min_len == 3 and isinstance(r._min_len, int)
    assert r._max_len == 10 and isinstance(r._max_len, int)

    # "5" > "10" lexicographically would have raised before the fix.
    ok = LengthRule(name="l", params={"column": "s", "min": "5", "max": "10"})
    assert ok._min_len == 5 and ok._max_len == 10

    with pytest.raises(ValueError, match="must be an integer"):
        LengthRule(name="l", params={"column": "s", "min": "abc"})


def test_range_string_bounds_still_coerced_after_refactor():
    """Guard the shared _coerce_numeric_param signature change: existing
    behavior for RangeRule is unchanged."""
    r = RangeRule(name="r", params={"column": "age", "min": "18", "max": "120"})
    assert r.params["min"] == 18 and r.params["max"] == 120

    # Existing error semantics preserved.
    from kontra.errors import RuleParameterError
    with pytest.raises(RuleParameterError, match="min .* must be <= max"):
        RangeRule(name="r", params={"column": "age", "min": 10, "max": 5})