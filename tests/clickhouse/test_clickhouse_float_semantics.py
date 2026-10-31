"""Live ClickHouse float semantics: pushdown must answer as Polars does.

ClickHouse compares NaN as false and keeps -0.0 apart from 0.0 in DISTINCT;
Polars orders NaN above every number (NaN equals NaN) and counts -0.0 as 0.0.
Each case loads a few rows and checks that the pushed result (source "sql")
equals the Polars result (pushdown="off") in tally and fail-fast mode.
"""

from __future__ import annotations

import pytest

import kontra

R = kontra.rules
OPS = ["<", "<=", ">", ">=", "==", "!="]
_TABLE = "ch_float_semantics"
_URI = f"clickhouse://kontra:kontra_test@localhost:8123/kontra_test/{_TABLE}"
_COLUMNS = ("id", "f", "g", "h", "lc", "i", "s", "d")


@pytest.fixture(scope="module")
def table(_require_clickhouse):
    import clickhouse_connect

    c = clickhouse_connect.get_client(
        host="localhost", port=8123, username="kontra",
        password="kontra_test", database="kontra_test",
    )
    c.command(f"DROP TABLE IF EXISTS {_TABLE}")
    c.command(
        f"CREATE TABLE {_TABLE} ("
        "id UInt32, f Nullable(Float64), g Nullable(Float64), h Nullable(Float32), "
        "lc LowCardinality(Nullable(Float64)), i Nullable(Int64), s Nullable(String), "
        "d Nullable(Decimal(10, 2))"
        ") ENGINE = MergeTree ORDER BY id",
        settings={"allow_suspicious_low_cardinality_types": 1},
    )
    yield c
    c.command(f"DROP TABLE IF EXISTS {_TABLE}")


def _load(c, rows: list[dict]) -> None:
    """Replace the table's rows; a column a row leaves out is NULL."""
    c.command(f"TRUNCATE TABLE {_TABLE}")
    values = ", ".join(
        "(" + ", ".join([str(n)] + [row.get(col, "NULL") for col in _COLUMNS[1:]]) + ")"
        for n, row in enumerate(rows, start=1)
    )
    c.command(f"INSERT INTO {_TABLE} VALUES {values}")


def _assert_agree(rules: list[dict], source: str = "sql") -> None:
    """Pushdown equals Polars for every rule; the pushed rules ran on ``source``."""
    for tally in (True, False):
        pushed = kontra.validate(_URI, rules=rules, tally=tally, save=False)
        polars = kontra.validate(_URI, rules=rules, tally=tally, save=False, pushdown="off")
        expected = {r.rule_id: r for r in polars.rules}
        for r in pushed.rules:
            want = expected[r.rule_id]
            context = (r.rule_id, tally)
            assert r.source == source, context
            assert r.passed == want.passed, context
            if tally:
                assert r.failed_count == want.failed_count, context


def _compare_rules(left: str, right: str) -> list[dict]:
    return [R.compare(left, right, op, id=f"{left}{op}{right}") for op in OPS]


# (left, right) values, as ClickHouse literals.
FLOAT_PAIRS = [
    ("nan", "nan"),
    ("nan", "1"),
    ("1", "nan"),
    ("inf", "nan"),
    ("nan", "-inf"),
    ("-inf", "1"),
    ("-0.", "0."),
    ("0.", "-0."),
    ("1", "1"),
    ("NULL", "1"),
    ("nan", "NULL"),
]


@pytest.mark.parametrize("left,right", FLOAT_PAIRS)
def test_compare_float_columns(table, left, right):
    _load(table, [{"f": left, "g": right}])
    _assert_agree(_compare_rules("f", "g"))


@pytest.mark.parametrize("i,f", [("1", "nan"), ("1", "inf"), ("-5", "-inf"), ("0", "-0."), ("NULL", "nan")])
def test_compare_integer_with_float(table, i, f):
    # The reported case: compare("i", "f", "<") with i=1, f=NaN passes in Polars.
    _load(table, [{"i": i, "f": f}])
    _assert_agree(_compare_rules("i", "f") + _compare_rules("f", "i"))


def test_compare_integer_rounds_as_polars(table):
    # Polars compares Int64 with Float64 in Float64: 2^53 + 1 rounds to 2^53.
    _load(table, [{"i": str(2**53 + 1), "f": str(float(2**53))}])
    _assert_agree(_compare_rules("i", "f"))


def test_compare_float32_with_float64(table):
    # Float32(0.1) widens to 0.100000001..., which is not 0.1.
    _load(table, [{"h": "0.1", "f": "0.1"}, {"h": "nan", "f": "nan"}, {"h": "-0.", "f": "0."}])
    _assert_agree(_compare_rules("h", "f"))


def test_compare_float_with_decimal_left_to_polars(table):
    _load(table, [{"f": "nan", "d": "1.5"}, {"f": "1.5", "d": "1.5"}])
    _assert_agree(_compare_rules("f", "d"), source="polars")


@pytest.mark.parametrize("value", ["nan", "-0.", "0.", "inf", "-inf", "1", "-1", "NULL"])
@pytest.mark.parametrize("column", ["f", "lc"])
def test_conditional_not_null_when_on_float(table, value, column):
    # The reported case: when "f > 0" applies to f=NaN in Polars.
    _load(table, [{column: value}])
    _assert_agree(
        [R.conditional_not_null("s", f"{column} {op} 0", id=f"w{op}") for op in OPS]
    )


@pytest.mark.parametrize("value", ["nan", "-0.", "inf", "1", "NULL"])
def test_conditional_range_when_on_float(table, value):
    _load(table, [{"f": value, "i": "-1"}])
    _assert_agree(
        [R.conditional_range("i", f"f {op} 0", min=0, id=f"w{op}") for op in OPS]
    )


def test_when_literal_cast_to_float32(table):
    # Polars casts the literal to the column's type: Float32(0.1) == 0.1, and
    # 16777217 becomes Float32 16777216.
    _load(table, [{"h": "0.1"}, {"h": "16777216"}])
    rules = [R.conditional_not_null("s", f"h {op} 0.1", id=f"a{op}") for op in OPS]
    rules += [R.conditional_not_null("s", f"h {op} 16777217", id=f"b{op}") for op in OPS]
    _assert_agree(rules)


def test_conditional_range_float_target(table):
    # Polars' conditional_range only compares: NaN is out of range above a max,
    # not below a min.
    _load(table, [{"f": "nan", "h": "nan", "i": "1"}, {"f": "-0.", "h": "0.1", "i": "1"}])
    _assert_agree([
        R.conditional_range("f", "i == 1", min=0, id="f_min"),
        R.conditional_range("f", "i == 1", max=1, id="f_max"),
        R.conditional_range("h", "i == 1", min=0.1, id="h_min"),
        R.conditional_range("h", "i == 1", max=0.1, id="h_max"),
        R.conditional_range("f", "h > 0", min=0, max=1, id="both_float"),
    ])


def test_range_on_float(table):
    # NaN is out of any range; bounds compare as the column's type.
    _load(table, [{"f": "nan", "h": "0.1"}, {"f": "-0.", "h": "nan"}, {"f": "inf", "h": "-inf"}])
    _assert_agree([
        R.range("f", min=0, id="f_min"),
        R.range("f", max=1, id="f_max"),
        R.range("f", min=-1, max=1, id="f_both"),
        R.range("h", min=0.1, id="h_min"),
        R.range("h", max=0.1, id="h_max"),
    ])


@pytest.mark.parametrize(
    "values",
    [
        ["0.", "-0."],
        ["nan", "nan"],
        ["nan", "-nan", "1"],
        ["nan", "0.", "-0.", "NULL", "NULL", "1"],
        ["inf", "-inf", "1", "NULL"],
    ],
    ids=["zeros", "nans", "nan_signs", "mixed", "distinct"],
)
@pytest.mark.parametrize("column", ["f", "h", "lc"])
def test_unique_on_float(table, values, column):
    # The reported case: unique("f") with [0.0, -0.0] fails in Polars.
    _load(table, [{column: v} for v in values])
    _assert_agree([R.unique(column)])


def test_unexact_literals_left_to_polars(table):
    _load(table, [{"f": "nan", "s": "NULL"}, {"f": "1", "s": "'x'"}])
    _assert_agree(
        [
            R.conditional_not_null("s", "f == nan", id="nan_literal"),
            R.conditional_not_null("s", "f > 'a'", id="string_literal"),
            R.range("f", max=2**60, id="huge_bound"),
        ],
        source="polars",
    )


def test_non_float_rules_still_pushed(table):
    _load(table, [{"i": "1", "s": "'x'", "f": "nan"}, {"i": "1", "s": "NULL", "f": "1"}])
    _assert_agree([
        R.unique("i"),
        R.compare("i", "i", "==", id="ii"),
        R.conditional_not_null("s", "i == 1", id="when_int"),
        R.range("i", min=0, id="i_range"),
    ])
