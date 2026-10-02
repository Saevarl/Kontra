"""
Trino SQL that writes out where Trino and Polars differ (trino_sql module docstring):

* floats: ``range`` and ``compare`` with NaN, infinities, -0.0 and extreme
  values, ``range`` bounds on a ``real`` column, and ``unique`` in the fused
  scan and as a GROUP BY;
* ``char(n)``: the string rules on the padded value;
* ``freshness`` on naive timestamps and dates, on a caller's connection
  whose session time zone isn't UTC.

Each rule must be answered by SQL and agree with the Polars tier: pass/fail
in both modes, counts in tally.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

import kontra
from kontra.engine.executors import trino_sql

pytestmark = pytest.mark.integration

SCHEMA = "kontra_semantics"


def connect(**kwargs):
    import trino

    kwargs.setdefault("timezone", "UTC")
    return trino.dbapi.connect(host="localhost", port=8095, user="kontra", **kwargs)


def run_sql(*statements: str) -> None:
    with connect() as conn:
        cur = conn.cursor()
        for sql in statements:
            cur.execute(sql)
            cur.fetchall()


def _by_id(result) -> dict:
    return {r.rule_id: r for r in result.rules}


def _uri(fq: str) -> str:
    catalog, schema, table = fq.split(".")
    return f"trino://kontra@localhost:8095/{catalog}/{schema}.{table}"


def _assert_pushed_and_equal(source, rules, tally, **kwargs):
    """Every rule answered by SQL, with the Polars tier's answer."""
    rules = [dict(r, tally=tally) for r in rules]
    on = _by_id(
        kontra.validate(source, rules=rules, tally=tally, save=False, preplan="off", **kwargs)
    )
    off = _by_id(
        kontra.validate(
            source, rules=rules, tally=tally, save=False, preplan="off", pushdown="off", **kwargs
        )
    )
    assert set(on) == set(off)
    for rid in on:
        assert on[rid].source == "sql", rid
        assert off[rid].source == "polars", rid
        assert on[rid].passed == off[rid].passed, rid
        if tally:
            assert on[rid].failed_count == off[rid].failed_count, rid
        elif not on[rid].passed:
            assert on[rid].failed_count >= 1, rid
    return on


# --------------------------------------------------------------------------- #
# Floats
# --------------------------------------------------------------------------- #

_FLOAT_ROWS = [
    # d double, e double, r real
    ("nan()", "nan()", "nan()"),
    ("nan()", "1.0e0", "1.0e0"),
    ("0.0e0", "-0.0e0", "0.1e0"),
    ("-0.0e0", "0.0e0", "-0.0e0"),
    ("2.5e0", "2.5e0", "2.5e0"),
    ("2.5e0", "nan()", "0.1e0"),
    ("infinity()", "-infinity()", "infinity()"),
    ("-infinity()", "1.7976931348623157e308", "-infinity()"),
    ("1.7976931348623157e308", "4.9e-324", "16777216e0"),
    ("4.9e-324", "NULL", "NULL"),
    ("NULL", "2.5e0", "NULL"),
    ("1.0e0", "2.0e0", "16777216e0"),
]

_FLOAT_RULES = [
    {"name": "range", "id": "d_min", "params": {"column": "d", "min": 0}},
    {"name": "range", "id": "d_max", "params": {"column": "d", "max": 2.5}},
    {"name": "range", "id": "d_both", "params": {"column": "d", "min": 0, "max": 2.5}},
    {"name": "range", "id": "d_wide", "params": {"column": "d", "min": -1.5, "max": 1e308}},
    {"name": "range", "id": "d_zero", "params": {"column": "d", "min": 0, "max": 0}},
    # A real column against 0.1: Polars casts the bound to Float32.
    {"name": "range", "id": "r_min", "params": {"column": "r", "min": 0.1}},
    {"name": "range", "id": "r_max", "params": {"column": "r", "max": 0.1}},
    {"name": "range", "id": "r_both", "params": {"column": "r", "min": 0.1, "max": 16777216}},
    {"name": "range", "id": "r_int", "params": {"column": "r", "max": 16777217}},
    *(
        {"name": "compare", "id": f"de_{name}", "params": {"left": "d", "right": "e", "op": op}}
        for name, op in [
            ("eq", "=="),
            ("ne", "!="),
            ("lt", "<"),
            ("le", "<="),
            ("gt", ">"),
            ("ge", ">="),
        ]
    ),
    {"name": "compare", "id": "dr_lt", "params": {"left": "d", "right": "r", "op": "<"}},
    {"name": "compare", "id": "rr_eq", "params": {"left": "r", "right": "r", "op": "=="}},
    {"name": "unique", "params": {"column": "d"}},
    {"name": "unique", "params": {"column": "e"}},
    {"name": "unique", "params": {"column": "r"}},
]


@pytest.fixture(params=["memory", "iceberg", "iceberg_jdbc"])
def float_table(request, trino_container):
    fq = f"{request.param}.{SCHEMA}.floats"
    values = ", ".join(
        f"(CAST({d} AS double), CAST({e} AS double), CAST({r} AS real))" for d, e, r in _FLOAT_ROWS
    )
    run_sql(
        f"CREATE SCHEMA IF NOT EXISTS {request.param}.{SCHEMA}",
        f"DROP TABLE IF EXISTS {fq}",
        f"CREATE TABLE {fq} (d double, e double, r real)",
        f"INSERT INTO {fq} VALUES {values}",
    )
    yield fq
    run_sql(f"DROP TABLE IF EXISTS {fq}")


@pytest.mark.parametrize("tally", [True, False], ids=["tally", "fail_fast"])
@pytest.mark.parametrize("unique_form", ["scan", "group_by"])
def test_float_rules_match_polars(float_table, tally, unique_form, monkeypatch):
    if unique_form == "group_by":
        monkeypatch.setattr(trino_sql, "_UNIQUE_IN_SCAN_BELOW", 0)
    on = _assert_pushed_and_equal(_uri(float_table), _FLOAT_RULES, tally)
    if tally:
        # The cases that differ without the written-out forms (study M7).
        assert on["d_min"].failed_count == 4  # NULL, NaN x2, -inf; -0.0 is not < 0
        assert on["r_min"].failed_count == 5  # 0.1f is not < 0.1 once both are Float32
        assert on["de_eq"].failed_count == 8  # NaN = NaN and -0.0 = 0.0 hold
        assert on["de_lt"].failed_count == 9  # 2.5 < NaN holds
        assert on["COL:d:unique"].failed_count == 3  # NaN, 0.0 = -0.0, 2.5


# --------------------------------------------------------------------------- #
# char(n)
# --------------------------------------------------------------------------- #

_CHAR_RULES = [
    {"name": "allowed_values", "id": "in_ab", "params": {"column": "c", "values": ["ab"]}},
    {
        "name": "allowed_values",
        "id": "in_ab_pad",
        "params": {"column": "c", "values": ["ab ", None]},
    },
    {"name": "disallowed_values", "id": "not_ab_pad", "params": {"column": "c", "values": ["ab "]}},
    {"name": "disallowed_values", "id": "not_ab", "params": {"column": "c", "values": ["ab"]}},
    {"name": "length", "id": "len3", "params": {"column": "c", "min": 3, "max": 3}},
    {"name": "length", "id": "len_max2", "params": {"column": "c", "max": 2}},
    {"name": "regex", "id": "rx_exact", "params": {"column": "c", "pattern": "^ab$"}},
    {"name": "regex", "id": "rx_pad", "params": {"column": "c", "pattern": "b $"}},
    {"name": "regex", "id": "rx_dot3", "params": {"column": "c", "pattern": "^...$"}},
    {"name": "contains", "id": "has_space", "params": {"column": "c", "substring": " "}},
    {"name": "contains", "id": "has_us", "params": {"column": "c", "substring": "_"}},
    {"name": "starts_with", "id": "starts_a", "params": {"column": "c", "prefix": "a"}},
    {"name": "ends_with", "id": "ends_space", "params": {"column": "c", "suffix": " "}},
    {"name": "ends_with", "id": "ends_b", "params": {"column": "c", "suffix": "b"}},
]


@pytest.fixture
def char_table(trino_container):
    # Iceberg has no char type; the memory catalog keeps it.
    fq = f"memory.{SCHEMA}.chars"
    run_sql(
        f"CREATE SCHEMA IF NOT EXISTS memory.{SCHEMA}",
        f"DROP TABLE IF EXISTS {fq}",
        f"CREATE TABLE {fq} (c char(3))",
        f"INSERT INTO {fq} VALUES ('ab'), ('a'), ('abc'), (NULL), ('a_b'), ('ü'), (''), "
        "('b b'), ('ab ')",
    )
    yield fq
    run_sql(f"DROP TABLE IF EXISTS {fq}")


@pytest.mark.parametrize("tally", [True, False], ids=["tally", "fail_fast"])
def test_char_rules_match_polars(char_table, tally):
    on = _assert_pushed_and_equal(_uri(char_table), _CHAR_RULES, tally)
    if tally:
        # Trino's char drops the padding in casts and regexp_like; these
        # counts are those of the padded values Polars sees.
        assert on["in_ab"].failed_count == 9
        assert on["in_ab_pad"].failed_count == 6
        assert on["rx_pad"].failed_count == 7
        assert on["len3"].failed_count == 1


# --------------------------------------------------------------------------- #
# Freshness on naive values, caller's connection
# --------------------------------------------------------------------------- #


@pytest.fixture
def naive_table(trino_container):
    """Naive timestamps and a date, written in UTC wall-clock time."""
    now = datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)
    fq = f"memory.{SCHEMA}.naive"
    fresh, stale, day = (
        now - timedelta(hours=1),
        now - timedelta(hours=3),
        now.date() - timedelta(2),
    )
    run_sql(
        f"CREATE SCHEMA IF NOT EXISTS memory.{SCHEMA}",
        f"DROP TABLE IF EXISTS {fq}",
        f"CREATE TABLE {fq} (fresh timestamp(6), stale timestamp(6), day date)",
        f"INSERT INTO {fq} VALUES (TIMESTAMP '{fresh}', TIMESTAMP '{stale}', DATE '{day}')",
    )
    # The date's age in UTC: 48 h plus the time since midnight.
    day_age = int((now - datetime.combine(day, datetime.min.time())).total_seconds())
    yield fq, day_age
    run_sql(f"DROP TABLE IF EXISTS {fq}")


@pytest.mark.parametrize("zone", ["UTC", "America/Los_Angeles", "Asia/Kolkata"])
def test_naive_freshness_on_callers_connection(naive_table, zone):
    fq, day_age = naive_table
    rules = [
        {"name": "freshness", "id": "fresh_2h", "params": {"column": "fresh", "max_age": "2h"}},
        {"name": "freshness", "id": "stale_2h", "params": {"column": "stale", "max_age": "2h"}},
        {
            "name": "freshness",
            "id": "day_fresh",
            "params": {"column": "day", "max_age": f"{day_age + 3 * 3600}s"},
        },
        {
            "name": "freshness",
            "id": "day_stale",
            "params": {"column": "day", "max_age": f"{day_age - 3 * 3600}s"},
        },
    ]
    conn = connect(timezone=zone)
    try:
        on = _assert_pushed_and_equal(conn, rules, True, table=fq)
    finally:
        conn.close()
    # Comparing with the session's local "now" moves each answer by the zone's
    # offset: -7 h in Los Angeles, +5:30 in Kolkata, past these 3 h margins.
    assert {rid: r.passed for rid, r in on.items()} == {
        "fresh_2h": True,
        "stale_2h": False,
        "day_fresh": True,
        "day_stale": False,
    }
