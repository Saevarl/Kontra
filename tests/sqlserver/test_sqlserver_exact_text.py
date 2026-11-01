# tests/sqlserver/test_sqlserver_exact_text.py
"""
SQL Server pushdown compares text exactly, as Polars does.

The database's default collation (SQL_Latin1_General_CP1_CI_AS) ignores case,
``=`` and ``IN`` ignore trailing spaces, ``LEN`` counts an emoji as two, and
LIKE on VARCHAR and CHAR ignores the value's trailing spaces. Before,
``unique("s")`` on 'abc' and 'ABC' failed in SQL and passed in Polars.

Each case loads a few rows, runs one rule with pushdown on and off, in tally
and fail-fast mode, and checks the answers agree and which tier gave them.
The rows are picked so that the verdict SQL Server's collation gives differs
from Polars'. Ordering text is left to Polars ("polars" cases).

Requires the SQL Server container:
    cd tests/sqlserver && docker compose up -d
"""

from __future__ import annotations

import uuid

import pytest

import kontra
from kontra import rules

_HOST = "127.0.0.1"
_PORT = 1433
_USER = "sa"
_PASSWORD = "Kontra_Test123!"
_DATABASE = "kontra_test"

# CHAR(4)/NCHAR(4) values are padded to 4 in storage and reach Polars padded.
_COLUMNS = {
    "s": "NVARCHAR(50)",
    "s2": "NVARCHAR(50)",
    "v": "VARCHAR(50)",
    "c": "CHAR(4)",
    "nc": "NCHAR(4)",
    "t": "INT",
    "n": "INT",
}

# case -> (rule, rows, tier the pushdown route answers it in)
_CASES = {
    "unique_nvarchar": (rules.unique("s"), [{"s": v} for v in ("abc", "ABC", "x", "x ")], "sql"),
    "unique_varchar": (rules.unique("v"), [{"v": v} for v in ("abc", "ABC", "x", "x ")], "sql"),
    "unique_char": (rules.unique("c"), [{"c": v} for v in ("ab", "AB", "x ")], "sql"),
    "allowed_case_and_space": (
        rules.allowed_values("s", ["abc", "x"]), [{"s": "ABC"}, {"s": "x "}], "sql",
    ),
    "allowed_emoji": (rules.allowed_values("s", ["😀"]), [{"s": "😀"}], "sql"),
    "allowed_char_padding": (rules.allowed_values("c", ["ab"]), [{"c": "ab"}], "sql"),
    "allowed_null_listed": (
        rules.allowed_values("v", ["abc", None]), [{"v": "ABC"}, {"v": None}], "sql",
    ),
    "disallowed_case_and_space": (
        rules.disallowed_values("s", ["ABC", "x "]), [{"s": "abc"}, {"s": "x"}], "sql",
    ),
    "compare_eq": (
        rules.compare("s", "s2", "=="),
        [{"s": "abc", "s2": "ABC"}, {"s": "x", "s2": "x "}],
        "sql",
    ),
    "compare_ne": (
        rules.compare("s", "s2", "!="),
        [{"s": "abc", "s2": "ABC"}, {"s": "x ", "s2": "x"}],
        "sql",
    ),
    "compare_lt_to_polars": (rules.compare("s", "s2", "<"), [{"s": "x", "s2": "x "}], "polars"),
    "when_eq_space": (
        rules.conditional_not_null("t", "s == 'x'"), [{"s": "x ", "t": None}], "sql",
    ),
    "when_ne_case": (
        rules.conditional_range("t", "s != 'ABC'", min=0), [{"s": "abc", "t": -1}], "sql",
    ),
    "when_gt_to_polars": (
        rules.conditional_not_null("t", "s > 'x'"), [{"s": "x ", "t": None}], "polars",
    ),
    # The rules API takes numbers only; a contract can give a date-like string.
    # The collation sorts '_' before digits; code points put it after them.
    "range_text_to_polars": (
        {"name": "range", "params": {"column": "s", "min": "2024-01-01"}}, [{"s": "_"}], "polars",
    ),
    "when_range_text_to_polars": (
        {"name": "conditional_range", "params": {"column": "s", "when": "n == 3", "min": "2024-01-01"}},
        [{"s": "_", "n": 3}],
        "polars",
    ),
    "length_emoji": (rules.length("s", max=1), [{"s": "😀"}], "sql"),
    "length_nchar_emoji": (rules.length("nc", min=4, max=4), [{"nc": "😀"}], "sql"),
    "starts_with_case": (rules.starts_with("s", "a"), [{"s": "ABC"}], "sql"),
    "ends_with_case": (rules.ends_with("s", "C"), [{"s": "abc"}], "sql"),
    "ends_with_varchar_space": (rules.ends_with("v", "x"), [{"v": "x "}], "sql"),
    "ends_with_char_padding": (rules.ends_with("c", "x"), [{"c": "x"}], "sql"),
    "contains_emoji": (rules.contains("s", "😀"), [{"s": "a😀b"}], "sql"),
}


def _connect():
    pymssql = pytest.importorskip("pymssql")
    try:
        return pymssql.connect(_HOST, _USER, _PASSWORD, _DATABASE, port=_PORT, autocommit=True)
    except pymssql.Error as e:
        pytest.skip(f"SQL Server container unreachable: {e}")


@pytest.fixture
def make_table():
    """Create a dbo table with _COLUMNS holding ``rows``; drop it afterwards."""
    conn = _connect()
    created = []

    def make(rows: list[dict]) -> str:
        name = f"kontra_exact_text_{uuid.uuid4().hex[:10]}"
        cur = conn.cursor()
        ddl = ", ".join(f"{col} {typ}" for col, typ in _COLUMNS.items())
        cur.execute(f"CREATE TABLE dbo.{name} ({ddl})")
        created.append(name)
        marks = ", ".join(["%s"] * len(_COLUMNS))
        # Parameters reach the server as NVARCHAR, so an emoji arrives intact.
        cur.executemany(
            f"INSERT INTO dbo.{name} VALUES ({marks})",
            [tuple(row.get(col) for col in _COLUMNS) for row in rows],
        )
        return name

    yield conn, make
    cur = conn.cursor()
    for name in created:
        cur.execute(f"IF OBJECT_ID('dbo.{name}', 'U') IS NOT NULL DROP TABLE dbo.{name}")
    conn.close()


def _run(name: str, rule, pushdown: str, tally: bool):
    uri = f"mssql://{_USER}:{_PASSWORD}@localhost:{_PORT}/{_DATABASE}/dbo.{name}"
    result = kontra.validate(
        uri, rules=[rule], save=False, preplan="off", pushdown=pushdown, tally=tally
    )
    (r,) = result.rules
    assert "Rule execution failed" not in r.message
    return r


@pytest.mark.integration
@pytest.mark.parametrize("case", list(_CASES))
def test_pushdown_matches_polars(make_table, case):
    _conn, make = make_table
    rule, rows, tier = _CASES[case]
    name = make(rows)
    for tally in (True, False):
        sql = _run(name, rule, "on", tally)
        polars = _run(name, rule, "off", tally)

        assert sql.source == tier, f"tally={tally}"
        assert polars.source == "polars"
        assert sql.passed == polars.passed, f"tally={tally}"
        if tally:
            assert sql.failed_count == polars.failed_count


@pytest.mark.integration
def test_default_collation_ignores_case_and_trailing_spaces(make_table):
    """Control: the database's own comparisons merge what Polars keeps apart."""
    conn, make = make_table
    name = make([
        {"s": "abc", "s2": "ABC"},
        {"s": "ABC", "s2": "ABC"},
        {"s": "x", "s2": "x "},
        {"s": "x ", "s2": "x"},
    ])
    cur = conn.cursor()
    cur.execute(
        f"SELECT COUNT(DISTINCT s), "
        f"SUM(CASE WHEN s = N'x' THEN 1 ELSE 0 END), "
        f"SUM(CASE WHEN s < s2 THEN 1 ELSE 0 END) FROM dbo.{name}"
    )
    distinct, equal_to_x, below = cur.fetchone()
    assert distinct == 2  # Polars: 4
    assert equal_to_x == 2  # Polars: 1 ('x ' is not 'x')
    assert below == 0  # Polars: 2 ('ABC' < 'abc', 'x' < 'x ')


@pytest.mark.integration
def test_distinct_count_and_unique_keep_nul_and_case_apart(make_table):
    """'a' and 'a' + NUL are two values; so are 'abc'/'ABC' and 'x'/'x '."""
    conn, make = make_table
    name = make([{"s": v} for v in ("abc", "ABC", "x", "x ", "a")])
    # pymssql can't pass a NUL in a parameter.
    conn.cursor().execute(f"INSERT INTO dbo.{name} (s) VALUES (N'a' + NCHAR(0))")

    drift = rules.drift("distinct_count", "s", max_change=0)
    for pushdown, tier in (("on", "sql"), ("off", "polars")):
        measured = _run(name, drift, pushdown, True)
        assert measured.source == tier
        assert measured.details["value"] == 6, pushdown
    for tally in (True, False):
        sql = _run(name, rules.unique("s"), "on", tally)
        assert sql.source == "sql"
        assert sql.passed, f"tally={tally}"
        assert _run(name, rules.unique("s"), "off", tally).passed
