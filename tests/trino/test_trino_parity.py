"""Trino results must match the DuckDB (Parquet) and Polars (DataFrame) paths."""

from __future__ import annotations

import datetime as dt
import math

import polars as pl
import pytest

import kontra
from scripts.synthesize_users import generate_users

TABLE = "memory.kontra.parity_users"
URI = "trino://kontra@localhost:8095/memory/kontra.parity_users"

RULES = [
    {"name": "not_null", "params": {"column": "email"}},
    {"name": "not_null", "params": {"column": "last_login"}},
    {"name": "unique", "params": {"column": "user_id"}},
    {"name": "allowed_values", "params": {"column": "status", "values": ["active", "inactive"]}},
    {"name": "range", "params": {"column": "age", "min": 18, "max": 70}},
    {"name": "range", "params": {"column": "balance", "min": 0}},
    {"name": "regex", "params": {"column": "email", "pattern": "^[^@ ]+@[^@ ]+[.][a-z]+$"}},
    {"name": "length", "params": {"column": "country", "min": 2, "max": 2}},
    {"name": "starts_with", "params": {"column": "email", "prefix": "a"}},
    {"name": "min_rows", "params": {"threshold": 2500}},
    {"name": "max_rows", "params": {"threshold": 1500}},
    {"name": "freshness", "params": {"column": "last_login", "max_age": "36500d"}},
]


def _sql_literal(value) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, float):
        return "nan()" if math.isnan(value) else repr(value)
    if isinstance(value, int):
        return str(value)
    if isinstance(value, dt.datetime):
        return f"TIMESTAMP '{value.isoformat(sep=' ')}'"
    if isinstance(value, dt.date):
        return f"DATE '{value.isoformat()}'"
    return "'" + str(value).replace("'", "''") + "'"


@pytest.fixture(scope="module")
def parity_users(tmp_path_factory, trino_connect):
    df = generate_users(
        n=2000,
        seed=7,
        dup_rate=0.02,
        bad_email_rate=0.05,
        bad_status_rate=0.05,
        null_rate_email=0.03,
        null_rate_age=0.03,
        null_rate_last_login=0.05,
        allow_negative_balance=True,
    )
    path = tmp_path_factory.mktemp("trino_parity") / "users.parquet"
    df.write_parquet(path)

    conn = trino_connect()
    cur = conn.cursor()
    for sql in (
        "CREATE SCHEMA IF NOT EXISTS memory.kontra",
        f"DROP TABLE IF EXISTS {TABLE}",
        (
            f"CREATE TABLE {TABLE} (user_id bigint, email varchar, status varchar, "
            "country varchar, signup_date date, last_login timestamp(6), age smallint, "
            "is_premium boolean, balance double)"
        ),
    ):
        cur.execute(sql)
        cur.fetchall()
    rows = df.rows()
    for start in range(0, len(rows), 500):
        values = ", ".join(
            "(" + ", ".join(_sql_literal(v) for v in row) + ")" for row in rows[start : start + 500]
        )
        cur.execute(f"INSERT INTO {TABLE} VALUES {values}")
        cur.fetchall()
    yield str(path), df
    cur.execute(f"DROP TABLE IF EXISTS {TABLE}")
    cur.fetchall()
    conn.close()


@pytest.mark.integration
@pytest.mark.parametrize("tally", [True, False])
def test_trino_matches_duckdb_and_polars(write_contract, run_engine, parity_users, tally):
    parquet_path, df = parity_users
    dataset_rules = {"min_rows", "max_rows", "freshness"}
    rules = [r if r["name"] in dataset_rules else dict(r, tally=tally) for r in RULES]
    cpath = write_contract(dataset=parquet_path, rules=rules)

    duck, _ = run_engine(cpath, pushdown="on", stats_mode="summary")
    trino, _ = run_engine(cpath, data_override=URI, pushdown="on", stats_mode="summary")
    local = kontra.validate(df, cpath, tally=tally, save=False)

    by_duck = {r["rule_id"]: r for r in duck["results"]}
    by_trino = {r["rule_id"]: r for r in trino["results"]}
    by_polars = {r.rule_id: r for r in local.rules}
    assert set(by_trino) == set(by_duck) == set(by_polars)
    for rid, t in by_trino.items():
        assert t["passed"] == by_duck[rid]["passed"] == by_polars[rid].passed, rid
        if tally:
            assert t["failed_count"] == by_duck[rid]["failed_count"], rid
            assert t["failed_count"] == by_polars[rid].failed_count, rid

    # summary parity (counts add up and agree across engines)
    summary = trino["summary"]
    assert summary["rules_passed"] + summary["rules_failed"] == summary["total_rules"]
    assert summary["rules_passed"] == duck["summary"]["rules_passed"]

    # Something must actually have been pushed to Trino.
    assert any(r.get("execution_source") == "sql" for r in trino["results"])


@pytest.mark.integration
def test_regex_subset_matches_polars(trino_connect):
    """Every pattern the Trino translation accepts must match Polars on edge strings."""
    from kontra.engine.sql_ir import trino_regex

    strings = [
        "", "abc", "abc\n", "a\nb", "a\rb", "a b", "a\u0085b", "é", "ß", "😀",
        "a😀b", "a.b", "a\\b", "x-y", "[x]", "{1}", "a\tb", "ab\r\n", "$", "^", "AbC", "-",
    ]  # fmt: skip
    patterns = [
        "^abc$", "abc$", "^a.b$", "^.$", "a|b$", "^(?:a|b)+c?$", "[^a]", "[a-c]+$",
        r"a\.b", r"\$", r"\^", r"\[x\]", r"\{1\}", r"a\\b", r"\t", r"\n", r"\r", r"\x41",
        r"\Aabc\z", "a{1,2}?", "x-y", "😀", "^é$", "[é😀]",
        "^[a-z-]+$", "^[-a-c]+$", "^[^-a]$", r"^[a\-c]+$",
    ]  # fmt: skip
    conn = trino_connect()
    try:
        cur = conn.cursor()
        for pattern in patterns:
            translated = trino_regex(pattern)
            assert translated is not None, pattern
            expected = pl.Series(strings).str.contains(pattern).to_list()
            checks = ", ".join(
                "regexp_like('{}', '{}')".format(
                    s.replace("'", "''"), translated.replace("'", "''")
                )
                for s in strings
            )
            cur.execute(f"SELECT {checks}")
            assert list(cur.fetchall()[0]) == expected, pattern
    finally:
        conn.close()


EDGE_TABLE = "memory.kontra.parity_edges"
EDGE_URI = "trino://kontra@localhost:8095/memory/kontra.parity_edges"


@pytest.fixture(scope="module")
def parity_edges(trino_connect):
    """Values where a careless pushdown would disagree with the Polars tier."""
    conn = trino_connect()
    cur = conn.cursor()
    for sql in (
        "CREATE SCHEMA IF NOT EXISTS memory.kontra",
        f"DROP TABLE IF EXISTS {EDGE_TABLE}",
        f"CREATE TABLE {EDGE_TABLE} (s varchar, i bigint, d decimal(10, 2), ts timestamp(9))",
        (
            f"INSERT INTO {EDGE_TABLE} VALUES "
            "('-', 9007199254740993, 9.99, TIMESTAMP '2026-01-01 00:00:00.000000001'), "
            "('ab', 1, 0.30, TIMESTAMP '2026-01-01 00:00:00.000000002')"
        ),
    ):
        cur.execute(sql)
        cur.fetchall()
    yield EDGE_URI
    cur.execute(f"DROP TABLE IF EXISTS {EDGE_TABLE}")
    cur.fetchall()
    conn.close()


EDGE_RULES = [
    {"name": "regex", "params": {"column": "s", "pattern": "^[a-z-A-Z]+$"}},
    {"name": "range", "params": {"column": "i", "max": 9007199254740992.0}},
    {
        "name": "conditional_range",
        "params": {"column": "i", "when": "i >= 0", "max": 9007199254740992.0},
    },
    {"name": "range", "params": {"column": "d", "min": 0.31}},
    {"name": "unique", "params": {"column": "ts"}},
]


@pytest.mark.integration
@pytest.mark.pushdown
@pytest.mark.parametrize("tally", [True, False])
def test_inexact_edges_match_residual_tier(parity_edges, tally):
    """Rules Trino would answer differently must fall back and agree with Polars."""
    rules = [dict(r, tally=tally) for r in EDGE_RULES]
    on = kontra.validate(parity_edges, rules=rules, tally=tally, save=False, preplan="off")
    off = kontra.validate(
        parity_edges, rules=rules, tally=tally, save=False, preplan="off", pushdown="off"
    )
    by_off = {r.rule_id: r for r in off.rules}
    assert len(on.rules) == len(EDGE_RULES)
    for rule in on.rules:
        assert rule.source == "polars", rule.rule_id
        assert rule.passed == by_off[rule.rule_id].passed, rule.rule_id
        if tally:
            assert rule.failed_count == by_off[rule.rule_id].failed_count, rule.rule_id


SPARSE_TABLE = "memory.kontra.parity_sparse"
SPARSE_URI = "trino://kontra@localhost:8095/memory/kontra.parity_sparse"
SPARSE_LEADING_NULLS = 150


@pytest.fixture(scope="module")
def parity_sparse(trino_connect):
    """Columns that stay NULL past Polars' default inference window, then hold a value."""
    conn = trino_connect()
    cur = conn.cursor()
    for sql in (
        "CREATE SCHEMA IF NOT EXISTS memory.kontra",
        f"DROP TABLE IF EXISTS {SPARSE_TABLE}",
        (
            f"CREATE TABLE {SPARSE_TABLE} AS "
            "SELECT n, "
            f"IF(n > {SPARSE_LEADING_NULLS}, BIGINT '36164') AS i, "
            f"IF(n > {SPARSE_LEADING_NULLS}, 'x') AS s, "
            f"IF(n > {SPARSE_LEADING_NULLS}, DECIMAL '9.99') AS d, "
            f"IF(n > {SPARSE_LEADING_NULLS}, TRUE) AS b, "
            f"IF(n > {SPARSE_LEADING_NULLS}, TIMESTAMP '2026-01-01 00:00:00.123') AS ts, "
            "CAST(NULL AS varchar) AS always_null, "
            "CAST(NULL AS bigint) AS always_null_i "
            f"FROM UNNEST(sequence(1, {SPARSE_LEADING_NULLS + 1})) AS t(n) ORDER BY n"
        ),
        f"DROP TABLE IF EXISTS {SPARSE_TABLE}_empty",
        f"CREATE TABLE {SPARSE_TABLE}_empty AS SELECT * FROM {SPARSE_TABLE} WITH NO DATA",
    ):
        cur.execute(sql)
        cur.fetchall()
    yield SPARSE_URI
    for table in (SPARSE_TABLE, f"{SPARSE_TABLE}_empty"):
        cur.execute(f"DROP TABLE IF EXISTS {table}")
        cur.fetchall()
    conn.close()


SPARSE_RULES = [
    {"name": "not_null", "params": {"column": "i"}},
    {"name": "range", "params": {"column": "i", "min": 0}},
    {"name": "regex", "params": {"column": "s", "pattern": "^x$"}},
    {"name": "range", "params": {"column": "d", "max": 10}},
    {"name": "allowed_values", "params": {"column": "b", "values": [True]}},
    {"name": "not_null", "params": {"column": "ts"}},
    {"name": "not_null", "params": {"column": "always_null"}},
]


@pytest.mark.integration
@pytest.mark.parametrize("tally", [True, False])
def test_sparse_columns_materialize(parity_sparse, tally):
    """Leading NULLs beyond the inference window must not break the Polars tier."""
    rules = [dict(r, tally=tally) for r in SPARSE_RULES]
    off = kontra.validate(
        parity_sparse, rules=rules, tally=tally, save=False, preplan="off", pushdown="off"
    )
    on = kontra.validate(parity_sparse, rules=rules, tally=tally, save=False, preplan="off")
    by_on = {r.rule_id: r for r in on.rules}
    assert len(off.rules) == len(SPARSE_RULES)
    for rule in off.rules:
        assert rule.source == "polars", rule.rule_id
        assert rule.passed == by_on[rule.rule_id].passed, rule.rule_id
        if tally:
            assert rule.failed_count == by_on[rule.rule_id].failed_count, rule.rule_id


SPARSE_DTYPES = [
    {"name": "dtype", "params": {"column": "i", "type": "int64"}},
    {"name": "dtype", "params": {"column": "s", "type": "utf8"}},
    {"name": "dtype", "params": {"column": "b", "type": "bool"}},
    {"name": "dtype", "params": {"column": "ts", "type": "datetime"}},
    {"name": "dtype", "params": {"column": "always_null", "type": "utf8"}},
    {"name": "dtype", "params": {"column": "always_null_i", "type": "int64"}},
]


@pytest.mark.integration
@pytest.mark.parametrize("suffix", ["", "_empty"])
def test_null_and_empty_columns_keep_declared_types(parity_sparse, suffix):
    """Columns with no values take their dtype from Trino, not from inference."""
    result = kontra.validate(parity_sparse + suffix, rules=SPARSE_DTYPES, save=False)
    failed = [(r.rule_id, r.message) for r in result.rules if not r.passed]
    assert failed == []
