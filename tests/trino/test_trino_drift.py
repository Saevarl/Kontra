"""drift measurements through the Trino executor match the Polars tier."""

from __future__ import annotations

import uuid

import pytest

import kontra
from kontra import rules

_HOST, _PORT, _USER = "localhost", 8095, "kontra"


def run_sql(*statements: str) -> None:
    import trino

    with trino.dbapi.connect(host=_HOST, port=_PORT, user=_USER, timezone="UTC") as conn:
        cur = conn.cursor()
        for sql in statements:
            cur.execute(sql)
            cur.fetchall()


METRICS = [
    ("row_count", None),
    ("null_count", "score"),
    ("null_rate", "score"),
    ("distinct_count", "grp"),
    ("mean", "qty"),
    ("mean", "amount"),
    ("min", "amount"),
    ("max", "qty"),
]


@pytest.fixture()
def table(trino_container):
    name = f"drift_{uuid.uuid4().hex[:10]}"
    rows = ", ".join(
        f"({i}, {'NULL' if i % 7 == 0 else repr(f'g{i % 5}')}, {i % 4}, "
        f"{'NULL' if i % 3 == 0 else f'CAST({i / 8!r} AS double)'}, DECIMAL '{i * 1.25:.2f}')"
        for i in range(1, 301)
    )
    run_sql(
        "CREATE SCHEMA IF NOT EXISTS memory.kontra",
        f"CREATE TABLE memory.kontra.{name} (id integer, grp varchar, qty integer, "
        "score double, amount decimal(10, 2))",
        f"INSERT INTO memory.kontra.{name} VALUES {rows}",
    )
    yield f"trino://{_USER}@{_HOST}:{_PORT}/memory/kontra.{name}"
    run_sql(f"DROP TABLE IF EXISTS memory.kontra.{name}")


def measured(uri, **kwargs):
    drift = [rules.drift(m, c, max_change=0, id=f"{m}:{c}") for m, c in METRICS]
    result = kontra.validate(uri, rules=drift, save=False, **kwargs)
    return {r.rule_id: (r.details["value"], r.source) for r in result.rules}


def test_sql_measurements_match_polars(table):
    sql = measured(table)
    polars = measured(table, pushdown="off", preplan="off")
    assert {source for _, source in sql.values()} == {"sql"}
    for rule_id, (value, _) in polars.items():
        assert sql[rule_id][0] == pytest.approx(value), rule_id


def test_float_extremes_stay_in_polars(table):
    # MIN/MAX over doubles differ on NaN between Trino and Polars.
    result = kontra.validate(table, rules=[rules.drift("max", "score", max_change=0)], save=False)
    assert result.rules[0].source == "polars"
