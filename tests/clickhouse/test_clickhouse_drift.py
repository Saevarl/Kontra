"""drift measurements through the ClickHouse executor match the Polars tier."""

from __future__ import annotations

import uuid

import pytest

import kontra
from kontra import rules

_HOST, _PORT = "localhost", 8123
_USER, _PASSWORD, _DB = "kontra", "kontra_test", "kontra_test"


def _client():
    import clickhouse_connect

    return clickhouse_connect.get_client(
        host=_HOST, port=_PORT, username=_USER, password=_PASSWORD, database=_DB
    )


METRICS = [
    ("row_count", None),
    ("null_count", "score"),
    ("null_rate", "score"),
    ("distinct_count", "grp"),
    ("mean", "qty"),
    ("mean", "score"),  # Nullable: toFloat64 keeps the NULLs out of AVG
    ("min", "score"),
    ("max", "qty"),
]


@pytest.fixture()
def table():
    name = f"drift_{uuid.uuid4().hex[:10]}"
    c = _client()
    c.command(
        f"CREATE TABLE {name} (id UInt32, grp Nullable(String), qty Int32, score Nullable(Float64)) "
        "ENGINE = MergeTree ORDER BY id"
    )
    rows = ",".join(
        f"({i}, {'NULL' if i % 7 == 0 else repr(f'g{i % 5}')}, {i % 4}, {'NULL' if i % 3 == 0 else i / 8})"
        for i in range(1, 301)
    )
    c.command(f"INSERT INTO {name} VALUES {rows}")
    yield f"clickhouse://{_USER}:{_PASSWORD}@{_HOST}:{_PORT}/{_DB}/{name}"
    c.command(f"DROP TABLE IF EXISTS {name}")


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
    assert sql["null_rate:score"][0] == pytest.approx(1 / 3)
