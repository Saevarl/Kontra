"""drift rule against live SQL Server: SQL measurement matches Polars, history works."""

from __future__ import annotations

import uuid

import pytest

import kontra
from kontra import rules

pytestmark = pytest.mark.integration

_BASE = "mssql://sa:Kontra_Test123!@localhost:1433/kontra_test"

METRICS = [
    ("row_count", None),
    ("null_count", "score"),
    ("null_rate", "score"),
    ("distinct_count", "grp"),
    ("mean", "qty"),  # INT column: AVG must not truncate
    ("mean", "score"),
    ("min", "score"),
    ("max", "qty"),
]


def metric_rules():
    return [rules.drift(m, c, max_change=0, id=f"{m}:{c}") for m, c in METRICS]


def _connect():
    import pymssql

    return pymssql.connect(
        server="localhost", port=1433, user="sa", password="Kontra_Test123!", database="kontra_test"
    )


@pytest.fixture()
def table():
    name = f"drift_{uuid.uuid4().hex[:10]}"
    conn = _connect()
    cur = conn.cursor()
    cur.execute(f"CREATE TABLE dbo.{name} (id INT, grp NVARCHAR(10), qty INT, score FLOAT)")
    cur.executemany(
        f"INSERT INTO dbo.{name} VALUES (%s, %s, %s, %s)",
        [
            (i, None if i % 7 == 0 else f"g{i % 5}", i % 4, None if i % 3 == 0 else i / 8)
            for i in range(1, 301)
        ],
    )
    conn.commit()
    yield name
    cur.execute(f"DROP TABLE IF EXISTS dbo.{name}")
    conn.commit()
    conn.close()


def measured(uri, **kwargs):
    result = kontra.validate(uri, rules=metric_rules(), save=False, **kwargs)
    return {r.rule_id: (r.details["value"], r.source) for r in result.rules}


def test_sql_measurements_match_polars(table):
    uri = f"{_BASE}/dbo.{table}"
    sql = measured(uri)
    polars = measured(uri, pushdown="off", preplan="off")
    assert {source for _, source in sql.values()} == {"sql"}
    assert {source for _, source in polars.values()} == {"polars"}
    for rule_id, (value, _) in polars.items():
        assert sql[rule_id][0] == pytest.approx(value), rule_id
    assert sql["mean:qty"][0] == pytest.approx(1.5)
    assert sql["null_rate:score"][0] == pytest.approx(1 / 3)


def test_history_detects_drift(table, tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    uri = f"{_BASE}/dbo.{table}"
    contract_rules = [
        rules.drift("null_rate", "score", max_increase=0.05),
        rules.drift("mean", "qty", max_change_pct=10),
    ]
    first = kontra.validate(uri, rules=contract_rules)
    assert {r.drift_status for r in first.rules} == {"no_baseline"}
    assert {r.drift_status for r in kontra.validate(uri, rules=contract_rules).rules} == {"ok"}

    conn = _connect()
    cur = conn.cursor()
    cur.execute(f"UPDATE dbo.{table} SET score = NULL, qty = qty + 1 WHERE id % 2 = 0")
    conn.commit()
    conn.close()

    third = {r.rule_id: r for r in kontra.validate(uri, rules=contract_rules).rules}
    assert third["COL:score:drift:null_rate"].drift_status == "drifted"
    assert third["COL:qty:drift:mean"].drift_status == "drifted"
    assert third["COL:qty:drift:mean"].source == "sql"
