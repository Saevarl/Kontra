"""drift rule against live PostgreSQL: SQL measurement and PostgresStore history."""

from __future__ import annotations

import uuid

import pytest

import kontra
from kontra import rules

pytestmark = pytest.mark.integration

_BASE = "postgres://kontra:kontra_test@localhost:5433"
# A database of its own: other suites create and reset kontra_* state tables
# in kontra_test.
_STATE_DB = "kontra_drift_state"

METRICS = [
    ("row_count", None),
    ("null_count", "score"),
    ("null_rate", "score"),
    ("distinct_count", "grp"),
    ("mean", "qty"),
    ("mean", "score"),
    ("min", "score"),
    ("max", "qty"),
]


def metric_rules():
    return [rules.drift(m, c, max_change=0, id=f"{m}:{c}") for m, c in METRICS]


@pytest.fixture()
def table(postgres_connection):
    """A fresh table: integer, numeric and float columns with NULLs."""
    name = f"drift_{uuid.uuid4().hex[:10]}"
    with postgres_connection.cursor() as cur:
        cur.execute(
            f"CREATE TABLE public.{name} (id INT, grp TEXT, qty INT, score DOUBLE PRECISION, amount NUMERIC(10,2))"
        )
        cur.executemany(
            f"INSERT INTO public.{name} VALUES (%s, %s, %s, %s, %s)",
            [
                (i, None if i % 7 == 0 else f"g{i % 5}", i % 4, None if i % 3 == 0 else i / 8, i * 1.25)
                for i in range(1, 301)
            ],
        )
    postgres_connection.commit()
    yield name
    with postgres_connection.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS public.{name}")
    postgres_connection.commit()


@pytest.fixture()
def state_store(postgres_container):
    import psycopg

    with psycopg.connect(f"{_BASE}/postgres", autocommit=True) as conn:
        exists = conn.execute("SELECT 1 FROM pg_database WHERE datname = %s", (_STATE_DB,)).fetchone()
        if not exists:
            conn.execute(f"CREATE DATABASE {_STATE_DB}")
    from kontra.state.backends.postgres import PostgresStore

    store = PostgresStore(f"{_BASE}/{_STATE_DB}")
    yield store
    store.close()


def measured(uri, **kwargs):
    result = kontra.validate(uri, rules=metric_rules(), save=False, **kwargs)
    return {r.rule_id: (r.details["value"], r.source) for r in result.rules}


def test_sql_measurements_match_polars(table):
    uri = f"{_BASE}/kontra_test/public.{table}"
    sql = measured(uri)
    polars = measured(uri, pushdown="off", preplan="off")
    assert {source for _, source in sql.values()} == {"sql"}
    assert {source for _, source in polars.values()} == {"polars"}
    for rule_id, (value, _) in polars.items():
        assert sql[rule_id][0] == pytest.approx(value), rule_id
    assert sql["row_count:None"][0] == 300
    assert sql["null_count:score"][0] == 100
    assert sql["null_rate:score"][0] == pytest.approx(1 / 3)
    assert sql["distinct_count:grp"][0] == 5


def test_decimal_mean_is_a_plain_number(table):
    uri = f"{_BASE}/kontra_test/public.{table}"
    result = kontra.validate(uri, rules=[rules.drift("mean", "amount", max_change=0)], save=False)
    value = result.rules[0].details["value"]
    assert type(value) is float and value == pytest.approx(1.25 * 150.5)


def test_text_mean_is_a_config_error(table):
    uri = f"{_BASE}/kontra_test/public.{table}"
    result = kontra.validate(uri, rules=[rules.drift("mean", "grp", max_change=0)], save=False)
    assert result.rules[0].failure_mode == "config_error"


def test_text_max_is_a_config_error(table):
    uri = f"{_BASE}/kontra_test/public.{table}"
    result = kontra.validate(uri, rules=[rules.drift("max", "grp", max_change=0)], save=False)
    assert result.rules[0].failure_mode == "config_error"


@pytest.fixture()
def odd_table(postgres_connection):
    """NaN, integers past 2**53 and numeric-looking text."""
    name = f"drift_odd_{uuid.uuid4().hex[:10]}"
    with postgres_connection.cursor() as cur:
        cur.execute(f"CREATE TABLE public.{name} (f DOUBLE PRECISION, b BIGINT, s TEXT)")
        cur.execute(
            f"INSERT INTO public.{name} VALUES "
            "(1, 9007199254740993, '1'), ('NaN', 1, '2'), (-2, 2, '3'), (NULL, NULL, NULL)"
        )
    postgres_connection.commit()
    yield f"{_BASE}/kontra_test/public.{name}"
    with postgres_connection.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS public.{name}")
    postgres_connection.commit()


def test_nan_and_large_integers_match_polars(odd_table):
    drift = [
        rules.drift(m, c, max_change=0, id=f"{m}:{c}")
        for m, c in [("min", "f"), ("max", "f"), ("mean", "f"), ("max", "b"), ("min", "b")]
    ]
    sql = {r.rule_id: r for r in kontra.validate(odd_table, rules=drift, save=False).rules}
    polars = {
        r.rule_id: r
        for r in kontra.validate(odd_table, rules=drift, save=False, pushdown="off", preplan="off").rules
    }
    assert {r.source for r in sql.values()} == {"sql"}
    for rule_id, rule in polars.items():
        assert sql[rule_id].details["value"] == rule.details["value"], rule_id
    assert sql["max:f"].details["value"] == 1.0
    assert sql["mean:f"].details["value"] is None  # NaN mean is undefined
    assert sql["max:b"].details["value"] == 2**53 + 1


def test_numeric_looking_text_mean_is_a_config_error(odd_table):
    for kwargs in ({}, {"pushdown": "off", "preplan": "off"}):
        result = kontra.validate(odd_table, rules=[rules.drift("mean", "s", max_change=0)], save=False, **kwargs)
        assert result.rules[0].failure_mode == "config_error", kwargs


def test_empty_table_values(postgres_connection, table):
    with postgres_connection.cursor() as cur:
        cur.execute(f"DELETE FROM public.{table}")
    postgres_connection.commit()
    sql = measured(f"{_BASE}/kontra_test/public.{table}")
    assert sql["row_count:None"][0] == 0
    assert sql["null_count:score"][0] == 0
    assert sql["null_rate:score"][0] is None
    assert sql["mean:qty"][0] is None


def test_history_in_postgres_store(postgres_connection, table, state_store):
    uri = f"{_BASE}/kontra_test/public.{table}"
    # Unique per test run: the store's database outlives the test.
    contract_rules = [
        rules.drift("null_rate", "score", max_increase=0.05, id=f"nr_{table}"),
        rules.drift("row_count", baseline="median", window=3, max_decrease_pct=20),
    ]

    first = kontra.validate(uri, rules=contract_rules, state_store=state_store)
    assert {r.details["status"] for r in first.rules} == {"no_baseline"}
    second = kontra.validate(uri, rules=contract_rules, state_store=state_store)
    assert {r.details["status"] for r in second.rules} == {"ok"}

    with postgres_connection.cursor() as cur:
        cur.execute(f"UPDATE public.{table} SET score = NULL WHERE id % 2 = 0")
        cur.execute(f"DELETE FROM public.{table} WHERE id > 200")
    postgres_connection.commit()

    third = {r.rule_id: r for r in kontra.validate(uri, rules=contract_rules, state_store=state_store).rules}
    null_rate = third[f"nr_{table}"]
    assert null_rate.details["status"] == "drifted"
    assert null_rate.details["baseline"] == pytest.approx(1 / 3)
    rows = third["DATASET:drift:row_count"]
    assert rows.details["status"] == "drifted"
    assert rows.details["baseline"] == 300 and rows.details["value"] == 200

    # The measurement survives the JSONB round trip.
    history = state_store.get_history(latest_fingerprint(uri), limit=5)
    saved = history[0].get_rule("DATASET:drift:row_count")
    assert saved.rule_name == "drift"
    assert saved.details["value"] == 200
    assert saved.details["status"] == "drifted"


def latest_fingerprint(uri):
    """The fingerprint of the most recent run on this dataset."""
    import psycopg

    with psycopg.connect(f"{_BASE}/{_STATE_DB}") as conn:
        row = conn.execute(
            "SELECT contract_fingerprint FROM kontra_runs WHERE dataset_name = %s ORDER BY run_at DESC LIMIT 1",
            (uri,),
        ).fetchone()
    return row[0]
