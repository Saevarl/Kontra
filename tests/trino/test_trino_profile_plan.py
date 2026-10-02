# tests/trino/test_trino_profile_plan.py
"""
The Trino scout backend's query plan (kontra.scout.backends.trino_backend):
scout's one aggregate, value queries run concurrently, and TABLESAMPLE SYSTEM
on Iceberg tables with enough data files.

Each case checks the profile against exact answers from the same table, and
the statements the profile sent.
"""

from __future__ import annotations

import threading
import time

import pytest

import kontra
from kontra.scout.backends.trino_backend import TrinoBackend

pytestmark = [pytest.mark.integration, pytest.mark.pushdown]

CATALOGS = ["memory", "iceberg", "iceberg_jdbc"]
SCHEMA = "kontra_profile_plan"
ROWS = 5000


def connect(**kwargs):
    import trino

    return trino.dbapi.connect(host="localhost", port=8095, user="kontra", timezone="UTC", **kwargs)


def run_sql(*statements: str) -> list:
    rows: list = []
    with connect() as conn:
        cur = conn.cursor()
        for sql in statements:
            cur.execute(sql)
            rows = cur.fetchall()
    return rows


def uri(fq: str) -> str:
    catalog, schema, table = fq.split(".")
    return f"trino://kontra@localhost:8095/{catalog}/{schema}.{table}"


@pytest.fixture(params=CATALOGS)
def table(request, trino_container):
    """5,000 rows. ``g`` holds value v 2v+1 times, so its top values have no ties."""
    catalog = request.param
    fq = f"{catalog}.{SCHEMA}.t"
    run_sql(
        f"CREATE SCHEMA IF NOT EXISTS {catalog}.{SCHEMA}",
        f"DROP TABLE IF EXISTS {fq}",
        f"""CREATE TABLE {fq} AS SELECT
            CAST(i AS bigint) AS id,
            CASE WHEN i % 50 = 0 THEN NULL
                 WHEN i % 5 < 3 THEN 'a' WHEN i % 5 = 3 THEN 'b' ELSE 'c' END AS k,
            CAST(floor(sqrt(i - 1)) AS integer) AS g,
            CASE WHEN i % 7 = 0 THEN NULL ELSE CAST(i % 1000 AS double) / 4 END AS x,
            CASE WHEN i % 9 = 0 THEN NULL
                 ELSE MAP(ARRAY['k'], ARRAY[i % 3]) END AS m
        FROM UNNEST(sequence(1, {ROWS})) AS t(i)""",
    )
    yield fq
    run_sql(f"DROP TABLE IF EXISTS {fq}")


class _Sent(list):
    """Statements sent; ``peak`` is the most in flight at once."""

    peak = 0


@pytest.fixture
def statements(monkeypatch):
    """Every statement sent, and the most sent at once."""
    import trino

    sent = _Sent()
    state = {"in_flight": 0}
    lock = threading.Lock()
    execute = trino.dbapi.Cursor.execute

    def spy(self, operation, params=None):
        with lock:
            sent.append(operation)
            state["in_flight"] += 1
            sent.peak = max(sent.peak, state["in_flight"])
        try:
            time.sleep(0.02)  # so queries that can overlap do
            return execute(self, operation, params)
        finally:
            with lock:
                state["in_flight"] -= 1

    monkeypatch.setattr(trino.dbapi.Cursor, "execute", spy)
    return sent


def _reads(sent: list[str], fq: str) -> list[str]:
    _, schema, table = fq.split(".")
    return [s for s in sent if f'"{schema}"."{table}"' in s]


def _exact(fq: str, column: str) -> tuple[int, int]:
    ((nulls, distinct),) = run_sql(
        f"SELECT count_if({column} IS NULL), count(DISTINCT {column}) FROM {fq}"
    )
    return int(nulls), int(distinct)


def _by_name(profile):
    return {c.name: c for c in profile.columns}


def test_scout_counts_are_exact_and_distincts_estimated(table, statements):
    profile = kontra.profile(uri(table), preset="scout", save=False)

    assert (profile.row_count, profile.row_count_estimated) == (ROWS, False)
    cols = _by_name(profile)
    for name in ("id", "k", "g", "x", "m"):
        nulls, distinct = _exact(table, name)
        col = cols[name]
        assert (col.null_count, col.null_count_estimated) == (nulls, False), name
        assert col.distinct_count_estimated is True, name
        # approx_distinct's standard error is 2.3%.
        assert abs(col.distinct_count - distinct) <= max(1, 0.05 * distinct), name

    # One aggregate reads the columns, without COUNT(DISTINCT). The other read
    # is the profiler's row count, as in every preset.
    scan, count = sorted(_reads(statements, table), key=len, reverse=True)
    assert "approx_distinct" in scan and "count(DISTINCT" not in scan.replace("COUNT", "count")
    assert count.startswith("SELECT COUNT(*) FROM")
    assert not any("SET SESSION" in s for s in statements)


@pytest.mark.parametrize("preset", ["scan", "interrogate"])
def test_value_queries_run_concurrently_and_match_exact_counts(table, statements, preset):
    profile = kontra.profile(uri(table), preset=preset, save=False)
    cols = _by_name(profile)
    top_n = {"scan": 5, "interrogate": 10}[preset]

    # g: 71 values, so top values by GROUP BY; no ties among the top.
    expected = run_sql(
        f"SELECT g, count(*) c FROM {table} GROUP BY g ORDER BY c DESC LIMIT {top_n}"
    )
    assert [(v.value, v.count) for v in cols["g"].top_values] == [(g, c) for g, c in expected]

    # k: 3 values, listed with their exact frequencies from one GROUP BY.
    expected_k = run_sql(
        f"SELECT k, count(*) FROM {table} WHERE k IS NOT NULL GROUP BY k ORDER BY k"
    )
    assert cols["k"].values == [k for k, _ in expected_k]
    assert {v.value: v.count for v in cols["k"].top_values} == dict(expected_k)

    # m: Trino can't order a MAP, so its list falls back to the separate
    # queries, as before; its frequencies are still exact.
    assert sorted(v.count for v in cols["m"].top_values) == sorted(
        c for (c,) in run_sql(f"SELECT count(*) FROM {table} WHERE m IS NOT NULL GROUP BY m")
    )

    # Exact distinct counts, under Trino's default distinct strategy.
    for name in ("id", "k", "g", "x"):
        assert (cols[name].distinct_count, cols[name].distinct_count_estimated) == (
            _exact(table, name)[1],
            False,
        ), name
    assert not any("SET SESSION" in s or "distinct_aggregations" in s for s in statements)

    group_bys = [s for s in _reads(statements, table) if "GROUP BY" in s]
    assert len(group_bys) >= 3
    assert 1 < statements.peak <= TrinoBackend.value_query_concurrency


def _comparable(profile) -> dict:
    """A profile without what Trino doesn't fix: the order of tied top values,
    and float sums' last bits (parallel summation order)."""
    out = {}
    for col in profile.columns:
        numeric = col.numeric
        out[col.name] = (
            col.null_count,
            col.distinct_count,
            col.values,
            sorted((v.count, repr(v.value)) for v in col.top_values)
            if len({v.count for v in col.top_values}) == len(col.top_values)
            else sorted(v.count for v in col.top_values),
            None
            if numeric is None
            else [pytest.approx(getattr(numeric, f)) for f in ("min", "max", "mean", "std")],
        )
    return out


def test_value_queries_give_the_sequential_answers(table, monkeypatch):
    """The concurrent plan and the one-at-a-time plan give the same profile."""
    concurrent = _comparable(kontra.profile(uri(table), preset="interrogate", save=False))
    monkeypatch.setattr(TrinoBackend, "prefetch_value_counts", lambda self, requests: None)
    sequential = _comparable(kontra.profile(uri(table), preset="interrogate", save=False))
    assert concurrent == sequential


# --------------------------------------------------------------------------- #
# Sampling
# --------------------------------------------------------------------------- #


@pytest.fixture(params=[(catalog, buckets) for catalog in CATALOGS[1:] for buckets in (99, 100)])
def files_table(request, trino_container):
    """An Iceberg table with exactly 99 or 100 data files (one per bucket)."""
    catalog, buckets = request.param
    fq = f"{catalog}.{SCHEMA}.files_{buckets}"
    run_sql(
        f"CREATE SCHEMA IF NOT EXISTS {catalog}.{SCHEMA}",
        f"DROP TABLE IF EXISTS {fq}",
        f"CREATE TABLE {fq} WITH (partitioning = ARRAY['bucket(id, {buckets})']) AS "
        "SELECT CAST(i AS bigint) AS id, "
        "CASE WHEN i % 10 = 0 THEN NULL ELSE i % 37 END AS v "
        f"FROM UNNEST(sequence(1, {ROWS})) AS t(i)",
    )
    _, schema, name = fq.split(".")
    ((files,),) = run_sql(f'SELECT count(*) FROM {catalog}.{schema}."{name}$files"')
    assert files == buckets
    yield fq, buckets
    run_sql(f"DROP TABLE IF EXISTS {fq}")


def test_system_sampling_needs_100_data_files(files_table, statements):
    fq, files = files_table
    profile = kontra.profile(uri(fq), preset="scan", sample=500, save=False)

    assert profile.sampled is True and profile.sample_size == 500
    assert (profile.row_count, profile.row_count_estimated) == (ROWS, False)
    for col in profile.columns:
        assert col.null_count_estimated and col.distinct_count_estimated

    (stats,) = [s for s in _reads(statements, fq) if "_kontra_sample" in s]
    if files >= 100:
        # 10% at least: ten files of 100.
        assert "TABLESAMPLE SYSTEM (10.0) LIMIT 500" in stats
    else:
        assert "TABLESAMPLE" not in stats and "LIMIT 500) AS _kontra_sample" in stats
    assert sum("$files" in s for s in statements) == 1


def test_an_empty_system_sample_falls_back_to_the_head(files_table, monkeypatch, statements):
    fq, _ = files_table
    # SYSTEM (0) picks no file, every time.
    monkeypatch.setattr(TrinoBackend, "_sample_percent", lambda self: 0.0)
    profile = kontra.profile(uri(fq), preset="scan", sample=500, save=False)

    sampled = [s for s in statements if "_kontra_sample" in s]
    assert len(sampled) == 2
    assert "TABLESAMPLE SYSTEM (0.0)" in sampled[0]
    assert "TABLESAMPLE" not in sampled[1] and "LIMIT 500) AS _kontra_sample" in sampled[1]
    # The head sample's answers: some of its 500 rows (a tenth of the table's v is NULL).
    v = _by_name(profile)["v"]
    assert 0 < v.null_count < 500 and v.null_count_estimated
    assert profile.row_count == ROWS


def test_no_files_read_off_iceberg(trino_container, statements):
    fq = f"memory.{SCHEMA}.plain"
    run_sql(
        f"CREATE SCHEMA IF NOT EXISTS memory.{SCHEMA}",
        f"DROP TABLE IF EXISTS {fq}",
        f"CREATE TABLE {fq} AS SELECT i AS id FROM UNNEST(sequence(1, 300)) AS t(i)",
    )
    try:
        profile = kontra.profile(uri(fq), preset="scan", sample=50, save=False)
    finally:
        run_sql(f"DROP TABLE IF EXISTS {fq}")
    assert profile.sampled and profile.row_count == 300
    assert not any("$files" in s or "TABLESAMPLE" in s for s in statements)
