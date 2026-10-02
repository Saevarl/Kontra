"""
The Trino scan plan (kontra.engine.executors.trino_sql): one fused scan per
table in both modes, unique rules inside it or as their own GROUP BY by table
size, and the side queries concurrent with it.

Each case compares the pushed-down answers with the Polars tier's, rule by
rule, and counts the queries that read the table.
"""

from __future__ import annotations

import time

import pytest

import kontra
from kontra.connectors import trino_read
from kontra.engine.executors import trino_sql

pytestmark = pytest.mark.integration

CATALOGS = ["iceberg", "iceberg_jdbc"]
SCHEMA = "kontra_scan"
ROWS = 2000

# Every pushable kind, passing and failing.
RULES = [
    {"name": "not_null", "params": {"column": "k"}},
    {"name": "not_null", "params": {"column": "id"}},
    {"name": "unique", "params": {"column": "id"}},
    {"name": "unique", "params": {"column": "email"}},
    {"name": "unique", "params": {"column": "code"}},
    {"name": "allowed_values", "params": {"column": "k", "values": ["a", "b", "c"]}},
    {
        "name": "allowed_values",
        "id": "k_or_null",
        "params": {"column": "k", "values": ["a", "b", "c", None]},
    },
    {"name": "disallowed_values", "params": {"column": "k", "values": ["c"]}},
    {"name": "range", "params": {"column": "qty", "min": 0, "max": 998}},
    {"name": "range", "params": {"column": "amount", "min": 0}},
    {"name": "length", "params": {"column": "email", "min": 8, "max": 11}},
    {"name": "regex", "params": {"column": "email", "pattern": "^u[0-9]+@x[.]com$"}},
    {"name": "contains", "params": {"column": "email", "substring": "@"}},
    {"name": "starts_with", "params": {"column": "email", "prefix": "u1"}},
    {"name": "ends_with", "params": {"column": "email", "suffix": ".com"}},
    {"name": "compare", "params": {"left": "qty", "right": "id", "op": "<="}},
    {"name": "conditional_not_null", "params": {"column": "email", "when": "k == 'a'"}},
    {
        "name": "conditional_range",
        "params": {"column": "qty", "when": "k == 'b'", "min": 0, "max": 500},
    },
    {"name": "min_rows", "params": {"threshold": 1000}},
    {"name": "max_rows", "params": {"threshold": 1500}},
    {"name": "custom_sql_check", "params": {"sql": "SELECT * FROM {table} WHERE qty > 990"}},
]
DATASET = {"min_rows", "max_rows", "custom_sql_check"}


def connect(**kwargs):
    import trino

    return trino.dbapi.connect(host="localhost", port=8095, user="kontra", timezone="UTC", **kwargs)


def run_sql(*statements: str) -> None:
    with connect() as conn:
        cur = conn.cursor()
        for sql in statements:
            cur.execute(sql)
            cur.fetchall()


@pytest.fixture(params=CATALOGS)
def table(request, trino_container):
    catalog = request.param
    fq = f"{catalog}.{SCHEMA}.t"
    run_sql(
        f"CREATE SCHEMA IF NOT EXISTS {catalog}.{SCHEMA}",
        f"DROP TABLE IF EXISTS {fq}",
        f"""CREATE TABLE {fq} AS SELECT
            CAST(CASE WHEN i % 500 = 0 THEN i - 1 ELSE i END AS bigint) AS id,
            CASE WHEN i % 97 = 0 THEN NULL
                 ELSE element_at(ARRAY['a', 'b', 'c'], i % 3 + 1) END AS k,
            CAST(i % 1000 AS integer) AS qty,
            CAST(i * 0.25 - 1 AS decimal(10, 2)) AS amount,
            CASE WHEN i % 50 = 0 THEN NULL ELSE 'u' || CAST(i AS varchar) || '@x.com' END AS email,
            CAST(i % 7 AS integer) AS code
        FROM UNNEST(sequence(1, {ROWS})) AS t(i)""",
    )
    yield catalog, fq
    run_sql(f"DROP TABLE IF EXISTS {fq}")


@pytest.fixture
def statements(monkeypatch):
    """Every statement Kontra sends to Trino, in order."""
    import trino.dbapi

    seen: list[str] = []
    execute = trino.dbapi.Cursor.execute

    def spy(self, operation, params=None):
        seen.append(operation)
        return execute(self, operation, params)

    monkeypatch.setattr(trino.dbapi.Cursor, "execute", spy)
    return seen


def _validate(source, tally, **kwargs):
    rules = [r if r["name"] in DATASET else dict(r, tally=tally) for r in RULES]
    return kontra.validate(source, rules=rules, tally=tally, save=False, preplan="off", **kwargs)


def _reads_of(statements: list[str], fq: str) -> list[str]:
    """Statements that read the table itself (not its $ metadata tables)."""
    catalog, schema, name = fq.split(".")
    quoted = f'"{catalog}"."{schema}"."{name}"'
    return [s for s in statements if quoted in s]


def _assert_matches_polars(result, reference, tally):
    by_ref = {r.rule_id: r for r in reference.rules}
    assert len(result.rules) == len(RULES)
    for rule in result.rules:
        ref = by_ref[rule.rule_id]
        assert rule.source in ("sql", "trino"), rule.rule_id
        assert rule.passed == ref.passed, rule.rule_id
        if tally or rule.rule_id.startswith("DATASET:"):
            assert rule.failed_count == ref.failed_count, rule.rule_id
        else:
            # Fail-fast: a violation is reported as at least 1, as before.
            assert rule.failed_count == (0 if ref.passed else 1), rule.rule_id
    # The contract fails some rules and passes others.
    assert 0 < sum(r.passed for r in result.rules) < len(RULES)


@pytest.mark.parametrize("tally", [True, False])
@pytest.mark.parametrize("unique", ["in the scan", "group by"])
def test_scan_matches_polars(table, statements, monkeypatch, tally, unique):
    catalog, fq = table
    uri = f"trino://kontra@localhost:8095/{catalog}/{SCHEMA}.t"
    if unique == "group by":
        monkeypatch.setattr(trino_sql, "_UNIQUE_IN_SCAN_BELOW", 0)
    reference = _validate(uri, tally, pushdown="off")
    statements.clear()

    result = _validate(uri, tally)

    _assert_matches_polars(result, reference, tally)
    assert not any("EXISTS" in s for s in statements)
    reads = _reads_of(statements, fq)
    fused = [s for s in reads if '"__row_count"' in s]
    grouped = [s for s in reads if "HAVING count(*) > 1" in s]
    assert len(fused) == 1
    # One fused scan, the GROUP BYs, the custom SQL query: nothing else reads the table.
    if unique == "in the scan":
        assert grouped == [] and fused[0].count("COUNT(DISTINCT") == 3
        assert sum(s.startswith("SET SESSION") for s in statements) == 1
    else:
        assert len(grouped) == 3 and "COUNT(DISTINCT" not in fused[0]
        assert not any(s.startswith("SET SESSION") for s in statements)
    assert len(reads) == 1 + len(grouped) + 1


@pytest.mark.parametrize("tally", [True, False])
@pytest.mark.parametrize("connection", ["caller transaction", "caller autocommit"])
def test_callers_connection_matches_polars_and_keeps_its_session(
    table, statements, tally, connection
):
    from trino.transaction import IsolationLevel

    _, fq = table
    level = (
        IsolationLevel.READ_UNCOMMITTED
        if connection == "caller transaction"
        else IsolationLevel.AUTOCOMMIT
    )
    conn = connect(isolation_level=level)
    try:
        before = dict(conn._client_session.properties)
        reference = _validate(conn, tally, table=fq, pushdown="off")
        statements.clear()
        result = _validate(conn, tally, table=fq)
        assert conn._client_session.properties == before
        if conn.transaction is not None:
            conn.commit()
    finally:
        conn.close()

    _assert_matches_polars(result, reference, tally)
    assert not any(s.startswith("SET SESSION") for s in statements)
    assert not any("EXISTS" in s for s in statements)
    # A caller's transaction or pinned snapshot: unique stays in the one scan
    # (small table), under the server's default distinct strategy.
    fused = [s for s in statements if '"__row_count"' in s]
    assert len(fused) == 1 and fused[0].count("COUNT(DISTINCT") == 3


def test_table_size_comes_from_files_once(table, statements, monkeypatch):
    """Without preplan's $files round, one record count sizes the unique strategy."""
    catalog, _ = table
    uri = f"trino://kontra@localhost:8095/{catalog}/{SCHEMA}.t"
    sizes = []
    data_rows = trino_sql.TrinoSqlExecutor._data_rows

    def spy(self, conn, handle):
        rows = data_rows(self, conn, handle)
        sizes.append((rows, trino_read.state_of(handle).files_read))
        return rows

    monkeypatch.setattr(trino_sql.TrinoSqlExecutor, "_data_rows", spy)
    _validate(uri, True)
    assert sizes == [(ROWS, True)]
    assert sum("$files" in s and "record_count" in s for s in statements) == 1


def test_size_from_preplan_files_round_is_reused(table, statements):
    """When preplan read $files, the scan plan reuses its record count: no second read."""
    catalog, _ = table
    uri = f"trino://kontra@localhost:8095/{catalog}/{SCHEMA}.t"
    rules = [
        {"name": "not_null", "params": {"column": "k"}},
        {"name": "unique", "params": {"column": "id"}},
    ]
    result = kontra.validate(uri, rules=rules, save=False)
    files = [s for s in statements if '$files"' in s]
    assert len(files) == 1
    assert any('COUNT(DISTINCT "id")' in s for s in statements)
    assert {r.rule_id: r.passed for r in result.rules} == {
        "COL:k:not_null": False,
        "COL:id:unique": False,
    }


FAILING = [
    {"name": "not_null", "params": {"column": "k"}, "tally": True},
    {"name": "unique", "params": {"column": "code"}, "tally": True},
    {
        "name": "custom_sql_check",
        "id": "bad",
        "params": {"sql": "SELECT * FROM {table} WHERE 1 / (qty - 5) > 0"},
    },
    {
        "name": "custom_sql_check",
        "id": "good",
        "params": {"sql": "SELECT * FROM {table} WHERE qty > 990"},
    },
    # A float bound on an integer column stays in Polars: the frame is fetched after the scan.
    {"name": "range", "params": {"column": "qty", "min": 0.5}, "tally": True},
]


@pytest.fixture
def modes(monkeypatch):
    seen: list[str | None] = []
    begin = trino_read.begin

    def spy(handle):
        begin(handle)
        state = trino_read.state_of(handle)
        seen.append(state.mode if state else None)

    monkeypatch.setattr(trino_read, "begin", spy)
    return seen


def test_failing_custom_sql_leaves_the_concurrent_scan_alone(table, modes):
    """
    In Kontra's transaction, a custom SQL query that fails at run time aborts the
    transaction and the queries beside it. Each failed query runs again alone in
    a new transaction: the failing one is that rule's failure, the rest answer.
    """
    catalog, _ = table
    uri = f"trino://kontra@localhost:8095/{catalog}/{SCHEMA}.t"
    result = kontra.validate(uri, rules=FAILING, tally=True, save=False, preplan="off")
    by_id = {r.rule_id: r for r in result.rules}
    assert not by_id["bad"].passed and "Division by zero" in by_id["bad"].message
    assert by_id["COL:k:not_null"].failed_count == ROWS // 97
    assert by_id["COL:code:unique"].failed_count == ROWS - 7
    assert by_id["good"].failed_count == 2 * 9  # qty 991..999, twice each
    assert by_id["COL:qty:range"].source == "polars"
    assert by_id["COL:qty:range"].failed_count == 2  # qty 0
    assert modes == [trino_read.TRANSACTION]


def test_a_commit_before_the_retry_reruns_the_validation(table, modes, monkeypatch):
    """The retry's new transaction may read a newer state: the guard sees it, and the run reruns."""
    catalog, fq = table
    uri = f"trino://kontra@localhost:8095/{catalog}/{SCHEMA}.t"
    retry = trino_sql.TrinoSqlExecutor._retry_alone
    commits = [f"INSERT INTO {fq} (id, k, qty) VALUES (9999, NULL, 1)"]

    def commit_then_retry(self, conn, task):
        if commits:
            run_sql(commits.pop())
        return retry(self, conn, task)

    monkeypatch.setattr(trino_sql.TrinoSqlExecutor, "_retry_alone", commit_then_retry)
    result = kontra.validate(uri, rules=FAILING, tally=True, save=False, preplan="off")
    by_id = {r.rule_id: r for r in result.rules}
    assert modes == [trino_read.TRANSACTION, trino_read.TRANSACTION]
    # Every rule describes the state with the new row.
    assert by_id["COL:k:not_null"].failed_count == ROWS // 97 + 1
    assert by_id["COL:code:unique"].failed_count == ROWS - 7  # the new row's code is NULL
    assert by_id["good"].failed_count == 2 * 9
    assert by_id["COL:qty:range"].failed_count == 2


ABORTING = [
    {"name": "not_null", "params": {"column": "x"}, "tally": True},
    {
        "name": "custom_sql_check",
        "id": "bad",
        "params": {"sql": "SELECT * FROM {table} WHERE 1/(x-5)>0"},
    },
]


@pytest.fixture(params=["memory", "iceberg_view"])
def stateless(request, trino_container):
    """A relation with no Iceberg read state: a memory table, or a view on an Iceberg table."""
    if request.param == "memory":
        fq, base = "memory.kontra_scan.abort_probe", None
        run_sql("CREATE SCHEMA IF NOT EXISTS memory.kontra_scan", f"DROP TABLE IF EXISTS {fq}")
    else:
        fq, base = f"iceberg.{SCHEMA}.abort_view", f"iceberg.{SCHEMA}.abort_base"
        run_sql(
            f"CREATE SCHEMA IF NOT EXISTS iceberg.{SCHEMA}",
            f"DROP VIEW IF EXISTS {fq}",
            f"DROP TABLE IF EXISTS {base}",
        )
    source = base or fq
    run_sql(f"CREATE TABLE {source} AS SELECT i AS x FROM UNNEST(sequence(1, 10000)) t(i)")
    if base:
        run_sql(f"CREATE VIEW {fq} AS SELECT * FROM {base}")
    yield fq
    if base:
        run_sql(f"DROP VIEW IF EXISTS {fq}")
    run_sql(f"DROP TABLE IF EXISTS {source}")


def test_callers_transaction_without_read_state_runs_custom_sql_after_the_scan(
    stateless, modes, monkeypatch
):
    """
    A caller's transaction on a relation with no Iceberg read state still aborts
    on a failed query. Custom SQL must run after the scan, or a failing query
    takes the scan down with it. The scan is held back so that, run beside it,
    the custom query would always fail first.
    """
    from trino.transaction import IsolationLevel

    run = trino_sql.TrinoSqlExecutor._run

    def slow_scan(conn, sql):
        if '"__row_count"' in sql:
            time.sleep(0.3)
        return run(conn, sql)

    monkeypatch.setattr(trino_sql.TrinoSqlExecutor, "_run", staticmethod(slow_scan))
    with connect(isolation_level=IsolationLevel.READ_UNCOMMITTED) as conn:
        result = kontra.validate(
            conn, table=stateless, rules=ABORTING, tally=True, save=False, preplan="off"
        )
    by_id = {r.rule_id: r for r in result.rules}
    assert modes == [None]
    assert by_id["COL:x:not_null"].passed and by_id["COL:x:not_null"].failed_count == 0
    assert not by_id["bad"].passed and "Division by zero" in by_id["bad"].message
