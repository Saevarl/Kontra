"""
One table state per Trino validation (kontra.connectors.trino_read).

A writer commits between Kontra's own queries, on schedule, and every rule of
the validation must still describe one table state: the pushed-down
``not_null``, the same predicate as custom SQL, and a rule the Polars tier
measures from a fetched frame. Or the validation raises; it never mixes states.

Trino's Iceberg connector holding one table state per transaction is measured,
not documented, so these run on two catalog types (file metastore and JDBC) and
belong in every Trino version bump.
"""

from __future__ import annotations

import threading
from typing import ClassVar

import pytest

import kontra
from kontra import rules
from kontra.connectors import trino_read
from kontra.engine.executors.trino_sql import TrinoSqlExecutor
from kontra.errors import DataError

pytestmark = pytest.mark.integration

CATALOGS = ["iceberg", "iceberg_jdbc"]
SCHEMA = "kontra_consistency"

# Null count of x, three ways: pushed-down SQL, custom SQL, and the Polars tier
# (a float bound stays in Polars for an integer column, so the frame is fetched).
CHECKS = [
    rules.not_null("x"),
    rules.custom_sql_check("SELECT * FROM {table} WHERE x IS NULL"),
    rules.range("x", min=0.5),
]


def _null_counts(result) -> tuple[int, int, int]:
    by_id = {r.rule_id: r for r in result.rules}
    assert by_id["COL:x:range"].source == "polars"
    return (
        by_id["COL:x:not_null"].failed_count,
        by_id["DATASET:custom_sql_check"].failed_count,
        by_id["COL:x:range"].failed_count,
    )


def connect(**kwargs):
    import trino

    return trino.dbapi.connect(host="localhost", port=8095, user="kontra", timezone="UTC", **kwargs)


def run_sql(*statements: str) -> None:
    with connect() as conn:
        cur = conn.cursor()
        for sql in statements:
            cur.execute(sql)
            cur.fetchall()


def _uri(catalog: str, table: str) -> str:
    return f"trino://kontra@localhost:8095/{catalog}/{SCHEMA}.{table}"


def _snapshot(catalog: str, table: str) -> int:
    with connect() as conn:
        cur = conn.cursor()
        cur.execute(
            f"SELECT snapshot_id FROM {catalog}.{SCHEMA}.\"{table}$refs\" WHERE name = 'main'"
        )
        return cur.fetchall()[0][0]


def _scalar(sql: str):
    with connect() as conn:
        cur = conn.cursor()
        cur.execute(sql)
        return cur.fetchall()[0][0]


@pytest.fixture(params=CATALOGS)
def catalog(request, trino_container):
    run_sql(f"CREATE SCHEMA IF NOT EXISTS {request.param}.{SCHEMA}")
    yield request.param


@pytest.fixture
def table(catalog):
    """A fresh table: x = 1 and x = NULL, then a column added (metadata-only)."""
    fq = f"{catalog}.{SCHEMA}.t"
    run_sql(
        f"DROP TABLE IF EXISTS {fq}",
        f"CREATE TABLE {fq} (x integer)",
        f"INSERT INTO {fq} VALUES (1)",
    )
    before_null = _snapshot(catalog, "t")
    run_sql(f"INSERT INTO {fq} VALUES (NULL)", f"ALTER TABLE {fq} ADD COLUMN y integer")
    yield fq, before_null
    run_sql(f"DROP TABLE IF EXISTS {fq}")


@pytest.fixture
def modes(monkeypatch):
    """The read mode of every validation attempt, in order."""
    seen: list[str | None] = []
    begin = trino_read.begin

    def spy(handle):
        begin(handle)
        state = trino_read.state_of(handle)
        seen.append(state.mode if state else None)

    monkeypatch.setattr(trino_read, "begin", spy)
    return seen


@pytest.fixture
def writer(monkeypatch):
    """
    Commit writer statements from a separate connection between Kontra's queries.

    ``schedule(before_scan, before_custom_sql, every_attempt)``: the first list
    commits after the validation read its starting state and before the pushed-
    down scan; the second after the scan and before custom SQL. The Polars
    fetch runs after both.
    """
    plan = {"scan": [], "custom": [], "every": False, "attempts": 0}
    execute = TrinoSqlExecutor.execute
    custom = TrinoSqlExecutor._execute_custom_sql_queries

    def first_or_every() -> bool:
        return plan["every"] or plan["attempts"] == 1

    def patched_execute(self, handle, compiled_plan, **kwargs):
        plan["attempts"] += 1
        if first_or_every():
            run_sql(*plan["scan"])
        return execute(self, handle, compiled_plan, **kwargs)

    def patched_custom(self, cursor, handle, specs):
        if first_or_every():
            run_sql(*plan["custom"])
        return custom(self, cursor, handle, specs)

    monkeypatch.setattr(TrinoSqlExecutor, "execute", patched_execute)
    monkeypatch.setattr(TrinoSqlExecutor, "_execute_custom_sql_queries", patched_custom)

    def schedule(before_scan=(), before_custom_sql=(), every_attempt=False):
        plan.update(scan=list(before_scan), custom=list(before_custom_sql), every=every_attempt)
        return plan

    return schedule


# The writer's operations, six cases. Each list commits in order: the
# first statement before the scan, the rest before custom SQL.
def _cases(fq: str, before_null: int) -> dict[str, list[str]]:
    rollback = f"ALTER TABLE {fq} EXECUTE rollback_to_snapshot({before_null})"
    return {
        "insert NULL": [f"INSERT INTO {fq} (x) VALUES (NULL)"],
        "rollback to the snapshot before the NULL": [rollback],
        "rollback, then insert NULL": [rollback, f"INSERT INTO {fq} (x, y) VALUES (NULL, NULL)"],
        "add column": [f"ALTER TABLE {fq} ADD COLUMN z integer"],
        "widen x to bigint": [f"ALTER TABLE {fq} ALTER COLUMN x SET DATA TYPE bigint"],
        "delete the NULL rows": [f"DELETE FROM {fq} WHERE x IS NULL"],
    }


CASE_NAMES = list(_cases("t", 0))


class TestOneTransaction:
    """Kontra's own connection, and a caller's transactional one: one state, whatever the writer does."""

    @pytest.mark.parametrize("case", CASE_NAMES)
    @pytest.mark.parametrize("connection", ["owned", "caller transaction"])
    def test_scheduled_writer(self, catalog, table, modes, writer, case, connection):
        fq, before_null = table
        steps = _cases(fq, before_null)[case]
        writer(before_scan=steps[:1], before_custom_sql=steps[1:])

        if connection == "owned":
            result = kontra.validate(_uri(catalog, "t"), rules=CHECKS, tally=True, save=False)
            assert modes == [trino_read.TRANSACTION]
        else:
            from trino.transaction import IsolationLevel

            conn = connect_in_transaction(IsolationLevel.READ_UNCOMMITTED)
            try:
                result = kontra.validate(conn, table=fq, rules=CHECKS, tally=True, save=False)
                conn.commit()
            finally:
                conn.close()
            assert modes == [trino_read.CALLER_TRANSACTION]

        # The table had one NULL when the validation started; every rule reads that state.
        assert _null_counts(result) == (1, 1, 1)

    def test_continuous_writer(self, catalog, table):
        fq, _ = table
        stop = threading.Event()
        commits = [0]

        def write():
            while not stop.is_set():
                run_sql(f"INSERT INTO {fq} (x) VALUES (NULL), (1)")
                commits[0] += 1

        thread = threading.Thread(target=write)
        thread.start()
        try:
            counts = [
                _null_counts(
                    kontra.validate(_uri(catalog, "t"), rules=CHECKS, tally=True, save=False)
                )
                for _ in range(20)
            ]
        finally:
            stop.set()
            thread.join()
        assert commits[0] >= 20, "the writer must commit during the validations"
        disagreements = [c for c in counts if len(set(c)) != 1]
        assert disagreements == []

    def test_caller_transaction_is_left_open(self, catalog, table, modes):
        from trino.transaction import IsolationLevel

        fq, _ = table
        conn = connect_in_transaction(IsolationLevel.READ_UNCOMMITTED)
        try:
            transaction = conn.transaction
            transaction_id = transaction.id
            kontra.validate(conn, table=fq, rules=CHECKS, tally=True, save=False)
            # Neither committed nor rolled back: the same transaction is still open.
            assert conn.transaction is transaction
            assert transaction.id == transaction_id
            conn.commit()
        finally:
            conn.close()
        assert modes == [trino_read.CALLER_TRANSACTION]

    def test_guard_that_fires_reruns_then_raises(self, catalog, table, modes, monkeypatch):
        """A catalog whose transaction didn't hold one state: the guard sees a new file every read."""
        newest = trino_read._newest_entry
        reads = [0]

        def moving(conn, parts):
            reads[0] += 1
            entry = newest(conn, parts)
            return (f"{entry[0]}#{reads[0]}",) + tuple(entry[1:])

        monkeypatch.setattr(trino_read, "_newest_entry", moving)
        with pytest.raises(DataError, match="changed during validation"):
            kontra.validate(_uri(catalog, "t"), rules=CHECKS, tally=True, save=False)
        assert modes == [trino_read.TRANSACTION, trino_read.TRANSACTION]


@pytest.fixture
def custom_agg():
    """A custom rule that pushes ``agg`` to Trino as a custom_agg spec."""
    from kontra.rule_defs.base import BaseRule
    from kontra.rule_defs.registry import RULE_REGISTRY, register_rule

    name = "consistency_custom_agg"

    @register_rule(name)
    class _Rule(BaseRule):
        def validate(self, df):  # pragma: no cover - the rule pushes down
            raise AssertionError("expected SQL pushdown")

        def to_sql_spec(self):
            return {
                "kind": "custom_agg",
                "rule_id": self.rule_id,
                "sql_agg": {"trino": self.params["agg"]},
            }

    yield lambda agg: {"name": name, "params": {"agg": agg}}
    RULE_REGISTRY.pop(name, None)


def connect_in_transaction(level):
    conn = connect(isolation_level=level)
    cur = conn.cursor()  # opens the caller's transaction
    cur.execute("SELECT 1")
    cur.fetchall()
    return conn


class TestAutocommitCaller:
    """A caller's autocommit connection: pinned when pinnable, else bracketed, never mixed."""

    def _validate(self, fq, checks=CHECKS):
        conn = connect()
        try:
            return kontra.validate(conn, table=fq, rules=checks, tally=True, save=False)
        finally:
            conn.close()

    def test_guard_fires_twice_raises(self, table, modes, writer):
        """
        A rollback and an insert between queries, on both attempts.

        The table's last change is ADD COLUMN, so it isn't pinnable. The rollback
        commits last, so the rerun isn't pinnable either. Unguarded, the scan
        would count 2 NULLs and custom SQL 0.
        """
        fq, before_null = table
        writer(
            before_scan=[f"INSERT INTO {fq} (x, y) VALUES (NULL, NULL)"],
            before_custom_sql=[f"ALTER TABLE {fq} EXECUTE rollback_to_snapshot({before_null})"],
            every_attempt=True,
        )
        with pytest.raises(DataError, match="isolation level"):
            self._validate(fq)
        assert modes == [trino_read.BRACKETED, trino_read.BRACKETED]

    def test_guard_fires_once_reruns(self, table, modes, writer):
        fq, _ = table
        writer(before_custom_sql=[f"INSERT INTO {fq} (x) VALUES (NULL)"])
        counts = _null_counts(self._validate(fq))
        # The rerun reads the table after the insert, in one state.
        assert counts == (2, 2, 2)
        assert modes == [trino_read.BRACKETED, trino_read.PINNED]

    def test_pinned_ignores_a_commit_mid_run(self, catalog, table, modes, writer):
        """Without user SQL every read is pinned, so a commit mid-run changes nothing."""
        fq, _ = table
        run_sql(f"INSERT INTO {fq} (x, y) VALUES (5, 5)")  # newest file is a data commit again
        writer(before_scan=[f"INSERT INTO {fq} (x) VALUES (NULL)"])
        by_id = {r.rule_id: r for r in self._validate(fq, [CHECKS[0], CHECKS[2]]).rules}
        assert by_id["COL:x:not_null"].failed_count == 1
        assert by_id["COL:x:range"].failed_count == 1
        assert by_id["COL:x:range"].source == "polars"
        assert modes == [trino_read.PINNED]

    def test_pinned_custom_sql_naming_the_table_reruns(self, catalog, modes, writer):
        """
        Custom SQL can name the table directly, past the pin, so a pinned run
        that pushes custom SQL keeps the guard.

        (id=1, x=10), then (id=2, x=10) commits before custom SQL. Unguarded,
        unique(x) reads 0 at the pin and the self-subquery 1 across states.
        """
        fq = f"{catalog}.{SCHEMA}.dup"
        run_sql(
            f"DROP TABLE IF EXISTS {fq}",
            f"CREATE TABLE {fq} (id integer, x integer)",
            f"INSERT INTO {fq} VALUES (1, 10)",
        )
        writer(before_custom_sql=[f"INSERT INTO {fq} VALUES (2, 10)"])
        checks = [
            rules.unique("x"),
            rules.custom_sql_check(
                f"SELECT * FROM {{table}} a WHERE EXISTS "
                f"(SELECT 1 FROM {fq} b WHERE b.x = a.x AND b.id <> a.id)"
            ),
        ]
        try:
            by_id = {r.rule_id: r for r in self._validate(fq, checks).rules}
        finally:
            run_sql(f"DROP TABLE IF EXISTS {fq}")
        # The rerun is pinned to the insert, and both rules read it.
        assert by_id["COL:x:unique"].failed_count == 1
        assert by_id["DATASET:custom_sql_check"].failed_count == 2
        assert modes == [trino_read.PINNED, trino_read.PINNED]

    def test_pinned_custom_agg_naming_the_table_reruns(self, catalog, modes, writer, custom_agg):
        """A custom rule's SQL aggregate can name the table too, past the pin."""
        fq = f"{catalog}.{SCHEMA}.dup"
        run_sql(
            f"DROP TABLE IF EXISTS {fq}",
            f"CREATE TABLE {fq} (id integer, x integer)",
            f"INSERT INTO {fq} VALUES (1, 10)",
        )
        writer(before_scan=[f"INSERT INTO {fq} VALUES (2, 10)"])
        agg = (
            f"SUM(CASE WHEN x IN (SELECT x FROM {fq} GROUP BY x HAVING count(*) > 1) "
            "THEN 1 ELSE 0 END)"
        )
        try:
            by_id = {r.rule_id: r for r in self._validate(fq, [custom_agg(agg)]).rules}
        finally:
            run_sql(f"DROP TABLE IF EXISTS {fq}")
        (result,) = by_id.values()
        assert result.source == "sql"
        # Unguarded: 1 (the pinned row, against the current duplicates).
        assert result.failed_count == 2
        assert modes == [trino_read.PINNED, trino_read.PINNED]

    # Schema-only changes. Each must match the current table (the schema the
    # caller sees), and pin only when the snapshot reads under that schema.
    SCHEMA_CASES: ClassVar[dict[str, tuple[list[str], str, str]]] = {
        "no change": ([], "x", trino_read.PINNED),
        "widen x": (["ALTER TABLE {T} ALTER COLUMN x SET DATA TYPE bigint"], "x", "bracketed"),
        "add y": (["ALTER TABLE {T} ADD COLUMN y integer"], "y", "bracketed"),
        "rename z to w": (["ALTER TABLE {T} RENAME COLUMN z TO w"], "w", "bracketed"),
        "drop and re-add z": (
            ["ALTER TABLE {T} DROP COLUMN z", "ALTER TABLE {T} ADD COLUMN z varchar"],
            "z",
            "bracketed",
        ),
        "table property only": (
            ["ALTER TABLE {T} SET PROPERTIES max_commit_retry = 5"],
            "x",
            "bracketed",
        ),
        "add y, then insert": (
            ["ALTER TABLE {T} ADD COLUMN y integer", "INSERT INTO {T} VALUES (3, 'c', 7)"],
            "y",
            trino_read.PINNED,
        ),
        "rollback to before add y": (
            [
                "ALTER TABLE {T} ADD COLUMN y integer",
                "INSERT INTO {T} VALUES (3, 'c', 7)",
                "ALTER TABLE {T} EXECUTE rollback_to_snapshot({S0})",
            ],
            "x",
            "bracketed",
        ),
    }

    @pytest.mark.parametrize("case", list(SCHEMA_CASES))
    def test_schema_only_changes(self, catalog, modes, case):
        alters, column, mode = self.SCHEMA_CASES[case]
        fq = f"{catalog}.{SCHEMA}.s"
        run_sql(
            f"DROP TABLE IF EXISTS {fq}",
            f"CREATE TABLE {fq} (x integer, z varchar)",
            f"INSERT INTO {fq} VALUES (1, 'a'), (2, NULL)",
        )
        try:
            s0 = _snapshot(catalog, "s")
            run_sql(*(a.format(T=fq, S0=s0) for a in alters))
            truth = _scalar(f'SELECT count_if("{column}" IS NULL) FROM {fq}')
            checks = [
                rules.not_null(column),
                rules.custom_sql_check(f'SELECT * FROM {{table}} WHERE "{column}" IS NULL'),
            ]
            by_id = {r.rule_id: r for r in self._validate(fq, checks).rules}
        finally:
            run_sql(f"DROP TABLE IF EXISTS {fq}")
        assert by_id[f"COL:{column}:not_null"].failed_count == truth
        assert by_id["DATASET:custom_sql_check"].failed_count == truth
        assert modes == [mode]

    def test_table_reference_without_catalog(self, catalog, table, modes):
        """'schema.table' on a caller's connection: the session catalog decides."""
        conn = connect(catalog=catalog)
        try:
            result = kontra.validate(
                conn, table=f"{SCHEMA}.t", rules=CHECKS, tally=True, save=False
            )
        finally:
            conn.close()
        assert _null_counts(result) == (1, 1, 1)
        assert modes == [trino_read.BRACKETED]

    def test_decimal_type_strings_match(self, catalog, modes):
        """The client describes decimal(12, 2); information_schema says decimal(12,2)."""
        fq = f"{catalog}.{SCHEMA}.d"
        run_sql(
            f"DROP TABLE IF EXISTS {fq}",
            f"CREATE TABLE {fq} (amount decimal(12, 2))",
            f"INSERT INTO {fq} VALUES (1.50), (NULL)",
        )
        try:
            by_id = {r.rule_id: r for r in self._validate(fq, [rules.not_null("amount")]).rules}
        finally:
            run_sql(f"DROP TABLE IF EXISTS {fq}")
        assert by_id["COL:amount:not_null"].failed_count == 1
        assert modes == [trino_read.PINNED]


@pytest.mark.parametrize("connection", ["owned", "caller transaction", "caller autocommit"])
@pytest.mark.parametrize("kind", ["view", "materialized view"])
def test_views_run_as_before(catalog, table, modes, connection, kind):
    """A view reads its base tables, and a stale materialized view its definition: no held state."""
    from trino.transaction import IsolationLevel

    if kind == "materialized view" and catalog == "iceberg_jdbc":
        pytest.skip("Iceberg JDBC catalogs have no materialized views")
    fq, _ = table
    view = f"{catalog}.{SCHEMA}.v"
    create = "CREATE MATERIALIZED VIEW" if kind == "materialized view" else "CREATE VIEW"
    drop = "DROP MATERIALIZED VIEW" if kind == "materialized view" else "DROP VIEW"
    run_sql(f"{create} {view} AS SELECT * FROM {fq}")
    try:
        if connection == "owned":
            result = kontra.validate(_uri(catalog, "v"), rules=CHECKS, tally=True, save=False)
        else:
            level = (
                IsolationLevel.READ_UNCOMMITTED
                if connection == "caller transaction"
                else IsolationLevel.AUTOCOMMIT
            )
            conn = connect(isolation_level=level)
            try:
                result = kontra.validate(conn, table=view, rules=CHECKS, tally=True, save=False)
                if conn.transaction is not None:
                    conn.commit()
            finally:
                conn.close()
    finally:
        run_sql(f"{drop} IF EXISTS {view}")
    assert _null_counts(result) == (1, 1, 1)
    assert modes == [None]


def test_other_catalogs_run_as_before(trino_users_uri, modes):
    """The memory connector has no snapshots: no transaction, no pin."""
    kontra.validate(trino_users_uri, rules=[rules.not_null("email")], save=False)
    assert modes == [None]
