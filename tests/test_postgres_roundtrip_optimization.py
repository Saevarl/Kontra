from contextlib import contextmanager
from types import SimpleNamespace


class _Cursor:
    def __init__(self, row, columns):
        self._row = row
        self.description = [(column,) for column in columns]
        self.statements = []

    def execute(self, sql, params=None):
        self.statements.append((sql, params))

    def fetchone(self):
        return self._row

    def close(self):
        pass


class _Connection:
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor


def _executor_with_cursor(cursor):
    from kontra.engine.executors.postgres_sql import PostgresSqlExecutor

    executor = PostgresSqlExecutor()

    @contextmanager
    def connection_ctx(_handle):
        yield _Connection(cursor)

    executor._get_connection_ctx = connection_ctx
    executor._get_table_reference = lambda _handle: '"public"."tiny"'
    return executor


def test_postgres_tally_folds_row_count_into_aggregate_query():
    cursor = _Cursor((2, 10), ["email_unique", "__row_count"])
    executor = _executor_with_cursor(cursor)
    plan = executor.compile(
        [{
            "kind": "unique",
            "column": "email",
            "rule_id": "email_unique",
            "tally": True,
        }]
    )

    result = executor.execute(object(), plan)

    assert len(cursor.statements) == 1
    assert 'COUNT(*) AS "__row_count"' in cursor.statements[0][0]
    assert result["row_count"] == 10
    assert [item["rule_id"] for item in result["results"]] == ["email_unique"]
    assert result["results"][0]["failed_count"] == 2


def test_postgres_fail_fast_does_not_add_row_count_to_exists_query():
    cursor = _Cursor((True,), ["email_not_null"])
    executor = _executor_with_cursor(cursor)
    plan = executor.compile(
        [{
            "kind": "not_null",
            "column": "email",
            "rule_id": "email_not_null",
            "tally": False,
        }]
    )

    result = executor.execute(object(), plan)

    assert len(cursor.statements) == 1
    assert 'AS "__row_count"' not in cursor.statements[0][0]
    assert result["row_count"] is None
    assert result["results"][0]["failed_count"] == 1


def test_pushdown_uses_preplan_estimate_and_plan_columns(monkeypatch):
    from kontra.engine.phases.pushdown import execute_pushdown
    import kontra.engine.executors.registry as registry

    class Executor:
        name = "postgres"

        def compile(self, specs):
            return {"specs": specs}

        def execute(self, handle, plan, **kwargs):
            return {
                "results": [{
                    "rule_id": "email_not_null",
                    "passed": False,
                    "failed_count": 1,
                }],
                "row_count": None,
                "staging": None,
            }

        def introspect(self, handle, **kwargs):
            raise AssertionError("plan columns and reltuples should avoid introspection")

    monkeypatch.setattr(registry, "pick_executor", lambda handle, specs: Executor())
    compiled = SimpleNamespace(
        sql_rules=[{
            "kind": "not_null",
            "column": "email",
            "rule_id": "email_not_null",
        }],
        required_cols=["email"],
    )
    ctx = SimpleNamespace(
        compiled_full=compiled,
        tally_map={"email_not_null": False},
        severity_map={"email_not_null": "blocking"},
    )

    result, _handle, _tmpdir = execute_pushdown(
        handle=object(),
        ctx=ctx,
        handled_ids_meta=set(),
        pushdown_mode="on",
        csv_mode="auto",
        preplan_total_rows=123,
    )

    assert result.row_count == 123
    assert result.available_cols == ["email"]
    assert result.results_by_id["email_not_null"]["failed_count"] == 1


def test_postgres_preplan_skips_rules_without_supported_metadata_predicates(
    monkeypatch,
):
    from kontra.engine.phases.preplan import _execute_postgres_preplan
    import kontra.preplan.postgres as postgres_preplan

    rule = SimpleNamespace(
        name="regex",
        rule_id="email_regex",
        params={"column": "email", "pattern": ".*@.*"},
    )
    ctx = SimpleNamespace(
        rules=[rule],
        compiled_full=SimpleNamespace(required_cols=["email"]),
        tally_map={"email_regex": False},
        severity_map={"email_regex": "blocking"},
    )
    handle = SimpleNamespace(
        scheme="postgres",
        db_params=SimpleNamespace(schema="public", table="tiny"),
    )
    monkeypatch.setattr(
        postgres_preplan,
        "preplan_postgres",
        lambda **kwargs: (_ for _ in ()).throw(
            AssertionError("ineligible rules should not query PostgreSQL catalogs")
        ),
    )

    result = _execute_postgres_preplan(handle, ctx)

    assert result.effective is False
    assert result.handled_ids == set()
