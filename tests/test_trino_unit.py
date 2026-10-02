"""Trino connector unit tests (no Trino server needed)."""

from __future__ import annotations

import math
import subprocess
import sys
from types import SimpleNamespace
from typing import ClassVar

import polars as pl
import pytest

from kontra.connectors.trino import resolve_connection_params
from kontra.engine.sql_ir import trino_regex


class TestTrinoUri:
    def test_catalog_schema_dot_table(self):
        p = resolve_connection_params("trino://alice@trino.example.com:8080/lake/sales.orders")
        assert (p.host, p.port, p.user, p.password) == ("trino.example.com", 8080, "alice", None)
        assert (p.catalog, p.schema, p.table) == ("lake", "sales", "orders")
        assert p.secure is False
        assert p.connect_kwargs()["http_scheme"] == "http"
        assert p.connect_kwargs()["timezone"] == "UTC"

    def test_catalog_schema_table_path(self):
        p = resolve_connection_params("trino://alice@host/lake/sales/orders")
        assert (p.catalog, p.schema, p.table, p.port) == ("lake", "sales", "orders", 8080)

    def test_https_scheme_and_password(self):
        p = resolve_connection_params("trinos://alice:s%40cret@host/lake/sales.orders")
        assert p.secure is True and p.port == 443
        assert p.password == "s@cret"
        assert p.connect_kwargs()["http_scheme"] == "https"

    @pytest.mark.parametrize(
        "uri",
        ["trino://host/lake", "trino://host/lake/orders", "trino://host/lake/a/b/c"],
    )
    def test_missing_parts_raise(self, uri):
        with pytest.raises(ValueError, match="catalog, schema and table"):
            resolve_connection_params(uri)

    @pytest.mark.parametrize(
        "uri",
        ["trinos://alice:example-password@host/lake/orders", "trinos://alice:ex%40mple@host/lake"],
    )
    def test_uri_error_masks_password(self, uri):
        with pytest.raises(ValueError) as exc:
            resolve_connection_params(uri)
        message = str(exc.value)
        assert "example-password" not in message and "ex%40mple" not in message
        assert "ex@mple" not in message and "alice:***@host" in message

    def test_handle_and_paths(self):
        from kontra.connectors.handle import DatasetHandle
        from kontra.engine.paths import get_database_type

        h = DatasetHandle.from_uri("trino://alice@host:8080/lake/sales.orders")
        assert h.scheme == "trino" and h.format == "trino"
        assert h.db_params.catalog == "lake"
        assert get_database_type("trinos://host/lake/s.t") == "trino"

    def test_named_datasource_uri(self, tmp_path):
        from kontra.config.settings import KontraConfig, resolve_datasource

        cfg = KontraConfig.model_validate(
            {
                "datasources": {
                    "lake": {
                        "type": "trino",
                        "host": "trino.example.com",
                        "port": 443,
                        "user": "alice",
                        "password": "p@ss",
                        "catalog": "iceberg",
                        "secure": True,
                        "tables": {"orders": "sales.orders"},
                    }
                }
            }
        )
        uri = resolve_datasource("lake.orders", config=cfg)
        assert uri == "trinos://alice:p%40ss@trino.example.com:443/iceberg/sales.orders"
        p = resolve_connection_params(uri)
        assert (p.password, p.catalog, p.schema, p.table) == ("p@ss", "iceberg", "sales", "orders")

    def test_detects_trino_connection(self):
        from kontra.connectors.detection import detect_connection_dialect

        fake = type("Connection", (), {"__module__": "trino.dbapi"})()
        assert detect_connection_dialect(fake) == "trino"


class TestTrinoRegexSubset:
    @pytest.mark.parametrize(
        "pattern, expected",
        [
            ("^abc$", "^abc\\z"),
            ("^[a-z0-9._-]+@[a-z]+[.]com$", "^[a-z0-9._-]+@[a-z]+[.]com\\z"),
            (r"a\.b", r"a\.b"),
            (r"a\$", r"a\$"),
            ("a{2,3}?", "a{2,3}?"),
            (r"\x41\t", r"\x41\t"),
            ("(?:ab|cd)+", "(?:ab|cd)+"),
            ("[^$]", "[^$]"),
            ("[a-z-]", "[a-z-]"),
            ("[-a-z]", "[-a-z]"),
            ("[^-a]", "[^-a]"),
            (r"[a\-z]", r"[a\-z]"),
        ],
    )
    def test_accepted(self, pattern, expected):
        assert trino_regex(pattern) == expected

    @pytest.mark.parametrize(
        "pattern",
        [
            r"\w+",  # Unicode in Polars, ASCII in Java
            r"\d",
            r"\s",
            r"\bx",
            r"\p{L}",
            "(?i)abc",  # inline flags
            "(?m)^a",
            "(?<name>a)",  # named group
            "(?=a)",  # lookaround
            r"(a)\1",  # backreference
            "a*+",  # possessive
            "a**",
            "[[:alpha:]]",  # POSIX class
            "[a-z&&[^b]]",  # set operations
            "[a--b]",
            "[a-z-A-Z]",  # '-' after a range: literal in Rust, not in Java
            "[a-c-x]",
            r"[\x41-Z]",  # escaped range endpoint
            r"[a-\x5a]",
            "[z-a]",  # reversed range
            "[]a]",  # leading ']'
            "a]",
            "a{,2}",
            "(a",
            "a)",
            r"\Z",
            r"\Q.\E",
        ],
    )
    def test_rejected(self, pattern):
        assert trino_regex(pattern) is None


class TestTrinoRendering:
    def test_freshness_uses_bigint_date_add(self):
        from kontra.engine.sql_utils import agg_freshness

        sql = agg_freshness("ts", 3_153_600_000, "r", "trino")
        assert "date_add('second', -3153600000, current_timestamp)" in sql

    def test_regex_renders_translated_pattern(self):
        from kontra.engine.sql_utils import agg_regex

        sql = agg_regex("email", "^it's$", "r", "trino")
        assert "regexp_like(\"email\", '^it''s\\z')" in sql

    def test_custom_sql_table_placeholder(self):
        from kontra.engine.sql_validator import replace_table_placeholder

        sql = replace_table_placeholder("SELECT * FROM {table}", "lake.sales", "orders", "trino")
        assert sql == 'SELECT * FROM "lake"."sales"."orders"'


_FAMILIES = {
    "id": "integer",
    "amount": "decimal",
    "name": "varchar",
    "code": "char",
    "score": "float",
    "score2": "float",
    "day": "date",
    "ts": "timestamp",
    "tsz": "timestamptz",
    "flag": "boolean",
    "x": "integer",
}


class _ScanConn:
    """A fake Trino connection for the scan plan: records statements and concurrency."""

    def __init__(self, files_rows=10, delay=0.0):
        import threading

        self.sql: list[str] = []
        self.files_rows = files_rows
        self.delay = delay
        self.in_flight = 0
        self.max_in_flight = 0
        self._lock = threading.Lock()
        self.events: list[tuple[str, str]] = []  # ("start" | "end", sql)
        self.fail: dict[str, int] = {}  # sql substring -> times to fail
        self.transaction = object()
        self.rollbacks = 0

    def cursor(self):
        return _ScanCursor(self)

    def rollback(self):
        self.rollbacks += 1
        self.transaction = None


class _ScanCursor:
    def __init__(self, conn):
        self.conn = conn
        self.description = None
        self._rows = []

    def execute(self, sql):
        import re
        import time

        c = self.conn
        if c.transaction is None:
            c.transaction = object()  # the client starts a new transaction
        with c._lock:
            c.sql.append(sql)
            c.events.append(("start", sql))
            c.in_flight += 1
            c.max_in_flight = max(c.max_in_flight, c.in_flight)
        time.sleep(c.delay)
        with c._lock:
            c.in_flight -= 1
            c.events.append(("end", sql))
            failing = [k for k, n in c.fail.items() if k in sql and n > 0]
            for k in failing:
                c.fail[k] -= 1
        if failing:
            raise RuntimeError(f"failed: {sql}")
        if sql.startswith("SET SESSION"):
            self._rows, self.description = [], None
        elif "information_schema.columns" in sql:
            self._rows = [["a", "integer"], ["b", "varchar"]]
            self.description = [("column_name",), ("data_type",)]
        elif "$files" in sql:
            self._rows, self.description = [[c.files_rows]], [("_col0",)]
        elif sql.startswith("SELECT coalesce(sum(n - 1), 0)"):
            alias = re.search(r'AS "([^"]+)" FROM', sql).group(1)
            self._rows, self.description = [[2]], [(alias,)]
        elif sql.startswith(("SELECT COUNT(*) FROM", "SELECT COUNT(*) AS")):
            self._rows, self.description = [[0]], [("_col0",)]
        else:  # the fused scan: 3 violations per rule, 10 rows
            head = sql[: sql.rindex(" FROM ")]
            aliases = re.findall(r'AS "([^"]+)"', head)
            self._rows = [[10 if a == "__row_count" else 3 for a in aliases]]
            self.description = [(a,) for a in aliases]

    def fetchall(self):
        return self._rows

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def close(self):
        pass


class TestTrinoScanPlan:
    """One fused scan in both modes, unique by table size, concurrent side queries."""

    SPECS: ClassVar[list[dict]] = [
        {"kind": "not_null", "column": "a", "rule_id": "nn", "tally": False},
        {"kind": "range", "column": "a", "min": 0, "rule_id": "rg", "tally": True},
        {"kind": "unique", "column": "b", "rule_id": "uq", "tally": False},
        {"kind": "min_rows", "threshold": 5, "rule_id": "mr", "tally": False},
        {
            "kind": "custom_agg",
            "rule_id": "ca",
            "tally": False,
            "sql_agg": {"trino": 'count_if("a" > 100)'},
            "sql_exists": {"trino": '"a" < 0'},
        },
        {"kind": "custom_sql_check", "sql": "SELECT * FROM {table} WHERE a < 0", "rule_id": "cs"},
    ]

    @staticmethod
    def _run(conn, mode="transaction", data_rows=None, specs=None, held=True):
        from kontra.connectors import trino_read
        from kontra.connectors.handle import DatasetHandle
        from kontra.engine.executors.trino_sql import TrinoSqlExecutor

        handle = DatasetHandle.from_uri("trino://u@host/lake/s.t")
        object.__setattr__(handle, "owned_conn", conn)
        if held:
            state = trino_read.TrinoReadState(
                mode, [("a", "integer", True), ("b", "varchar", True)], data_rows=data_rows
            )
            trino_read._set_state(handle, state)
        ex = TrinoSqlExecutor()
        out = ex.execute(handle, ex.compile(specs or TestTrinoScanPlan.SPECS))
        return out, handle

    @staticmethod
    def _table_reads(conn):
        return [q for q in conn.sql if 'FROM "lake"."s"."t"' in q]

    def test_one_fused_scan_in_fail_fast(self):
        conn = _ScanConn()
        out, _ = self._run(conn, data_rows=10)
        assert not any("EXISTS" in q for q in conn.sql)
        assert not any(q.rstrip().endswith(";") for q in conn.sql)
        fused = [q for q in conn.sql if '"__row_count"' in q]
        assert len(fused) == 1 and "count_if(" in fused[0] and "SUM(CASE" not in fused[0]
        by_id = {r["rule_id"]: r for r in out["results"]}
        # Fail-fast rules report a violation as at least 1, as the probe did;
        # tally rules and dataset rules keep their counts.
        assert by_id["nn"]["failed_count"] == 1 and by_id["uq"]["failed_count"] == 1
        assert by_id["ca"]["failed_count"] == 1
        assert by_id["rg"]["failed_count"] == 3 and by_id["mr"]["failed_count"] == 3
        assert out["row_count"] == 10
        # The fail-fast custom rule counts its to_sql_exists() condition.
        assert 'count_if("a" < 0) AS "ca"' in fused[0]

    def test_small_table_unique_in_scan_with_mark_distinct(self):
        conn = _ScanConn()
        self._run(conn, data_rows=999_999)
        sets = [i for i, q in enumerate(conn.sql) if q.startswith("SET SESSION")]
        fused = [i for i, q in enumerate(conn.sql) if '"__row_count"' in q]
        assert len(sets) == 1 and "mark_distinct" in conn.sql[sets[0]] and sets[0] < fused[0]
        assert 'COUNT(DISTINCT "b")' in conn.sql[fused[0]]
        assert not any("GROUP BY" in q for q in conn.sql)
        assert not any("$files" in q for q in conn.sql)

    @pytest.mark.parametrize("mode", ["caller_transaction", "pinned", "bracketed"])
    def test_callers_session_is_not_changed(self, mode):
        conn = _ScanConn()
        self._run(conn, mode=mode, data_rows=10)
        assert not any(q.startswith("SET SESSION") for q in conn.sql)
        assert any('COUNT(DISTINCT "b")' in q for q in conn.sql)

    def test_large_table_unique_gets_its_own_group_by(self):
        conn = _ScanConn()
        out, _ = self._run(conn, data_rows=1_000_000)
        grouped = [q for q in conn.sql if "GROUP BY" in q]
        assert len(grouped) == 1 and "HAVING count(*) > 1" in grouped[0]
        assert not any("COUNT(DISTINCT" in q for q in conn.sql)
        assert not any(q.startswith("SET SESSION") for q in conn.sql)
        # One fused scan, one GROUP BY, one custom SQL query read the table.
        assert len(self._table_reads(conn)) == 3
        by_id = {r["rule_id"]: r for r in out["results"]}
        assert by_id["uq"]["failed_count"] == 1  # fail-fast

    def test_tally_unique_from_group_by_keeps_its_count(self):
        conn = _ScanConn()
        spec = {"kind": "unique", "column": "b", "rule_id": "uq", "tally": True}
        out, _ = self._run(conn, data_rows=5_000_000, specs=[spec])
        assert out["results"][0]["failed_count"] == 2

    def test_size_read_from_files_when_preplan_did_not(self):
        conn = _ScanConn(files_rows=20)
        _, handle = self._run(conn, data_rows=None)
        from kontra.connectors import trino_read

        files = [q for q in conn.sql if "$files" in q]
        assert len(files) == 1 and "record_count" in files[0]
        state = trino_read.state_of(handle)
        assert state.files_read and state.data_rows == 20
        assert any('COUNT(DISTINCT "b")' in q for q in conn.sql)

    def test_no_size_query_without_unique_rules(self):
        conn = _ScanConn()
        self._run(conn, data_rows=None, specs=self.SPECS[:2])
        assert not any("$files" in q for q in conn.sql)

    def test_unknown_size_counts_as_large(self):
        conn = _ScanConn()
        self._run(conn, held=False)
        assert not any("$files" in q for q in conn.sql)
        assert any("GROUP BY" in q for q in conn.sql)
        assert not any(q.startswith("SET SESSION") for q in conn.sql)

    def test_side_queries_run_concurrently_at_most_four(self):
        conn = _ScanConn(delay=0.1)
        specs = [
            {"kind": "unique", "column": "b", "rule_id": f"u{i}", "tally": True} for i in range(3)
        ] + [
            {
                "kind": "custom_sql_check",
                "sql": f"SELECT * FROM {{table}} WHERE a < {i}",
                "rule_id": f"c{i}",
            }
            for i in range(4)
        ]
        out, _ = self._run(conn, data_rows=2_000_000, specs=specs)
        assert conn.max_in_flight == 4
        assert {r["rule_id"] for r in out["results"]} == {s["rule_id"] for s in specs}
        assert all(r["passed"] for r in out["results"] if r["rule_id"].startswith("c"))


class TestTrinoScanRecovery:
    """A failed query aborts a Trino transaction; Kontra's own recovers, a caller's is left alone."""

    CUSTOM: ClassVar[list[dict]] = [
        {"kind": "not_null", "column": "a", "rule_id": "nn", "tally": True},
        {"kind": "custom_sql_check", "sql": "SELECT * FROM {table} WHERE a = 1", "rule_id": "c1"},
        {"kind": "custom_sql_check", "sql": "SELECT * FROM {table} WHERE a = 2", "rule_id": "c2"},
    ]

    def test_failed_queries_rerun_alone_in_new_transactions(self):
        conn = _ScanConn(delay=0.05)
        conn.fail = {"a = 1": 99, '"__row_count"': 1, "a = 2": 1}  # c1 always; the others once
        out, _ = TestTrinoScanPlan._run(conn, specs=self.CUSTOM)
        by_id = {r["rule_id"]: r for r in out["results"]}
        assert by_id["nn"]["failed_count"] == 3  # the scan, rerun
        assert by_id["c2"]["passed"]  # rerun alone, answered
        assert not by_id["c1"]["passed"] and "failed: " in by_id["c1"]["message"]
        # One new transaction per retry, and one after the last retry failed.
        assert conn.rollbacks == 4
        assert sum('"__row_count"' in q for q in conn.sql) == 2

    def test_a_scan_that_fails_alone_raises(self):
        conn = _ScanConn()
        conn.fail = {'"__row_count"': 2}
        with pytest.raises(RuntimeError, match="failed: SELECT count_if"):
            TestTrinoScanPlan._run(conn, specs=self.CUSTOM)

    @pytest.mark.parametrize("mode", ["pinned", "bracketed"])
    def test_autocommit_failures_are_not_retried(self, mode):
        conn = _ScanConn()
        conn.fail = {"a = 1": 1}
        out, _ = TestTrinoScanPlan._run(conn, mode=mode, specs=self.CUSTOM)
        assert conn.rollbacks == 0
        assert not {r["rule_id"]: r for r in out["results"]}["c1"]["passed"]

    def test_callers_transaction_runs_user_sql_after_the_scan_one_by_one(self):
        conn = _ScanConn(delay=0.05)
        specs = self.CUSTOM + [
            {"kind": "unique", "column": "b", "rule_id": "uq", "tally": True},
        ]
        TestTrinoScanPlan._run(conn, mode="caller_transaction", data_rows=5_000_000, specs=specs)
        assert conn.rollbacks == 0
        custom = [i for i, (_, q) in enumerate(conn.events) if "WHERE a =" in q]
        scan_ends = [
            i for i, (e, q) in enumerate(conn.events) if e == "end" and "WHERE a =" not in q
        ]
        # The fused scan and the GROUP BY ran together, then each custom query alone.
        assert max(scan_ends) < min(custom)
        assert [e for e, q in conn.events if "WHERE a =" in q] == ["start", "end", "start", "end"]

    def test_callers_transaction_is_never_rolled_back(self):
        conn = _ScanConn()
        conn.fail = {"a = 1": 1}
        out, _ = TestTrinoScanPlan._run(conn, mode="caller_transaction", specs=self.CUSTOM)
        assert conn.rollbacks == 0
        assert not {r["rule_id"]: r for r in out["results"]}["c1"]["passed"]

    @pytest.mark.parametrize(
        ("level", "expected"), [("READ_UNCOMMITTED", True), ("AUTOCOMMIT", False)]
    )
    def test_callers_transaction_without_read_state(self, level, expected):
        """A non-Iceberg table or a view has no read state; the caller's connection decides."""
        from types import SimpleNamespace

        from trino.transaction import IsolationLevel

        from kontra.connectors import trino_read
        from kontra.connectors.handle import DatasetHandle

        handle = DatasetHandle.from_uri("trino://u@host/lake/s.t")
        assert trino_read.caller_transaction(handle) is False  # Kontra's own connection
        object.__setattr__(handle, "scheme", "byoc")
        conn = SimpleNamespace(isolation_level=getattr(IsolationLevel, level))
        object.__setattr__(handle, "external_conn", conn)
        assert trino_read.caller_transaction(handle) is expected


class TestTrinoExactnessGate:
    def _exact(self, spec):
        from kontra.engine.executors.trino_sql import TrinoSqlExecutor

        return TrinoSqlExecutor()._is_exact(spec, _FAMILIES)

    @pytest.mark.parametrize(
        "spec",
        [
            {"kind": "not_null", "column": "score"},
            {"kind": "unique", "column": "name"},
            {"kind": "allowed_values", "column": "name", "values": ["a", None]},
            {"kind": "allowed_values", "column": "id", "values": [1, 2]},
            {"kind": "allowed_values", "column": "flag", "values": [True]},
            {"kind": "range", "column": "amount", "min": 0, "max": 10},
            {"kind": "length", "column": "name", "min": 1},
            {"kind": "regex", "column": "name", "pattern": "^a"},
            {"kind": "compare", "left": "id", "right": "amount", "op": "<"},
            {"kind": "compare", "left": "day", "right": "day", "op": "<="},
            {
                "kind": "conditional_not_null",
                "column": "x",
                "when_column": "name",
                "when_op": "==",
                "when_value": "a",
            },
            {"kind": "freshness", "column": "ts"},
            {"kind": "min_rows"},
            # Closed fallbacks (floats, char(n)).
            {"kind": "unique", "column": "score"},
            {"kind": "range", "column": "score", "min": 0},
            {"kind": "range", "column": "score", "min": -1.5, "max": 2**53},
            {"kind": "compare", "left": "score", "right": "score2", "op": "<"},
            {"kind": "compare", "left": "score", "right": "score", "op": "!="},
            {"kind": "allowed_values", "column": "code", "values": ["ab", None]},
            {"kind": "disallowed_values", "column": "code", "values": ["ab"]},
            {"kind": "length", "column": "code", "min": 1},
            {"kind": "regex", "column": "code", "pattern": "^a"},
            {"kind": "contains", "column": "code", "substring": "b"},
            {"kind": "starts_with", "column": "code", "prefix": "a"},
            {"kind": "ends_with", "column": "code", "suffix": " "},
        ],
    )
    def test_pushed(self, spec):
        assert self._exact(spec) is True

    @pytest.mark.parametrize(
        "spec",
        [
            {"kind": "not_null", "column": "missing"},
            {"kind": "unique", "column": "code"},
            {"kind": "allowed_values", "column": "id", "values": ["1"]},
            {"kind": "allowed_values", "column": "code", "values": [1]},
            {"kind": "allowed_values", "column": "flag", "values": [1]},
            # Floats: equality with a float literal isn't measured; bounds
            # that don't convert to one float alike; float vs other families.
            {"kind": "allowed_values", "column": "score", "values": [1.5]},
            {"kind": "disallowed_values", "column": "score", "values": [1.5]},
            {"kind": "range", "column": "score", "min": float("nan")},
            {"kind": "range", "column": "score", "max": float("inf")},
            {"kind": "range", "column": "score", "max": 2**53 + 1},
            {"kind": "range", "column": "score", "min": True},
            {"kind": "range", "column": "score", "min": "0"},
            {"kind": "compare", "left": "score", "right": "id", "op": "<"},
            {"kind": "compare", "left": "code", "right": "code", "op": "=="},
            {
                "kind": "conditional_range",
                "column": "score",
                "when_column": "x",
                "when_op": "==",
                "when_value": 1,
                "min": 0,
            },
            # Float literals on exact numerics: Trino and Polars compare them differently.
            {"kind": "range", "column": "id", "max": 9007199254740992.0},
            {"kind": "range", "column": "amount", "min": 0.31},
            {"kind": "allowed_values", "column": "amount", "values": [1.5]},
            {
                "kind": "conditional_range",
                "column": "id",
                "when_column": "x",
                "when_op": ">=",
                "when_value": 0,
                "max": 1.0,
            },
            {"kind": "range", "column": "day", "min": 0},
            {"kind": "contains", "column": "id", "substring": "1"},
            {"kind": "compare", "left": "day", "right": "ts", "op": "<"},
            {
                "kind": "conditional_range",
                "column": "id",
                "when_column": "id",
                "when_op": "==",
                "when_value": "5",
                "min": 0,
            },
        ],
    )
    def test_left_to_polars(self, spec):
        assert self._exact(spec) is False

    @pytest.mark.parametrize(
        "data_type, family",
        [
            ("timestamp", "timestamp"),
            ("timestamp(6)", "timestamp"),
            ("timestamp(3) with time zone", "timestamptz"),
            ("timestamp(9)", "other"),  # finer than the materializer's microseconds
            ("timestamp(12) with time zone", "other"),
        ],
    )
    def test_timestamp_precision(self, data_type, family):
        from kontra.engine.executors.trino_sql import _family

        assert _family(data_type) == family

    @pytest.mark.parametrize("column", ["tsz", "ts", "day"])
    def test_freshness_pushes_on_any_connection(self, column):
        assert self._exact({"kind": "freshness", "column": column}) is True

    @pytest.mark.parametrize(
        "data_type, length",
        [("char", 1), ("char(3)", 3), ("CHAR(12)", 12), ("varchar(3)", None), ("char(x)", None)],
    )
    def test_char_length(self, data_type, length):
        from kontra.engine.executors.trino_sql import _char_length, _family

        assert _char_length(data_type) == length
        assert (_family(data_type) == "char") is (length is not None)


_TYPES = {
    "score": "double",
    "r": "real",
    "code": "char(3)",
    "ts": "timestamp(6)",
    "tsz": "timestamp(6) with time zone",
    "day": "date",
    "name": "varchar",
}


class TestTrinoTypedSql:
    """The SQL written for floats, char(n) and naive freshness."""

    def _select(self, spec):
        from kontra.engine.executors.trino_sql import TrinoSqlExecutor

        return TrinoSqlExecutor()._count_select({"rule_id": "r1", **spec}, False, _TYPES)

    def test_float_range_counts_nan_and_casts_bounds(self):
        sql = self._select({"kind": "range", "column": "score", "min": 0, "max": 0.1})
        assert sql == (
            'count_if("score" IS NULL OR is_nan("score") OR "score" < DOUBLE \'0.0\' '
            'OR "score" > DOUBLE \'0.1\') AS "r1"'
        )

    def test_real_range_bound_is_a_real(self):
        sql = self._select({"kind": "range", "column": "r", "min": 0.1})
        assert "\"r\" < CAST(DOUBLE '0.1' AS real)" in sql

    def test_float_compare_uses_polars_order(self):
        sql = self._select({"kind": "compare", "left": "score", "right": "r", "op": "<="})
        assert sql == (
            'count_if("score" IS NULL OR "r" IS NULL OR NOT (is_nan("r") OR '
            '(NOT is_nan("score") AND "score" <= "r"))) AS "r1"'
        )

    @pytest.mark.parametrize(
        "spec, condition",
        [
            (
                {"kind": "allowed_values", "column": "code", "values": ["ab"]},
                "{p} IS NULL OR {p} NOT IN ('ab')",
            ),
            (
                {"kind": "allowed_values", "column": "code", "values": ["ab", None]},
                "{p} IS NOT NULL AND {p} NOT IN ('ab')",
            ),
            ({"kind": "disallowed_values", "column": "code", "values": []}, "false"),
            ({"kind": "length", "column": "code", "max": 2}, "{p} IS NULL OR LENGTH({p}) > 2"),
            (
                {"kind": "ends_with", "column": "code", "suffix": "b_"},
                "{p} IS NULL OR {p} NOT LIKE '%b\\_' ESCAPE '\\'",
            ),
            (
                {"kind": "regex", "column": "code", "pattern": "^a"},
                "{p} IS NULL OR NOT regexp_like({p}, '^a')",
            ),
        ],
    )
    def test_char_rules_read_the_padded_value(self, spec, condition):
        padded = "rpad(CAST(\"code\" AS varchar), 3, ' ')"
        assert self._select(spec) == f'count_if({condition.format(p=padded)}) AS "r1"'

    def test_varchar_rules_are_unchanged(self):
        from kontra.engine.executors.trino_sql import TrinoSqlExecutor

        spec = {"rule_id": "r1", "kind": "length", "column": "name", "max": 2}
        assert self._select(spec) == TrinoSqlExecutor()._count_select(spec, False)

    @pytest.mark.parametrize("column", ["ts", "day"])
    def test_naive_freshness_compares_with_utc_now(self, column):
        sql = self._select({"kind": "freshness", "column": column, "max_age_seconds": 60})
        assert "date_add('second', -60, CAST(current_timestamp AT TIME ZONE 'UTC' AS " in sql

    def test_zoned_freshness_compares_with_current_timestamp(self):
        sql = self._select({"kind": "freshness", "column": "tsz", "max_age_seconds": 60})
        assert "date_add('second', -60, current_timestamp)" in sql

    @pytest.mark.parametrize("column", ["ts", "day", "tsz"])
    def test_freshness_marks_a_column_without_timestamps(self, column):
        sql = self._select({"kind": "freshness", "column": column, "max_age_seconds": 60})
        assert sql.startswith(f'CASE WHEN MAX("{column}") IS NULL THEN -1 WHEN ')

    @pytest.mark.parametrize("rows", [2, 0])
    def test_no_timestamps_fails_with_the_row_count(self, rows):
        from kontra.engine.executors.trino_sql import _no_timestamps

        result = _no_timestamps("r1", "ts", rows)
        assert result["passed"] is False
        assert result["failed_count"] == rows
        assert result["message"] == "Column 'ts' has no non-null timestamps"


class TestTrinoPreplan:
    def test_declared_not_null_is_proven(self, monkeypatch):
        import kontra.preplan.trino as trino_preplan
        from kontra.connectors.handle import DatasetHandle

        monkeypatch.setattr(
            trino_preplan,
            "_fetch_columns",
            lambda handle: [("id", "bigint", False), ("email", "varchar", True)],
        )
        handle = DatasetHandle.from_uri("trino://u@host/lake/s.t")
        pre = trino_preplan.preplan_trino(
            handle,
            ["ID", "email"],
            [
                ("r1", "ID", "not_null", True),
                ("r2", "email", "not_null", True),
                ("r3", "id", "unique", True),
            ],
        )
        assert pre.rule_decisions == {"r1": "pass_meta", "r2": "unknown", "r3": "unknown"}

    @staticmethod
    def _dtype_plan(monkeypatch, rules):
        import kontra.preplan.trino as trino_preplan
        from kontra.config.models import RuleSpec
        from kontra.connectors.handle import DatasetHandle
        from kontra.engine.phases.compilation import _ensure_builtin_rules_registered
        from kontra.rule_defs.factory import RuleFactory
        from kontra.rule_defs.static_predicates import extract_static_predicates

        monkeypatch.setattr(
            trino_preplan,
            "_fetch_columns",
            lambda handle: [
                ("n", "integer", True),
                ("d", "decimal(12,2)", True),
                ("u", "uuid", True),
            ],
        )
        _ensure_builtin_rules_registered()
        built = RuleFactory([RuleSpec(**r) for r in rules]).build_rules()
        handle = DatasetHandle.from_uri("trino://u@host/lake/s.t")
        return trino_preplan.preplan_trino(
            handle, [], extract_static_predicates(rules=built), rules=built
        )

    def test_dtype_is_decided_from_the_declared_type(self, monkeypatch):
        pre = self._dtype_plan(
            monkeypatch,
            [
                {"name": "dtype", "id": "int32", "params": {"column": "n", "type": "int32"}},
                {"name": "dtype", "id": "int", "params": {"column": "n", "type": "int"}},
                {"name": "dtype", "id": "int64", "params": {"column": "n", "type": "int64"}},
                {"name": "dtype", "id": "dec", "params": {"column": "d", "type": "numeric"}},
            ],
        )
        assert pre.rule_decisions == {
            "int32": "pass_meta",
            "int": "pass_meta",
            "int64": "fail_meta",
            "dec": "fail_meta",
        }
        assert pre.fail_details["int64"] == {"expected": "int64", "actual": "Int32"}

    def test_dtype_falls_back_without_a_fixed_mapping(self, monkeypatch):
        """Unmapped types, other cases of the name, missing columns and non-strict modes."""
        pre = self._dtype_plan(
            monkeypatch,
            [
                {"name": "dtype", "id": "uuid", "params": {"column": "u", "type": "string"}},
                {"name": "dtype", "id": "case", "params": {"column": "N", "type": "int32"}},
                {"name": "dtype", "id": "gone", "params": {"column": "x", "type": "int32"}},
                {
                    "name": "dtype",
                    "id": "mode",
                    "params": {"column": "n", "type": "int32", "mode": "relaxed"},
                },
            ],
        )
        assert set(pre.rule_decisions.values()) == {"unknown"}


class TestTrinoFilesDecisions:
    """Decisions from $files aggregates, including metrics the container can't write."""

    @staticmethod
    def _files(delete_files=0, records=10, **column):
        from kontra.preplan.trino import _ColumnFiles, _Files

        stats = _ColumnFiles(
            unknown_nulls=column.get("unknown_nulls", 0),
            nulls=column.get("nulls", 0),
            unbounded=column.get("unbounded", 0),
            min_lower=column.get("min_lower", 1),
            max_upper=column.get("max_upper", 5),
            max_lower=column.get("max_lower", 3),
            min_upper=column.get("min_upper", 2),
        )
        return _Files(1, delete_files, records, {"x": stats}), stats

    def test_range_needs_bounds_on_every_file_with_values(self):
        from kontra.preplan.trino import _range_decision

        files, stats = self._files()
        assert _range_decision(files, stats, 0, 10) == "pass_meta"
        files, stats = self._files(unbounded=1)  # null counts present, bounds missing
        assert _range_decision(files, stats, 0, 10) == "unknown"

    def test_range_fail_needs_a_file_wholly_outside_and_no_deletes(self):
        from kontra.preplan.trino import _range_decision

        files, stats = self._files()
        assert _range_decision(files, stats, 0, 4) == "unknown"  # straddles
        assert _range_decision(files, stats, None, 2) == "fail_meta"  # a file starts at 3
        assert _range_decision(files, stats, 3, None) == "fail_meta"  # a file ends at 2
        files, stats = self._files(delete_files=1)
        assert _range_decision(files, stats, None, 2) == "unknown"
        assert _range_decision(files, stats, 0, 10) == "pass_meta"

    def test_nulls(self):
        from kontra.preplan.trino import _not_null_decision, _range_decision

        files, stats = self._files(nulls=2)
        assert _not_null_decision(files, stats) == "fail_meta"
        assert _range_decision(files, stats, 0, 10) == "fail_meta"
        files, stats = self._files(nulls=2, delete_files=1)
        assert _not_null_decision(files, stats) == "unknown"
        files, stats = self._files(unknown_nulls=1)
        assert _not_null_decision(files, stats) == "unknown"
        assert _range_decision(files, stats, 0, 10) == "unknown"

    @pytest.mark.parametrize(
        ("value", "bound_type", "usable"),
        [
            (3, "integer", True),
            (True, "integer", False),
            (2.5, "decimal(12,2)", False),
            (2.0, "bigint", False),
            ("2020-01-01", "date", True),
            ("2020-01-01T00:00:00", "date", False),
            ("not a date", "date", False),
            (None, "date", True),
        ],
    )
    def test_range_literals(self, value, bound_type, usable):
        from kontra.preplan.trino import _range_literal

        assert _range_literal(value, bound_type) is usable

    def test_bound_types(self):
        from kontra.preplan.trino import _bound_type

        assert _bound_type("decimal(12, 2)") == "decimal(12,2)"
        assert _bound_type("date") == "date"
        for data_type in ("double", "real", "varchar", "timestamp(6)", "boolean"):
            assert _bound_type(data_type) is None


class TestTrinoPartitionValues:
    """allowed_values from identity-partition values."""

    @staticmethod
    def _files(values, unknown_parts=0, delete_files=0):
        from kontra.preplan.trino import _ColumnFiles, _Files

        stats = _ColumnFiles(0, 0, unknown_parts=unknown_parts, part_values=values)
        return _Files(1, delete_files, 10, {"x": stats}), stats

    def test_decisions(self):
        from kontra.preplan.trino import _allowed_values_decision as decide

        assert decide(*self._files(["a", "b"]), ["a", "b"]) == "pass_meta"
        assert decide(*self._files(["a", "b"]), ["a"]) == "fail_meta"
        assert decide(*self._files(["a", None]), ["a"]) == "fail_meta"
        assert decide(*self._files(["a", None]), ["a", None]) == "pass_meta"
        assert decide(*self._files([]), ["a"]) == "pass_meta"  # no non-empty files
        # A file whose value can't be trusted (its spec lacks the field, or it has no null count).
        assert decide(*self._files(["a"], unknown_parts=1), ["a"]) == "unknown"
        assert decide(*self._files(["b"], unknown_parts=1), ["a"]) == "fail_meta"
        # Delete files: the value's rows may be gone.
        assert decide(*self._files(["b"], delete_files=1), ["a"]) == "unknown"
        assert decide(*self._files(["a"], delete_files=1), ["a"]) == "pass_meta"

    @pytest.mark.parametrize(
        ("data_type", "values", "usable"),
        [
            ("varchar", ["a", None], True),
            ("varchar(3)", ["a"], True),
            ("varchar", ["a", 1], False),
            ("integer", [1, 2, None], True),
            ("bigint", [1, True], False),
            ("integer", [1.0], False),
            ("integer", ["1"], False),
            ("date", ["2020-01-01"], False),
            ("double", [1.0], False),
            ("varchar", "a", False),
            ("varchar", [], False),
        ],
    )
    def test_usable_values(self, data_type, values, usable):
        from kontra.preplan.trino import _allowed_values_usable

        assert _allowed_values_usable(data_type, values) is usable

    def test_row_fields(self):
        from kontra.preplan.trino import _row_fields

        assert _row_fields('row("cat" varchar, "ts_month" integer, "a""b, c" decimal(12, 2))') == {
            "cat": "varchar",
            "ts_month": "integer",
            'a"b, c': "decimal(12, 2)",
        }
        assert _row_fields("row(cat varchar)") == {"cat": "varchar"}
        assert _row_fields("varchar") == {}

    def test_sql_reads_partition_values_with_a_null_count_check(self):
        from kontra.preplan.trino import _files_sql

        sql = _files_sql('"c"."s"."t$files"', {"cat": None}, ("cat",))
        assert "partition AS p" in sql
        assert 'array_agg(DISTINCT p."cat")' in sql
        assert "null_value_count = record_count" in sql
        assert "partition" not in _files_sql('"c"."s"."t$files"', {"cat": None})


class TestTrinoMaterializerTypes:
    @pytest.mark.parametrize(
        ("trino_type", "expected"),
        [
            ("boolean", pl.Boolean),
            ("tinyint", pl.Int8),
            ("smallint", pl.Int16),
            ("integer", pl.Int32),
            ("bigint", pl.Int64),
            ("real", pl.Float32),
            ("double", pl.Float64),
            ("decimal(10, 2)", pl.Decimal(10, 2)),
            ("decimal(38,0)", pl.Decimal(38, 0)),
            ("decimal(5)", pl.Decimal(5, 0)),
            ("varchar(12)", pl.Utf8),
            ("char(3)", pl.Utf8),
            ("json", pl.Utf8),
            ("date", pl.Date),
            ("timestamp(3)", pl.Datetime("us")),
            ("timestamp(6) with time zone", pl.Datetime("us", "UTC")),
            ("time(0)", pl.Time),
            ("time(3) with time zone", None),
            ("varbinary", pl.Binary),
            ("uuid", None),
            ("array(integer)", None),
            (None, None),
        ],
    )
    def test_declared_dtype(self, trino_type, expected):
        from kontra.connectors.trino_types import polars_dtype

        assert polars_dtype(trino_type) == expected


def _raw_columns(*specs):
    """Result columns as the Trino client describes them: (name, rawType, arguments)."""
    return [
        {
            "name": name,
            "type": data_type,
            "typeSignature": {"rawType": raw, "arguments": args},
        }
        for name, data_type, raw, args in specs
    ]


class _RawCursor:
    def __init__(self, columns, rows, log):
        self._query = SimpleNamespace(columns=columns)
        self.description = [(c["name"], c["type"]) for c in columns]
        self._rows = list(rows)
        self._log = log

    def execute(self, sql):
        self._log.append(("execute", sql))

    def fetchmany(self, size):
        self._log.append(("fetchmany", size))
        chunk, self._rows = self._rows[:size], self._rows[size:]
        return chunk

    def fetchall(self):
        raise AssertionError("the column decoder fetches in chunks")

    def close(self):
        pass


class TestTrinoFrameDecoder:
    _COLUMNS = (
        ("i", "integer", "integer", []),
        ("d", "double", "double", []),
        (
            "m",
            "decimal(5, 2)",
            "decimal",
            [{"kind": "LONG", "value": 5}, {"kind": "LONG", "value": 2}],
        ),
        ("t", "timestamp(3)", "timestamp", [{"kind": "LONG", "value": 3}]),
        ("u", "uuid", "uuid", []),
    )
    _ROWS: ClassVar[list] = [
        [1, 1.5, "1.10", "2026-01-01 00:00:00.123", "12151fd2-7586-11e9-8f9e-2a86e4085a59"],
        [None, "NaN", None, None, None],
        [3, -0.0, "-0.05", "2026-01-02 10:00:00.000", None],
    ]

    def _materialize(self, monkeypatch, rows, columns=_COLUMNS, chunk_rows=2):
        from kontra.connectors.handle import DatasetHandle
        from kontra.engine.materializers.trino import TrinoMaterializer

        log: list = []
        cursor = _RawCursor(_raw_columns(*columns), rows, log)

        class Conn:
            __module__ = "trino.dbapi"  # detected as a Trino connection

            def cursor(self, legacy_primitive_types=None):
                log.append(("cursor", legacy_primitive_types))
                return cursor

        monkeypatch.setattr(TrinoMaterializer, "chunk_rows", chunk_rows)
        handle = DatasetHandle.from_connection(Conn(), "c.s.t")
        return TrinoMaterializer(handle).to_polars(None), log

    def test_raw_values_fetched_in_chunks(self, monkeypatch):
        import datetime as dt
        import uuid
        from decimal import Decimal

        frame, log = self._materialize(monkeypatch, self._ROWS)
        assert log[0] == ("cursor", True)
        assert [e for e in log if e[0] == "fetchmany"] == [("fetchmany", 2)] * 3
        assert frame.schema == {
            "i": pl.Int32,
            "d": pl.Float64,
            "m": pl.Decimal(5, 2),
            "t": pl.Datetime("us"),
            "u": pl.Object,
        }
        assert frame["i"].to_list() == [1, None, 3]
        d = frame["d"].to_list()
        assert d[0] == 1.5 and math.isnan(d[1]) and str(d[2]) == "-0.0"
        assert frame["m"].to_list() == [Decimal("1.10"), None, Decimal("-0.05")]
        assert frame["t"].to_list() == [
            dt.datetime.fromisoformat("2026-01-01 00:00:00.123"),
            None,
            dt.datetime.fromisoformat("2026-01-02 10:00:00"),
        ]
        assert frame["u"].to_list() == [
            uuid.UUID("12151fd2-7586-11e9-8f9e-2a86e4085a59"),
            None,
            None,
        ]

    def test_empty_result_has_the_declared_schema(self, monkeypatch):
        frame, _ = self._materialize(monkeypatch, [])
        assert frame.height == 0
        assert frame.schema["m"] == pl.Decimal(5, 2)
        assert frame.schema["u"] == pl.Utf8

    def test_client_without_raw_values_builds_rows_as_before(self, monkeypatch):
        from kontra.connectors.handle import DatasetHandle
        from kontra.engine.materializers.trino import TrinoMaterializer

        class Cursor:
            description = (("i", "integer"), ("d", "double"))

            def execute(self, sql):
                pass

            def fetchall(self):
                return [(1, 1.5), (None, None)]

            def close(self):
                pass

        class Conn:
            __module__ = "trino.dbapi"

            def cursor(self):  # no legacy_primitive_types argument
                return Cursor()

        monkeypatch.setenv("KONTRA_IO_DEBUG", "1")
        mat = TrinoMaterializer(DatasetHandle.from_connection(Conn(), "c.s.t"))
        frame = mat.to_polars(None)
        assert mat.io_debug()["decode"] == "rows"
        assert frame.schema == {"i": pl.Int32, "d": pl.Float64}
        assert frame["i"].to_list() == [1, None]

    def test_special_floats_decode_as_the_mapper_maps_them(self):
        from trino.mapper import RowMapperFactory

        from kontra.engine.materializers.trino_decode import FrameDecoder

        columns = _raw_columns(("d", "double", "double", []))
        mappers = RowMapperFactory().create(columns=columns, legacy_primitive_types=False).columns
        decoder = FrameDecoder(["d"], ["double"], [pl.Float64], mappers)
        decoder.add([[1.5], ["NaN"]])
        decoder.add([["Infinity"], ["-Infinity"], [None]])
        assert decoder.mapped_chunks == 0
        # Any other string isn't guessed at: that chunk takes the client's mapper.
        decoder.add([["1e5"], [2.0]])
        assert decoder.mapped_chunks == 1
        d = decoder.frame()["d"].to_list()
        assert d[0] == 1.5 and math.isnan(d[1])
        assert d[2:] == [math.inf, -math.inf, None, 100000.0, 2.0]

    def test_values_python_cant_hold_raise_the_clients_error(self):
        from trino.exceptions import TrinoDataError
        from trino.mapper import RowMapperFactory

        from kontra.engine.materializers.trino_decode import FrameDecoder

        columns = _raw_columns(("d", "date", "date", []))
        mappers = RowMapperFactory().create(columns=columns, legacy_primitive_types=False).columns
        decoder = FrameDecoder(["d"], ["date"], [pl.Date], mappers)
        with pytest.raises(TrinoDataError, match="Could not convert '-0001-01-01'"):
            decoder.add([["2026-01-01"], ["-0001-01-01"]])

    @pytest.mark.parametrize(
        ("trino_type", "fast"),
        [
            ("integer", True),
            ("double", True),
            ("decimal(38, 10)", True),
            ("varchar(3)", True),
            ("char(3)", True),
            ("date", True),
            ("timestamp(0)", True),
            ("timestamp(6)", True),
            ("timestamp(6) with time zone", True),
            ("time(3)", True),
            ("varbinary", True),
            ("timestamp(9)", False),  # the client rounds to microseconds
            ("timestamp(9) with time zone", False),
            ("time(9)", False),
            ("time(3) with time zone", False),
            ("uuid", False),
            ("array(integer)", False),
            ("interval day to second", False),
        ],
    )
    def test_fast_decoder_per_type(self, trino_type, fast):
        from kontra.connectors.trino_types import polars_dtype
        from kontra.engine.materializers.trino_decode import _fast_decoder

        assert (_fast_decoder(trino_type, polars_dtype(trino_type)) is not None) is fast

    def test_no_fast_decoder_when_declared_and_cursor_types_differ(self):
        from kontra.engine.materializers.trino_decode import _fast_decoder

        assert _fast_decoder("integer", pl.Int64) is None
        assert _fast_decoder("varchar", None) is None


def test_profile_reports_trino_unsupported():
    import kontra

    with pytest.raises(ValueError, match="Profiling Trino sources is not supported yet"):
        kontra.profile("trino://u@host/lake/s.t")


@pytest.mark.lazy_loading
def test_import_kontra_does_not_load_trino():
    code = "import sys, kontra\nprint('trino' in sys.modules)"
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True)
    assert out.stdout.strip() == "False"
