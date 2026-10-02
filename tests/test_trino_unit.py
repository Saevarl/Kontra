"""Trino connector unit tests (no Trino server needed)."""

from __future__ import annotations

import subprocess
import sys

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
    def test_statements_have_no_trailing_semicolon(self):
        from kontra.engine.executors.trino_sql import TrinoSqlExecutor

        ex = TrinoSqlExecutor()
        assert not ex._assemble_single_row(["COUNT(*) AS n"], '"c"."s"."t"').endswith(";")
        assert not ex._assemble_exists_query(["EXISTS (SELECT 1) AS x"]).endswith(";")

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
    "code": "other",  # char(n)
    "score": "float",
    "day": "date",
    "ts": "timestamp",
    "tsz": "timestamptz",
    "flag": "boolean",
    "x": "integer",
}


class TestTrinoExactnessGate:
    def _exact(self, spec, byoc=False):
        from kontra.engine.executors.trino_sql import TrinoSqlExecutor

        return TrinoSqlExecutor()._is_exact(spec, _FAMILIES, byoc)

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
        ],
    )
    def test_pushed(self, spec):
        assert self._exact(spec) is True

    @pytest.mark.parametrize(
        "spec",
        [
            {"kind": "not_null", "column": "missing"},
            {"kind": "unique", "column": "score"},
            {"kind": "unique", "column": "code"},
            {"kind": "allowed_values", "column": "id", "values": ["1"]},
            {"kind": "allowed_values", "column": "code", "values": ["ab"]},
            {"kind": "allowed_values", "column": "flag", "values": [1]},
            {"kind": "range", "column": "score", "min": 0},
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
            {"kind": "length", "column": "code", "min": 1},
            {"kind": "contains", "column": "id", "substring": "1"},
            {"kind": "compare", "left": "day", "right": "ts", "op": "<"},
            {"kind": "compare", "left": "score", "right": "score", "op": "<"},
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

    def test_freshness_on_callers_connection(self):
        assert self._exact({"kind": "freshness", "column": "tsz"}, byoc=True) is True
        assert self._exact({"kind": "freshness", "column": "ts"}, byoc=True) is False
        assert self._exact({"kind": "freshness", "column": "day"}, byoc=True) is False


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


def test_profile_reports_trino_unsupported():
    import kontra

    with pytest.raises(ValueError, match="Profiling Trino sources is not supported yet"):
        kontra.profile("trino://u@host/lake/s.t")


@pytest.mark.lazy_loading
def test_import_kontra_does_not_load_trino():
    code = "import sys, kontra\nprint('trino' in sys.modules)"
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True)
    assert out.stdout.strip() == "False"
