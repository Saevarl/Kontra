"""Live ClickHouse validation, preplan, and compare tests."""

from __future__ import annotations

import polars as pl
import pytest

import kontra


def _by_id(result):
    return {r.rule_id: r for r in result.rules}


class TestClickHouseValidation:
    def test_pushdown_counts(self, clickhouse_uri):
        rules = [
            kontra.rules.not_null("status"),
            kontra.rules.unique("user_id"),
            kontra.rules.range("age", min=0, max=100),
            kontra.rules.regex("email", "@"),
            kontra.rules.allowed_values("status", ["active", "inactive"]),
            kontra.rules.length("note", min=1, max=2),
            kontra.rules.contains("note", "z"),
        ]
        r = _by_id(kontra.validate(clickhouse_uri, rules=rules, tally=True, save=False))
        assert r["COL:status:not_null"].failed_count == 1
        assert r["COL:user_id:unique"].failed_count == 0
        assert r["COL:age:range"].failed_count == 3       # -1, 120, NULL
        assert r["COL:email:regex"].failed_count == 1     # 'bad'
        assert r["COL:status:allowed_values"].failed_count == 1  # NULL
        assert r["COL:note:length"].failed_count == 2     # 'abc   '=6, 'zzz'=3
        assert r["COL:note:contains"].failed_count == 3   # only 'zzz' has z

    def test_tier_equivalence_pushdown_vs_residual(self, clickhouse_uri):
        rules = [
            kontra.rules.not_null("status"),
            kontra.rules.range("age", min=0, max=100),
            kontra.rules.regex("email", "@"),
            kontra.rules.length("note", min=1, max=2),
            kontra.rules.contains("note", "z"),
        ]
        on = _by_id(kontra.validate(clickhouse_uri, rules=rules, tally=True, save=False, pushdown="on"))
        off = _by_id(kontra.validate(clickhouse_uri, rules=rules, tally=True, save=False, pushdown="off"))
        for rid in on:
            assert on[rid].passed == off[rid].passed, rid
            assert on[rid].failed_count == off[rid].failed_count, rid

    def test_preplan_not_null_from_schema(self, clickhouse_uri):
        # email and note are non-Nullable -> not_null proven from schema (no scan).
        rules = [kontra.rules.not_null("email"), kontra.rules.not_null("note"),
                 kontra.rules.not_null("status")]
        pre = _by_id(kontra.validate(clickhouse_uri, rules=rules, tally=True, save=False))
        off = _by_id(kontra.validate(clickhouse_uri, rules=rules, tally=True, save=False,
                                     preplan="off", pushdown="off"))
        # pass/fail must agree with the exact tier
        for rid in pre:
            assert pre[rid].passed == off[rid].passed, rid
        assert pre["COL:email:not_null"].passed is True
        assert pre["COL:note:not_null"].passed is True
        assert pre["COL:status:not_null"].passed is False

    def test_preplan_dtype_matches_polars_int_family(self, clickhouse_uri):
        # user_id is UInt32: exact "uint32" passes, signed "int" family fails
        # (must match the Polars dtype rule, which is signed-only).
        rules = [
            kontra.rules.dtype("user_id", "uint32"),
            {"name": "dtype", "id": "uid_int", "params": {"column": "user_id", "type": "int"}},
        ]
        pre = _by_id(kontra.validate(clickhouse_uri, rules=rules, tally=True, save=False))
        off = _by_id(kontra.validate(clickhouse_uri, rules=rules, tally=True, save=False,
                                     preplan="off", pushdown="off"))
        assert pre["COL:user_id:dtype"].passed == off["COL:user_id:dtype"].passed is True
        assert pre["uid_int"].passed == off["uid_int"].passed is False


class TestClickHouseCompare:
    def test_table_vs_dataframe(self, clickhouse_uri):
        after = pl.DataFrame({"user_id": pl.Series([1, 2, 3, 99], dtype=pl.UInt32)})
        r = kontra.compare(clickhouse_uri, after, key="user_id")
        assert r.preserved == 3 and r.dropped == 1 and r.added == 1

    def test_cross_engine_vs_postgres(self, clickhouse_uri):
        pg = "postgres://kontra:kontra_test@localhost:5433/kontra_test/public.users"
        try:
            import psycopg
            with psycopg.connect(host="localhost", port=5433, user="kontra",
                                 password="kontra_test", dbname="kontra_test", connect_timeout=3):
                pass
        except Exception:
            pytest.skip("PostgreSQL container not available for cross-engine test")
        # 4 CH users all exist among the 1002 PG users
        r = kontra.compare(clickhouse_uri, pg, key="user_id")
        assert r.dropped == 0 and r.preserved == 4


class TestClickHouseNullableWrappers:
    """Nullability can hide inside LowCardinality(Nullable(T)); the preplan
    must not prove not_null PASS for such columns (tier-equivalence)."""

    def _seed(self):
        import clickhouse_connect
        c = clickhouse_connect.get_client(
            host="localhost", port=8123, username="kontra",
            password="kontra_test", database="kontra_test",
        )
        c.command("DROP TABLE IF EXISTS lc_nullable_test")
        c.command(
            "CREATE TABLE lc_nullable_test ("
            "id UInt32, tag LowCardinality(Nullable(String)), grp LowCardinality(String)"
            ") ENGINE = MergeTree ORDER BY id"
        )
        c.command("INSERT INTO lc_nullable_test VALUES (1,'a','x'),(2,NULL,'y'),(3,'b','x')")
        return c

    def test_lowcardinality_nullable_not_proven(self, _require_clickhouse):
        c = self._seed()
        try:
            uri = "clickhouse://kontra:kontra_test@localhost:8123/kontra_test/lc_nullable_test"
            # tag is LowCardinality(Nullable) with a NULL -> all tiers must agree FAIL
            for tog in (dict(), dict(preplan="off", pushdown="off"), dict(preplan="off", pushdown="on")):
                r = kontra.validate(uri, rules=[kontra.rules.not_null("tag")], tally=True, save=False, **tog)
                assert r.rules[0].passed is False, tog
            # grp is LowCardinality(String), non-nullable -> proven pass, all tiers agree
            for tog in (dict(), dict(preplan="off", pushdown="off"), dict(preplan="off", pushdown="on")):
                r = kontra.validate(uri, rules=[kontra.rules.not_null("grp")], tally=True, save=False, **tog)
                assert r.rules[0].passed is True, tog
        finally:
            c.command("DROP TABLE IF EXISTS lc_nullable_test")


class TestClickHouseEscapingAndDialect:
    """Reviewer-found tier-equivalence bugs: backslash escaping, Unicode regex
    classes, and FixedString/Enum dtype materialization."""

    def _seed(self, ddl_cols, rows):
        import clickhouse_connect
        c = clickhouse_connect.get_client(
            host="localhost", port=8123, username="kontra",
            password="kontra_test", database="kontra_test",
        )
        c.command("DROP TABLE IF EXISTS esc_dialect_test")
        c.command(f"CREATE TABLE esc_dialect_test (id UInt32, {ddl_cols}) ENGINE=MergeTree ORDER BY id")
        c.command(f"INSERT INTO esc_dialect_test VALUES {rows}")
        return c

    def _agree(self, rule):
        uri = "clickhouse://kontra:kontra_test@localhost:8123/kontra_test/esc_dialect_test"
        on = kontra.validate(uri, rules=[rule], tally=True, save=False, pushdown="on")
        off = kontra.validate(uri, rules=[rule], tally=True, save=False, pushdown="off")
        return (on.rules[0].passed == off.rules[0].passed
                and on.rules[0].failed_count == off.rules[0].failed_count)

    def test_backslash_in_like_and_regex(self, _require_clickhouse):
        c = self._seed("v String", r"(1,'a%'),(2,'a\\b'),(3,'plain'),(4,'x9y')")
        try:
            assert self._agree(kontra.rules.starts_with("v", "a\\"))   # LIKE backslash
            assert self._agree(kontra.rules.contains("v", "\\"))
            assert self._agree(kontra.rules.regex("v", r"a\\b"))       # literal-backslash regex
        finally:
            c.command("DROP TABLE IF EXISTS esc_dialect_test")

    def test_unicode_regex_classes_deferred(self, _require_clickhouse):
        # \w/\d are ASCII in ClickHouse RE2 but Unicode in Polars; must agree
        # (executor defers these patterns to Polars).
        c = self._seed("v String", r"(1,'абв'),(2,'x9y'),(3,'abc')")
        try:
            assert self._agree(kontra.rules.regex("v", r"^\w+$"))
            assert self._agree(kontra.rules.regex("v", r"\d"))
            assert self._agree(kontra.rules.regex("v", r"^a"))   # safe pattern still pushes
        finally:
            c.command("DROP TABLE IF EXISTS esc_dialect_test")

    def test_fixedstring_and_enum_dtype(self, _require_clickhouse):
        c = self._seed("fs FixedString(4), e8 Enum8('x'=1,'y'=2)",
                       "(1,'abcd','x'),(2,'wxyz','y')")
        try:
            uri = "clickhouse://kontra:kontra_test@localhost:8123/kontra_test/esc_dialect_test"
            def passed(rule, **tog):
                return kontra.validate(uri, rules=[rule], tally=True, save=False, **tog).rules[0].passed
            # FixedString materializes as Polars Binary -> dtype("string") fails on all tiers
            fs = {"name": "dtype", "id": "fs", "params": {"column": "fs", "type": "string"}}
            assert passed(fs) == passed(fs, preplan="off", pushdown="off") is False
            # Enum8 materializes as Polars Int8 -> dtype("int8") passes on all tiers
            e8 = {"name": "dtype", "id": "e8", "params": {"column": "e8", "type": "int8"}}
            assert passed(e8) == passed(e8, preplan="off", pushdown="off") is True
        finally:
            c.command("DROP TABLE IF EXISTS esc_dialect_test")
