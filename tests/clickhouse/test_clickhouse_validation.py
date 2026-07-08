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
