"""Live Trino validation, pushdown, preplan and fallback tests."""

from __future__ import annotations

import pytest

import kontra
from kontra import rules

pytestmark = pytest.mark.integration


def _by_id(result):
    return {r.rule_id: r for r in result.rules}


# Every rule kind the executor can push, on columns where Trino is exact.
PUSHABLE = [
    rules.not_null("email"),
    rules.unique("user_id"),
    rules.allowed_values("status", ["active", "inactive"]),
    rules.disallowed_values("status", ["pending"]),
    rules.range("age", min=0, max=100),
    rules.range("balance", min=0),
    rules.length("note", min=1, max=2),
    rules.regex("email", r"^[a-z]+@[a-z]+\.com$"),
    rules.contains("email", "@"),
    rules.starts_with("note", "a"),
    rules.ends_with("email", ".com"),
    rules.compare("user_id", "age", "<"),
    rules.conditional_not_null("age", when="status == 'active'"),
    rules.conditional_range("age", when="status != 'inactive'", min=0, max=50),
    rules.min_rows(10),
    rules.max_rows(3),
    rules.freshness("updated_at", max_age="1d"),
    rules.custom_sql_check("SELECT * FROM {table} WHERE age < 0"),
]


class TestTrinoValidation:
    def test_pushdown_counts(self, trino_users_uri):
        r = _by_id(kontra.validate(trino_users_uri, rules=PUSHABLE, tally=True, save=False))
        assert r["COL:email:not_null"].failed_count == 0
        assert r["COL:user_id:unique"].failed_count == 1  # user_id 3 twice
        assert r["COL:status:allowed_values"].failed_count == 2  # NULL, 'pending'
        assert r["COL:status:disallowed_values"].failed_count == 1
        assert r["COL:age:range"].failed_count == 3  # NULL, 120, -1
        assert r["COL:balance:range"].failed_count == 2  # NULL, -5.25
        assert r["COL:note:length"].failed_count == 3  # 'abc   ', 'zzz', 'abc\n'
        assert r["COL:email:regex"].failed_count == 2  # 'bad', 'Ünï@cødé.com'
        assert r["COL:email:contains"].failed_count == 1
        assert r["COL:note:starts_with"].failed_count == 3
        assert r["COL:email:ends_with"].failed_count == 1
        assert r["DATASET:min_rows"].failed_count == 5
        assert r["DATASET:max_rows"].failed_count == 2
        assert r["DATASET:custom_sql_check"].failed_count == 1
        for rid, rule in r.items():
            expected = "trino" if rid == "DATASET:custom_sql_check" else "sql"
            assert rule.source == expected, rid

    @pytest.mark.pushdown
    @pytest.mark.parametrize("tally", [True, False])
    def test_tier_equivalence_pushdown_vs_residual(self, trino_users_uri, tally):
        on = _by_id(kontra.validate(trino_users_uri, rules=PUSHABLE, tally=tally, save=False))
        off = _by_id(
            kontra.validate(
                trino_users_uri,
                rules=PUSHABLE,
                tally=tally,
                save=False,
                preplan="off",
                pushdown="off",
            )
        )
        assert set(on) == set(off)
        for rid in on:
            if rid == "DATASET:custom_sql_check":
                continue  # SQL-only measurement
            assert on[rid].passed == off[rid].passed, rid
            if tally:
                assert on[rid].failed_count == off[rid].failed_count, rid

    @pytest.mark.pushdown
    def test_inexact_rules_fall_back_to_polars(self, trino_users_uri):
        """Constructs where Trino and Polars disagree must run in Polars."""
        deferred = [
            rules.allowed_values("score", [1.5]),  # double: float literal equality unmeasured
            rules.allowed_values("age", ["25"]),  # string values on an integer column
            {"name": "regex", "id": "word_rx", "params": {"column": "email", "pattern": r"^\w+@"}},
            {"name": "regex", "id": "flags_rx", "params": {"column": "email", "pattern": "(?i)^A"}},
        ]
        kept = [
            {"name": "regex", "id": "end_rx", "params": {"column": "note", "pattern": "^abc$"}},
            # Written out in SQL (NaN, char padding): tests/trino/test_trino_semantics.py.
            rules.range("score", min=0, max=2),
            rules.allowed_values("code", ["ab"]),
            rules.unique("score"),
        ]
        on = _by_id(kontra.validate(trino_users_uri, rules=deferred + kept, tally=True, save=False))
        off = _by_id(
            kontra.validate(
                trino_users_uri,
                rules=deferred + kept,
                tally=True,
                save=False,
                preplan="off",
                pushdown="off",
            )
        )
        for rid in ("COL:score:allowed_values", "COL:age:allowed_values", "word_rx", "flags_rx"):
            assert on[rid].source == "polars", rid
        for rid in ("COL:score:range", "COL:code:allowed_values", "COL:score:unique"):
            assert on[rid].source == "sql", rid
        # '$' is pushed as '\z': 'abc\n' must not match, as in Polars.
        assert on["end_rx"].source == "sql"
        assert on["end_rx"].failed_count == off["end_rx"].failed_count == 5
        for rid in on:
            assert on[rid].failed_count == off[rid].failed_count, rid

    def test_preplan_not_null_from_declared_nullability(self, trino_users_uri):
        checks = [rules.not_null("user_id"), rules.not_null("status")]
        pre = _by_id(kontra.validate(trino_users_uri, rules=checks, save=False))
        off = _by_id(
            kontra.validate(
                trino_users_uri, rules=checks, save=False, preplan="off", pushdown="off"
            )
        )
        assert pre["COL:user_id:not_null"].source == "metadata"
        assert pre["COL:user_id:not_null"].passed is True
        assert pre["COL:status:not_null"].source == "sql"
        assert pre["COL:status:not_null"].passed is False
        for rid in pre:
            assert pre[rid].passed == off[rid].passed, rid

    def test_freshness_naive_timestamp_and_date(self, trino_users_uri):
        checks = [
            rules.freshness("created_at", max_age="1d"),
            rules.freshness("signup", max_age="36500d"),
        ]
        on = _by_id(kontra.validate(trino_users_uri, rules=checks, save=False))
        off = _by_id(kontra.validate(trino_users_uri, rules=checks, save=False, pushdown="off"))
        assert on["COL:created_at:freshness"].source == "sql"
        assert on["COL:signup:freshness"].source == "sql"
        for rid in on:
            assert on[rid].passed == off[rid].passed, rid

    def test_iceberg_table(self, trino_iceberg_uri):
        checks = [
            rules.not_null("event_id"),
            rules.unique("event_id"),
            rules.allowed_values("kind", ["click", "view"]),
            rules.range("amount", min=0),
            rules.freshness("ts", max_age="1d"),
        ]
        on = _by_id(kontra.validate(trino_iceberg_uri, rules=checks, tally=True, save=False))
        pre = _by_id(kontra.validate(trino_iceberg_uri, rules=checks[:1], save=False))
        off = _by_id(
            kontra.validate(
                trino_iceberg_uri,
                rules=checks,
                tally=True,
                save=False,
                preplan="off",
                pushdown="off",
            )
        )
        assert pre["COL:event_id:not_null"].source == "metadata"
        assert pre["COL:event_id:not_null"].passed is True
        assert on["COL:event_id:unique"].failed_count == 1
        assert on["COL:kind:allowed_values"].failed_count == 1
        assert on["COL:amount:range"].failed_count == 2
        for rid in on:
            assert on[rid].passed == off[rid].passed, rid
            assert on[rid].failed_count == off[rid].failed_count, rid


class TestTrinoSources:
    def test_named_datasource(self, trino_users_uri, tmp_path, monkeypatch):
        config_dir = tmp_path / ".kontra"
        config_dir.mkdir()
        (config_dir / "config.yml").write_text(
            'version: "1"\n'
            "datasources:\n"
            "  lake:\n"
            "    type: trino\n"
            "    host: localhost\n"
            "    port: 8095\n"
            "    user: kontra\n"
            "    catalog: memory\n"
            "    tables:\n"
            "      users: kontra.users\n"
        )
        monkeypatch.chdir(tmp_path)
        r = kontra.validate("lake.users", rules=[rules.unique("user_id")], tally=True, save=False)
        assert r.rules[0].failed_count == 1
        assert r.rules[0].source == "sql"

    def test_bring_your_own_connection(self, trino_users_uri, trino_connect):
        conn = trino_connect()
        try:
            r = _by_id(
                kontra.validate(
                    conn,
                    table="memory.kontra.users",
                    rules=[
                        rules.unique("user_id"),
                        rules.freshness("created_at", max_age="1d"),
                        rules.freshness("updated_at", max_age="1d"),
                    ],
                    tally=True,
                    save=False,
                )
            )
        finally:
            conn.close()
        assert r["COL:user_id:unique"].failed_count == 1
        assert r["COL:user_id:unique"].source == "sql"
        # Naive timestamps compare with UTC now, as in Polars, so both push.
        assert r["COL:created_at:freshness"].source == "sql"
        assert r["COL:updated_at:freshness"].source == "sql"

    def test_compare_table_vs_dataframe(self, trino_users_uri):
        import polars as pl

        after = pl.DataFrame({"user_id": [1, 2, 3, 99]})
        r = kontra.compare(trino_users_uri, after, key="user_id")
        assert r.dropped == 1 and r.added == 1  # 5 dropped, 99 added

    def test_missing_table_errors(self, trino_container):
        from trino.exceptions import TrinoUserError

        with pytest.raises(TrinoUserError, match="TABLE_NOT_FOUND"):
            kontra.validate(
                "trino://kontra@localhost:8095/memory/kontra.no_such_table",
                rules=[rules.not_null("x")],
                save=False,
            )


class TestTrinoEscaping:
    """LIKE wildcards and backslashes in rule literals must match Polars."""

    def test_like_and_regex_literals(self, trino_connect):
        table = "memory.kontra.escapes"
        conn = trino_connect()
        cur = conn.cursor()
        for sql in (
            "CREATE SCHEMA IF NOT EXISTS memory.kontra",
            f"DROP TABLE IF EXISTS {table}",
            f"CREATE TABLE {table} (v varchar)",
            (
                f"INSERT INTO {table} VALUES ('a%b'), ('a\\b'), ('a_b'), ('axb'), ('plain'), "
                "(NULL), ('100%'), ('x_'), ('\\')"
            ),
        ):
            cur.execute(sql)
            cur.fetchall()
        uri = "trino://kontra@localhost:8095/memory/kontra.escapes"
        checks = [
            rules.contains("v", "%"),
            rules.starts_with("v", "a\\"),
            rules.ends_with("v", "_"),
            {"name": "contains", "id": "backslash", "params": {"column": "v", "substring": "\\"}},
            {"name": "starts_with", "id": "underscore", "params": {"column": "v", "prefix": "a_"}},
            {"name": "regex", "id": "literal_rx", "params": {"column": "v", "pattern": r"a\\b"}},
        ]
        try:
            on = _by_id(kontra.validate(uri, rules=checks, tally=True, save=False))
            off = _by_id(
                kontra.validate(
                    uri, rules=checks, tally=True, save=False, preplan="off", pushdown="off"
                )
            )
        finally:
            cur.execute(f"DROP TABLE IF EXISTS {table}")
            cur.fetchall()
            conn.close()
        for rid in on:
            assert on[rid].source == "sql", rid
            assert on[rid].failed_count == off[rid].failed_count, rid
