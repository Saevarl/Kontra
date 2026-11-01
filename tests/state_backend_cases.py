# tests/state_backend_cases.py
"""
Shared behavioural cases for the SQL state backends (PostgresStore,
SQLServerStore).

Not collected on its own (no ``test_`` prefix). The live suites under
tests/postgres and tests/sqlserver subclass ``StateStoreCases`` and provide a
``store_uri`` fixture pointing at a dedicated, empty database, so the same
assertions pin both dialects.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import pytest

from kontra.state.types import Annotation, RuleState, StateSummary, ValidationState

STATE_TABLES = ("kontra_annotations", "kontra_rule_results", "kontra_runs")

FP = "fp_state_cases"
OTHER_FP = "fp_state_cases_other"
BASE_TIME = datetime(2026, 3, 1, 12, 0, 0, 123456, tzinfo=timezone.utc)


def make_state(
    fingerprint: str = FP,
    *,
    minutes: int = 0,
    passed: bool = False,
    rules: Optional[List[RuleState]] = None,
) -> ValidationState:
    if rules is None:
        rules = [
            RuleState(
                rule_id="COL:email:not_null",
                rule_name="not_null",
                passed=False,
                failed_count=7 + minutes,
                execution_source="sql",
                severity="blocking",
                failure_mode="null_values",
                details={"null_count": 7 + minutes, "nested": {"a": [1, 2]}},
                message="email has nulls",
                column="email",
            ),
            RuleState(
                rule_id="DATASET:min_rows",
                rule_name="min_rows",
                passed=True,
                failed_count=0,
                execution_source="metadata",
                severity="warning",
            ),
        ]
    failed = sum(1 for r in rules if not r.passed)
    return ValidationState(
        contract_fingerprint=fingerprint,
        dataset_fingerprint=f"ds_{minutes}",
        contract_name="users_contract",
        dataset_uri="postgres://host/db/public.users",
        run_at=BASE_TIME + timedelta(minutes=minutes),
        summary=StateSummary(
            passed=passed,
            total_rules=len(rules),
            passed_rules=len(rules) - failed,
            failed_rules=failed,
            row_count=1000 + minutes,
            blocking_failures=failed,
            warning_failures=0,
            info_failures=0,
        ),
        rules=rules,
        schema_version="2.0",
        engine_version="9.9.9",
        duration_ms=250 + minutes,
    )


class _CountingCursor:
    def __init__(self, cursor, counter: List[str]):
        self._cursor = cursor
        self._counter = counter

    def execute(self, sql, *args, **kwargs):
        self._counter.append(sql)
        return self._cursor.execute(sql, *args, **kwargs)

    def __enter__(self):
        self._cursor.__enter__()
        return self

    def __exit__(self, *exc):
        return self._cursor.__exit__(*exc)

    def __getattr__(self, name):
        return getattr(self._cursor, name)


class _CountingConnection:
    """Wraps a DB-API connection and records every statement sent."""

    def __init__(self, conn):
        self._conn = conn
        self.statements: List[str] = []

    def cursor(self, *args, **kwargs):
        return _CountingCursor(self._conn.cursor(*args, **kwargs), self.statements)

    def __getattr__(self, name):
        return getattr(self._conn, name)


def _count_rows(store, table: str) -> int:
    cur = store._get_conn().cursor()
    cur.execute(f"SELECT COUNT(*) FROM {table}")
    n = cur.fetchone()[0]
    cur.close()
    return n


class StateStoreCases:
    """Behaviour every SQL state backend must share. Subclasses supply
    ``store_class`` and a ``store_uri`` fixture."""

    store_class: Any = None

    # Statements sent per operation for a contract holding 3 runs of 2 rules.
    # Pins round trips so refactors cannot quietly add queries.
    expected_statements: Dict[str, int] = {
        "save": 3,  # run insert + one insert per rule
        "get_latest": 2,
        "get_history": 2,
        "get_at": 2,
        "delete_old": 1,
        "list_contracts": 1,
        "clear": 1,
        "save_annotation": 1,
        "get_annotations": 1,
        "get_annotations_for_contract": 1,
        "get_run_with_annotations": 3,
        "get_history_with_annotations": 3,
    }

    # SQLServerStore has never implemented the cross-run annotation query and
    # falls back to StateBackend's empty default.
    supports_contract_annotations = True

    @pytest.fixture
    def store(self, store_uri):
        store = self.store_class(store_uri)
        store.clear()
        yield store
        store.close()

    def _save_three(self, store) -> List[ValidationState]:
        states = [make_state(minutes=m) for m in (0, 10, 20)]
        for s in states:
            store.save(s)
        return states

    # ------------------------------------------------------------------
    # Schema
    # ------------------------------------------------------------------

    def test_creates_all_tables_and_is_idempotent(self, store, store_uri):
        store.save(make_state())
        for table in STATE_TABLES:
            assert _count_rows(store, table) >= 0

        # A second instance must run the DDL again without error
        again = self.store_class(store_uri)
        try:
            assert again.get_latest(FP) is not None
        finally:
            again.close()

    # ------------------------------------------------------------------
    # Runs
    # ------------------------------------------------------------------

    def test_save_assigns_id_and_round_trips_every_field(self, store):
        state = make_state(minutes=5, passed=False)
        store.save(state)
        assert isinstance(state.id, int)

        loaded = store.get_latest(FP)
        assert loaded is not None
        assert loaded.id == state.id
        assert loaded.contract_fingerprint == FP
        assert loaded.contract_name == "users_contract"
        assert loaded.dataset_fingerprint == "ds_5"
        assert loaded.dataset_uri == "postgres://host/db/public.users"
        assert loaded.run_at == state.run_at
        assert loaded.run_at.utcoffset() == timedelta(0)
        assert loaded.duration_ms == 255
        assert loaded.schema_version == "2.0"
        assert loaded.engine_version == "9.9.9"

        s = loaded.summary
        assert s.passed is False
        assert (s.total_rules, s.passed_rules, s.failed_rules) == (2, 1, 1)
        assert s.row_count == 1005
        assert (s.blocking_failures, s.warning_failures, s.info_failures) == (1, 0, 0)

        assert [r.rule_id for r in loaded.rules] == ["COL:email:not_null", "DATASET:min_rows"]
        r0, r1 = loaded.rules
        assert r0.rule_name == "not_null"
        assert r0.passed is False
        assert r0.failed_count == 12
        assert r0.execution_source == "sql"
        assert r0.severity == "blocking"
        assert r0.failure_mode == "null_values"
        assert r0.details == {"null_count": 12, "nested": {"a": [1, 2]}}
        assert r0.message == "email has nulls"
        assert r0.column == "email"
        assert isinstance(r0.id, int)
        assert r1.passed is True
        assert r1.details is None
        assert r1.message is None
        assert r1.column is None
        assert r1.failure_mode is None
        assert r1.id > r0.id

    def test_unicode_and_large_counts_round_trip(self, store):
        rule = RuleState(
            rule_id="COL:navn:allowed_values",
            rule_name="allowed_values",
            passed=False,
            failed_count=9_000_000_000,
            execution_source="polars",
            details={"unexpected": ["Þórður", "Ærøskøbing", "日本"]},
            message="Gildi utan leyfðra: Þórður",
        )
        state = make_state(rules=[rule])
        store.save(state)
        loaded = store.get_latest(FP).rules[0]
        assert loaded.failed_count == 9_000_000_000
        assert loaded.details == {"unexpected": ["Þórður", "Ærøskøbing", "日本"]}
        assert loaded.message == "Gildi utan leyfðra: Þórður"

    def test_save_with_no_rules(self, store):
        state = make_state(rules=[])
        store.save(state)
        loaded = store.get_latest(FP)
        assert loaded.id == state.id
        assert loaded.rules == []
        assert [h.rules for h in store.get_history(FP)] == [[]]

    def test_get_latest_unknown_contract(self, store):
        assert store.get_latest("does_not_exist") is None

    def test_get_latest_picks_newest_run_at_not_newest_insert(self, store):
        newer = make_state(minutes=30)
        older = make_state(minutes=1)
        store.save(newer)
        store.save(older)
        assert store.get_latest(FP).id == newer.id

    def test_get_history_order_limit_and_rule_grouping(self, store):
        states = self._save_three(store)
        store.save(make_state(OTHER_FP, minutes=99))

        history = store.get_history(FP)
        assert [h.id for h in history] == [s.id for s in reversed(states)]
        for h, minutes in zip(history, (20, 10, 0)):
            assert h.run_at == BASE_TIME + timedelta(minutes=minutes)
            assert [r.rule_id for r in h.rules] == ["COL:email:not_null", "DATASET:min_rows"]
            assert h.rules[0].failed_count == 7 + minutes

        assert [h.id for h in store.get_history(FP, limit=2)] == [states[2].id, states[1].id]
        assert store.get_history("does_not_exist") == []

    def test_get_at_and_get_previous(self, store):
        states = self._save_three(store)

        assert store.get_at(FP, BASE_TIME + timedelta(minutes=15)).id == states[1].id
        assert store.get_at(FP, BASE_TIME + timedelta(minutes=10)).id == states[1].id
        assert store.get_at(FP, BASE_TIME - timedelta(minutes=1)) is None
        assert len(store.get_at(FP, BASE_TIME + timedelta(days=1)).rules) == 2

        prev = store.get_previous(FP, before=states[2].run_at)
        assert prev.id == states[1].id

    def test_get_run_summaries(self, store):
        states = self._save_three(store)
        summaries = store.get_run_summaries(FP, limit=2)
        assert [s.run_id for s in summaries] == [str(states[2].id), str(states[1].id)]
        assert summaries[0].contract_name == "users_contract"

    def test_delete_old_keeps_newest_and_cascades(self, store):
        states = self._save_three(store)
        store.save(make_state(OTHER_FP))
        ann = Annotation(
            run_id=states[0].id, actor_id="t", annotation_type="note", summary="old"
        )
        store.save_annotation(ann)
        rule_ann = Annotation(
            run_id=states[0].id,
            rule_result_id=store.get_history(FP)[-1].rules[0].id,
            actor_id="t",
            annotation_type="note",
            summary="old rule",
        )
        store.save_annotation(rule_ann)

        assert store.delete_old(FP, keep_count=2) == 1
        assert [h.id for h in store.get_history(FP)] == [states[2].id, states[1].id]
        assert store.get_annotations(states[0].id) == []
        assert _count_rows(store, "kontra_rule_results") == 3 * 2
        assert _count_rows(store, "kontra_annotations") == 0
        # Other contracts untouched
        assert store.get_latest(OTHER_FP) is not None

        assert store.delete_old(FP, keep_count=5) == 0

    def test_list_contracts(self, store):
        assert store.list_contracts() == []
        store.save(make_state(OTHER_FP))
        store.save(make_state(FP))
        store.save(make_state(FP, minutes=1))
        assert store.list_contracts() == sorted([FP, OTHER_FP])

    def test_clear_one_contract_then_all(self, store):
        self._save_three(store)
        other = make_state(OTHER_FP)
        store.save(other)
        store.save_annotation(
            Annotation(
                run_id=other.id,
                rule_result_id=store.get_latest(OTHER_FP).rules[0].id,
                actor_id="t",
                annotation_type="note",
                summary="s",
            )
        )

        assert store.clear(FP) == 3
        assert store.list_contracts() == [OTHER_FP]
        assert store.clear() == 1
        assert store.list_contracts() == []
        for table in STATE_TABLES:
            assert _count_rows(store, table) == 0

    # ------------------------------------------------------------------
    # Annotations
    # ------------------------------------------------------------------

    def _annotate(self, store) -> Dict[str, Any]:
        states = self._save_three(store)
        latest = store.get_latest(FP)
        rule_ids = [r.id for r in latest.rules]
        older = store.get_history(FP)[1]

        anns = {
            "run_note": Annotation(
                run_id=latest.id,
                actor_type="agent",
                actor_id="repair-agent",
                annotation_type="note",
                summary="looked at it",
                payload={"ticket": "DATA-1", "tags": ["a", "b"]},
                created_at=BASE_TIME + timedelta(hours=1),
            ),
            "rule_resolution": Annotation(
                run_id=latest.id,
                rule_result_id=rule_ids[0],
                actor_type="human",
                actor_id="alice@example.com",
                annotation_type="resolution",
                summary="upstream fixed",
                created_at=BASE_TIME + timedelta(hours=2),
            ),
            "older_rule_note": Annotation(
                run_id=older.id,
                rule_result_id=older.rules[0].id,
                actor_type="system",
                actor_id="kontra",
                annotation_type="note",
                summary="first seen",
                created_at=BASE_TIME + timedelta(minutes=30),
            ),
        }
        for a in anns.values():
            store.save_annotation(a)
        return {"states": states, "latest": latest, "older": older, "anns": anns}

    def test_save_annotation_assigns_id(self, store):
        state = make_state()
        store.save(state)
        ann = Annotation(run_id=state.id, actor_id="x", annotation_type="t", summary="s")
        ann_id = store.save_annotation(ann)
        assert isinstance(ann_id, int)
        assert ann.id == ann_id

        loaded = store.get_annotations(state.id)
        assert len(loaded) == 1
        assert loaded[0].id == ann_id
        assert loaded[0].payload is None
        # created_at defaults to "now" when not supplied
        assert loaded[0].created_at is not None
        assert abs(loaded[0].created_at - datetime.now(timezone.utc)) < timedelta(minutes=5)

    def test_save_annotation_for_missing_run_raises(self, store):
        with pytest.raises(IOError):
            store.save_annotation(
                Annotation(run_id=999_999, actor_id="x", annotation_type="t", summary="s")
            )
        # The store stays usable after the failed write
        state = make_state()
        store.save(state)
        assert store.get_latest(FP).id == state.id

    def test_get_annotations_run_and_rule_filters(self, store):
        ctx = self._annotate(store)
        latest, anns = ctx["latest"], ctx["anns"]

        all_for_run = store.get_annotations(latest.id)
        assert [a.id for a in all_for_run] == [
            anns["rule_resolution"].id,
            anns["run_note"].id,
        ]
        note = all_for_run[1]
        assert note.run_id == latest.id
        assert note.rule_result_id is None
        assert note.actor_type == "agent"
        assert note.actor_id == "repair-agent"
        assert note.annotation_type == "note"
        assert note.summary == "looked at it"
        assert note.payload == {"ticket": "DATA-1", "tags": ["a", "b"]}
        assert note.created_at == BASE_TIME + timedelta(hours=1)

        only_rule = store.get_annotations(latest.id, rule_result_id=latest.rules[0].id)
        assert [a.id for a in only_rule] == [anns["rule_resolution"].id]
        assert store.get_annotations(999_999) == []

    def test_get_run_with_annotations(self, store):
        ctx = self._annotate(store)
        latest, older, anns = ctx["latest"], ctx["older"], ctx["anns"]

        state = store.get_run_with_annotations(FP)
        assert state.id == latest.id
        assert [a.id for a in state.annotations] == [anns["run_note"].id]
        assert [a.id for a in state.rules[0].annotations] == [anns["rule_resolution"].id]
        assert state.rules[1].annotations == []

        by_id = store.get_run_with_annotations(FP, run_id=older.id)
        assert by_id.id == older.id
        assert by_id.annotations == []
        assert [a.id for a in by_id.rules[0].annotations] == [anns["older_rule_note"].id]

        assert store.get_run_with_annotations(OTHER_FP, run_id=older.id) is None
        assert store.get_run_with_annotations("does_not_exist") is None

    def test_get_history_with_annotations(self, store):
        ctx = self._annotate(store)
        anns = ctx["anns"]

        history = store.get_history_with_annotations(FP)
        assert [h.id for h in history] == [s.id for s in reversed(ctx["states"])]
        newest, middle, oldest = history
        assert [a.id for a in newest.annotations] == [anns["run_note"].id]
        assert [a.id for a in newest.rules[0].annotations] == [anns["rule_resolution"].id]
        assert newest.rules[1].annotations == []
        assert middle.annotations == []
        assert [a.id for a in middle.rules[0].annotations] == [anns["older_rule_note"].id]
        assert oldest.annotations == []
        assert all(r.annotations == [] for r in oldest.rules)

        assert store.get_history_with_annotations("does_not_exist") == []

    def test_get_annotations_for_contract(self, store):
        ctx = self._annotate(store)
        anns = ctx["anns"]
        store.save(make_state(OTHER_FP))

        result = store.get_annotations_for_contract(FP)
        if not self.supports_contract_annotations:
            assert result == []
            return

        assert [a.id for a in result] == [
            anns["rule_resolution"].id,
            anns["run_note"].id,
            anns["older_rule_note"].id,
        ]
        assert [a.rule_id for a in result] == ["COL:email:not_null", None, "COL:email:not_null"]

        by_rule = store.get_annotations_for_contract(FP, rule_id="COL:email:not_null")
        assert [a.id for a in by_rule] == [anns["rule_resolution"].id, anns["older_rule_note"].id]

        by_type = store.get_annotations_for_contract(FP, annotation_type="note")
        assert [a.id for a in by_type] == [anns["run_note"].id, anns["older_rule_note"].id]

        both = store.get_annotations_for_contract(
            FP, rule_id="COL:email:not_null", annotation_type="note"
        )
        assert [a.id for a in both] == [anns["older_rule_note"].id]

        assert len(store.get_annotations_for_contract(FP, limit=1)) == 1
        assert store.get_annotations_for_contract(OTHER_FP) == []

    # ------------------------------------------------------------------
    # Round trips
    # ------------------------------------------------------------------

    def test_statements_per_operation(self, store):
        ctx = self._annotate(store)
        latest = ctx["latest"]

        counting = _CountingConnection(store._get_conn())
        store._conn = counting
        try:
            def sent(op, *args, **kwargs) -> int:
                before = len(counting.statements)
                op(*args, **kwargs)
                return len(counting.statements) - before

            observed = {
                "save": sent(store.save, make_state(OTHER_FP)),
                "get_latest": sent(store.get_latest, FP),
                "get_history": sent(store.get_history, FP),
                "get_at": sent(store.get_at, FP, BASE_TIME + timedelta(minutes=15)),
                "list_contracts": sent(store.list_contracts),
                "save_annotation": sent(
                    store.save_annotation,
                    Annotation(run_id=latest.id, actor_id="x", annotation_type="t", summary="s"),
                ),
                "get_annotations": sent(store.get_annotations, latest.id),
                "get_annotations_for_contract": sent(store.get_annotations_for_contract, FP),
                "get_run_with_annotations": sent(store.get_run_with_annotations, FP),
                "get_history_with_annotations": sent(store.get_history_with_annotations, FP),
                "delete_old": sent(store.delete_old, FP, keep_count=2),
                "clear": sent(store.clear, OTHER_FP),
            }
        finally:
            store._conn = counting._conn

        expected = dict(self.expected_statements)
        if not self.supports_contract_annotations:
            expected["get_annotations_for_contract"] = 0
        assert observed == expected
