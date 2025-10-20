import types
import pytest

# We'll monkeypatch pick_executor to return a fake executor whose execute() raises
import kontra.engine.executors.registry as exec_registry

class BoomExecutor:
    name = "boom"
    def compile(self, sql_rules): return "SELECT 1"
    def execute(self, handle, sql): raise RuntimeError("boom during execute()")
    def introspect(self, handle): return {"row_count": None, "available_cols": []}

@pytest.mark.integration
def test_executor_failure_falls_back_to_polars(monkeypatch, write_contract, small_mixed_users, run_engine):
    RULES = [
        {"name": "not_null", "params": {"column": "email"}},  # would be SQL-capable
        {"name": "unique", "params": {"column": "user_id"}},  # residual
    ]
    cpath = write_contract(dataset=small_mixed_users, rules=RULES)

    # Force our boom executor to be picked
    monkeypatch.setattr(exec_registry, "pick_executor", lambda handle, sql_rules: BoomExecutor())

    # Run with pushdown auto — the executor will blow up; engine should recover
    out, label = run_engine(cpath, pushdown="auto", stats_mode="summary")
    # Engine label should still mention polars; pushdown may show 'on' (executor selected)
    assert "polars" in label
    # Results should still exist; not crash
    assert isinstance(out["results"], list) and len(out["results"]) >= 1
