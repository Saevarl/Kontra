from __future__ import annotations
import math
import pytest

# Reuse your dynamic contract writer
from .conftest import write_contract  # adjust import if needed

pytestmark = [pytest.mark.integration, pytest.mark.pushdown, pytest.mark.projection]

RULES_MIXED = [
    # SQL-capable (pushdown candidates)
    {"name": "not_null", "params": {"column": "email"}},
    {"name": "not_null", "params": {"column": "status"}},
    {"name": "not_null", "params": {"column": "last_login"}},
    # Residual rules (Polars)
    {"name": "unique", "params": {"column": "user_id"}},
    {"name": "dtype", "params": {"column": "country", "type": "utf8"}},
    {"name": "dtype", "params": {"column": "signup_date", "type": "date"}},
    {"name": "dtype", "params": {"column": "age", "type": "int16"}},
    {"name": "dtype", "params": {"column": "is_premium", "type": "boolean"}},
    {"name": "dtype", "params": {"column": "balance", "type": "float64"}},
]

def _counts(out):
    summary = out["summary"]
    stats = out.get("stats", {})
    proj = stats.get("projection", {})
    # Handle nested structure: required_count is under "full", loaded_count under "residual"
    full = proj.get("full", {})
    residual = proj.get("residual", {})
    phases = stats.get("run_meta", {}).get("phases_ms", {})
    return {
        "rules_total": summary["total_rules"],
        "rules_failed": summary["rules_failed"],
        "required_count": full.get("required_count"),
        "loaded_count": residual.get("loaded_count"),
        "available_count": proj.get("available_count"),
        "pushdown_ms": phases.get("pushdown", 0),
        "duration_ms_total": stats.get("run_meta", {}).get("duration_ms_total", 0),
        "phases_sum": sum((phases or {}).values()),
        "staged_override": stats.get("run_meta", {}).get("staged_override", False),
        "engine": stats.get("run_meta", {}).get("engine", ""),
        "pushdown_effective": stats.get("run_meta", {}).get("pushdown", ""),
    }

@pytest.mark.parametrize("csv_mode", ["auto", "duckdb", "parquet"])
def test_csv_pushdown_modes_timing_and_staging(write_contract, small_mixed_users_csv, run_engine, csv_mode):
    cpath = write_contract(dataset=small_mixed_users_csv, rules=RULES_MIXED)
    out, _ = run_engine(
        contract_path=cpath,
        pushdown="on",
        csv_mode=csv_mode,
        enable_projection=True,
        stats_mode="summary",
    )
    c = _counts(out)

    # Projection sanity (we specified 9 rules)
    assert c["required_count"] == 9
    assert c["available_count"] >= 9

    if csv_mode == "parquet":
        # We expect real staging & measurable pushdown time
        assert c["staged_override"] is True
        assert c["pushdown_ms"] > 0
        # Duration must include pushdown; allow small rounding drift
        assert abs(c["duration_ms_total"] - c["phases_sum"]) <= 5
        assert "pushdown:on" in c["engine"].replace(" ", "")
    else:
        # In auto/duckdb, pushdown may run directly over CSV; still expect consistency
        assert c["required_count"] == 9
        assert "polars" in c["engine"]  # validator is polars
        assert c["pushdown_effective"] in ("on", "off")

def test_csv_projection_independence(write_contract, small_mixed_users_csv, run_engine):
    cpath = write_contract(dataset=small_mixed_users_csv, rules=RULES_MIXED)

    # ON / ON
    out_on_on, _ = run_engine(cpath, pushdown="on", csv_mode="auto", enable_projection=True)
    c1 = _counts(out_on_on)

    # ON / OFF
    out_on_off, _ = run_engine(cpath, pushdown="on", csv_mode="auto", enable_projection=False)
    c2 = _counts(out_on_off)

    # OFF / ON
    out_off_on, _ = run_engine(cpath, pushdown="off", csv_mode="auto", enable_projection=True)
    c3 = _counts(out_off_on)

    # OFF / OFF
    out_off_off, _ = run_engine(cpath, pushdown="off", csv_mode="auto", enable_projection=False)
    c4 = _counts(out_off_off)

    # Independence invariants
    assert c1["required_count"] == 9
    assert c3["required_count"] == 9
    # Projection off should load >= available (i.e., all columns)
    assert c2["loaded_count"] >= c2["available_count"] or c2["loaded_count"] == c2["available_count"]
    assert c4["loaded_count"] >= c4["available_count"] or c4["loaded_count"] == c4["available_count"]
    # Projection on should load <= available
    assert c1["loaded_count"] <= c1["available_count"]
    assert c3["loaded_count"] <= c3["available_count"]

def test_csv_vs_parquet_parity_when_pushdown_off(write_contract, small_mixed_users, small_mixed_users_csv, run_engine):
    """
    With pushdown OFF and projection ON, CSV and Parquet should produce identical
    rule outcomes (messages may differ slightly; we compare core fields).
    """
    RULES = [
        {"name": "not_null", "params": {"column": "email"}},
        {"name": "unique", "params": {"column": "user_id"}},
        {"name": "dtype", "params": {"column": "age", "type": "int"}},
    ]

    c_csv = write_contract(dataset=small_mixed_users_csv, rules=RULES)
    # Disable preplan so both CSV and Parquet count violations the same way
    out_csv, _ = run_engine(c_csv, pushdown="off", preplan="off", csv_mode="auto", enable_projection=True)

    c_parq = write_contract(dataset=small_mixed_users, rules=RULES)
    out_parq, _ = run_engine(c_parq, pushdown="off", preplan="off", csv_mode="auto", enable_projection=True)

    # Compare by rule_id → (passed, failed_count)
    def pick(o):
        return {r["rule_id"]: (r["passed"], r["failed_count"]) for r in o["results"]}

    assert pick(out_csv) == pick(out_parq)

def test_timers_sum_includes_pushdown_for_staging(write_contract, small_clean_users_csv, run_engine):
    cpath = write_contract(dataset=small_clean_users_csv, rules=[
        {"name": "not_null", "params": {"column": "email"}},
        {"name": "dtype", "params": {"column": "age", "type": "int16"}},
    ])
    out, _ = run_engine(cpath, pushdown="on", csv_mode="parquet", enable_projection=True)
    stats = out.get("stats", {}).get("run_meta", {})
    phases = stats.get("phases_ms", {})
    total = stats.get("duration_ms_total", 0)

    assert phases.get("pushdown", 0) > 0
    # duration must be the sum of phases (± a few ms for rounding)
    assert abs(total - sum(phases.values())) <= 5
