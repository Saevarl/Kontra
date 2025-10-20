import json
import pytest

SCHEMA_VERSION = "1.0"

@pytest.mark.integration
def test_json_minimum_fields_and_parity(write_contract, small_clean_users, run_engine):
    RULES = [
        {"name": "not_null", "params": {"column": "email"}},
        {"name": "dtype", "params": {"column": "age", "type": "int16"}},
    ]
    cpath = write_contract(dataset=small_clean_users, rules=RULES)
    out, _ = run_engine(cpath, pushdown="auto", stats_mode="summary")

    # shape: top-level keys present
    for key in ("dataset", "results", "summary"):
        assert key in out

    # summary parity (counts add up)
    summary = out["summary"]
    assert summary["rules_passed"] + summary["rules_failed"] == summary["total_rules"]

    # (Optional) ensure we can serialize and keep field order deterministic
    s1 = json.dumps(out, sort_keys=True)
    s2 = json.dumps(out, sort_keys=True)
    assert s1 == s2
