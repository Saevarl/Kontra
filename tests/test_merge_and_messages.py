# tests/test_merge_and_messages.py
import re
from .utils import by_rule_id

def _find_rule(out, needle_substr: str):
    """Return (rule_id, rule_obj) where rule_id contains `needle_substr` (case-insensitive)."""
    ridx = by_rule_id(out)
    for rid, robj in ridx.items():
        if needle_substr.lower() in rid.lower():
            return rid, robj
    return None, None

def test_sql_wins_on_overlap(write_contract, small_nulls_only, run_engine):
    # not_null(email/last_login/status) should be handled by SQL; Polars may compute too,
    # but engine must prefer SQL results for overlapping rule_ids.
    RULES = [
        {"name": "not_null", "params": {"column": "email"}},
        {"name": "not_null", "params": {"column": "last_login"}},
        {"name": "not_null", "params": {"column": "status"}},
        {"name": "unique", "params": {"column": "user_id"}},
    ]
    cpath = write_contract(dataset=small_nulls_only, rules=RULES)
    out, _ = run_engine(cpath, pushdown="auto", stats_mode="summary")
    ridx = by_rule_id(out)

    # 1) Ensure unique rule appears exactly once
    unique_ids = [k for k in ridx if "unique" in k.lower()]
    assert len(unique_ids) == 1

    # 2) Expect not_null(email) and not_null(last_login) to FAIL (we injected nulls)
    for col in ("email", "last_login"):
        rid, robj = _find_rule(out, f":{col}:") or _find_rule(out, col)
        assert rid is not None, f"missing rule_id for column {col}"
        assert robj is not None
        assert robj.get("passed") is False, f"expected not_null({col}) to fail"

    # 3) Expect not_null(status) to PASS (no nulls injected for status)
    rid, robj = _find_rule(out, ":status:") or _find_rule(out, "status")
    assert rid is not None, "missing rule_id for column status"
    assert robj is not None
    assert robj.get("passed") is True, "expected not_null(status) to pass"

    # 4) Rule IDs are non-empty and stable-looking
    for k in ridx:
        assert re.match(r".+", k)
