import copy
import pytest

@pytest.mark.integration
def test_identical_runs_identical_outputs(write_contract, small_clean_users, run_engine):
    RULES = [
        {"name": "not_null", "params": {"column": "email"}},
        {"name": "not_null", "params": {"column": "status"}},
        {"name": "dtype", "params": {"column": "country", "type": "utf8"}},
    ]
    cpath = write_contract(dataset=small_clean_users, rules=RULES)

    out1, _ = run_engine(cpath, pushdown="auto", stats_mode="summary")
    out2, _ = run_engine(cpath, pushdown="auto", stats_mode="summary")

    # Remove volatile timing fields before compare
    def strip(o):
        o = copy.deepcopy(o)
        if "stats" in o and "run_meta" in o["stats"]:
            o["stats"]["run_meta"].pop("phases_ms", None)
            o["stats"]["run_meta"].pop("duration_ms_total", None)
        return o

    assert strip(out1) == strip(out2)
