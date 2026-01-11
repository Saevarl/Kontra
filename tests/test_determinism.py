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
        o.pop("stats", None)  # drop stats entirely; timing & ordering can vary
        return o


    assert strip(out1) == strip(out2)
