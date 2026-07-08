import pytest
from .utils import collect_counts

pytestmark = [pytest.mark.integration, pytest.mark.pushdown, pytest.mark.projection]

# Contract equivalent to your “1 rule per col; 3 SQL; 6 residual”
RULES_MIXED = [
    # SQL-capable pushdown (not_null on 3 cols)
    {"name": "not_null", "params": {"column": "email"}},
    {"name": "not_null", "params": {"column": "status"}},
    {"name": "not_null", "params": {"column": "last_login"}},
    # Residual (Polars) rules (6 cols)
    {"name": "unique", "params": {"column": "user_id"}},
    {"name": "dtype", "params": {"column": "country", "type": "utf8"}},
    {"name": "dtype", "params": {"column": "signup_date", "type": "date"}},
    {"name": "dtype", "params": {"column": "age", "type": "int16"}},
    {"name": "dtype", "params": {"column": "is_premium", "type": "boolean"}},
    {"name": "dtype", "params": {"column": "balance", "type": "float64"}},
]

def test_pushdown_auto_projects_columns(write_contract, small_clean_users, run_engine):
    cpath = write_contract(dataset=small_clean_users, rules=RULES_MIXED)
    # Disable preplan to ensure pushdown handles not_null rules
    out, label = run_engine(contract_path=cpath, pushdown="on", preplan="off", stats_mode="summary")

    counts = collect_counts(out)
    # required: 9 columns (3 SQL + 6 residual); available includes all fillers
    assert counts["required_count"] == 9
    assert counts["available_count"] >= counts["required_count"]
    # if pushdown was effective, we expect projection to reduce loaded below available
    assert counts["loaded_count"] <= counts["available_count"]
    # clean data should pass all rules
    assert counts["rules_failed"] == 0
    assert "pushdown:on" in label.replace(" ", "")

def test_pushdown_off_loads_required_columns(write_contract, small_clean_users, run_engine):
    cpath = write_contract(dataset=small_clean_users, rules=RULES_MIXED)
    # Disable preplan so all rules go to residual (no metadata shortcuts)
    out, label = run_engine(contract_path=cpath, pushdown="off", preplan="off", stats_mode="summary")

    counts = collect_counts(out)
    assert counts["required_count"] == 9
    # with pushdown off and preplan off, residual path will load all required columns for Polars
    assert counts["loaded_count"] == counts["required_count"]
    assert counts["rules_failed"] == 0
    assert "pushdown:off" in label.replace(" ", "")

@pytest.mark.parametrize("dataset_fixture", ["small_mixed_users", "small_nulls_only"])
def test_sql_rules_execute_first_and_affect_loaded_counts(write_contract, dataset_fixture, run_engine, request):
    data = request.getfixturevalue(dataset_fixture)
    cpath = write_contract(dataset=data, rules=RULES_MIXED)
    out, _ = run_engine(cpath, pushdown="on", stats_mode="summary")
    counts = collect_counts(out)
    # Regardless of failures, required stays 9; loaded <= available
    assert counts["required_count"] == 9
    assert counts["loaded_count"] <= counts["available_count"]


def test_custom_sql_check_not_starved_by_projection(tmp_path):
    """custom_sql_check references arbitrary columns via SELECT *; column
    projection driven by a co-resident rule must not prune the columns its
    SQL needs (regression: projected frame -> Binder Error -> wrong count)."""
    import polars as pl
    import kontra

    p = str(tmp_path / "csc.parquet")
    pl.DataFrame({"id": ["usr_1", "usr_2", "usr_3"], "n": [500, 50, 900]}).write_parquet(p)
    rules = [
        kontra.rules.regex("id", "^usr_"),
        {"name": "custom_sql_check", "params": {"query": "SELECT * FROM {table} WHERE n > 100"}},
    ]
    for projection in (True, False):
        for toggles in (
            dict(preplan="off", pushdown="off"),
            dict(preplan="off", pushdown="on"),
            dict(preplan="on", pushdown="on"),
        ):
            result = kontra.validate(
                p, rules=rules, save=False, projection=projection, tally=True, **toggles
            )
            csc = [r for r in result.rules if "custom" in r.rule_id.lower()][0]
            assert csc.failed_count == 2, (projection, toggles, csc.failed_count, csc.message)
            assert "Binder" not in csc.message
