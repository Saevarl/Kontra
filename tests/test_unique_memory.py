"""Duplicate diagnostics retain exact counts and masks after frequency reuse."""

import polars as pl
import pytest

from kontra.rule_defs.builtin.unique import UniqueRule


@pytest.mark.parametrize("name", ["id", "count", "value"])
def test_duplicate_details_and_mask_include_all_groups(name):
    values = [None] * 4 + [1] * 3 + [2] * 2 + [3]
    df = pl.DataFrame({name: values, "unused": range(len(values))})
    result = UniqueRule("unique", {"column": name}).validate(df)
    assert result["failed_count"] == 3  # extra non-null duplicates
    assert result["details"] == {
        "duplicate_value_count": 3,
        "top_duplicates": [
            {"value": None, "count": 4},
            {"value": 1, "count": 3},
            {"value": 2, "count": 2},
        ],
    }
    assert df.filter(result["_failure_mask"])[name].to_list() == [1, 1, 1, 2, 2]


def test_duplicate_details_count_all_groups_but_limit_examples():
    values = [f"group-{i}" for i in range(15) for _ in range(i + 2)]
    df = pl.DataFrame({"id": values + ["once"]})
    result = UniqueRule("unique", {"column": "id"}).validate(df)
    assert result["failed_count"] == len(values) - 15
    assert result["details"]["duplicate_value_count"] == 15
    assert result["details"]["top_duplicates"] == [
        {"value": f"group-{i}", "count": i + 2} for i in range(14, 4, -1)
    ]
