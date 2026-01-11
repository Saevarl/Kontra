# tests/test_dtype_and_rule_ids.py
from __future__ import annotations

import re
from pathlib import Path

import polars as pl
import pytest


def _tiny_int_table() -> pl.DataFrame:
    # small deterministic frame; we'll specialize dtypes per format before writing
    return pl.DataFrame(
        {
            "user_id": [1, 2, 2, 3],  # dup on purpose
            "age": [18, 21, 34, 40],  # narrow integer values
            "email": ["a@x.com", "b@x.com", None, "d@x.com"],  # one NULL for not_null
        }
    )


def _write_parquet_int16(df: pl.DataFrame, path: Path) -> None:
    # enforce exact width for parquet
    df = df.with_columns(pl.col("age").cast(pl.Int16))
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(str(path))


def _write_csv_default(df: pl.DataFrame, path: Path) -> None:
    # duckdb/polars will typically infer age as Int64 (wider than Int16)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_csv(str(path), include_header=True)


# ----------------------------- dtype tests ---------------------------------- #

@pytest.mark.integration
def test_dtype_exact_width_vs_logical_family(write_contract, run_engine, tmp_path):
    """
    Parquet preserves exact width; CSV inference is usually wider.
    - With type=int16: parquet passes; csv fails (wider Int64).
    - With type=int:   both pass (logical family).
    """
    df = _tiny_int_table()
    p_parq = tmp_path / "tiny.parquet"
    p_csv = tmp_path / "tiny.csv"
    _write_parquet_int16(df, p_parq)
    _write_csv_default(df, p_csv)

    rules_exact = [
        {"name": "dtype", "params": {"column": "age", "type": "int16"}},
    ]
    rules_family = [
        {"name": "dtype", "params": {"column": "age", "type": "int"}},
    ]

    # exact width
    c_parq = write_contract(dataset=str(p_parq), rules=rules_exact)
    o_parq, _ = run_engine(c_parq, pushdown="off")  # no pushdown; pure polars compare
    c_csv = write_contract(dataset=str(p_csv), rules=rules_exact)
    o_csv, _ = run_engine(c_csv, pushdown="off")

    by_id_parq = {r["rule_id"]: r for r in o_parq["results"]}
    by_id_csv = {r["rule_id"]: r for r in o_csv["results"]}

    # parquet should pass exact width
    assert next(v for k, v in by_id_parq.items() if ":age:" in k)["passed"] is True
    # csv should fail exact width (inferred wider)
    csv_rule = next(v for k, v in by_id_csv.items() if ":age:" in k)
    assert csv_rule["passed"] is False
    assert isinstance(csv_rule.get("failed_count"), int) and csv_rule["failed_count"] >= 1

    # logical family: both pass
    c_parq2 = write_contract(dataset=str(p_parq), rules=rules_family)
    o_parq2, _ = run_engine(c_parq2, pushdown="off")
    c_csv2 = write_contract(dataset=str(p_csv), rules=rules_family)
    o_csv2, _ = run_engine(c_csv2, pushdown="off")

    assert next(v for k, v in {r["rule_id"]: r for r in o_parq2["results"]}.items() if ":age:" in k)["passed"] is True
    assert next(v for k, v in {r["rule_id"]: r for r in o_csv2["results"]}.items() if ":age:" in k)["passed"] is True


# -------------------------- rule_id format/stability ------------------------ #

@pytest.mark.integration
def test_rule_id_contains_rule_and_column_and_is_stable(write_contract, run_engine, tmp_path):
    """
    rule_id should contain both the rule key and column name and be stable
    across YAML order changes.
    """
    df = _tiny_int_table()
    p = tmp_path / "tiny.parquet"
    _write_parquet_int16(df, p)

    rules_a = [
        {"name": "not_null", "params": {"column": "email"}},
        {"name": "unique", "params": {"column": "user_id"}},
        {"name": "dtype", "params": {"column": "age", "type": "int"}},
    ]
    rules_b = list(reversed(rules_a))  # different YAML order

    c_a = write_contract(dataset=str(p), rules=rules_a)
    o_a, _ = run_engine(c_a, pushdown="off")

    c_b = write_contract(dataset=str(p), rules=rules_b)
    o_b, _ = run_engine(c_b, pushdown="off")

    def ruleset(out):
        return {r["rule_id"]: (r["passed"], r["failed_count"], r["message"]) for r in out["results"]}

    ra, rb = ruleset(o_a), ruleset(o_b)
    assert set(ra.keys()) == set(rb.keys()), "rule_id set should be independent of YAML order"

    # format expectation: COL:<col>:<rule> or similar; be flexible but assert signal
    for rid in ra.keys():
        assert re.search(r":", rid), f"rule_id should be namespaced: {rid}"
        assert any(tok in rid.lower() for tok in ("not_null", "unique", "dtype")), f"missing rule key in {rid}"
        assert any(tok in rid.lower() for tok in ("email", "user_id", "age")), f"missing column name in {rid}"


# -------------------------- message parity (not_null) ----------------------- #

@pytest.mark.integration
@pytest.mark.parametrize("pushdown", ["on", "off"])
def test_not_null_message_parity_basic(write_contract, run_engine, tmp_path, pushdown):
    """
    We don't require identical strings, but messages should clearly indicate nulls regardless
    of pushdown path.
    """
    df = _tiny_int_table()
    p = tmp_path / "tiny.parquet"
    _write_parquet_int16(df, p)

    rules = [{"name": "not_null", "params": {"column": "email"}}]
    c = write_contract(dataset=str(p), rules=rules)
    out, _ = run_engine(c, pushdown=pushdown)

    msg = next(r for r in out["results"] if "not_null" in r["rule_id"])["message"].lower()
    # accept common variants
# Accept SQL generic "failed" or Polars-style messages that mention null/missing
    assert any(kw in msg for kw in ("null", "none", "missing", "fail")), f"unexpected message for not_null: {msg}"
