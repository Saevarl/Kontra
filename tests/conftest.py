from __future__ import annotations

import os
from pathlib import Path
from typing import Literal, Optional
import polars as pl
import pytest

# import the generator directly from your script/module
# adjust this import if your synth module path differs
from scripts.synthesize_users import generate_users   # <- your file name/module path
from kontra.engine.engine import ValidationEngine
# tests/conftest.py
from .fixtures_csv import small_mixed_users_csv, small_clean_users_csv  # registers CSV fixtures


# ---------- knobs ----------
SMALL_N = 100_000  # fast for CI; override with --small-n if you want
WIDE_EXTRA_COLS = 10  # enough to see projection effectiveness
SEED = 123

def _write_parquet(df: pl.DataFrame, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(str(path))

@pytest.fixture(scope="session")
def tmp_data_dir(tmp_path_factory):
    return tmp_path_factory.mktemp("data")

@pytest.fixture(scope="session")
def small_clean_users(tmp_data_dir) -> str:
    """Perfect data: all rules should pass."""
    out = Path(tmp_data_dir) / "users_clean.parquet"
    if not out.exists():
        df = generate_users(
            n=SMALL_N,
            seed=SEED,
            dup_rate=0.0,
            bad_email_rate=0.0,
            bad_status_rate=0.0,
            null_rate_email=0.0,
            null_rate_age=0.0,
            null_rate_last_login=0.0,
            extra_cols=WIDE_EXTRA_COLS,
        )
        _write_parquet(df, out)
    return str(out)

@pytest.fixture(scope="session")
def small_mixed_users(tmp_data_dir) -> str:
    """Intentionally imperfect: not_null failures + duplicate user_id."""
    out = Path(tmp_data_dir) / "users_mixed.parquet"
    if not out.exists():
        df = generate_users(
            n=SMALL_N,
            seed=SEED + 1,
            dup_rate=0.001,                # small % duplicates
            bad_email_rate=0.02,           # invalid emails (regex-ish)
            bad_status_rate=0.01,          # invalid status values
            null_rate_email=0.01,          # triggers not_null fail
            null_rate_age=0.0,
            null_rate_last_login=0.0,
            extra_cols=WIDE_EXTRA_COLS,
        )
        _write_parquet(df, out)
    return str(out)

@pytest.fixture(scope="session")
def small_nulls_only(tmp_data_dir) -> str:
    """Only nulls in SQL-pushdown columns to verify SQL handles them."""
    out = Path(tmp_data_dir) / "users_nulls_only.parquet"
    if not out.exists():
        df = generate_users(
            n=SMALL_N,
            seed=SEED + 2,
            dup_rate=0.0,
            bad_email_rate=0.0,
            bad_status_rate=0.0,
            null_rate_email=0.03,          # email NULLs
            null_rate_age=0.0,
            null_rate_last_login=0.02,     # last_login NULLs
            extra_cols=WIDE_EXTRA_COLS,
        )
        _write_parquet(df, out)
    return str(out)

@pytest.fixture()
def write_contract(tmp_path):
    """
    Write a minimal contract on the fly.
    Return path as string.
    Usage:
        cpath = write_contract(dataset=..., rules=[{name:..., params:{...}}, ...])
    """
    def _writer(dataset: str, rules: list[dict]) -> str:
        content = "dataset: \"{0}\"\n\nrules:\n".format(dataset)
        for r in rules:
            content += f"  - name: {r['name']}\n"
            if "params" in r and r["params"]:
                content += "    params: { " + ", ".join(
                    f"{k}: {repr(v) if isinstance(v, str) else v}" for k, v in r["params"].items()
                ) + " }\n"
            # Include tally field if specified
            if "tally" in r and r["tally"] is not None:
                content += f"    tally: {'true' if r['tally'] else 'false'}\n"
        out = tmp_path / "contract.yml"
        out.write_text(content)
        return str(out)
    return _writer

@pytest.fixture()
def run_engine():
    """
    Convenience runner that returns (out_dict, engine_label).

    Args:
        contract_path: Path to the generated contract.yml
        data_override: Optional dataset URI override
        pushdown: 'on' | 'off' | 'auto'
        stats_mode: 'none' | 'summary' | 'profile'
        csv_mode: 'auto' | 'duckdb' | 'parquet'   # controls CSV handling
        enable_projection: True → project required columns, False → load all

    Usage:
        out, label = run_engine(
            contract_path=cpath,
            pushdown="on",
            csv_mode="parquet",
            enable_projection=True,
            stats_mode="summary",
        )
    """
    def _run(
        contract_path: str,
        data_override: Optional[str] = None,
        pushdown: Literal["on", "off", "auto"] = "auto",
        preplan: Literal["on", "off", "auto"] = "auto",
        stats_mode: Literal["none", "summary", "profile"] = "summary",
        csv_mode: Literal["auto", "duckdb", "parquet"] = "auto",
        enable_projection: bool = True,
    ):
        from kontra.engine.engine import ValidationEngine

        eng = ValidationEngine(
            contract_path=contract_path,
            data_path=data_override,
            emit_report=False,
            stats_mode=stats_mode,
            preplan=preplan,
            pushdown=pushdown,
            csv_mode=csv_mode,
            enable_projection=enable_projection,
        )
        out = eng.run()
        return out, out["run_meta"]["engine_label"]

    return _run
