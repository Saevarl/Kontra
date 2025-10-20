import os
from pathlib import Path
import polars as pl
import pytest

# import the generator directly from your script/module
# adjust this import if your synth module path differs
from scripts.synthesize_users import generate_users   # <- your file name/module path
from kontra.engine.engine import ValidationEngine

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
        out = tmp_path / "contract.yml"
        out.write_text(content)
        return str(out)
    return _writer

@pytest.fixture()
def run_engine():
    """Convenience runner that returns (out_dict, engine_label)."""
    def _run(contract_path: str, data_override: str | None = None, pushdown: str = "auto", stats_mode="summary"):
        eng = ValidationEngine(
            contract_path=contract_path,
            data_path=data_override,
            emit_report=False,
            stats_mode=stats_mode,
            pushdown=pushdown,
        )
        out = eng.run()
        return out, out["run_meta"]["engine_label"]
    return _run
