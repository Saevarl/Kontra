# tests/fixtures_csv.py
from __future__ import annotations

from pathlib import Path
import polars as pl
import pytest

from scripts.synthesize_users import generate_users

# Keep constants local in this file to avoid import cycles with conftest.py
SMALL_N = 100_000
WIDE_EXTRA_COLS = 10
SEED = 123


def _write_csv(df: pl.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_csv(str(path), include_header=True)


@pytest.fixture(scope="session")
def small_mixed_users_csv(tmp_path_factory) -> str:
    """CSV twin of the 'mixed' dataset (not_null failures + duplicate user_id)."""
    tmp_dir = tmp_path_factory.mktemp("data_csv")
    out = Path(tmp_dir) / "users_mixed.csv"
    if not out.exists():
        df = generate_users(
            n=SMALL_N,
            seed=SEED + 1,
            dup_rate=0.001,
            bad_email_rate=0.02,
            bad_status_rate=0.01,
            null_rate_email=0.01,
            null_rate_age=0.0,
            null_rate_last_login=0.0,
            extra_cols=WIDE_EXTRA_COLS,
        )
        _write_csv(df, out)
    return str(out)


@pytest.fixture(scope="session")
def small_clean_users_csv(tmp_path_factory) -> str:
    """CSV twin of the 'clean' dataset (all rules pass)."""
    tmp_dir = tmp_path_factory.mktemp("data_csv")
    out = Path(tmp_dir) / "users_clean.csv"
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
        _write_csv(df, out)
    return str(out)
