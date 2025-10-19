#!/usr/bin/env python3
"""
synthesize_users.py

Generate a large, realistic user table for data-quality testing (CSV or Parquet).

Core Columns (contract-relevant)
--------------------------------
user_id        : Int64
email          : Utf8
status         : Utf8
country        : Utf8
signup_date    : Date
last_login     : Datetime (naive)
age            : Int16
is_premium     : Boolean
balance        : Float64

Extra / Filler Columns (contract-agnostic; for pruning tests)
-------------------------------------------------------------
filler_<i>_<type> where <type> ∈ {int, float, str, bool, date, datetime}
The number, naming, types are configurable via CLI flags.

Imperfection knobs are per-row probabilities; set to 0.0 for "perfect" data.
"""

from __future__ import annotations

import argparse
import os
from datetime import datetime, timezone
from typing import List, Tuple, Dict, Iterable

import numpy as np
import polars as pl


ALLOWED_STATUSES = ["active", "inactive", "pending"]
DEFAULT_EMAIL_DOMAINS = [
    "gmail.com", "yahoo.com", "outlook.com", "icloud.com", "proton.me",
    "example.com", "company.com", "hotmail.com", "aol.com",
]
DEFAULT_COUNTRIES = [
    ("US", 30), ("GB", 8), ("CA", 7), ("DE", 7), ("FR", 6),
    ("IN", 10), ("BR", 6), ("AU", 4), ("NL", 3), ("SE", 3),
    ("NO", 2), ("IS", 1), ("ES", 4), ("IT", 4), ("JP", 5),
]

DEFAULT_TYPE_CYCLE = ["int", "float", "str", "bool", "date", "datetime"]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Generate large synthetic user data.")
    p.add_argument("--rows", type=int, default=1_000_000, help="Number of rows to generate.")
    p.add_argument("--out", type=str, required=True, help="Output file path (.csv or .parquet).")
    p.add_argument("--format", type=str, choices=["csv", "parquet"], default=None,
                   help="Output format (infer from extension if omitted).")
    p.add_argument("--seed", type=int, default=123, help="Random seed for reproducibility.")

    # Imperfection rates
    p.add_argument("--dup-rate", type=float, default=0.0, help="Fraction of rows that duplicate user_id.")
    p.add_argument("--bad-email-rate", type=float, default=0.0, help="Fraction of rows with invalid email.")
    p.add_argument("--bad-status-rate", type=float, default=0.0, help="Fraction of rows with invalid status.")
    p.add_argument("--null-rate-email", type=float, default=0.0, help="Fraction of rows with NULL email.")
    p.add_argument("--null-rate-age", type=float, default=0.0, help="Fraction of rows with NULL age.")
    p.add_argument("--null-rate-last-login", type=float, default=0.0, help="Fraction rows with NULL last_login.")

    # Optional spice
    p.add_argument("--allow-negative-balance", action="store_true",
                   help="Inject a tiny fraction of negative balances.")

    # NEW: wide-table / pruning stress
    p.add_argument("--extra-cols", type=int, default=0,
                   help="Number of additional filler columns to add (not referenced by the contract).")
    p.add_argument("--extra-prefix", type=str, default="filler",
                   help="Prefix for filler columns, e.g., 'filler' -> filler_001_int.")
    p.add_argument("--extra-type-cycle", action="append",
                   choices=["int", "float", "str", "bool", "date", "datetime"],
                   help="Type cycle for filler columns; repeat this flag to define the rotation. "
                        "Default cycle: int,float,str,bool,date,datetime.")
    return p.parse_args()


def _rng(seed: int) -> np.random.Generator:
    return np.random.default_rng(seed)


def _weighted_choices(items_with_weights: List[Tuple[str, int]], n: int, rng: np.random.Generator) -> List[str]:
    items, weights = zip(*items_with_weights)
    probs = np.array(weights, dtype=float)
    probs = probs / probs.sum()
    idx = rng.choice(len(items), size=n, p=probs)
    return [items[i] for i in idx]


def _random_signup_dates_np(n: int, years_back: int, rng: np.random.Generator) -> np.ndarray:
    """
    Return numpy datetime64[D] array uniformly distributed over the last `years_back` years.
    Using datetime64 avoids object dtype -> Polars cast issues.
    """
    today = np.datetime64(datetime.now(tz=timezone.utc).date(), "D")
    start = today - np.timedelta64(365 * years_back, "D")
    # random offsets [0, days]
    days = (today - start).astype(int)
    offsets = rng.integers(0, days + 1, size=n, dtype=np.int32)
    return start + offsets.astype("timedelta64[D]")


def _random_dates_np(n: int, years_back: int, rng: np.random.Generator) -> np.ndarray:
    """Uniform random date range helper for filler date columns."""
    base = _random_signup_dates_np(n, years_back, rng)
    return base  # already datetime64[D]


def _random_datetimes_np(n: int, years_back: int, rng: np.random.Generator) -> np.ndarray:
    """Uniform random datetime range helper for filler datetime columns."""
    d = _random_signup_dates_np(n, years_back, rng).astype("datetime64[s]")
    # add [0..86400*30) seconds to spread within ~a month window from the date
    sec_offsets = rng.integers(0, 86_400 * 30, size=n, dtype=np.int32).astype("timedelta64[s]")
    return (d + sec_offsets).astype("datetime64[ns]")


def _local_parts(n: int, rng: np.random.Generator) -> List[str]:
    samples = []
    alpha = np.array(list("abcdefghijklmnopqrstuvwxyz"))
    for _ in range(n):
        mode = rng.integers(0, 4)  # 0..3
        if mode == 0:
            samples.append(f"user{rng.integers(1000, 999999)}")
        elif mode == 1:
            a = "".join(rng.choice(alpha, size=rng.integers(5, 9)))
            b = "".join(rng.choice(alpha, size=rng.integers(4, 8)))
            samples.append(f"{a}.{b}".lower())
        elif mode == 2:
            a = "".join(rng.choice(alpha, size=rng.integers(6, 11)))
            samples.append(f"{a.lower()}{rng.integers(10, 9999)}")
        else:
            a = "".join(rng.choice(alpha, size=rng.integers(2, 4)))
            samples.append(f"{a.lower()}{rng.integers(100, 999)}")
    return samples


def _emails(local_parts: List[str], domains: List[str]) -> List[str]:
    return [f"{lp}@{dm}" for lp, dm in zip(local_parts, domains)]


def _inject_probability_mask(n: int, rate: float, rng: np.random.Generator) -> np.ndarray:
    if rate <= 0.0:
        return np.zeros(n, dtype=bool)
    if rate >= 1.0:
        return np.ones(n, dtype=bool)
    return rng.uniform(0, 1, size=n) < rate


def _make_invalid_emails(emails: List[str], mask: np.ndarray, rng: np.random.Generator) -> List[str]:
    emails = emails[:]  # copy
    for i, corrupt in enumerate(mask):
        if not corrupt:
            continue
        mode = rng.integers(0, 3)
        if mode == 0:
            emails[i] = emails[i].replace("@", "")  # remove '@'
        elif mode == 1:
            at = emails[i].find("@")
            emails[i] = emails[i][: at + 1]  # drop domain
        else:
            at = emails[i].find("@")
            emails[i] = emails[i][at:]  # empty local part
    return emails


def _log_skewed_balance(n: int, rng: np.random.Generator) -> np.ndarray:
    vals = rng.lognormal(mean=6.8, sigma=0.7, size=n) / 10.0
    return np.clip(vals, 0.0, 5000.0)


def _noise_columns(
    n: int,
    rng: np.random.Generator,
    count: int,
    prefix: str,
    type_cycle: Iterable[str],
) -> Dict[str, pl.Series]:
    """
    Generate a dict of filler columns of mixed types to widen the table
    without affecting the contract. Column names: {prefix}_{idx:03d}_{type}.
    """
    noise: Dict[str, pl.Series] = {}
    cycle = list(type_cycle) if type_cycle else DEFAULT_TYPE_CYCLE
    if not cycle:
        cycle = DEFAULT_TYPE_CYCLE

    # Precompute some shared pools for speed
    alpha = np.array(list("abcdefghijklmnopqrstuvwxyz"))
    def rand_str(n_: int) -> List[str]:
        # variable length 6..14
        lengths = rng.integers(6, 15, size=n_)
        out = []
        for L in lengths:
            out.append("".join(rng.choice(alpha, size=L)))
        return out

    for i in range(count):
        t = cycle[i % len(cycle)]
        name = f"{prefix}_{i+1:03d}_{t}"

        if t == "int":
            arr = rng.integers(0, 10_000_000, size=n, dtype=np.int32)
            noise[name] = pl.Series(name, arr, dtype=pl.Int32)

        elif t == "float":
            arr = rng.normal(loc=0.0, scale=1.0, size=n).astype(np.float32)
            noise[name] = pl.Series(name, arr, dtype=pl.Float32)

        elif t == "str":
            arr = rand_str(n)
            noise[name] = pl.Series(name, arr, dtype=pl.Utf8)

        elif t == "bool":
            arr = rng.uniform(0, 1, size=n) < 0.5
            noise[name] = pl.Series(name, arr, dtype=pl.Boolean)

        elif t == "date":
            arr = _random_dates_np(n, years_back=12, rng=rng)  # datetime64[D]
            noise[name] = pl.Series(name, arr).cast(pl.Date)

        elif t == "datetime":
            arr = _random_datetimes_np(n, years_back=8, rng=rng)  # datetime64[ns]
            noise[name] = pl.Series(name, arr).cast(pl.Datetime)

        else:
            # Fallback: Utf8
            arr = rand_str(n)
            noise[name] = pl.Series(name, arr, dtype=pl.Utf8)

    return noise


def generate_users(
    n: int,
    seed: int = 123,
    dup_rate: float = 0.0,
    bad_email_rate: float = 0.0,
    bad_status_rate: float = 0.0,
    null_rate_email: float = 0.0,
    null_rate_age: float = 0.0,
    null_rate_last_login: float = 0.0,
    allow_negative_balance: bool = False,
    # NEW
    extra_cols: int = 0,
    extra_prefix: str = "filler",
    extra_type_cycle: Iterable[str] | None = None,
) -> pl.DataFrame:
    rng = _rng(seed)

    # user_id 1..n, then inject duplicates by copying onto dup positions
    user_id = np.arange(1, n + 1, dtype=np.int64)
    if dup_rate > 0.0:
        dup_mask = _inject_probability_mask(n, dup_rate, rng)
        dup_sources = rng.integers(0, n, size=int(dup_mask.sum()))
        user_id[np.where(dup_mask)[0]] = user_id[dup_sources]

    # Emails
    local = _local_parts(n, rng)
    domains = _weighted_choices([(d, 1) for d in DEFAULT_EMAIL_DOMAINS], n, rng)
    emails = _emails(local, domains)

    # Countries & statuses
    countries = _weighted_choices(DEFAULT_COUNTRIES, n, rng)
    statuses = _weighted_choices([(s, w) for s, w in zip(ALLOWED_STATUSES, [6, 2, 2])], n, rng)

    # Signup dates (datetime64[D]) & last_login after signup as datetime64[ns] (naive)
    signup_np = _random_signup_dates_np(n, years_back=5, rng=rng)  # datetime64[D]
    # last_login = signup + offset_days(0..1200)
    login_offsets = rng.integers(0, 1200, size=n, dtype=np.int32)
    last_login_np = signup_np.astype("datetime64[s]") + (login_offsets.astype("timedelta64[D]")).astype(
        "timedelta64[s]"
    )
    last_login_np = last_login_np.astype("datetime64[ns]")

    # Age distribution: trimmed normal 13..95
    age = np.clip((rng.normal(loc=34, scale=10, size=n)).round().astype(np.int16), 13, 95)

    # Premium: 15% True
    is_premium = (rng.uniform(0, 1, size=n) < 0.15)

    # Balance: log-skewed; optional tiny negatives
    balance = _log_skewed_balance(n, rng)
    if allow_negative_balance:
        neg_mask = _inject_probability_mask(n, 0.001, rng)
        balance[neg_mask] = -np.abs(balance[neg_mask])

    # Bad emails/statuses (do BEFORE building DF; still pure strings)
    if bad_email_rate > 0.0:
        emails = _make_invalid_emails(emails, _inject_probability_mask(n, bad_email_rate, rng), rng)

    if bad_status_rate > 0.0:
        bad_mask = _inject_probability_mask(n, bad_status_rate, rng)
        invalids = np.array(["invalid", "unknown", "archived"])
        statuses = np.where(bad_mask, rng.choice(invalids, size=n), statuses)

    # Build Polars DataFrame with native dtypes (no NULLs yet to avoid object arrays)
    df = pl.DataFrame(
        {
            "user_id": pl.Series(user_id, dtype=pl.Int64),
            "email": pl.Series(emails, dtype=pl.Utf8),
            "status": pl.Series(statuses, dtype=pl.Utf8),
            "country": pl.Series(countries, dtype=pl.Utf8),
            "signup_date": pl.Series(signup_np).cast(pl.Date),
            "last_login": pl.Series(last_login_np).cast(pl.Datetime),
            "age": pl.Series(age, dtype=pl.Int16),
            "is_premium": pl.Series(is_premium, dtype=pl.Boolean),
            "balance": pl.Series(balance, dtype=pl.Float64),
        }
    )

    # Now apply NULL injections via Polars expressions (keeps column dtypes correct)
    updates = []
    if null_rate_email > 0.0:
        mask = _inject_probability_mask(n, null_rate_email, rng)
        updates.append(pl.when(pl.Series(mask)).then(None).otherwise(pl.col("email")).alias("email"))
    if null_rate_age > 0.0:
        mask = _inject_probability_mask(n, null_rate_age, rng)
        updates.append(pl.when(pl.Series(mask)).then(None).otherwise(pl.col("age")).alias("age"))
    if null_rate_last_login > 0.0:
        mask = _inject_probability_mask(n, null_rate_last_login, rng)
        updates.append(pl.when(pl.Series(mask)).then(None).otherwise(pl.col("last_login")).alias("last_login"))
    if updates:
        df = df.with_columns(updates)

    # ---- NEW: add extra/filler columns for pruning tests ---------------------
    if extra_cols and extra_cols > 0:
        cycle = list(extra_type_cycle) if extra_type_cycle else DEFAULT_TYPE_CYCLE
        noise = _noise_columns(
            n=n,
            rng=rng,
            count=extra_cols,
            prefix=extra_prefix,
            type_cycle=cycle,
        )
        if noise:
            df = pl.concat([df, pl.DataFrame(noise)], how="horizontal")

    return df


def write_output(df: pl.DataFrame, out_path: str, fmt: str | None) -> None:
    if fmt is None:
        fmt = "parquet" if out_path.lower().endswith(".parquet") else "csv"

    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)

    if fmt == "parquet":
        df.write_parquet(out_path)
    elif fmt == "csv":
        df.write_csv(out_path)
    else:
        raise ValueError(f"Unsupported format: {fmt}")


def main():
    args = parse_args()

    df = generate_users(
        n=args.rows,
        seed=args.seed,
        dup_rate=args.dup_rate,
        bad_email_rate=args.bad_email_rate,
        bad_status_rate=args.bad_status_rate,
        null_rate_email=args.null_rate_email,
        null_rate_age=args.null_rate_age,
        null_rate_last_login=args.null_rate_last_login,
        allow_negative_balance=args.allow_negative_balance,
        # NEW
        extra_cols=args.extra_cols,
        extra_prefix=args.extra_prefix,
        extra_type_cycle=args.extra_type_cycle,
    )

    write_output(df, args.out, args.format)

    # Quick summary for sanity — unchanged, focuses on contract columns only.
    email_fail = (~df["email"].cast(pl.Utf8).str.contains(r"^[^@]+@[^@]+\.[^@]+$")).fill_null(True).sum()
    status_fail = (~df["status"].is_in(ALLOWED_STATUSES)).fill_null(True).sum()
    dup_user_id = df["user_id"].is_duplicated().sum()
    print("Wrote:", args.out)
    print("Summary:", {
        "rows": int(df.height),
        "dup_user_id": int(dup_user_id),
        "invalid_status": int(status_fail),
        "bad_email_guess": int(email_fail),
        "null_email": int(df["email"].is_null().sum()),
        # Wide table info (may help verify pruning effectiveness)
        "total_columns": int(len(df.columns)),
    })


if __name__ == "__main__":
    main()
