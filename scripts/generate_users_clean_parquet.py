#!/usr/bin/env python3
"""
Generate a clean users dataset for passing validations.

- No NULLs
- No duplicate user_id
- Valid email format
- status in {"active","inactive","pending"}

Writes:
  - data/users_clean.parquet
  - (optional) data/users_clean.csv if --csv flag is used
"""

from pathlib import Path
import argparse
import polars as pl


def build_clean_users_df() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "user_id": [1, 2, 3, 4, 5],
            "email": [
                "a@example.com",
                "b@example.com",
                "c@example.com",
                "d@example.com",
                "e@example.com",
            ],
            "status": ["active", "inactive", "pending", "active", "inactive"],
        }
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--csv",
        action="store_true",
        help="Also write a CSV alongside the Parquet file.",
    )
    parser.add_argument(
        "--out-dir",
        default="data",
        help="Output directory (default: data)",
    )
    parser.add_argument(
        "--basename",
        default="users_clean",
        help="Base filename without extension (default: users_clean)",
    )
    args = parser.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    df = build_clean_users_df()

    parquet_path = out_dir / f"{args.basename}.parquet"
    df.write_parquet(parquet_path)

    if args.csv:
        csv_path = out_dir / f"{args.basename}.csv"
        df.write_csv(csv_path)

    print(f"✅ Wrote {parquet_path}")
    if args.csv:
        print(f"✅ Wrote {csv_path}")


if __name__ == "__main__":
    main()
