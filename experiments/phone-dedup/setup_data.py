"""
Generate test data for the phone dedup experiment.

Scenario: Users table + Phones table with a bug where some users
have multiple primary phone numbers.

The agent's task: Create user_with_primary view with one row per user.
"""

import polars as pl
from pathlib import Path

OUTPUT_DIR = Path(__file__).parent / "data"
OUTPUT_DIR.mkdir(exist_ok=True)

# =============================================================================
# Users table (clean)
# =============================================================================
users = pl.DataFrame({
    "user_id": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
    "name": [
        "Alice", "Bob", "Charlie", "Diana", "Eve",
        "Frank", "Grace", "Henry", "Ivy", "Jack"
    ],
    "created_at": [
        "2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05",
        "2024-01-06", "2024-01-07", "2024-01-08", "2024-01-09", "2024-01-10"
    ],
})

# =============================================================================
# Phones table (BUGGY - multiple primary flags per user)
# =============================================================================
# The bug: Users 1, 3, and 7 have multiple phones marked as primary
# This happened due to a race condition in the app that allowed
# concurrent "set as primary" requests to succeed.

phones = pl.DataFrame({
    "phone_id": list(range(1, 21)),
    "user_id": [
        1, 1, 1,      # Alice: 3 phones, 2 marked primary (BUG)
        2, 2,         # Bob: 2 phones, 1 primary (OK)
        3, 3, 3, 3,   # Charlie: 4 phones, 3 marked primary (BUG)
        4,            # Diana: 1 phone, primary (OK)
        5, 5,         # Eve: 2 phones, 1 primary (OK)
        6,            # Frank: 1 phone, primary (OK)
        7, 7, 7,      # Grace: 3 phones, 2 marked primary (BUG)
        8, 8,         # Henry: 2 phones, 1 primary (OK)
        9,            # Ivy: 1 phone, primary (OK)
        10,           # Jack: 1 phone, primary (OK)
    ],
    "phone_number": [
        "555-0101", "555-0102", "555-0103",
        "555-0201", "555-0202",
        "555-0301", "555-0302", "555-0303", "555-0304",
        "555-0401",
        "555-0501", "555-0502",
        "555-0601",
        "555-0701", "555-0702", "555-0703",
        "555-0801", "555-0802",
        "555-0901",
        "555-1001",
    ],
    "is_primary": [
        True, True, False,       # Alice: BUG - 2 primaries
        True, False,             # Bob: OK
        True, True, True, False, # Charlie: BUG - 3 primaries
        True,                    # Diana: OK
        True, False,             # Eve: OK
        True,                    # Frank: OK
        True, True, False,       # Grace: BUG - 2 primaries
        True, False,             # Henry: OK
        True,                    # Ivy: OK
        True,                    # Jack: OK
    ],
    "created_at": [
        "2024-01-01", "2024-01-01", "2024-01-15",  # Alice
        "2024-01-02", "2024-01-20",  # Bob
        "2024-01-03", "2024-01-03", "2024-01-03", "2024-01-25",  # Charlie
        "2024-01-04",  # Diana
        "2024-01-05", "2024-01-22",  # Eve
        "2024-01-06",  # Frank
        "2024-01-07", "2024-01-07", "2024-01-28",  # Grace
        "2024-01-08", "2024-01-24",  # Henry
        "2024-01-09",  # Ivy
        "2024-01-10",  # Jack
    ],
})

# =============================================================================
# Save data
# =============================================================================
users.write_parquet(OUTPUT_DIR / "users.parquet")
phones.write_parquet(OUTPUT_DIR / "phones.parquet")

print("Data generated:")
print(f"  users: {len(users)} rows")
print(f"  phones: {len(phones)} rows")
print(f"  phones marked primary: {phones.filter(pl.col('is_primary')).height}")

# Show the bug
users_with_multiple_primaries = (
    phones
    .filter(pl.col("is_primary"))
    .group_by("user_id")
    .agg(pl.len().alias("primary_count"))
    .filter(pl.col("primary_count") > 1)
)
print(f"\nUsers with multiple primaries (BUG):")
print(users_with_multiple_primaries)

# =============================================================================
# What naive join produces (for reference)
# =============================================================================
naive_join = (
    users
    .join(
        phones.filter(pl.col("is_primary")),
        on="user_id",
        how="left"
    )
)
print(f"\nNaive join result: {len(naive_join)} rows (should be 10)")
print(f"Unique user_ids in result: {naive_join['user_id'].n_unique()}")
