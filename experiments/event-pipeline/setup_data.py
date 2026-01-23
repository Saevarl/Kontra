"""
Setup test data for event-pipeline experiment.

Creates a multi-step transformation scenario that requires compare() to debug:
- events: Raw event logs with duplicates and various statuses
- users: User lookup table

Pipeline steps:
1. Deduplicate events by event_id (keep most recent)
2. Enrich with user attributes
3. Filter to completed events only
4. Aggregate to daily user activity

Bugs that compound across steps:
1. ~50 events are duplicates (same event_id, different timestamps)
2. ~20 events reference user_id that doesn't exist (orphan FK)
3. Status field has inconsistent casing ('completed' vs 'COMPLETED' vs 'Completed')
4. Some users have events but none completed (edge case for aggregation)
"""

import polars as pl
from pathlib import Path
import random
from datetime import datetime, timedelta

random.seed(42)

DATA_DIR = Path(__file__).parent / "data"
DATA_DIR.mkdir(exist_ok=True)

# =============================================================================
# Users (50 users)
# =============================================================================
user_names = [
    "Alice", "Bob", "Carol", "David", "Emma", "Frank", "Grace", "Henry", "Ivy", "Jack",
    "Kate", "Leo", "Mia", "Noah", "Olivia", "Paul", "Quinn", "Rose", "Sam", "Tina",
    "Uma", "Victor", "Wendy", "Xavier", "Yuki", "Zara", "Adam", "Beth", "Chris", "Diana",
    "Eric", "Fiona", "George", "Hannah", "Ian", "Julia", "Kevin", "Laura", "Mike", "Nancy",
    "Oscar", "Penny", "Quentin", "Rachel", "Steve", "Tracy", "Ulrich", "Vera", "Walter", "Xena"
]

users = pl.DataFrame({
    "user_id": list(range(1, 51)),
    "user_name": user_names,
    "user_tier": ["gold"] * 10 + ["silver"] * 20 + ["bronze"] * 20,
    "signup_date": [f"2023-{(i % 12) + 1:02d}-{(i % 28) + 1:02d}" for i in range(50)],
})

# =============================================================================
# Events (500 raw events, ~450 unique after dedup)
# =============================================================================
# Generate base events
events_data = {
    "event_id": [],
    "user_id": [],
    "event_type": [],
    "status": [],
    "event_date": [],  # Pre-extracted date (YYYY-MM-DD) to avoid Polars API confusion
    "event_timestamp": [],
    "amount": [],
}

event_types = ["purchase", "signup", "view", "click", "checkout"]
base_date = datetime(2024, 1, 1)

# Generate 450 unique events
for i in range(450):
    event_id = i + 1

    # Bug 1: ~20 events reference non-existent user_id (51-70)
    if i < 20:
        user_id = 50 + (i % 20) + 1  # user_ids 51-70 don't exist
    else:
        user_id = (i % 50) + 1

    event_type = random.choice(event_types)

    # Bug 2: Inconsistent status casing
    # 60% completed (various casings), 25% pending, 15% failed
    r = random.random()
    if r < 0.3:
        status = "completed"
    elif r < 0.45:
        status = "COMPLETED"  # Different casing!
    elif r < 0.60:
        status = "Completed"  # Another casing!
    elif r < 0.85:
        status = "pending"
    else:
        status = "failed"

    # Spread events across 30 days
    event_date = base_date + timedelta(days=i % 30, hours=random.randint(0, 23), minutes=random.randint(0, 59))

    amount = round(random.uniform(10, 500), 2) if event_type in ["purchase", "checkout"] else 0

    events_data["event_id"].append(event_id)
    events_data["user_id"].append(user_id)
    events_data["event_type"].append(event_type)
    events_data["status"].append(status)
    events_data["event_date"].append(event_date.strftime("%Y-%m-%d"))  # Pre-extracted date
    events_data["event_timestamp"].append(event_date.isoformat())
    events_data["amount"].append(amount)

# Bug 3: Add ~50 duplicate events (same event_id, later timestamp)
for i in range(50):
    original_idx = random.randint(0, 399)  # Pick a random event to duplicate
    original_event_id = events_data["event_id"][original_idx]

    # Duplicate with slightly later timestamp
    original_ts = datetime.fromisoformat(events_data["event_timestamp"][original_idx])
    new_ts = original_ts + timedelta(minutes=random.randint(1, 60))

    events_data["event_id"].append(original_event_id)  # Same event_id!
    events_data["user_id"].append(events_data["user_id"][original_idx])
    events_data["event_type"].append(events_data["event_type"][original_idx])
    events_data["status"].append(events_data["status"][original_idx])
    events_data["event_date"].append(new_ts.strftime("%Y-%m-%d"))  # Date of duplicate
    events_data["event_timestamp"].append(new_ts.isoformat())
    events_data["amount"].append(events_data["amount"][original_idx])

# Bug 4: Ensure some users have ONLY non-completed events (users 46-50)
# This creates edge cases in the final aggregation
for i in range(len(events_data["event_id"])):
    if events_data["user_id"][i] in [46, 47, 48, 49, 50]:
        if events_data["status"][i] in ["completed", "COMPLETED", "Completed"]:
            events_data["status"][i] = "pending"

events = pl.DataFrame(events_data)

# Shuffle to make duplicates non-obvious
events = events.sample(fraction=1.0, shuffle=True, seed=42)

# =============================================================================
# Write to parquet
# =============================================================================
users.write_parquet(DATA_DIR / "users.parquet")
events.write_parquet(DATA_DIR / "events.parquet")

print(f"Created users: {len(users)} rows")
print(f"Created events: {len(events)} rows")
print()

# Count bugs
unique_event_ids = events["event_id"].n_unique()
duplicate_count = len(events) - unique_event_ids
orphan_users = events.filter(pl.col("user_id") > 50)["user_id"].n_unique()
status_counts = events.group_by("status").len().sort("status")

print("Bugs introduced:")
print(f"  - {duplicate_count} duplicate event records (same event_id)")
print(f"  - {orphan_users} unique orphan user_ids (don't exist in users table)")
print(f"  - Status casing inconsistency:")
for row in status_counts.iter_rows():
    print(f"      '{row[0]}': {row[1]} events")
print(f"  - Users 46-50 have no completed events")
print()
print(f"Files written to {DATA_DIR}")
print()
print("Expected pipeline steps:")
print("  1. Deduplicate: 500 → ~450 rows")
print("  2. Enrich with users: ~450 rows (inner join loses orphans)")
print("  3. Filter completed: depends on casing handling")
print("  4. Aggregate to daily user: ~unique(user_id, date) combinations")
