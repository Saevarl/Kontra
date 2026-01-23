"""
Setup test data for session-revenue experiment (Exp 4).

Creates a 5-step transformation scenario with compounding traps:
- clickstream: 10,000 clicks with session/user/product data
- products: 200 products (with hidden merges)
- users: 500 users

Pipeline traps:
1. Bot filter is BUGGY - is_bot=True for 60% of rows, but only 20% are real bots
2. Duplicate clicks - same (session_id, timestamp, event_type) with different click_ids
3. Product merge - 20 product_ids map to 10 canonical products (row explosion on join)
4. Session duration - some negative (timezone bug), some zero (single-event sessions)
5. Orphan users - 50 clicks reference user_ids not in users table
"""

import polars as pl
from pathlib import Path
import random
from datetime import datetime, timedelta

random.seed(42)

DATA_DIR = Path(__file__).parent / "data"
DATA_DIR.mkdir(exist_ok=True)

# =============================================================================
# Users (500 users, but we'll have orphan references)
# =============================================================================
print("Generating users...")

user_segments = ["premium", "standard", "trial"]
users_data = {
    "user_id": list(range(1, 501)),
    "user_name": [f"User_{i}" for i in range(1, 501)],
    "signup_date": [f"2023-{((i-1) % 12) + 1:02d}-{((i-1) % 28) + 1:02d}" for i in range(1, 501)],
    "user_segment": [user_segments[i % 3] for i in range(500)],
}
users = pl.DataFrame(users_data)

# =============================================================================
# Products (200 products, but 20 are "merged" duplicates)
# =============================================================================
print("Generating products...")

categories = ["Electronics", "Clothing", "Home", "Sports", "Books", "Food", "Toys", "Beauty"]

products_data = {
    "product_id": [],
    "product_name": [],
    "price": [],
    "category": [],
    "canonical_product_id": [],  # Hidden field - some products map to same canonical
}

# First 180 products are normal (1:1 mapping)
for i in range(1, 181):
    products_data["product_id"].append(i)
    products_data["product_name"].append(f"Product_{i}")
    products_data["price"].append(round(random.uniform(9.99, 299.99), 2))
    products_data["category"].append(random.choice(categories))
    products_data["canonical_product_id"].append(i)  # Maps to itself

# Products 181-200 are "merged" - they map to products 1-10
# This creates a hidden many-to-many: joining on product_id will cause row explosion
for i in range(181, 201):
    canonical = ((i - 181) % 10) + 1  # Maps to products 1-10
    products_data["product_id"].append(i)
    products_data["product_name"].append(f"Product_{i}_MERGED")
    products_data["price"].append(products_data["price"][canonical - 1])  # Same price as canonical
    products_data["category"].append(products_data["category"][canonical - 1])
    products_data["canonical_product_id"].append(canonical)

products = pl.DataFrame(products_data)

# Remove canonical_product_id from the "public" products table
# The agent won't know about the merge unless they notice via profile_relationship
products_public = products.drop("canonical_product_id")

# =============================================================================
# Clickstream (10,000 clicks with multiple traps)
# =============================================================================
print("Generating clickstream...")

event_types = ["page_view", "add_to_cart", "purchase"]
base_time = datetime(2024, 1, 1, 0, 0, 0)

clicks_data = {
    "click_id": [],
    "session_id": [],
    "user_id": [],
    "product_id": [],
    "event_type": [],
    "timestamp_seconds": [],  # Epoch seconds for easy math (no datetime parsing needed)
    "is_bot": [],
}

# Generate 800 sessions with varying numbers of clicks
session_id = 0
click_id = 0
target_clicks = 9000  # Leave room for duplicates

while click_id < target_clicks:
    session_id += 1

    # Each session has 5-20 clicks
    num_clicks = random.randint(5, 20)

    # Pick a user (TRAP: 5% of sessions use orphan user_ids 501-550)
    if random.random() < 0.05:
        user_id = random.randint(501, 550)  # Orphan!
    else:
        user_id = random.randint(1, 500)

    # Session start time
    session_start = base_time + timedelta(
        days=random.randint(0, 30),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59)
    )

    # TRAP: 5% of sessions have timezone bug - end before start
    has_timezone_bug = random.random() < 0.05

    # Is this session from a bot?
    # TRAP: is_bot flag is UNRELIABLE
    # - 20% of sessions are actual bots
    # - But is_bot=True for 60% of all clicks (many false positives!)
    is_actual_bot = random.random() < 0.20

    for i in range(num_clicks):
        if click_id >= target_clicks:
            break

        click_id += 1

        # Product (TRAP: 10% of clicks use merged product_ids 181-200)
        if random.random() < 0.10:
            product_id = random.randint(181, 200)
        else:
            product_id = random.randint(1, 180)

        # Event type (weighted: more page_views than purchases)
        r = random.random()
        if r < 0.6:
            event_type = "page_view"
        elif r < 0.9:
            event_type = "add_to_cart"
        else:
            event_type = "purchase"

        # Timestamp within session
        if has_timezone_bug and i == num_clicks - 1:
            # Last click has timestamp BEFORE first click (timezone bug)
            click_time = session_start - timedelta(minutes=random.randint(1, 30))
        else:
            click_time = session_start + timedelta(seconds=i * random.randint(10, 120))

        # TRAP: is_bot is buggy
        # Real bots get is_bot=True 95% of the time
        # Real users get is_bot=True 50% of the time (false positive!)
        if is_actual_bot:
            is_bot = random.random() < 0.95
        else:
            is_bot = random.random() < 0.50  # 50% false positive rate!

        clicks_data["click_id"].append(click_id)
        clicks_data["session_id"].append(session_id)
        clicks_data["user_id"].append(user_id)
        clicks_data["product_id"].append(product_id)
        clicks_data["event_type"].append(event_type)
        clicks_data["timestamp_seconds"].append(int(click_time.timestamp()))  # Epoch seconds for easy math
        clicks_data["is_bot"].append(is_bot)

# TRAP: Add ~500 duplicate clicks (same session_id, timestamp, event_type but different click_id)
print("Adding duplicate clicks...")
num_duplicates = 500
for _ in range(num_duplicates):
    # Pick a random existing click to duplicate
    idx = random.randint(0, len(clicks_data["click_id"]) - 1)

    click_id += 1
    clicks_data["click_id"].append(click_id)  # New click_id
    clicks_data["session_id"].append(clicks_data["session_id"][idx])  # Same session
    clicks_data["user_id"].append(clicks_data["user_id"][idx])  # Same user
    clicks_data["product_id"].append(clicks_data["product_id"][idx])  # Same product
    clicks_data["event_type"].append(clicks_data["event_type"][idx])  # Same event
    clicks_data["timestamp_seconds"].append(clicks_data["timestamp_seconds"][idx])  # Same timestamp!
    clicks_data["is_bot"].append(clicks_data["is_bot"][idx])  # Same is_bot

# Add some single-event sessions (TRAP: duration = 0)
print("Adding single-event sessions...")
for _ in range(50):
    session_id += 1
    click_id += 1

    user_id = random.randint(1, 500)
    product_id = random.randint(1, 180)
    event_time = base_time + timedelta(days=random.randint(0, 30), hours=random.randint(0, 23))

    clicks_data["click_id"].append(click_id)
    clicks_data["session_id"].append(session_id)
    clicks_data["user_id"].append(user_id)
    clicks_data["product_id"].append(product_id)
    clicks_data["event_type"].append("page_view")
    clicks_data["timestamp_seconds"].append(int(event_time.timestamp()))
    clicks_data["is_bot"].append(random.random() < 0.50)  # 50% marked as bot

clickstream = pl.DataFrame(clicks_data)

# Shuffle to hide patterns
clickstream = clickstream.sample(fraction=1.0, shuffle=True, seed=42)

# =============================================================================
# Write to parquet
# =============================================================================
print("\nWriting files...")
users.write_parquet(DATA_DIR / "users.parquet")
products_public.write_parquet(DATA_DIR / "products.parquet")
clickstream.write_parquet(DATA_DIR / "clickstream.parquet")

# =============================================================================
# Print statistics
# =============================================================================
print(f"\nCreated users: {len(users)} rows")
print(f"Created products: {len(products_public)} rows")
print(f"Created clickstream: {len(clickstream)} rows")

# Calculate trap statistics
is_bot_true = clickstream.filter(pl.col("is_bot") == True)
is_bot_false = clickstream.filter(pl.col("is_bot") == False)
orphan_users = clickstream.filter(pl.col("user_id") > 500)
merged_products = clickstream.filter(pl.col("product_id") > 180)

# Find duplicates
dup_key = clickstream.group_by(["session_id", "timestamp_seconds", "event_type"]).len()
duplicates = dup_key.filter(pl.col("len") > 1)

# Single-event sessions
session_sizes = clickstream.group_by("session_id").len()
single_event = session_sizes.filter(pl.col("len") == 1)

print("\n" + "="*60)
print("TRAPS EMBEDDED IN DATA:")
print("="*60)
print(f"\n1. BOT FILTER TRAP:")
print(f"   - is_bot=True: {len(is_bot_true)} clicks ({len(is_bot_true)/len(clickstream)*100:.1f}%)")
print(f"   - is_bot=False: {len(is_bot_false)} clicks ({len(is_bot_false)/len(clickstream)*100:.1f}%)")
print(f"   - PROBLEM: Filtering is_bot=False removes {len(is_bot_true)/len(clickstream)*100:.1f}% of data!")
print(f"   - HINT: is_bot is unreliable (50% false positive rate)")

print(f"\n2. DUPLICATE CLICKS TRAP:")
print(f"   - {len(duplicates)} unique (session_id, timestamp, event_type) combinations have duplicates")
print(f"   - PROBLEM: Deduping by click_id alone won't remove these")
print(f"   - HINT: Dedupe by (session_id, timestamp, event_type)")

print(f"\n3. PRODUCT MERGE TRAP:")
print(f"   - {len(merged_products)} clicks reference merged product_ids (181-200)")
print(f"   - PROBLEM: Products 181-200 are duplicates of products 1-10")
print(f"   - HINT: profile_relationship will show 20 right-side duplicates")

print(f"\n4. SESSION DURATION TRAP:")
print(f"   - ~{session_id * 0.05:.0f} sessions have negative duration (timezone bug)")
print(f"   - {len(single_event)} sessions have only 1 event (zero duration)")
print(f"   - PROBLEM: Negative durations will fail validation")

print(f"\n5. ORPHAN USER TRAP:")
print(f"   - {len(orphan_users)} clicks reference user_ids 501-550 (don't exist)")
print(f"   - PROBLEM: LEFT join will have NULLs; must use INNER join")

print("\n" + "="*60)
print("EXPECTED PIPELINE RESULTS:")
print("="*60)
print(f"\nStep 1 (Filter bots - WRONG WAY):")
print(f"   - Filtering is_bot=False: {len(clickstream)} → {len(is_bot_false)} clicks")
print(f"   - This loses {len(is_bot_true)/len(clickstream)*100:.1f}% of data - TOO MUCH!")

print(f"\nStep 1 (Filter bots - BETTER WAY):")
print(f"   - Should notice 60% drop with compare() and investigate")
print(f"   - Could use session-level bot detection or skip filter entirely")

print(f"\nFiles written to {DATA_DIR}")
