# Task: Daily User Activity Pipeline

## Objective

Create a multi-step transformation pipeline that produces `daily_user_activity` from raw event logs.

**Source Tables**:
- `data/events.parquet` - Raw event logs (may contain duplicates)
- `data/users.parquet` - User lookup table

## Pipeline Steps

You must implement these steps in order:

### Step 1: Deduplicate Events
- Raw events may contain duplicate records (same `event_id`)
- Keep only one record per `event_id` (the one with the latest `event_timestamp`)

### Step 2: Enrich with User Data
- Join events with users to get `user_name` and `user_tier`
- Handle cases where `user_id` doesn't exist in users table

### Step 3: Filter to Completed Events
- Only include events where `status` indicates completion
- **Watch out**: Status values may have inconsistent casing!

### Step 4: Aggregate to Daily Summary
- Group by `user_id` and `event_date` (date part of timestamp)
- Calculate:
  - `completed_events`: count of events
  - `total_amount`: sum of amounts

## Target Schema

The output `daily_user_activity` must have:

| Column | Type | Description |
|--------|------|-------------|
| user_date_key | string | Composite key: "{user_id}_{event_date}" |
| user_id | int | User identifier |
| user_name | string | From users table |
| user_tier | string | From users table (gold/silver/bronze) |
| event_date | string | Date in YYYY-MM-DD format |
| completed_events | int | Count of completed events that day |
| total_amount | float | Sum of amounts for completed events |

## Success Criteria

The output must pass validation against `target_contract.yml`:
- `user_date_key` is unique
- All columns are non-null
- `completed_events` >= 1 per row
- `total_amount` >= 0
- Between 100-800 rows

## Hints

This is a **multi-step pipeline**. Errors can compound:
- If deduplication is wrong, counts will be inflated
- If user join loses valid records, you'll have fewer rows than expected
- If status filtering is wrong (case sensitivity!), you'll miss completed events
- If aggregation is wrong, you might have NULL values or missing rows

**Recommended approach**: Verify each step produces expected results before moving to the next.
