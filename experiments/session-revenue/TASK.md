# Task: Session Revenue Pipeline

## Objective

Create a transformation pipeline that produces `session_summary` from e-commerce clickstream data.

**Source Tables**:
- `data/clickstream.parquet` - Click events (click_id, session_id, user_id, product_id, event_type, timestamp_seconds, is_bot)
  - `timestamp_seconds` is epoch seconds (integer) - duration = max - min
- `data/products.parquet` - Product catalog (product_id, product_name, price, category)
- `data/users.parquet` - User attributes (user_id, user_name, signup_date, user_segment)

## Pipeline Steps

### Step 1: Clean Bot Traffic
- Filter out bot traffic using the `is_bot` flag
- **Be careful**: Check the data distribution before blindly filtering!

### Step 2: Deduplicate Clicks
- Raw data may contain duplicate clicks
- Think about what makes a click unique (not just click_id!)

### Step 3: Enrich with Products
- Join with products table to get price information
- Handle any foreign key issues

### Step 4: Calculate Session Metrics
- Group by session_id to calculate:
  - Session duration (max timestamp - min timestamp)
  - Click count
  - Purchase count (event_type = 'purchase')
  - Total revenue (sum of prices for purchases)
- **Watch out**: Duration calculation has edge cases!

### Step 5: Enrich with Users
- Join with users table to get user attributes
- Handle missing user data appropriately

## Target Schema

The output `session_summary` must have:

| Column | Type | Description |
|--------|------|-------------|
| session_id | int | Unique session identifier |
| user_id | int | User identifier |
| user_name | string | From users table |
| user_segment | string | From users table (premium/standard/trial) |
| session_duration_seconds | float | Duration in seconds (must be >= 0) |
| click_count | int | Number of clicks in session (>= 1) |
| purchase_count | int | Number of purchases (>= 0) |
| total_revenue | float | Sum of product prices for purchases (>= 0) |

## Success Criteria

The output must pass validation against `target_contract.yml`:
- session_id is unique
- All columns are non-null
- session_duration_seconds >= 0
- click_count >= 1
- purchase_count >= 0
- total_revenue >= 0
- Between 300-1000 sessions

## Warnings

This pipeline has **multiple traps** that compound:
- A problem in step 1 affects all downstream steps
- The final validation may pass but produce **wrong business results**
- Think carefully about data quality at each step
