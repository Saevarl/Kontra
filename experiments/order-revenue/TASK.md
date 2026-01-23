# Task: Customer Revenue Summary

## Objective

Create a transformation that produces `customer_summary` from three source tables:
- `data/customers.parquet` - Customer records
- `data/orders.parquet` - Orders with status (completed, shipped, cancelled, pending)
- `data/order_items.parquet` - Line items with quantity and unit_price

## Target Schema

The output `customer_summary` must have:

| Column | Type | Description |
|--------|------|-------------|
| customer_id | int | Unique customer identifier |
| customer_name | string | Customer's name |
| total_revenue | float | Sum of (quantity × unit_price) for valid orders |
| order_count | int | Number of valid orders |
| last_order_date | string | Most recent order date |

## Business Rules

1. **Exclude cancelled orders** - Only include orders with status != 'cancelled'
2. **Handle orphan data** - Some order_items may reference non-existent orders, some orders may reference non-existent customers
3. **Handle NULL prices** - Some items have NULL unit_price, treat as 0 or exclude
4. **One row per customer** - Aggregate all orders per customer
5. **Only customers with orders** - Don't include customers who have no valid orders

## Success Criteria

The output must pass validation against `target_contract.yml`:
- customer_id is unique
- All columns are non-null
- total_revenue >= 0
- order_count >= 1
- Between 20-30 rows

## Hints

This task requires:
- Joining 3 tables correctly
- Filtering by order status
- Handling missing foreign keys
- Aggregating with SUM, COUNT, MAX
- Handling NULL values in calculations
