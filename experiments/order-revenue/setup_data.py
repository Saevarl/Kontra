"""
Setup test data for order-revenue experiment.

Creates 3 tables with subtle bugs that require careful transformation:
- customers: 30 customers
- orders: 100 orders (some cancelled, some orphan customer_ids)
- order_items: 300 items (some orphan order_ids, some NULL prices)

Target task: Create customer_summary with:
- customer_id, customer_name, total_revenue, order_count, last_order_date

Bugs:
1. 5 orders reference customer_id 99 (doesn't exist) - orphan FK
2. 10 orders are cancelled - should be excluded from revenue
3. 3 order_items reference order_id 999 (doesn't exist) - orphan FK
4. 5 order_items have NULL unit_price - must handle in aggregation
5. 3 customers have no orders - should still appear with 0 revenue? (contract decides)
"""

import polars as pl
from pathlib import Path
import random

random.seed(42)

DATA_DIR = Path(__file__).parent / "data"
DATA_DIR.mkdir(exist_ok=True)

# =============================================================================
# Customers (30)
# =============================================================================
customer_names = [
    "Alice Johnson", "Bob Smith", "Carol Williams", "David Brown", "Emma Davis",
    "Frank Miller", "Grace Wilson", "Henry Moore", "Ivy Taylor", "Jack Anderson",
    "Kate Thomas", "Leo Jackson", "Mia White", "Noah Harris", "Olivia Martin",
    "Paul Garcia", "Quinn Martinez", "Rose Robinson", "Sam Clark", "Tina Lewis",
    "Uma Lee", "Victor Walker", "Wendy Hall", "Xavier Allen", "Yuki Young",
    "Zara King", "Adam Wright", "Beth Scott", "Chris Green", "Diana Adams"
]

customers = pl.DataFrame({
    "customer_id": list(range(1, 31)),
    "name": customer_names,
    "created_at": [f"2023-{(i % 12) + 1:02d}-{(i % 28) + 1:02d}" for i in range(30)],
})

# =============================================================================
# Orders (100)
# =============================================================================
# Distribute orders across customers (some customers get more orders)
# Customers 28, 29, 30 get NO orders (edge case)
order_customer_ids = []
for i in range(100):
    if i < 5:
        # Bug: 5 orders reference non-existent customer 99
        order_customer_ids.append(99)
    else:
        # Distribute among customers 1-27 (skip 28, 29, 30)
        order_customer_ids.append((i % 27) + 1)

# Statuses: mostly completed, some shipped, some cancelled, some pending
statuses = []
for i in range(100):
    if i < 10:
        statuses.append("cancelled")  # Bug: 10 cancelled orders
    elif i < 20:
        statuses.append("shipped")
    elif i < 25:
        statuses.append("pending")
    else:
        statuses.append("completed")

# Order dates spread over 2024
order_dates = [f"2024-{((i * 3) % 12) + 1:02d}-{((i * 7) % 28) + 1:02d}" for i in range(100)]

orders = pl.DataFrame({
    "order_id": list(range(1, 101)),
    "customer_id": order_customer_ids,
    "order_date": order_dates,
    "status": statuses,
})

# =============================================================================
# Order Items (300)
# =============================================================================
# Each order gets 2-5 items
item_order_ids = []
item_products = []
item_quantities = []
item_prices = []

products = ["Widget", "Gadget", "Gizmo", "Doohickey", "Thingamajig",
            "Whatchamacallit", "Contraption", "Device", "Tool", "Apparatus"]

item_id = 1
for order_id in range(1, 101):
    num_items = random.randint(2, 4)
    for _ in range(num_items):
        item_order_ids.append(order_id)
        item_products.append(random.choice(products))
        item_quantities.append(random.randint(1, 5))

        # Bug: Some items have NULL price (items 50, 100, 150, 200, 250)
        if item_id in [50, 100, 150, 200, 250]:
            item_prices.append(None)
        else:
            item_prices.append(round(random.uniform(9.99, 99.99), 2))

        item_id += 1

# Bug: Add 3 orphan items referencing non-existent order 999
for _ in range(3):
    item_order_ids.append(999)
    item_products.append("Orphan Product")
    item_quantities.append(1)
    item_prices.append(19.99)

order_items = pl.DataFrame({
    "item_id": list(range(1, len(item_order_ids) + 1)),
    "order_id": item_order_ids,
    "product_name": item_products,
    "quantity": item_quantities,
    "unit_price": item_prices,
})

# =============================================================================
# Write to parquet
# =============================================================================
customers.write_parquet(DATA_DIR / "customers.parquet")
orders.write_parquet(DATA_DIR / "orders.parquet")
order_items.write_parquet(DATA_DIR / "order_items.parquet")

print(f"Created customers: {len(customers)} rows")
print(f"Created orders: {len(orders)} rows")
print(f"Created order_items: {len(order_items)} rows")
print()
print("Bugs introduced:")
print("  - 5 orders reference customer_id=99 (doesn't exist)")
print("  - 10 orders have status='cancelled'")
print("  - 3 order_items reference order_id=999 (doesn't exist)")
print("  - 5 order_items have NULL unit_price")
print("  - Customers 28, 29, 30 have no orders")
print()
print(f"Files written to {DATA_DIR}")
