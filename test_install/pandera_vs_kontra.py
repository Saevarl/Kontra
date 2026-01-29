#!/usr/bin/env python3
"""
Pandera + Ibis vs Kontra: Side-by-Side Comparison
Run: python pandera_vs_kontra.py
"""

import polars as pl
import time
import random

# ============================================================
print("="*70)
print("SETUP")
print("="*70)

import pandera as pa
import pandera.polars as pap
import kontra
from kontra import rules

print(f"Pandera version: {pa.__version__}")
print(f"Kontra version: {kontra.__version__}")

# ============================================================
print("\n" + "="*70)
print("CREATE TEST DATA (1M rows with intentional issues)")
print("="*70)

n_rows = 1_000_000

df = pl.DataFrame({
    "user_id": list(range(n_rows)),
    "email": [f"user{i}@example.com" if i % 100 != 0 else None for i in range(n_rows)],
    "age": [random.randint(18, 80) if i % 200 != 0 else -5 for i in range(n_rows)],
    "status": [random.choice(["active", "inactive", "pending"]) if i % 500 != 0 else "INVALID" for i in range(n_rows)],
    "start_date": ["2024-01-01"] * n_rows,
    "end_date": ["2024-12-31" if i % 1000 != 0 else "2023-12-31" for i in range(n_rows)],
    "shipping_date": ["2024-02-01" if i % 3 == 0 else None for i in range(n_rows)],
    "order_status": ["shipped" if i % 3 == 0 else "pending" for i in range(n_rows)],
})

# Create some failures for conditional test (shipped but no shipping_date)
df = df.with_columns(
    pl.when(pl.col("user_id") % 2000 == 0)
    .then(pl.lit("shipped"))
    .otherwise(pl.col("order_status"))
    .alias("order_status")
)

print(f"Created {len(df):,} rows")
print(f"Columns: {df.columns}")

# ============================================================
print("\n" + "="*70)
print("TEST 1: Basic Validation (not_null, range, allowed_values)")
print("="*70)

print("\n--- PANDERA ---")
pandera_schema = pap.DataFrameSchema({
    "user_id": pap.Column(int, nullable=False, unique=True),
    "email": pap.Column(str, nullable=False),
    "age": pap.Column(int, pa.Check.ge(0)),
    "status": pap.Column(str, pa.Check.isin(["active", "inactive", "pending"])),
})

start = time.time()
try:
    pandera_schema.validate(df, lazy=True)
    print("Pandera: PASSED")
except pa.errors.SchemaErrors as e:
    elapsed = time.time() - start
    print(f"Pandera: FAILED in {elapsed:.2f}s")
    print(f"Number of schema errors: {len(e.schema_errors)}")
    error_str = str(e)
    print(f"Error message size: {len(error_str):,} characters")

print("\n--- KONTRA ---")
start = time.time()
result = kontra.validate(df, rules=[
    rules.not_null("user_id"),
    rules.unique("user_id"),
    rules.not_null("email"),
    rules.range("age", min=0),
    rules.allowed_values("status", ["active", "inactive", "pending"]),
])
elapsed = time.time() - start

print(f"Kontra: {'PASSED' if result.passed else 'FAILED'} in {elapsed:.2f}s")
print(f"\nDetailed results:")
for r in result.rules:
    status = "✓" if r.passed else "✗"
    print(f"  {status} {r.name}({r.column}): {r.failed_count:,} failures")

# ============================================================
print("\n" + "="*70)
print("TEST 2: Error Output Comparison (small dataset)")
print("="*70)

small_df = pl.DataFrame({
    "id": [1, 2, 3, 4, 5],
    "email": ["a@b.com", None, "c@d.com", None, "e@f.com"],
    "age": [25, 30, -5, 40, 150],
})
print("Test data:")
print(small_df)

print("\n--- PANDERA ERROR OUTPUT ---")
small_schema = pap.DataFrameSchema({
    "email": pap.Column(str, nullable=False),
    "age": pap.Column(int, pa.Check.in_range(0, 120)),
})

try:
    small_schema.validate(small_df, lazy=True)
except pa.errors.SchemaErrors as e:
    print(e)

print("\n--- KONTRA ERROR OUTPUT ---")
result = kontra.validate(small_df, rules=[
    rules.not_null("email"),
    rules.range("age", min=0, max=120),
])
print(result)

# ============================================================
print("\n" + "="*70)
print("TEST 3: Conditional Validation")
print("="*70)
print("Rule: shipping_date must not be null when order_status == 'shipped'")

failures = df.filter(
    (pl.col("order_status") == "shipped") &
    (pl.col("shipping_date").is_null())
)
print(f"\nExpected failures: {len(failures):,} rows")

print("\n--- PANDERA (requires custom check function) ---")

# Pandera requires a Check object with a custom function
def check_shipping_date_fn(df: pl.DataFrame) -> pl.Series:
    """shipping_date required when order_status is 'shipped'"""
    condition_met = df["order_status"] == "shipped"
    has_value = df["shipping_date"].is_not_null()
    return ~condition_met | has_value

conditional_schema = pap.DataFrameSchema({
    "order_status": pap.Column(str),
    "shipping_date": pap.Column(str, nullable=True, checks=[
        pa.Check(check_shipping_date_fn, name="shipping_date_when_shipped")
    ]),
})

start = time.time()
try:
    conditional_schema.validate(df, lazy=True)
    print("Pandera: PASSED")
except pa.errors.SchemaErrors as e:
    elapsed = time.time() - start
    print(f"Pandera: FAILED in {elapsed:.2f}s")
    print(f"(Custom function required, cannot push down to SQL)")

print("\n--- KONTRA (built-in rule, one line) ---")
start = time.time()
result = kontra.validate(df, rules=[
    rules.conditional_not_null("shipping_date", when="order_status == 'shipped'"),
])
elapsed = time.time() - start
print(f"Kontra: {'PASSED' if result.passed else 'FAILED'} in {elapsed:.2f}s")
print(f"Failures: {result.rules[0].failed_count:,}")

# ============================================================
print("\n" + "="*70)
print("TEST 4: Cross-Column Comparison (end_date >= start_date)")
print("="*70)

date_failures = df.filter(pl.col("end_date") < pl.col("start_date"))
print(f"Expected failures: {len(date_failures):,} rows")

print("\n--- PANDERA (requires custom check) ---")

def check_dates_fn(df: pl.DataFrame) -> pl.Series:
    return df["end_date"] >= df["start_date"]

date_schema = pap.DataFrameSchema({
    "start_date": pap.Column(str),
    "end_date": pap.Column(str, checks=[
        pa.Check(check_dates_fn, name="end_after_start")
    ]),
})

start = time.time()
try:
    date_schema.validate(df, lazy=True)
    print("Pandera: PASSED")
except pa.errors.SchemaErrors as e:
    elapsed = time.time() - start
    print(f"Pandera: FAILED in {elapsed:.2f}s")

print("\n--- KONTRA (built-in rule) ---")
start = time.time()
result = kontra.validate(df, rules=[
    rules.compare("end_date", "start_date", op=">="),
])
elapsed = time.time() - start
print(f"Kontra: {'PASSED' if result.passed else 'FAILED'} in {elapsed:.2f}s")
print(f"Failures: {result.rules[0].failed_count:,}")

# ============================================================
print("\n" + "="*70)
print("TEST 5: Severity Levels")
print("="*70)

print("--- PANDERA ---")
print("Binary pass/fail only. No severity levels.")

print("\n--- KONTRA ---")
result = kontra.validate(df, rules=[
    rules.not_null("user_id", severity="blocking"),
    rules.not_null("email", severity="warning"),
    rules.range("age", min=0, max=120, severity="info"),
])

print(f"Overall passed: {result.passed}")
print(f"\nBy severity:")
for r in result.rules:
    icon = "✓" if r.passed else "✗"
    print(f"  {icon} [{r.severity}] {r.name}({r.column}): {r.failed_count:,} failures")

# ============================================================
print("\n" + "="*70)
print("TEST 6: Profiling / Data Discovery")
print("="*70)

print("--- PANDERA ---")
print("No built-in profiling. Must know schema upfront.")

print("\n--- KONTRA ---")
profile = kontra.profile(small_df)
print(profile)

# ============================================================
print("\n" + "="*70)
print("TEST 7: LLM-Friendly Output")
print("="*70)

print("--- PANDERA ---")
print("No LLM-specific output. Error messages can be huge (4MB+).")

print("\n--- KONTRA ---")
result = kontra.validate(small_df, rules=[
    rules.not_null("email"),
    rules.range("age", min=0, max=120),
])
print("result.to_llm():")
print(result.to_llm())

# ============================================================
print("\n" + "="*70)
print("SUMMARY TABLE")
print("="*70)

print("""
| Feature                | Pandera + Ibis      | Kontra              |
|------------------------|---------------------|---------------------|
| Profiling / Draft      | ✗                   | ✓                   |
| CLI                    | ✗                   | ✓                   |
| State / Diff           | ✗                   | ✓                   |
| Conditional checks     | Custom code         | Built-in            |
| Cross-column compare   | Custom code         | Built-in            |
| Severity levels        | Binary only         | blocking/warning/info|
| Error output           | ALL failures        | Counts + samples    |
| LLM output             | ✗                   | .to_llm()           |
| Context metadata       | ✗                   | owner, tags, fix_hint|
| Parquet metadata       | ✗                   | ✓ (no data scan)    |
| Backends               | 20+ via Ibis        | Postgres, SQL Server, DuckDB |
""")

print("Done!")
