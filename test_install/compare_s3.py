#!/usr/bin/env python3
"""
Pandera vs Kontra: S3 Parquet (5M rows) + Postgres
"""

import os
import time

# S3/MinIO credentials
os.environ["AWS_ACCESS_KEY_ID"] = "saevarl"
os.environ["AWS_SECRET_ACCESS_KEY"] = "abc12345"
os.environ["AWS_ENDPOINT_URL"] = "http://100.111.146.121:9000"
os.environ["AWS_REGION"] = "us-east-1"

S3_FILE = "s3://test/data/users_5m.parquet"
POSTGRES_URI = "postgresql://kontra:kontra123@localhost:5432/testdb/public.users"

print("="*70)
print("PANDERA vs KONTRA: Real Data Sources")
print("="*70)
print(f"S3 File: {S3_FILE} (5M rows, 290 MB)")
print(f"Postgres: {POSTGRES_URI}")

# ============================================================
print("\n" + "="*70)
print("TEST 1: S3 PARQUET VALIDATION (5M rows)")
print("="*70)

# --- PANDERA ---
print("\n--- PANDERA ---")
print("Pandera must download entire file, then validate:")

import polars as pl
import pandera as pa
import pandera.polars as pap

schema = pap.DataFrameSchema({
    "user_id": pap.Column(int, nullable=False),
    "email": pap.Column(str, nullable=False),
    "age": pap.Column(int, pa.Check.in_range(0, 120)),
    "status": pap.Column(str, pa.Check.isin(["active", "inactive", "pending"])),
})

start = time.time()
df = pl.read_parquet(S3_FILE)
load_time = time.time() - start

start = time.time()
try:
    schema.validate(df, lazy=True)
    print(f"PASSED in {load_time + (time.time() - start):.2f}s")
except pa.errors.SchemaErrors as e:
    validate_time = time.time() - start
    total = load_time + validate_time
    print(f"FAILED in {total:.2f}s")
    print(f"  Download from S3: {load_time:.2f}s")
    print(f"  Validate in memory: {validate_time:.2f}s")
    print(f"  Errors: {len(e.schema_errors)}")

del df

# --- KONTRA ---
print("\n--- KONTRA ---")
print("Kontra validates S3 directly (DuckDB httpfs):")

import kontra
from kontra import rules

start = time.time()
result = kontra.validate(
    S3_FILE,
    rules=[
        rules.not_null("user_id"),
        rules.not_null("email"),
        rules.range("age", min=0, max=120),
        rules.allowed_values("status", ["active", "inactive", "pending"]),
    ],
    save=False,
)
elapsed = time.time() - start

print(f"{'PASSED' if result.passed else 'FAILED'} in {elapsed:.2f}s")
print(f"  Rows: {result.total_rows:,}")
for r in result.rules:
    status = "✓" if r.passed else "✗"
    print(f"  {status} {r.name}({r.column}): {r.failed_count:,} failures")

# ============================================================
print("\n" + "="*70)
print("TEST 2: S3 PARQUET METADATA (no data scan)")
print("="*70)

print("\n--- PANDERA ---")
print("No metadata optimization. Must download 290MB to count rows.")

start = time.time()
df = pl.read_parquet(S3_FILE)
row_count = len(df)
elapsed = time.time() - start
print(f"Row count via download: {row_count:,} in {elapsed:.2f}s")
del df

print("\n--- KONTRA ---")
print("Uses Parquet footer metadata (downloads ~KB, not 290MB):")

start = time.time()
result = kontra.validate(
    S3_FILE,
    rules=[rules.min_rows(1000)],
    save=False,
)
elapsed = time.time() - start
print(f"min_rows check: {elapsed:.2f}s")
print(f"  Rows (from metadata): {result.total_rows:,}")

# ============================================================
print("\n" + "="*70)
print("TEST 3: POSTGRES VALIDATION")
print("="*70)

print("\n--- PANDERA + IBIS ---")
print("Must download all rows to client:")

try:
    import ibis
    from urllib.parse import urlparse

    # Parse Kontra-style URI (has table in path)
    pg_base = "postgresql://kontra:kontra123@localhost:5432/testdb"
    parsed = urlparse(pg_base)

    ibis_conn = ibis.postgres.connect(
        host=parsed.hostname,
        port=parsed.port or 5432,
        user=parsed.username,
        password=parsed.password,
        database=parsed.path.lstrip('/'),
    )
    table = ibis_conn.table("users")

    start = time.time()
    df = table.to_polars()
    load_time = time.time() - start

    start = time.time()
    try:
        schema.validate(df, lazy=True)
        print("PASSED")
    except pa.errors.SchemaErrors as e:
        validate_time = time.time() - start
        print(f"FAILED in {load_time + validate_time:.2f}s")
        print(f"  Load from Postgres: {load_time:.2f}s ({len(df):,} rows)")
        print(f"  Validate in memory: {validate_time:.2f}s")

    del df
    ibis_conn.disconnect()

except Exception as e:
    print(f"Error: {e}")

print("\n--- KONTRA ---")
print("SQL pushdown (runs on Postgres, returns only counts):")

start = time.time()
result = kontra.validate(
    POSTGRES_URI,
    rules=[
        rules.not_null("user_id"),
        rules.not_null("email"),
        rules.range("age", min=0, max=120),
        rules.allowed_values("status", ["active", "inactive", "pending"]),
    ],
    save=False,
)
elapsed = time.time() - start

print(f"{'PASSED' if result.passed else 'FAILED'} in {elapsed:.2f}s")
print(f"  Rows: {result.total_rows:,}")
for r in result.rules:
    status = "✓" if r.passed else "✗"
    print(f"  {status} {r.name}({r.column}): {r.failed_count:,} failures")

# ============================================================
print("\n" + "="*70)
print("TEST 4: CONDITIONAL RULES (Kontra only)")
print("="*70)

print("Pandera: Requires custom Python function (cannot push down)")
print("\nKontra: Built-in, pushes down to SQL:")

start = time.time()
result = kontra.validate(
    S3_FILE,
    rules=[
        rules.conditional_not_null("email", when="is_premium == true"),
        rules.conditional_range("balance", when="is_premium == true", min=0),
        rules.compare("last_login", "signup_date", op=">="),
    ],
    save=False,
)
elapsed = time.time() - start

print(f"{'PASSED' if result.passed else 'FAILED'} in {elapsed:.2f}s")
for r in result.rules:
    status = "✓" if r.passed else "✗"
    print(f"  {status} {r.name}: {r.failed_count:,} failures")

# ============================================================
print("\n" + "="*70)
print("SUMMARY")
print("="*70)

print("""
                          | Pandera              | Kontra
--------------------------|----------------------|----------------------
S3 Parquet (5M rows)      | Download all → RAM   | DuckDB httpfs (stream)
Parquet metadata          | No                   | Yes (KB vs 290MB)
Postgres                  | Download → validate  | SQL pushdown
Conditional checks        | Custom Python        | Built-in (pushdown)
Cross-column compare      | Custom Python        | Built-in (pushdown)
""")
