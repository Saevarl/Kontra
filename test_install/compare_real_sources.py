#!/usr/bin/env python3
"""
Pandera vs Kontra: Real Data Sources (Parquet & Postgres)
"""

import time
import os
import polars as pl
import random

# ============================================================
print("="*70)
print("SETUP: Create test Parquet file (1M rows)")
print("="*70)

PARQUET_FILE = "/tmp/test_users.parquet"
N_ROWS = 1_000_000

if not os.path.exists(PARQUET_FILE):
    df = pl.DataFrame({
        "user_id": list(range(N_ROWS)),
        "email": [f"user{i}@example.com" if i % 100 != 0 else None for i in range(N_ROWS)],
        "age": [random.randint(18, 80) if i % 200 != 0 else -5 for i in range(N_ROWS)],
        "status": [random.choice(["active", "inactive"]) if i % 500 != 0 else "INVALID" for i in range(N_ROWS)],
    })
    df.write_parquet(PARQUET_FILE)
    print(f"Created {PARQUET_FILE} ({os.path.getsize(PARQUET_FILE) / 1024 / 1024:.1f} MB)")
else:
    print(f"Using existing {PARQUET_FILE}")

# ============================================================
print("\n" + "="*70)
print("TEST 1: PARQUET VALIDATION")
print("="*70)

# --- PANDERA ---
print("\n--- PANDERA ---")
print("Pandera cannot validate a Parquet file directly.")
print("You must: pl.read_parquet() -> DataFrame -> schema.validate()")

import pandera as pa
import pandera.polars as pap

schema = pap.DataFrameSchema({
    "user_id": pap.Column(int, nullable=False),
    "email": pap.Column(str, nullable=False),
    "age": pap.Column(int, pa.Check.ge(0)),
    "status": pap.Column(str, pa.Check.isin(["active", "inactive"])),
})

start = time.time()
df = pl.read_parquet(PARQUET_FILE)  # Must load into memory first
load_time = time.time() - start

start = time.time()
try:
    schema.validate(df, lazy=True)
    print("PASSED")
except pa.errors.SchemaErrors as e:
    validate_time = time.time() - start
    print(f"FAILED in {load_time + validate_time:.2f}s (load={load_time:.2f}s, validate={validate_time:.2f}s)")
    print(f"Errors: {len(e.schema_errors)}")

del df  # Free memory

# --- KONTRA ---
print("\n--- KONTRA ---")
print("Kontra validates Parquet file directly (path as string):")
print(f'  kontra.validate("{PARQUET_FILE}", rules=[...])')

import kontra
from kontra import rules

start = time.time()
result = kontra.validate(PARQUET_FILE, rules=[
    rules.not_null("user_id"),
    rules.not_null("email"),
    rules.range("age", min=0),
    rules.allowed_values("status", ["active", "inactive"]),
])
elapsed = time.time() - start

print(f"{'PASSED' if result.passed else 'FAILED'} in {elapsed:.2f}s")
for r in result.rules:
    status = "✓" if r.passed else "✗"
    print(f"  {status} {r.name}({r.column}): {r.failed_count:,} failures")

# ============================================================
print("\n" + "="*70)
print("TEST 2: PARQUET METADATA OPTIMIZATION")
print("="*70)

print("\n--- PANDERA ---")
print("No metadata optimization. Must scan full file.")

start = time.time()
df = pl.read_parquet(PARQUET_FILE)
row_count = len(df)
elapsed = time.time() - start
print(f"To check row count: load entire file ({elapsed:.2f}s)")
del df

print("\n--- KONTRA ---")
print("Uses Parquet footer metadata (no data scan):")

start = time.time()
result = kontra.validate(PARQUET_FILE, rules=[
    rules.min_rows(100),
])
elapsed = time.time() - start
print(f"min_rows check: {elapsed*1000:.1f}ms")
print(f"  (Answered from metadata, not data scan)")

# ============================================================
print("\n" + "="*70)
print("TEST 3: POSTGRES VALIDATION")
print("="*70)

POSTGRES_URI = os.environ.get("DATABASE_URL", "postgresql://localhost:5432/testdb")

print(f"\nPostgres URI: {POSTGRES_URI}")

# Check if postgres is available
try:
    import psycopg2
    conn = psycopg2.connect(POSTGRES_URI)
    conn.close()
    postgres_available = True
    print("Postgres: Connected successfully")
except Exception as e:
    postgres_available = False
    print(f"Postgres: Not available ({e})")

if postgres_available:
    # Create test table
    import psycopg2
    conn = psycopg2.connect(POSTGRES_URI)
    cur = conn.cursor()

    cur.execute("DROP TABLE IF EXISTS test_users")
    cur.execute("""
        CREATE TABLE test_users (
            user_id INTEGER PRIMARY KEY,
            email VARCHAR(255),
            age INTEGER,
            status VARCHAR(50)
        )
    """)

    # Insert sample data (10K rows for quick test)
    print("Inserting 10,000 test rows...")
    for i in range(10000):
        email = f"user{i}@example.com" if i % 100 != 0 else None
        age = random.randint(18, 80) if i % 200 != 0 else -5
        status = random.choice(["active", "inactive"]) if i % 500 != 0 else "INVALID"
        cur.execute(
            "INSERT INTO test_users (user_id, email, age, status) VALUES (%s, %s, %s, %s)",
            (i, email, age, status)
        )
    conn.commit()
    cur.close()
    conn.close()
    print("Test table created")

    # --- PANDERA + IBIS ---
    print("\n--- PANDERA + IBIS ---")
    print("Pandera can use Ibis backend for Postgres:")

    try:
        import ibis

        # Connect via Ibis - parse URI manually
        from urllib.parse import urlparse
        parsed = urlparse(POSTGRES_URI)

        ibis_conn = ibis.postgres.connect(
            host=parsed.hostname,
            port=parsed.port or 5432,
            user=parsed.username,
            password=parsed.password,
            database=parsed.path.lstrip('/'),
        )
        table = ibis_conn.table("test_users")

        # Convert to DataFrame for Pandera (Ibis integration is limited)
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
            print(f"  Load from Postgres: {load_time:.2f}s")
            print(f"  Validate in memory: {validate_time:.2f}s")
            print(f"  (Data pulled to client, then validated)")

        del df
        ibis_conn.disconnect()

    except Exception as e:
        print(f"Ibis error: {e}")

    # --- KONTRA ---
    print("\n--- KONTRA ---")
    print("Kontra pushes validation to Postgres (SQL):")
    # Kontra expects table in URI: postgres://host/db/schema.table
    kontra_uri = POSTGRES_URI + "/public.test_users"
    print(f'  kontra.validate("{kontra_uri}", rules=[...])')

    start = time.time()
    result = kontra.validate(
        kontra_uri,
        rules=[
            rules.not_null("user_id"),
            rules.not_null("email"),
            rules.range("age", min=0),
            rules.allowed_values("status", ["active", "inactive"]),
        ]
    )
    elapsed = time.time() - start

    print(f"{'PASSED' if result.passed else 'FAILED'} in {elapsed:.2f}s")
    print(f"  (SQL executed on Postgres, only counts returned)")
    for r in result.rules:
        status = "✓" if r.passed else "✗"
        print(f"  {status} {r.name}({r.column}): {r.failed_count:,} failures")

else:
    print("\nSkipping Postgres tests (no connection)")
    print("Set DATABASE_URL env var to test Postgres")

# ============================================================
print("\n" + "="*70)
print("SUMMARY")
print("="*70)

print("""
                        | Pandera              | Kontra
------------------------|----------------------|----------------------
Parquet file input      | No (load first)      | Yes (path string)
Parquet metadata        | No                   | Yes (min_rows, etc)
Postgres input          | Via Ibis (loads data)| Yes (SQL pushdown)
SQL pushdown            | Limited (built-ins)  | Full (all rules)
Custom checks on DB     | No (must load)       | Yes (custom_sql_check)
""")
