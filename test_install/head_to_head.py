#!/usr/bin/env python3
"""
HEAD TO HEAD: Pandera+Ibis vs Kontra
Built-in rules only, on S3 Parquet (5M rows) and Postgres
Both tools use SQL pushdown for fair comparison.
"""

import os
import time

# S3/MinIO credentials
os.environ["AWS_ACCESS_KEY_ID"] = "saevarl"
os.environ["AWS_SECRET_ACCESS_KEY"] = "abc12345"
os.environ["AWS_ENDPOINT_URL"] = "http://100.111.146.121:9000"
os.environ["AWS_REGION"] = "us-east-1"

S3_FILE = "s3://test/data/users_5m.parquet"
PG_URI = "postgresql://kontra:kontra123@localhost:5432/testdb"
PG_TABLE = "users"

import ibis
import pandera.ibis as pai
import pandera as pa
import kontra
from kontra import rules
from urllib.parse import urlparse

print("="*70)
print("HEAD TO HEAD: Pandera+Ibis vs Kontra")
print("="*70)
print(f"S3: {S3_FILE} (5M rows)")
print(f"Postgres: {PG_URI}/{PG_TABLE}")
print("Both tools configured for SQL pushdown")
print()

# ============================================================
# S3 PARQUET (via DuckDB backend)
# ============================================================
print("="*70)
print("S3 PARQUET (5M rows) - via DuckDB")
print("="*70)

# Connect Ibis to DuckDB for S3 access
duck_conn = ibis.duckdb.connect()
duck_conn.raw_sql("INSTALL httpfs; LOAD httpfs;")
duck_conn.raw_sql(f"SET s3_access_key_id='{os.environ['AWS_ACCESS_KEY_ID']}';")
duck_conn.raw_sql(f"SET s3_secret_access_key='{os.environ['AWS_SECRET_ACCESS_KEY']}';")
duck_conn.raw_sql(f"SET s3_endpoint='{os.environ['AWS_ENDPOINT_URL'].replace('http://', '')}';")
duck_conn.raw_sql("SET s3_use_ssl=false;")
duck_conn.raw_sql("SET s3_url_style='path';")

# Warmup
print("\nWarming up...")
duck_conn.raw_sql(f"SELECT COUNT(*) FROM read_parquet('{S3_FILE}')").fetchone()
_ = kontra.validate(S3_FILE, rules=[rules.min_rows(1)], save=False)
print("Warmup done.\n")

# Pandera+Ibis schema for S3
# Note: unique=True doesn't work with Ibis (silently passes)
# Using simple regex (@) - complex patterns are slow in both tools
pandera_s3_schema = pai.DataFrameSchema({
    "user_id": pai.Column(int, nullable=False),
    "email": pai.Column(str, nullable=False, checks=pai.Check.str_matches(r"@")),
    "age": pai.Column(int, pai.Check.in_range(0, 120)),
    "status": pai.Column(str, pai.Check.isin(["active", "inactive", "pending"])),
})

# Pandera+Ibis schema for Postgres (matching actual table)
# Note: unique=True doesn't work with Ibis (silently passes)
pandera_pg_schema = pai.DataFrameSchema({
    "user_id": pai.Column(nullable=False),
    "email": pai.Column(nullable=False, checks=pai.Check.str_matches(r"@")),
    "age": pai.Column(checks=pai.Check.in_range(0, 120)),
    "status": pai.Column(checks=pai.Check.isin(["active", "inactive", "pending"])),
})

# Kontra rules
# Note: unique not included - Pandera's unique=True doesn't work with Ibis
# Using simple regex (@) - complex patterns are slow in both tools
kontra_rules = [
    rules.not_null("user_id"),
    rules.not_null("email"),
    rules.regex("email", pattern=r"@"),
    rules.range("age", min=0, max=120),
    rules.allowed_values("status", ["active", "inactive", "pending"]),
]

print("\n--- PANDERA + IBIS (DuckDB backend) ---")
start = time.time()
try:
    # Create Ibis table from S3 parquet
    s3_table = duck_conn.read_parquet(S3_FILE)
    row_count = s3_table.count().execute()

    # Validate using pandera.ibis
    pandera_s3_schema.validate(s3_table)
    pandera_s3_total = time.time() - start
    print(f"Validate: {pandera_s3_total:.2f}s - PASSED ({row_count:,} rows)")
except pa.errors.SchemaError as e:
    pandera_s3_total = time.time() - start
    print(f"Validate: {pandera_s3_total:.2f}s - FAILED")
    print(f"  Error: {str(e)[:100]}...")
except pa.errors.SchemaErrors as e:
    pandera_s3_total = time.time() - start
    print(f"Validate: {pandera_s3_total:.2f}s - FAILED ({len(e.schema_errors)} errors)")
except Exception as e:
    pandera_s3_total = time.time() - start
    print(f"Validate: {pandera_s3_total:.2f}s - ERROR: {e}")

print(f"TOTAL: {pandera_s3_total:.2f}s")

print("\n--- KONTRA (DuckDB pushdown) ---")
start = time.time()
result = kontra.validate(S3_FILE, rules=kontra_rules, save=False, sample=0)
kontra_s3_total = time.time() - start
status = "PASSED" if result.passed else f"FAILED ({result.failed_count} rules)"
print(f"Validate: {kontra_s3_total:.2f}s - {status} ({result.total_rows:,} rows)")
print(f"TOTAL: {kontra_s3_total:.2f}s")

# ============================================================
# POSTGRES
# ============================================================
print("\n" + "="*70)
print("POSTGRES")
print("="*70)

print("\n--- PANDERA + IBIS (Postgres backend) ---")
try:
    parsed = urlparse(PG_URI)
    pg_conn = ibis.postgres.connect(
        host=parsed.hostname,
        port=parsed.port or 5432,
        user=parsed.username,
        password=parsed.password,
        database=parsed.path.lstrip('/'),
    )

    start = time.time()
    pg_table = pg_conn.table(PG_TABLE)
    row_count = pg_table.count().execute()

    # Validate using pandera.ibis
    pandera_pg_schema.validate(pg_table)
    pandera_pg_total = time.time() - start
    print(f"Validate: {pandera_pg_total:.2f}s - PASSED ({row_count:,} rows)")

except pa.errors.SchemaError as e:
    pandera_pg_total = time.time() - start
    print(f"Validate: {pandera_pg_total:.2f}s - FAILED")
    print(f"  Error: {str(e)[:100]}...")
except pa.errors.SchemaErrors as e:
    pandera_pg_total = time.time() - start
    print(f"Validate: {pandera_pg_total:.2f}s - FAILED ({len(e.schema_errors)} errors)")
except Exception as e:
    pandera_pg_total = time.time() - start
    print(f"Error: {e}")
    pandera_pg_total = 0

print(f"TOTAL: {pandera_pg_total:.2f}s")

print("\n--- KONTRA (Postgres pushdown) ---")
start = time.time()
result = kontra.validate(
    f"{PG_URI}/public.{PG_TABLE}",
    rules=kontra_rules,
    save=False,
)
kontra_pg_total = time.time() - start
status = "PASSED" if result.passed else f"FAILED ({result.failed_count} rules)"
print(f"Validate: {kontra_pg_total:.2f}s - {status} ({result.total_rows:,} rows)")
print(f"TOTAL: {kontra_pg_total:.2f}s")

# ============================================================
# RESULTS
# ============================================================
print("\n" + "="*70)
print("RESULTS")
print("="*70)

def ratio(a, b):
    if a == 0 or b == 0:
        return "N/A"
    return f"{max(a,b)/min(a,b):.1f}x"

def winner(a, b, name_a="Pandera", name_b="Kontra"):
    if a == 0 or b == 0:
        return "N/A"
    return f"{name_b if b < a else name_a} ({ratio(a, b)})"

print(f"""
                    | Pandera+Ibis | Kontra       | Winner
--------------------|--------------|--------------|--------
S3 Parquet (5M)     | {pandera_s3_total:>10.2f}s | {kontra_s3_total:>10.2f}s | {winner(pandera_s3_total, kontra_s3_total)}
Postgres            | {pandera_pg_total:>10.2f}s | {kontra_pg_total:>10.2f}s | {winner(pandera_pg_total, kontra_pg_total)}

Both tools configured for SQL pushdown.
Rules tested: not_null, regex, range, allowed_values (built-in for both)
Note: unique skipped - Pandera unique=True doesn't work with Ibis backend
""")
