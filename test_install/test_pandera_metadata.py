#!/usr/bin/env python3
"""
Test what Pandera means by "metadata checks without .execute()"
"""

import polars as pl
import ibis
import pandera as pa
import pandera.polars as pap
import time

# Create a parquet file
PARQUET_FILE = "/tmp/test_meta.parquet"

df = pl.DataFrame({
    "id": list(range(100_000)),
    "name": ["test"] * 100_000,
    "value": list(range(100_000)),
})
df.write_parquet(PARQUET_FILE)
print(f"Created {PARQUET_FILE}")

# Test 1: Schema-only checks (dtype, nullable) - "metadata"
print("\n" + "="*60)
print("TEST 1: Schema checks only (dtype, nullable)")
print("="*60)

schema_only = pap.DataFrameSchema({
    "id": pap.Column(int, nullable=False),
    "name": pap.Column(str, nullable=False),
    "value": pap.Column(int),
})

# Try with Ibis backend
print("\nUsing Ibis (DuckDB) backend:")
try:
    con = ibis.duckdb.connect()
    table = con.read_parquet(PARQUET_FILE)
    print(f"  Table type: {type(table)}")

    # Can Pandera validate an Ibis table directly?
    start = time.time()
    result = schema_only.validate(table)
    elapsed = time.time() - start
    print(f"  Validated in {elapsed:.3f}s")
    print(f"  Result type: {type(result)}")
except Exception as e:
    print(f"  Error: {e}")

# Test 2: Data value checks (range, isin)
print("\n" + "="*60)
print("TEST 2: Data value checks (range)")
print("="*60)

data_check_schema = pap.DataFrameSchema({
    "id": pap.Column(int, nullable=False),
    "value": pap.Column(int, pa.Check.ge(0)),  # Data check
})

print("\nUsing Ibis (DuckDB) backend:")
try:
    con = ibis.duckdb.connect()
    table = con.read_parquet(PARQUET_FILE)

    start = time.time()
    result = data_check_schema.validate(table)
    elapsed = time.time() - start
    print(f"  Validated in {elapsed:.3f}s")
    print(f"  Result type: {type(result)}")
except Exception as e:
    print(f"  Error: {e}")

# Test 3: Check if Pandera can use Parquet metadata for row count
print("\n" + "="*60)
print("TEST 3: Can Pandera check row count without scanning?")
print("="*60)

# Pandera doesn't have min_rows built-in, so this would require custom check
print("Pandera has no built-in min_rows/max_rows check.")
print("Custom checks cannot push down to SQL/metadata.")

# Test 4: What does Pandera mean by "metadata"?
print("\n" + "="*60)
print("TEST 4: What Pandera calls 'metadata'")
print("="*60)

print("""
From Pandera docs context, 'metadata' likely refers to:
- Schema metadata (dtypes, column names, nullable flags)
- NOT Parquet file metadata (row counts, column stats)

Pandera's 'metadata checks without execute':
- Checking column exists: ✓ (from schema)
- Checking dtype: ✓ (from schema)
- Checking nullable: ✓ (from schema)

Pandera CANNOT do without execute:
- min_rows / max_rows (needs row count)
- range checks (needs data values)
- null count (needs data scan)
- uniqueness (needs data scan)

Kontra's Parquet metadata optimization:
- Row count from footer ✓
- Column min/max stats ✓
- Null counts ✓
- These are FILE metadata, not SCHEMA metadata
""")

# Prove Kontra uses file metadata
print("\n" + "="*60)
print("TEST 5: Prove Kontra uses Parquet FILE metadata")
print("="*60)

import kontra
from kontra import rules
from kontra.preplan.planner import preplan_single_parquet

# Create file with known stats
test_df = pl.DataFrame({
    "id": list(range(50000)),  # 50K rows
    "value": [i * 2 for i in range(50000)],  # min=0, max=99998
})
test_file = "/tmp/test_preplan.parquet"
test_df.write_parquet(test_file)

# Show preplan decisions
predicates = [
    ("min_rows", "id", ">=", 0),  # Should pass from metadata
    ("not_null", "id", "not_null", None),  # Should pass from null_count=0
]

preplan = preplan_single_parquet(test_file, ["id", "value"], predicates)

print(f"File: {test_file}")
print(f"Total rows (from metadata): {preplan.stats['total_rows']:,}")
print(f"\nPreplan decisions (from FILE metadata, no data scan):")
for rule_id, decision in preplan.rule_decisions.items():
    print(f"  {rule_id}: {decision}")

print(f"\nThis is PARQUET FILE metadata, not schema metadata.")
print("Pandera does not use this optimization.")
