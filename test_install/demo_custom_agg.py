#!/usr/bin/env python3
"""
Demo: Custom rule with to_sql_agg() across Parquet, PostgreSQL, and SQL Server.

Shows how a single custom rule definition works across all data sources
with automatic SQL pushdown.
"""

import tempfile
from pathlib import Path

import polars as pl
import kontra
from kontra.rules.base import BaseRule
from kontra.rules.registry import register_rule, RULE_REGISTRY

# =============================================================================
# STEP 1: Define a custom rule with SQL pushdown
# =============================================================================

# Clean up if already registered (for re-running)
if "positive" in RULE_REGISTRY:
    del RULE_REGISTRY["positive"]

@register_rule("positive")
class PositiveRule(BaseRule):
    """
    Custom rule: Values must be > 0. NULL counts as violation.

    Supports SQL pushdown via to_sql_agg() - no data loading needed
    for Parquet files or database tables.
    """

    def __init__(self, name, params):
        super().__init__(name, params)
        self.column = params["column"]

    def validate(self, df):
        """Fallback: Polars execution (used for DataFrames)."""
        mask = df[self.column].is_null() | (df[self.column] <= 0)
        return self._failures(df, mask, f"{self.column} must be positive")

    def to_sql_agg(self, dialect="duckdb"):
        """
        SQL pushdown: Returns aggregate expression for counting violations.

        Called once per dialect. Return None to skip SQL for a dialect.
        """
        # Handle dialect-specific quoting
        if dialect == "mssql":
            col = f"[{self.column}]"
        else:
            col = f'"{self.column}"'

        # COUNT rows where value is NULL or <= 0
        return f"SUM(CASE WHEN {col} IS NULL OR {col} <= 0 THEN 1 ELSE 0 END)"


# =============================================================================
# STEP 2: Create test data
# =============================================================================

print("=" * 70)
print("CUSTOM RULE DEMO: 'positive' rule with to_sql_agg()")
print("=" * 70)
print()
print("Rule definition:")
print("  - Values must be > 0")
print("  - NULL counts as violation")
print("  - Implements to_sql_agg() for SQL pushdown")
print()

# Test data: 5 rows, 2 violations (amount: -50 and 0)
test_data = pl.DataFrame({
    "id": [1, 2, 3, 4, 5],
    "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
    "amount": [100, -50, 200, 0, 150],  # -50 and 0 are violations
})

print("Test data:")
print(test_data)
print()
print("Expected: 2 violations (rows with amount=-50 and amount=0)")
print()

# Rule to validate
rules = [{"name": "positive", "params": {"column": "amount"}}]


# =============================================================================
# STEP 3: Test with Parquet (DuckDB SQL pushdown)
# =============================================================================

print("=" * 70)
print("TEST 1: Parquet file (DuckDB SQL pushdown)")
print("=" * 70)

with tempfile.TemporaryDirectory() as tmpdir:
    parquet_path = Path(tmpdir) / "test_data.parquet"
    test_data.write_parquet(parquet_path)

    result = kontra.validate(str(parquet_path), rules=rules, save=False)

    print(f"Source: {parquet_path.name}")
    print(f"Passed: {result.passed}")
    print(f"Failed rules: {result.failed_count}")
    print(f"Rule result:")
    r = result.rules[0]
    print(f"  - rule_id: {r.rule_id}")
    print(f"  - failed_count: {r.failed_count}")
    print(f"  - source: {r.source}  <-- SQL pushdown!")
    print()


# =============================================================================
# STEP 4: Test with PostgreSQL
# =============================================================================

print("=" * 70)
print("TEST 2: PostgreSQL (native SQL pushdown)")
print("=" * 70)

PG_URI = "postgresql://kontra:kontra123@localhost:5432/testdb"

try:
    import psycopg

    # Create test table
    with psycopg.connect(PG_URI) as conn:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS demo_positive")
            cur.execute("""
                CREATE TABLE demo_positive (
                    id INT,
                    name TEXT,
                    amount INT
                )
            """)
            cur.executemany(
                "INSERT INTO demo_positive (id, name, amount) VALUES (%s, %s, %s)",
                test_data.rows()
            )
        conn.commit()

    # Validate
    result = kontra.validate(
        f"{PG_URI}/public.demo_positive",
        rules=rules,
        save=False
    )

    print(f"Source: PostgreSQL table 'demo_positive'")
    print(f"Passed: {result.passed}")
    print(f"Failed rules: {result.failed_count}")
    print(f"Rule result:")
    r = result.rules[0]
    print(f"  - rule_id: {r.rule_id}")
    print(f"  - failed_count: {r.failed_count}")
    print(f"  - source: {r.source}  <-- SQL pushdown!")
    print()

    # Cleanup
    with psycopg.connect(PG_URI) as conn:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS demo_positive")
        conn.commit()

except Exception as e:
    print(f"PostgreSQL not available: {e}")
    print()


# =============================================================================
# STEP 5: Test with SQL Server
# =============================================================================

print("=" * 70)
print("TEST 3: SQL Server (native SQL pushdown)")
print("=" * 70)

MSSQL_URI = "mssql://sa:YourStrong!Passw0rd@localhost:1433/master"

try:
    import pymssql

    # Parse connection
    conn = pymssql.connect(
        server="localhost",
        port=1433,
        user="sa",
        password="YourStrong!Passw0rd",
        database="master"
    )

    # Create test table
    with conn.cursor() as cur:
        cur.execute("IF OBJECT_ID('demo_positive', 'U') IS NOT NULL DROP TABLE demo_positive")
        cur.execute("""
            CREATE TABLE demo_positive (
                id INT,
                name NVARCHAR(100),
                amount INT
            )
        """)
        for row in test_data.rows():
            cur.execute(
                "INSERT INTO demo_positive (id, name, amount) VALUES (%s, %s, %s)",
                row
            )
    conn.commit()

    # Validate
    result = kontra.validate(
        f"{MSSQL_URI}/dbo.demo_positive",
        rules=rules,
        save=False
    )

    print(f"Source: SQL Server table 'demo_positive'")
    print(f"Passed: {result.passed}")
    print(f"Failed rules: {result.failed_count}")
    print(f"Rule result:")
    r = result.rules[0]
    print(f"  - rule_id: {r.rule_id}")
    print(f"  - failed_count: {r.failed_count}")
    print(f"  - source: {r.source}  <-- SQL pushdown!")
    print()

    # Cleanup
    with conn.cursor() as cur:
        cur.execute("IF OBJECT_ID('demo_positive', 'U') IS NOT NULL DROP TABLE demo_positive")
    conn.commit()
    conn.close()

except Exception as e:
    print(f"SQL Server not available: {e}")
    print()


# =============================================================================
# STEP 6: Test with DataFrame (Polars fallback)
# =============================================================================

print("=" * 70)
print("TEST 4: DataFrame (Polars execution - no SQL)")
print("=" * 70)

result = kontra.validate(test_data, rules=rules, save=False)

print(f"Source: Polars DataFrame")
print(f"Passed: {result.passed}")
print(f"Failed rules: {result.failed_count}")
print(f"Rule result:")
r = result.rules[0]
print(f"  - rule_id: {r.rule_id}")
print(f"  - failed_count: {r.failed_count}")
print(f"  - source: {r.source}  <-- Polars (no SQL for DataFrames)")
print()


# =============================================================================
# SUMMARY
# =============================================================================

print("=" * 70)
print("SUMMARY")
print("=" * 70)
print("""
The SAME custom rule definition works across:
  - Parquet files (via DuckDB SQL)
  - PostgreSQL (native SQL)
  - SQL Server (native SQL)
  - DataFrames (Polars fallback)

The to_sql_agg() method enables SQL pushdown WITHOUT modifying
any executor code. Just define the SQL aggregate expression once,
and Kontra handles the rest.

Key points:
  1. Define to_sql_agg(dialect) to return a SQL aggregate expression
  2. Handle dialect differences (mssql uses [brackets], others use "quotes")
  3. Return None for unsupported dialects (falls back to Polars)
  4. The 'source' field shows which execution path was used
""")
