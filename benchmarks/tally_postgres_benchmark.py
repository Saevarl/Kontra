#!/usr/bin/env python3
"""
Benchmark: EXISTS vs COUNT on PostgreSQL

Tests real database performance, not just DuckDB file scanning.
Requires PostgreSQL container running: cd tests/postgres && docker compose up -d
"""

import os
import sys
import time
from typing import List

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import psycopg
from psycopg import sql

# PostgreSQL connection settings (from tests/postgres)
PG_HOST = "localhost"
PG_PORT = 5433
PG_USER = "kontra"
PG_PASS = "kontra_test"
PG_DB = "kontra_test"


def create_test_table(conn, table_name: str, num_rows: int, violation_rate: float = 0.01):
    """Create a test table with controlled violations."""
    import random
    random.seed(42)

    with conn.cursor() as cur:
        # Drop if exists
        cur.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(sql.Identifier(table_name)))

        # Create table
        cur.execute(sql.SQL("""
            CREATE TABLE {} (
                id SERIAL PRIMARY KEY,
                col_0 TEXT,
                col_1 TEXT,
                col_2 TEXT,
                col_3 TEXT,
                col_4 TEXT,
                score INTEGER,
                status TEXT
            )
        """).format(sql.Identifier(table_name)))

        # Insert data in batches
        batch_size = 10000
        for batch_start in range(0, num_rows, batch_size):
            batch_end = min(batch_start + batch_size, num_rows)
            values = []
            for i in range(batch_start, batch_end):
                cols = []
                for j in range(5):
                    if random.random() < violation_rate:
                        cols.append(None)
                    else:
                        cols.append(f"value_{i % 1000}")

                score = -1 if random.random() < violation_rate else random.randint(0, 100)
                status = "INVALID" if random.random() < violation_rate else random.choice(["active", "inactive", "pending"])
                values.append((cols[0], cols[1], cols[2], cols[3], cols[4], score, status))

            cur.executemany(
                sql.SQL("INSERT INTO {} (col_0, col_1, col_2, col_3, col_4, score, status) VALUES (%s, %s, %s, %s, %s, %s, %s)").format(sql.Identifier(table_name)),
                values
            )

        conn.commit()

        # Analyze for stats
        cur.execute(sql.SQL("ANALYZE {}").format(sql.Identifier(table_name)))
        conn.commit()


def benchmark_exists_not_null(conn, table_name: str, num_rules: int) -> float:
    """Benchmark EXISTS queries for not_null rules."""
    with conn.cursor() as cur:
        start = time.perf_counter()

        # Run EXISTS query for each column
        for i in range(min(num_rules, 5)):
            col = f"col_{i}"
            cur.execute(sql.SQL(
                "SELECT EXISTS (SELECT 1 FROM {} WHERE {} IS NULL LIMIT 1)"
            ).format(sql.Identifier(table_name), sql.Identifier(col)))
            cur.fetchone()

        return (time.perf_counter() - start) * 1000


def benchmark_count_not_null(conn, table_name: str, num_rules: int) -> float:
    """Benchmark COUNT queries for not_null rules."""
    with conn.cursor() as cur:
        start = time.perf_counter()

        # Build single aggregate query
        aggs = []
        for i in range(min(num_rules, 5)):
            col = f"col_{i}"
            aggs.append(sql.SQL("SUM(CASE WHEN {} IS NULL THEN 1 ELSE 0 END)").format(sql.Identifier(col)))

        query = sql.SQL("SELECT {} FROM {}").format(
            sql.SQL(", ").join(aggs),
            sql.Identifier(table_name)
        )
        cur.execute(query)
        cur.fetchone()

        return (time.perf_counter() - start) * 1000


def benchmark_exists_unique(conn, table_name: str, num_rules: int) -> float:
    """Benchmark EXISTS queries for unique rules."""
    with conn.cursor() as cur:
        start = time.perf_counter()

        for i in range(min(num_rules, 5)):
            col = f"col_{i}"
            cur.execute(sql.SQL(
                "SELECT EXISTS (SELECT 1 FROM {} WHERE {} IS NOT NULL GROUP BY {} HAVING COUNT(*) > 1 LIMIT 1)"
            ).format(sql.Identifier(table_name), sql.Identifier(col), sql.Identifier(col)))
            cur.fetchone()

        return (time.perf_counter() - start) * 1000


def benchmark_count_unique(conn, table_name: str, num_rules: int) -> float:
    """Benchmark COUNT queries for unique rules."""
    with conn.cursor() as cur:
        start = time.perf_counter()

        # For unique, we need separate queries (can't easily batch GROUP BY)
        for i in range(min(num_rules, 5)):
            col = f"col_{i}"
            cur.execute(sql.SQL(
                "SELECT COUNT(*) - COUNT(DISTINCT {}) FROM {} WHERE {} IS NOT NULL"
            ).format(sql.Identifier(col), sql.Identifier(table_name), sql.Identifier(col)))
            cur.fetchone()

        return (time.perf_counter() - start) * 1000


def benchmark_exists_allowed_values(conn, table_name: str, num_rules: int) -> float:
    """Benchmark EXISTS queries for allowed_values rules."""
    allowed = ["active", "inactive", "pending"]
    with conn.cursor() as cur:
        start = time.perf_counter()

        for _ in range(num_rules):
            cur.execute(sql.SQL(
                "SELECT EXISTS (SELECT 1 FROM {} WHERE status IS NOT NULL AND status NOT IN ('active', 'inactive', 'pending') LIMIT 1)"
            ).format(sql.Identifier(table_name)))
            cur.fetchone()

        return (time.perf_counter() - start) * 1000


def benchmark_count_allowed_values(conn, table_name: str, num_rules: int) -> float:
    """Benchmark COUNT queries for allowed_values rules."""
    with conn.cursor() as cur:
        start = time.perf_counter()

        # Single query with multiple aggregates
        aggs = []
        for i in range(num_rules):
            aggs.append(sql.SQL(
                "SUM(CASE WHEN status IS NOT NULL AND status NOT IN ('active', 'inactive', 'pending') THEN 1 ELSE 0 END)"
            ))

        query = sql.SQL("SELECT {} FROM {}").format(
            sql.SQL(", ").join(aggs),
            sql.Identifier(table_name)
        )
        cur.execute(query)
        cur.fetchone()

        return (time.perf_counter() - start) * 1000


def run_benchmark():
    """Run the full PostgreSQL benchmark."""
    print("=" * 70)
    print("PostgreSQL Tally Mode Benchmark")
    print("=" * 70)

    conn = psycopg.connect(
        host=PG_HOST,
        port=PG_PORT,
        user=PG_USER,
        password=PG_PASS,
        dbname=PG_DB,
    )

    sizes = [10_000, 100_000, 500_000]
    rule_counts = [1, 5, 10]
    violation_rate = 0.01

    results = []

    for num_rows in sizes:
        table_name = f"bench_{num_rows}"
        print(f"\nCreating table with {num_rows:,} rows...")
        create_test_table(conn, table_name, num_rows, violation_rate)

        for num_rules in rule_counts:
            # Warmup
            benchmark_exists_not_null(conn, table_name, 1)
            benchmark_count_not_null(conn, table_name, 1)

            # not_null benchmarks
            exists_ms = benchmark_exists_not_null(conn, table_name, num_rules)
            count_ms = benchmark_count_not_null(conn, table_name, num_rules)
            speedup = count_ms / exists_ms if exists_ms > 0 else 0
            results.append(("not_null", num_rows, num_rules, exists_ms, count_ms, speedup))
            print(f"  not_null ({num_rules} rules): EXISTS={exists_ms:.2f}ms, COUNT={count_ms:.2f}ms, speedup={speedup:.2f}x")

            # unique benchmarks
            exists_ms = benchmark_exists_unique(conn, table_name, num_rules)
            count_ms = benchmark_count_unique(conn, table_name, num_rules)
            speedup = count_ms / exists_ms if exists_ms > 0 else 0
            results.append(("unique", num_rows, num_rules, exists_ms, count_ms, speedup))
            print(f"  unique ({num_rules} rules): EXISTS={exists_ms:.2f}ms, COUNT={count_ms:.2f}ms, speedup={speedup:.2f}x")

            # allowed_values benchmarks
            exists_ms = benchmark_exists_allowed_values(conn, table_name, num_rules)
            count_ms = benchmark_count_allowed_values(conn, table_name, num_rules)
            speedup = count_ms / exists_ms if exists_ms > 0 else 0
            results.append(("allowed_values", num_rows, num_rules, exists_ms, count_ms, speedup))
            print(f"  allowed_values ({num_rules} rules): EXISTS={exists_ms:.2f}ms, COUNT={count_ms:.2f}ms, speedup={speedup:.2f}x")

        # Cleanup
        with conn.cursor() as cur:
            cur.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(sql.Identifier(table_name)))
        conn.commit()

    conn.close()

    # Summary
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"{'Rule':<15} {'Rows':<10} {'Rules':<6} {'EXISTS(ms)':<12} {'COUNT(ms)':<12} {'Speedup':<10}")
    print("-" * 70)
    for rule, rows, rules, exists, count, speedup in results:
        print(f"{rule:<15} {rows:<10} {rules:<6} {exists:<12.2f} {count:<12.2f} {speedup:<10.2f}x")

    # Analysis
    print("\n" + "=" * 70)
    print("ANALYSIS")
    print("=" * 70)

    # Group by rule type
    by_rule = {}
    for rule, rows, rules, exists, count, speedup in results:
        if rule not in by_rule:
            by_rule[rule] = []
        by_rule[rule].append(speedup)

    for rule, speedups in by_rule.items():
        avg_speedup = sum(speedups) / len(speedups)
        print(f"{rule}: avg speedup = {avg_speedup:.2f}x (EXISTS vs COUNT)")

    overall_avg = sum(r[5] for r in results) / len(results)
    print(f"\nOverall average speedup: {overall_avg:.2f}x")

    if overall_avg > 1.2:
        print("\n→ EXISTS is faster. Default tally=False is appropriate.")
    elif overall_avg < 0.8:
        print("\n→ COUNT is faster. Consider changing default to tally=True.")
    else:
        print("\n→ Performance is similar. Consider tally=True for richer data.")


if __name__ == "__main__":
    run_benchmark()
