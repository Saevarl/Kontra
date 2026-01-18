#!/usr/bin/env python3
"""
Benchmark different SQL execution approaches for Kontra.

Tests:
1. Current 2-phase approach (EXISTS for not_null, then aggregate)
2. Adaptive single-pass (fold not_null into aggregate when mixed)
3. Different SQL patterns for null checking
4. Full count vs existence-only modes

Usage:
    python scripts/benchmark_sql_approaches.py
"""

import time
import psycopg
from dataclasses import dataclass
from typing import List, Dict, Any, Callable
import statistics

# Connection details
CONN_PARAMS = {
    "host": "localhost",
    "port": 5433,
    "dbname": "kontra_bench",
    "user": "kontra",
    "password": "kontra123",
}

TABLE = "orders"


@dataclass
class BenchmarkResult:
    name: str
    times: List[float]
    result: Any

    @property
    def mean_ms(self) -> float:
        return statistics.mean(self.times) * 1000

    @property
    def std_ms(self) -> float:
        return statistics.stdev(self.times) * 1000 if len(self.times) > 1 else 0

    @property
    def min_ms(self) -> float:
        return min(self.times) * 1000

    def __str__(self) -> str:
        return f"{self.name}: {self.mean_ms:.2f}ms ± {self.std_ms:.2f}ms (min: {self.min_ms:.2f}ms)"


def benchmark(name: str, func: Callable, iterations: int = 5) -> BenchmarkResult:
    """Run a benchmark function multiple times."""
    times = []
    result = None
    for i in range(iterations):
        start = time.perf_counter()
        result = func()
        elapsed = time.perf_counter() - start
        times.append(elapsed)
    return BenchmarkResult(name=name, times=times, result=result)


def get_connection():
    return psycopg.connect(**CONN_PARAMS)


# =============================================================================
# SQL Approaches to Test
# =============================================================================

def approach_2phase_exists_then_aggregate(conn, columns_not_null, columns_unique):
    """
    Current approach: EXISTS for not_null, then aggregate for others.
    2 round trips.
    """
    results = {}

    # Phase 1: EXISTS for not_null
    if columns_not_null:
        exists_parts = [
            f'EXISTS (SELECT 1 FROM {TABLE} WHERE "{col}" IS NULL LIMIT 1) AS "{col}_null"'
            for col in columns_not_null
        ]
        sql = f"SELECT {', '.join(exists_parts)}"
        with conn.cursor() as cur:
            cur.execute(sql)
            row = cur.fetchone()
            for i, col in enumerate(columns_not_null):
                results[f"{col}_not_null"] = {"has_violation": row[i], "count": 1 if row[i] else 0}

    # Phase 2: Aggregate for unique
    if columns_unique:
        agg_parts = [
            f'COUNT(*) - COUNT(DISTINCT "{col}") AS "{col}_dups"'
            for col in columns_unique
        ]
        sql = f"SELECT {', '.join(agg_parts)} FROM {TABLE}"
        with conn.cursor() as cur:
            cur.execute(sql)
            row = cur.fetchone()
            for i, col in enumerate(columns_unique):
                results[f"{col}_unique"] = {"count": row[i]}

    return results


def approach_single_aggregate(conn, columns_not_null, columns_unique):
    """
    Single aggregate query for all rules.
    1 round trip.
    """
    parts = []

    # not_null as SUM(CASE...)
    for col in columns_not_null:
        parts.append(f'SUM(CASE WHEN "{col}" IS NULL THEN 1 ELSE 0 END) AS "{col}_nulls"')

    # unique as COUNT - COUNT DISTINCT
    for col in columns_unique:
        parts.append(f'COUNT(*) - COUNT(DISTINCT "{col}") AS "{col}_dups"')

    if not parts:
        return {}

    sql = f"SELECT {', '.join(parts)} FROM {TABLE}"

    with conn.cursor() as cur:
        cur.execute(sql)
        row = cur.fetchone()

    results = {}
    idx = 0
    for col in columns_not_null:
        results[f"{col}_not_null"] = {"count": row[idx]}
        idx += 1
    for col in columns_unique:
        results[f"{col}_unique"] = {"count": row[idx]}
        idx += 1

    return results


def approach_adaptive(conn, columns_not_null, columns_unique):
    """
    Gemini's suggestion: Adaptive approach.
    - If only not_null: use EXISTS (fast, early termination)
    - If mixed: fold not_null into single aggregate
    """
    has_aggregates = bool(columns_unique)

    if not has_aggregates and columns_not_null:
        # Pure not_null - use EXISTS
        exists_parts = [
            f'EXISTS (SELECT 1 FROM {TABLE} WHERE "{col}" IS NULL LIMIT 1) AS "{col}_null"'
            for col in columns_not_null
        ]
        sql = f"SELECT {', '.join(exists_parts)}"
        with conn.cursor() as cur:
            cur.execute(sql)
            row = cur.fetchone()

        results = {}
        for i, col in enumerate(columns_not_null):
            results[f"{col}_not_null"] = {"has_violation": row[i], "count": 1 if row[i] else 0}
        return results

    # Mixed - single aggregate
    parts = []

    # not_null folded into aggregate
    for col in columns_not_null:
        parts.append(f'SUM(CASE WHEN "{col}" IS NULL THEN 1 ELSE 0 END) AS "{col}_nulls"')

    for col in columns_unique:
        parts.append(f'COUNT(*) - COUNT(DISTINCT "{col}") AS "{col}_dups"')

    if not parts:
        return {}

    sql = f"SELECT {', '.join(parts)} FROM {TABLE}"

    with conn.cursor() as cur:
        cur.execute(sql)
        row = cur.fetchone()

    results = {}
    idx = 0
    for col in columns_not_null:
        results[f"{col}_not_null"] = {"count": row[idx]}
        idx += 1
    for col in columns_unique:
        results[f"{col}_unique"] = {"count": row[idx]}
        idx += 1

    return results


def approach_bool_or(conn, columns_not_null, columns_unique):
    """
    Gemini's suggestion: Use bool_or for existence check in aggregate.

    bool_or(col IS NULL) returns true if ANY null exists.
    Potentially faster than SUM(CASE...) for existence-only.
    """
    parts = []

    # not_null as bool_or (existence only, no count)
    for col in columns_not_null:
        parts.append(f'bool_or("{col}" IS NULL) AS "{col}_has_null"')

    for col in columns_unique:
        parts.append(f'COUNT(*) - COUNT(DISTINCT "{col}") AS "{col}_dups"')

    if not parts:
        return {}

    sql = f"SELECT {', '.join(parts)} FROM {TABLE}"

    with conn.cursor() as cur:
        cur.execute(sql)
        row = cur.fetchone()

    results = {}
    idx = 0
    for col in columns_not_null:
        # bool_or returns True/False, not count
        results[f"{col}_not_null"] = {"has_violation": row[idx], "count": 1 if row[idx] else 0}
        idx += 1
    for col in columns_unique:
        results[f"{col}_unique"] = {"count": row[idx]}
        idx += 1

    return results


def approach_count_null(conn, columns_not_null, columns_unique):
    """
    Alternative: COUNT(*) - COUNT(col) for null count.

    COUNT(col) skips NULLs, so COUNT(*) - COUNT(col) = null count.
    """
    parts = []

    for col in columns_not_null:
        parts.append(f'COUNT(*) - COUNT("{col}") AS "{col}_nulls"')

    for col in columns_unique:
        parts.append(f'COUNT(*) - COUNT(DISTINCT "{col}") AS "{col}_dups"')

    if not parts:
        return {}

    sql = f"SELECT {', '.join(parts)} FROM {TABLE}"

    with conn.cursor() as cur:
        cur.execute(sql)
        row = cur.fetchone()

    results = {}
    idx = 0
    for col in columns_not_null:
        results[f"{col}_not_null"] = {"count": row[idx]}
        idx += 1
    for col in columns_unique:
        results[f"{col}_unique"] = {"count": row[idx]}
        idx += 1

    return results


# =============================================================================
# Benchmark Scenarios
# =============================================================================

def run_benchmarks():
    conn = get_connection()

    print("=" * 70)
    print("SQL EXECUTION APPROACH BENCHMARKS")
    print("=" * 70)
    print(f"Table: {TABLE} (1M rows, 1% nulls in email)")
    print()

    # Scenario 1: Pure not_null (single column)
    print("-" * 70)
    print("SCENARIO 1: Pure not_null (1 column)")
    print("-" * 70)

    cols_nn = ["email"]
    cols_unique = []

    results = []
    results.append(benchmark(
        "2-phase (EXISTS + agg)",
        lambda: approach_2phase_exists_then_aggregate(conn, cols_nn, cols_unique),
    ))
    results.append(benchmark(
        "Single aggregate (SUM CASE)",
        lambda: approach_single_aggregate(conn, cols_nn, cols_unique),
    ))
    results.append(benchmark(
        "Adaptive",
        lambda: approach_adaptive(conn, cols_nn, cols_unique),
    ))
    results.append(benchmark(
        "bool_or",
        lambda: approach_bool_or(conn, cols_nn, cols_unique),
    ))
    results.append(benchmark(
        "COUNT(*) - COUNT(col)",
        lambda: approach_count_null(conn, cols_nn, cols_unique),
    ))

    for r in results:
        print(f"  {r}")
    print()

    # Scenario 2: Pure not_null (multiple columns)
    print("-" * 70)
    print("SCENARIO 2: Pure not_null (3 columns)")
    print("-" * 70)

    cols_nn = ["email", "user_id", "status"]
    cols_unique = []

    results = []
    results.append(benchmark(
        "2-phase (EXISTS + agg)",
        lambda: approach_2phase_exists_then_aggregate(conn, cols_nn, cols_unique),
    ))
    results.append(benchmark(
        "Single aggregate (SUM CASE)",
        lambda: approach_single_aggregate(conn, cols_nn, cols_unique),
    ))
    results.append(benchmark(
        "Adaptive",
        lambda: approach_adaptive(conn, cols_nn, cols_unique),
    ))
    results.append(benchmark(
        "bool_or",
        lambda: approach_bool_or(conn, cols_nn, cols_unique),
    ))

    for r in results:
        print(f"  {r}")
    print()

    # Scenario 3: Mixed rules (not_null + unique)
    print("-" * 70)
    print("SCENARIO 3: Mixed rules (not_null + unique)")
    print("-" * 70)

    cols_nn = ["email"]
    cols_unique = ["user_id"]

    results = []
    results.append(benchmark(
        "2-phase (EXISTS + agg)",
        lambda: approach_2phase_exists_then_aggregate(conn, cols_nn, cols_unique),
    ))
    results.append(benchmark(
        "Single aggregate (SUM CASE)",
        lambda: approach_single_aggregate(conn, cols_nn, cols_unique),
    ))
    results.append(benchmark(
        "Adaptive (folded)",
        lambda: approach_adaptive(conn, cols_nn, cols_unique),
    ))
    results.append(benchmark(
        "bool_or (folded)",
        lambda: approach_bool_or(conn, cols_nn, cols_unique),
    ))

    for r in results:
        print(f"  {r}")
    print()

    # Scenario 4: Heavy mixed (multiple not_null + unique)
    print("-" * 70)
    print("SCENARIO 4: Heavy mixed (3 not_null + 1 unique)")
    print("-" * 70)

    cols_nn = ["email", "user_id", "status"]
    cols_unique = ["user_id"]

    results = []
    results.append(benchmark(
        "2-phase (EXISTS + agg)",
        lambda: approach_2phase_exists_then_aggregate(conn, cols_nn, cols_unique),
    ))
    results.append(benchmark(
        "Single aggregate (SUM CASE)",
        lambda: approach_single_aggregate(conn, cols_nn, cols_unique),
    ))
    results.append(benchmark(
        "Adaptive (folded)",
        lambda: approach_adaptive(conn, cols_nn, cols_unique),
    ))

    for r in results:
        print(f"  {r}")
    print()

    # Scenario 5: Only unique (no not_null)
    print("-" * 70)
    print("SCENARIO 5: Only unique (no not_null)")
    print("-" * 70)

    cols_nn = []
    cols_unique = ["user_id", "email"]

    results = []
    results.append(benchmark(
        "2-phase (EXISTS + agg)",
        lambda: approach_2phase_exists_then_aggregate(conn, cols_nn, cols_unique),
    ))
    results.append(benchmark(
        "Single aggregate",
        lambda: approach_single_aggregate(conn, cols_nn, cols_unique),
    ))

    for r in results:
        print(f"  {r}")
    print()

    # Explain plans
    print("=" * 70)
    print("EXPLAIN ANALYZE: Key Queries")
    print("=" * 70)

    explain_queries = [
        ("EXISTS (early termination)",
         f'SELECT EXISTS (SELECT 1 FROM {TABLE} WHERE "email" IS NULL LIMIT 1)'),
        ("bool_or",
         f'SELECT bool_or("email" IS NULL) FROM {TABLE}'),
        ("SUM(CASE...)",
         f'SELECT SUM(CASE WHEN "email" IS NULL THEN 1 ELSE 0 END) FROM {TABLE}'),
        ("COUNT(*) - COUNT(col)",
         f'SELECT COUNT(*) - COUNT("email") FROM {TABLE}'),
    ]

    for name, sql in explain_queries:
        print(f"\n{name}:")
        print(f"  SQL: {sql}")
        with conn.cursor() as cur:
            cur.execute(f"EXPLAIN ANALYZE {sql}")
            for row in cur.fetchall():
                if "Execution Time" in row[0] or "Planning Time" in row[0]:
                    print(f"  {row[0]}")

    conn.close()

    print()
    print("=" * 70)
    print("KEY INSIGHTS")
    print("=" * 70)
    print("""
1. EXISTS with LIMIT 1:
   - Stops at first violation (early termination)
   - Returns boolean, NOT count
   - Optimal when you only need to know IF violations exist

2. Aggregate approaches (SUM CASE, COUNT-COUNT, bool_or):
   - Full table scan required
   - Returns actual count (or boolean for bool_or)
   - All roughly equivalent performance

3. 2-phase vs Single pass:
   - 2-phase: 2 round trips, but EXISTS can terminate early
   - Single pass: 1 round trip, full scan
   - Winner depends on: network latency, null distribution, query mix

4. Gemini's bool_or suggestion:
   - bool_or returns TRUE/FALSE, like EXISTS
   - But requires full table scan (no early termination)
   - No advantage over SUM(CASE...) or EXISTS

RECOMMENDATION:
- Pure not_null: Use EXISTS (early termination)
- Mixed rules: Adaptive approach (fold not_null into aggregate)
- Need actual counts: Use aggregate (SUM CASE or COUNT-COUNT)
""")


if __name__ == "__main__":
    run_benchmarks()
