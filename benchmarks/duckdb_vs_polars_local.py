#!/usr/bin/env python3
"""
Benchmark: DuckDB SQL Pushdown vs Polars Vectorized for Local Files

Purpose: Determine if DuckDB SQL pushdown is worth the 100ms import cost
         for local Parquet files, or if Polars-only is sufficient.

Usage:
    python benchmarks/duckdb_vs_polars_local.py

Output:
    - Console table with timing results
    - benchmarks/results/duckdb_vs_polars_local.json
    - benchmarks/results/duckdb_vs_polars_local.md
"""

import json
import os
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

# Ensure we can import from src
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))


@dataclass
class BenchmarkResult:
    scenario: str
    row_count: int
    rule_type: str
    duckdb_time_ms: float
    polars_time_ms: float
    winner: str
    speedup: float  # positive = polars faster, negative = duckdb faster


def generate_test_data(row_count: int, temp_dir: str) -> str:
    """Generate test Parquet file with realistic data."""
    import polars as pl
    import random
    import string

    # Deterministic seed for reproducibility
    random.seed(42)

    # Generate data with various characteristics
    data = {
        "id": list(range(row_count)),
        "email": [
            f"user{i}@{''.join(random.choices(string.ascii_lowercase, k=5))}.com"
            if i % 10 != 0 else None  # 10% nulls
            for i in range(row_count)
        ],
        "amount": [
            random.uniform(0, 1000) if i % 20 != 0 else -5.0  # 5% negative
            for i in range(row_count)
        ],
        "status": [
            random.choice(["active", "inactive", "pending"])
            for _ in range(row_count)
        ],
        "age": [
            random.randint(18, 80) if i % 15 != 0 else None  # ~7% nulls
            for i in range(row_count)
        ],
    }

    df = pl.DataFrame(data)
    path = os.path.join(temp_dir, f"test_{row_count}.parquet")
    df.write_parquet(path)
    return path


def rules_to_specs(rules: List[Dict]) -> List:
    """Convert rule dicts to RuleSpec objects."""
    from kontra.config.models import RuleSpec
    return [
        RuleSpec(
            name=r.get("name", ""),
            id=r.get("id"),
            params=r.get("params", {}),
            severity=r.get("severity", "blocking"),
            context=r.get("context", {}),
        )
        for r in rules
    ]


def benchmark_duckdb_pushdown(parquet_path: str, rules: List[Dict], iterations: int = 5) -> float:
    """Benchmark DuckDB SQL pushdown execution."""
    import duckdb
    import polars as pl
    from kontra.connectors.handle import DatasetHandle
    from kontra.engine.executors.duckdb_sql import DuckDBSqlExecutor
    from kontra.rules.factory import RuleFactory
    from kontra.rules.execution_plan import RuleExecutionPlan

    handle = DatasetHandle.from_uri(parquet_path)
    rule_specs = rules_to_specs(rules)
    rule_objects = RuleFactory(rule_specs).build_rules()
    plan = RuleExecutionPlan(rule_objects)
    compiled = plan.compile()  # CompiledPlan with sql_rules

    executor = DuckDBSqlExecutor()
    # Compile sql_rules into executor's format
    sql_plan = executor.compile(compiled.sql_rules)

    # Warm up
    executor.execute(handle, sql_plan)

    # Benchmark
    times = []
    for _ in range(iterations):
        start = time.perf_counter()
        executor.execute(handle, sql_plan)
        times.append((time.perf_counter() - start) * 1000)

    return sum(times) / len(times)


def benchmark_polars_vectorized(parquet_path: str, rules: List[Dict], iterations: int = 5) -> float:
    """Benchmark Polars vectorized execution."""
    import polars as pl
    from kontra.rules.factory import RuleFactory
    from kontra.rules.execution_plan import RuleExecutionPlan

    # Load data once (simulating projection)
    df = pl.read_parquet(parquet_path)

    rule_specs = rules_to_specs(rules)
    rule_objects = RuleFactory(rule_specs).build_rules()
    plan = RuleExecutionPlan(rule_objects)
    compiled = plan.compile()

    # Warm up
    plan.execute_compiled(df, compiled)

    # Benchmark
    times = []
    for _ in range(iterations):
        start = time.perf_counter()
        plan.execute_compiled(df, compiled)
        times.append((time.perf_counter() - start) * 1000)

    return sum(times) / len(times)


def benchmark_cold_start_duckdb(parquet_path: str, rules: List[Dict]) -> float:
    """Benchmark cold start with DuckDB (includes import time)."""
    script = f'''
import time
t0 = time.perf_counter()

import duckdb
from kontra.connectors.handle import DatasetHandle
from kontra.engine.executors.duckdb_sql import DuckDBSqlExecutor
from kontra.rules.factory import RuleFactory
from kontra.rules.execution_plan import RuleExecutionPlan
from kontra.config.models import RuleSpec

handle = DatasetHandle.from_uri("{parquet_path}")
rules = {rules}
rule_specs = [RuleSpec(name=r["name"], params=r.get("params", {{}})) for r in rules]
rule_objects = RuleFactory(rule_specs).build_rules()
plan = RuleExecutionPlan(rule_objects)
compiled = plan.compile()
executor = DuckDBSqlExecutor()
sql_plan = executor.compile(compiled.sql_rules)
result = executor.execute(handle, sql_plan)

print(f"{{(time.perf_counter() - t0) * 1000:.2f}}")
'''
    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        cwd=Path(__file__).parent.parent,
        env={**os.environ, "PYTHONPATH": str(Path(__file__).parent.parent / "src")}
    )
    if result.returncode != 0:
        print(f"Error: {result.stderr}")
        return -1
    return float(result.stdout.strip())


def benchmark_cold_start_polars(parquet_path: str, rules: List[Dict]) -> float:
    """Benchmark cold start with Polars only (includes import time)."""
    script = f'''
import time
t0 = time.perf_counter()

import polars as pl
from kontra.rules.factory import RuleFactory
from kontra.rules.execution_plan import RuleExecutionPlan
from kontra.config.models import RuleSpec

df = pl.read_parquet("{parquet_path}")
rules = {rules}
rule_specs = [RuleSpec(name=r["name"], params=r.get("params", {{}})) for r in rules]
rule_objects = RuleFactory(rule_specs).build_rules()
plan = RuleExecutionPlan(rule_objects)
compiled = plan.compile()
results = plan.execute_compiled(df, compiled)

print(f"{{(time.perf_counter() - t0) * 1000:.2f}}")
'''
    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        cwd=Path(__file__).parent.parent,
        env={**os.environ, "PYTHONPATH": str(Path(__file__).parent.parent / "src")}
    )
    if result.returncode != 0:
        print(f"Error: {result.stderr}")
        return -1
    return float(result.stdout.strip())


def run_benchmarks() -> List[BenchmarkResult]:
    """Run all benchmark scenarios."""
    results = []

    # Test configurations
    row_counts = [1_000, 10_000, 100_000, 1_000_000]

    rule_sets = {
        "not_null": [
            {"name": "not_null", "params": {"column": "email"}},
        ],
        "unique": [
            {"name": "unique", "params": {"column": "id"}},
        ],
        "range": [
            {"name": "range", "params": {"column": "amount", "min": 0, "max": 1000}},
        ],
        "regex": [
            {"name": "regex", "params": {"column": "email", "pattern": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"}},
        ],
        "allowed_values": [
            {"name": "allowed_values", "params": {"column": "status", "values": ["active", "inactive", "pending"]}},
        ],
        "mixed_5_rules": [
            {"name": "not_null", "params": {"column": "id"}},
            {"name": "unique", "params": {"column": "id"}},
            {"name": "not_null", "params": {"column": "email"}},
            {"name": "range", "params": {"column": "amount", "min": 0, "max": 1000}},
            {"name": "allowed_values", "params": {"column": "status", "values": ["active", "inactive", "pending"]}},
        ],
    }

    with tempfile.TemporaryDirectory() as temp_dir:
        print("=" * 80)
        print("  DuckDB vs Polars Benchmark for Local Files")
        print("=" * 80)
        print()

        for row_count in row_counts:
            print(f"Generating test data: {row_count:,} rows...")
            parquet_path = generate_test_data(row_count, temp_dir)
            file_size_mb = os.path.getsize(parquet_path) / (1024 * 1024)
            print(f"  File size: {file_size_mb:.2f} MB")
            print()

            for rule_name, rules in rule_sets.items():
                print(f"  Testing: {rule_name} on {row_count:,} rows")

                # Warm execution (excludes import time)
                try:
                    duckdb_time = benchmark_duckdb_pushdown(parquet_path, rules)
                    polars_time = benchmark_polars_vectorized(parquet_path, rules)
                except Exception as e:
                    print(f"    Error: {e}")
                    continue

                winner = "polars" if polars_time < duckdb_time else "duckdb"
                if polars_time < duckdb_time:
                    speedup = duckdb_time / polars_time
                else:
                    speedup = -(polars_time / duckdb_time)

                result = BenchmarkResult(
                    scenario=f"{rule_name}_{row_count}",
                    row_count=row_count,
                    rule_type=rule_name,
                    duckdb_time_ms=duckdb_time,
                    polars_time_ms=polars_time,
                    winner=winner,
                    speedup=speedup,
                )
                results.append(result)

                print(f"    DuckDB: {duckdb_time:.2f}ms | Polars: {polars_time:.2f}ms | Winner: {winner} ({abs(speedup):.1f}x)")

            print()

        # Cold start comparison (includes import time)
        print("=" * 80)
        print("  Cold Start Comparison (includes import overhead)")
        print("=" * 80)
        print()

        # Use medium-sized file for cold start test
        parquet_path = generate_test_data(100_000, temp_dir)
        rules = rule_sets["mixed_5_rules"]

        print("  Running 3 cold start iterations each...")

        duckdb_cold_times = []
        polars_cold_times = []

        for i in range(3):
            print(f"    Iteration {i+1}...")
            duckdb_cold_times.append(benchmark_cold_start_duckdb(parquet_path, rules))
            polars_cold_times.append(benchmark_cold_start_polars(parquet_path, rules))

        avg_duckdb_cold = sum(duckdb_cold_times) / len(duckdb_cold_times)
        avg_polars_cold = sum(polars_cold_times) / len(polars_cold_times)

        print()
        print(f"  Cold Start Results (100K rows, 5 rules):")
        print(f"    DuckDB path: {avg_duckdb_cold:.0f}ms")
        print(f"    Polars path: {avg_polars_cold:.0f}ms")
        print(f"    Difference:  {avg_duckdb_cold - avg_polars_cold:.0f}ms")
        print()

    return results


def save_results(results: List[BenchmarkResult], output_dir: str = "benchmarks/results"):
    """Save results to JSON and Markdown."""
    os.makedirs(output_dir, exist_ok=True)

    # JSON output
    json_data = [
        {
            "scenario": r.scenario,
            "row_count": r.row_count,
            "rule_type": r.rule_type,
            "duckdb_time_ms": r.duckdb_time_ms,
            "polars_time_ms": r.polars_time_ms,
            "winner": r.winner,
            "speedup": r.speedup,
        }
        for r in results
    ]

    json_path = os.path.join(output_dir, "duckdb_vs_polars_local.json")
    with open(json_path, "w") as f:
        json.dump(json_data, f, indent=2)
    print(f"Saved JSON results to {json_path}")

    # Markdown output
    md_lines = [
        "# DuckDB vs Polars Benchmark Results",
        "",
        "## Warm Execution (excludes import time)",
        "",
        "| Rows | Rule Type | DuckDB (ms) | Polars (ms) | Winner | Speedup |",
        "|------|-----------|-------------|-------------|--------|---------|",
    ]

    for r in results:
        speedup_str = f"{abs(r.speedup):.1f}x"
        md_lines.append(
            f"| {r.row_count:,} | {r.rule_type} | {r.duckdb_time_ms:.2f} | {r.polars_time_ms:.2f} | {r.winner} | {speedup_str} |"
        )

    md_lines.extend([
        "",
        "## Analysis",
        "",
        "### Break-even Point",
        "",
        "DuckDB import overhead: ~100ms",
        "",
        "For DuckDB to be worth the import cost, it needs to save >100ms in execution time.",
        "",
        "### Recommendations",
        "",
        "- If DuckDB is consistently faster: Keep DuckDB for local files",
        "- If Polars is faster or similar: Use Polars-only for local files (save 100ms import)",
        "- If it depends on file size: Set a threshold (e.g., >1M rows → use DuckDB)",
        "",
    ])

    md_path = os.path.join(output_dir, "duckdb_vs_polars_local.md")
    with open(md_path, "w") as f:
        f.write("\n".join(md_lines))
    print(f"Saved Markdown results to {md_path}")


def print_summary(results: List[BenchmarkResult]):
    """Print summary analysis."""
    print()
    print("=" * 80)
    print("  SUMMARY")
    print("=" * 80)
    print()

    # Count wins by row count
    print("Wins by row count:")
    for row_count in [1_000, 10_000, 100_000, 1_000_000]:
        subset = [r for r in results if r.row_count == row_count]
        duckdb_wins = sum(1 for r in subset if r.winner == "duckdb")
        polars_wins = sum(1 for r in subset if r.winner == "polars")
        print(f"  {row_count:>10,} rows: DuckDB {duckdb_wins} | Polars {polars_wins}")

    print()

    # Count wins by rule type
    print("Wins by rule type:")
    rule_types = set(r.rule_type for r in results)
    for rule_type in sorted(rule_types):
        subset = [r for r in results if r.rule_type == rule_type]
        duckdb_wins = sum(1 for r in subset if r.winner == "duckdb")
        polars_wins = sum(1 for r in subset if r.winner == "polars")
        print(f"  {rule_type:>20}: DuckDB {duckdb_wins} | Polars {polars_wins}")

    print()

    # Overall recommendation
    total_duckdb = sum(1 for r in results if r.winner == "duckdb")
    total_polars = sum(1 for r in results if r.winner == "polars")

    print(f"Overall: DuckDB {total_duckdb} wins | Polars {total_polars} wins")
    print()

    if total_polars > total_duckdb:
        print("RECOMMENDATION: Consider Polars-only for local files")
        print("  - Saves ~100ms import overhead")
        print("  - Polars is faster or comparable for most scenarios")
    elif total_duckdb > total_polars * 2:
        print("RECOMMENDATION: Keep DuckDB for local files")
        print("  - DuckDB is significantly faster")
        print("  - 100ms import cost is worth it")
    else:
        print("RECOMMENDATION: Needs further analysis")
        print("  - Results are mixed")
        print("  - Consider threshold-based approach")


if __name__ == "__main__":
    results = run_benchmarks()
    save_results(results)
    print_summary(results)
