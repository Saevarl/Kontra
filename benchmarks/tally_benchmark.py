#!/usr/bin/env python3
"""
Benchmark: EXISTS (tally=False) vs COUNT (tally=True) Query Performance

This benchmark compares the performance of early termination (EXISTS) queries
versus full count (COUNT) queries across different scenarios.

Usage:
    python benchmarks/tally_benchmark.py
    python benchmarks/tally_benchmark.py --sizes 1000,10000,100000
    python benchmarks/tally_benchmark.py --rules 1,5,10
    python benchmarks/tally_benchmark.py --output results.json
"""

import argparse
import json
import os
import sys
import tempfile
import time
from dataclasses import dataclass, asdict
from typing import List, Optional, Dict, Any

# Add src to path for development
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import polars as pl


@dataclass
class BenchmarkResult:
    """Result of a single benchmark run."""
    scenario: str
    rows: int
    num_rules: int
    tally: bool
    violation_rate: float
    duration_ms: float
    rules_per_second: float
    rows_per_second: float


@dataclass
class BenchmarkSuite:
    """Collection of benchmark results."""
    results: List[BenchmarkResult]
    metadata: Dict[str, Any]

    def to_dict(self):
        return {
            "metadata": self.metadata,
            "results": [asdict(r) for r in self.results],
        }

    def summary(self) -> str:
        """Generate human-readable summary."""
        lines = []
        lines.append("=" * 80)
        lines.append("TALLY MODE BENCHMARK RESULTS")
        lines.append("=" * 80)
        lines.append("")

        # Group by scenario
        scenarios = {}
        for r in self.results:
            key = (r.scenario, r.rows, r.num_rules, r.violation_rate)
            if key not in scenarios:
                scenarios[key] = {"exists": None, "count": None}
            scenarios[key]["exists" if not r.tally else "count"] = r

        # Print comparison table
        lines.append(f"{'Scenario':<20} {'Rows':<10} {'Rules':<6} {'Viol%':<6} {'EXISTS(ms)':<12} {'COUNT(ms)':<12} {'Speedup':<10}")
        lines.append("-" * 80)

        for (scenario, rows, num_rules, viol_rate), times in sorted(scenarios.items()):
            exists_ms = times["exists"].duration_ms if times["exists"] else float("nan")
            count_ms = times["count"].duration_ms if times["count"] else float("nan")
            speedup = count_ms / exists_ms if exists_ms > 0 else float("nan")

            lines.append(
                f"{scenario:<20} {rows:<10} {num_rules:<6} {viol_rate*100:<5.0f}% "
                f"{exists_ms:<12.2f} {count_ms:<12.2f} {speedup:<10.2f}x"
            )

        lines.append("")
        lines.append("=" * 80)
        return "\n".join(lines)


def generate_test_data(
    num_rows: int,
    num_columns: int = 10,
    violation_rate: float = 0.01,
    seed: int = 42,
) -> pl.DataFrame:
    """Generate test data with controlled violation rate."""
    import random
    random.seed(seed)

    data = {}

    # Generate columns with violations
    for i in range(num_columns):
        col_name = f"col_{i}"
        values = []
        for j in range(num_rows):
            if random.random() < violation_rate:
                values.append(None)  # Violation (null)
            else:
                values.append(f"value_{j % 1000}")
        data[col_name] = values

    # Add numeric column for range tests
    data["score"] = [
        -1 if random.random() < violation_rate else random.randint(0, 100)
        for _ in range(num_rows)
    ]

    # Add status column for allowed_values tests
    statuses = ["active", "inactive", "pending"]
    data["status"] = [
        "INVALID" if random.random() < violation_rate else random.choice(statuses)
        for _ in range(num_rows)
    ]

    return pl.DataFrame(data)


def run_benchmark(
    data_path: str,
    rules: List[dict],
    tally: bool,
    warmup: bool = True,
) -> BenchmarkResult:
    """Run a single benchmark."""
    import kontra

    # Warmup run (not timed)
    if warmup:
        kontra.validate(data_path, rules=rules[:1], tally=tally)

    # Timed run
    start = time.perf_counter()
    result = kontra.validate(data_path, rules=rules, tally=tally)
    duration = time.perf_counter() - start

    duration_ms = duration * 1000
    rows = result.total_rows
    num_rules = len(rules)

    return BenchmarkResult(
        scenario="",  # Will be filled in by caller
        rows=rows,
        num_rules=num_rules,
        tally=tally,
        violation_rate=0.0,  # Will be filled in by caller
        duration_ms=duration_ms,
        rules_per_second=num_rules / duration if duration > 0 else 0,
        rows_per_second=rows / duration if duration > 0 else 0,
    )


def benchmark_not_null(
    data_path: str,
    num_rules: int,
    violation_rate: float,
    rows: int,
) -> List[BenchmarkResult]:
    """Benchmark not_null rules with varying tally settings."""
    from kontra import rules as r

    # Create rules for different columns
    rules = [r.not_null(f"col_{i}") for i in range(num_rules)]

    results = []

    # EXISTS (tally=False)
    result = run_benchmark(data_path, rules, tally=False)
    result.scenario = "not_null"
    result.violation_rate = violation_rate
    results.append(result)

    # COUNT (tally=True)
    result = run_benchmark(data_path, rules, tally=True)
    result.scenario = "not_null"
    result.violation_rate = violation_rate
    results.append(result)

    return results


def benchmark_unique(
    data_path: str,
    num_rules: int,
    violation_rate: float,
    rows: int,
) -> List[BenchmarkResult]:
    """Benchmark unique rules (complex, requires GROUP BY)."""
    from kontra import rules as r

    # Create unique rules for different columns
    rules = [r.unique(f"col_{i}") for i in range(num_rules)]

    results = []

    # EXISTS (tally=False)
    result = run_benchmark(data_path, rules, tally=False)
    result.scenario = "unique"
    result.violation_rate = violation_rate
    results.append(result)

    # COUNT (tally=True)
    result = run_benchmark(data_path, rules, tally=True)
    result.scenario = "unique"
    result.violation_rate = violation_rate
    results.append(result)

    return results


def benchmark_allowed_values(
    data_path: str,
    num_rules: int,
    violation_rate: float,
    rows: int,
) -> List[BenchmarkResult]:
    """Benchmark allowed_values rules."""
    from kontra import rules as r

    # Use status column for all rules (same column, different allowed sets)
    allowed = ["active", "inactive", "pending"]
    rules = [
        r.allowed_values("status", allowed, id=f"allowed_{i}")
        for i in range(num_rules)
    ]

    results = []

    # EXISTS (tally=False)
    result = run_benchmark(data_path, rules, tally=False)
    result.scenario = "allowed_values"
    result.violation_rate = violation_rate
    results.append(result)

    # COUNT (tally=True)
    result = run_benchmark(data_path, rules, tally=True)
    result.scenario = "allowed_values"
    result.violation_rate = violation_rate
    results.append(result)

    return results


def benchmark_range(
    data_path: str,
    num_rules: int,
    violation_rate: float,
    rows: int,
) -> List[BenchmarkResult]:
    """Benchmark range rules."""
    from kontra import rules as r

    # Use score column for all rules
    rules = [
        r.range("score", min=0, max=100, id=f"range_{i}")
        for i in range(num_rules)
    ]

    results = []

    # EXISTS (tally=False)
    result = run_benchmark(data_path, rules, tally=False)
    result.scenario = "range"
    result.violation_rate = violation_rate
    results.append(result)

    # COUNT (tally=True)
    result = run_benchmark(data_path, rules, tally=True)
    result.scenario = "range"
    result.violation_rate = violation_rate
    results.append(result)

    return results


def benchmark_mixed(
    data_path: str,
    num_rules: int,
    violation_rate: float,
    rows: int,
) -> List[BenchmarkResult]:
    """Benchmark mixed rule types (realistic scenario)."""
    from kontra import rules as r

    # Mix of rule types
    rules = []
    for i in range(num_rules):
        rule_type = i % 4
        if rule_type == 0:
            rules.append(r.not_null(f"col_{i % 10}"))
        elif rule_type == 1:
            rules.append(r.unique(f"col_{i % 10}", id=f"unique_{i}"))
        elif rule_type == 2:
            rules.append(r.allowed_values("status", ["active", "inactive", "pending"], id=f"allowed_{i}"))
        else:
            rules.append(r.range("score", min=0, max=100, id=f"range_{i}"))

    results = []

    # EXISTS (tally=False)
    result = run_benchmark(data_path, rules, tally=False)
    result.scenario = "mixed"
    result.violation_rate = violation_rate
    results.append(result)

    # COUNT (tally=True)
    result = run_benchmark(data_path, rules, tally=True)
    result.scenario = "mixed"
    result.violation_rate = violation_rate
    results.append(result)

    return results


def run_full_benchmark(
    sizes: List[int] = None,
    rule_counts: List[int] = None,
    violation_rates: List[float] = None,
    scenarios: List[str] = None,
) -> BenchmarkSuite:
    """Run the full benchmark suite."""
    sizes = sizes or [1_000, 10_000, 100_000, 1_000_000]
    rule_counts = rule_counts or [1, 5, 10, 20]
    violation_rates = violation_rates or [0.0, 0.01, 0.10]
    scenarios = scenarios or ["not_null", "unique", "allowed_values", "range", "mixed"]

    scenario_funcs = {
        "not_null": benchmark_not_null,
        "unique": benchmark_unique,
        "allowed_values": benchmark_allowed_values,
        "range": benchmark_range,
        "mixed": benchmark_mixed,
    }

    all_results = []
    total_runs = len(sizes) * len(rule_counts) * len(violation_rates) * len(scenarios) * 2
    current_run = 0

    with tempfile.TemporaryDirectory() as tmpdir:
        for num_rows in sizes:
            for violation_rate in violation_rates:
                # Generate data once per size/violation combo
                print(f"\nGenerating {num_rows:,} rows with {violation_rate*100:.0f}% violations...")
                df = generate_test_data(num_rows, violation_rate=violation_rate)
                data_path = os.path.join(tmpdir, f"data_{num_rows}_{int(violation_rate*100)}.parquet")
                df.write_parquet(data_path)

                for num_rules in rule_counts:
                    for scenario_name in scenarios:
                        if scenario_name not in scenario_funcs:
                            continue

                        current_run += 2  # EXISTS and COUNT
                        print(f"[{current_run}/{total_runs}] {scenario_name} - {num_rows:,} rows, {num_rules} rules, {violation_rate*100:.0f}% violations...")

                        try:
                            results = scenario_funcs[scenario_name](
                                data_path, num_rules, violation_rate, num_rows
                            )
                            all_results.extend(results)
                        except Exception as e:
                            print(f"  ERROR: {e}")

    metadata = {
        "sizes": sizes,
        "rule_counts": rule_counts,
        "violation_rates": violation_rates,
        "scenarios": scenarios,
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
    }

    return BenchmarkSuite(results=all_results, metadata=metadata)


def main():
    parser = argparse.ArgumentParser(description="Benchmark tally mode performance")
    parser.add_argument(
        "--sizes",
        type=str,
        default="1000,10000,100000",
        help="Comma-separated row counts (default: 1000,10000,100000)",
    )
    parser.add_argument(
        "--rules",
        type=str,
        default="1,5,10",
        help="Comma-separated rule counts (default: 1,5,10)",
    )
    parser.add_argument(
        "--violations",
        type=str,
        default="0.0,0.01,0.10",
        help="Comma-separated violation rates (default: 0.0,0.01,0.10)",
    )
    parser.add_argument(
        "--scenarios",
        type=str,
        default="not_null,unique,allowed_values,range,mixed",
        help="Comma-separated scenarios to run",
    )
    parser.add_argument(
        "--output",
        type=str,
        help="Output JSON file for results",
    )
    parser.add_argument(
        "--quick",
        action="store_true",
        help="Quick run with minimal parameters",
    )

    args = parser.parse_args()

    if args.quick:
        sizes = [1_000, 10_000]
        rule_counts = [1, 5]
        violation_rates = [0.01]
        scenarios = ["not_null", "mixed"]
    else:
        sizes = [int(x) for x in args.sizes.split(",")]
        rule_counts = [int(x) for x in args.rules.split(",")]
        violation_rates = [float(x) for x in args.violations.split(",")]
        scenarios = args.scenarios.split(",")

    print("=" * 60)
    print("TALLY MODE BENCHMARK")
    print("=" * 60)
    print(f"Row counts: {sizes}")
    print(f"Rule counts: {rule_counts}")
    print(f"Violation rates: {violation_rates}")
    print(f"Scenarios: {scenarios}")
    print("=" * 60)

    suite = run_full_benchmark(
        sizes=sizes,
        rule_counts=rule_counts,
        violation_rates=violation_rates,
        scenarios=scenarios,
    )

    print("\n")
    print(suite.summary())

    if args.output:
        with open(args.output, "w") as f:
            json.dump(suite.to_dict(), f, indent=2)
        print(f"\nResults saved to {args.output}")


if __name__ == "__main__":
    main()
