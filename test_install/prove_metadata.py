#!/usr/bin/env python3
"""
Prove that Kontra uses Parquet metadata while Pandera scans data.
"""

import time
import os
import polars as pl
import pyarrow.parquet as pq

# Create a large parquet file (10M rows)
PARQUET_FILE = "/tmp/large_test.parquet"
N_ROWS = 10_000_000

def create_test_file():
    """Create a 10M row parquet file."""
    if os.path.exists(PARQUET_FILE):
        print(f"Using existing {PARQUET_FILE}")
        return

    print(f"Creating {N_ROWS:,} row parquet file...")
    start = time.time()
    df = pl.DataFrame({
        "id": range(N_ROWS),
        "value": [i * 1.5 for i in range(N_ROWS)],
        "status": ["active"] * N_ROWS,
    })
    df.write_parquet(PARQUET_FILE, row_group_size=100_000)
    print(f"Created in {time.time() - start:.2f}s")
    print(f"File size: {os.path.getsize(PARQUET_FILE) / 1024 / 1024:.1f} MB")


def show_parquet_metadata():
    """Show what metadata is available without reading data."""
    print("\n" + "="*60)
    print("PARQUET METADATA (no data read)")
    print("="*60)

    pf = pq.ParquetFile(PARQUET_FILE)
    md = pf.metadata

    print(f"Total rows: {md.num_rows:,}")
    print(f"Row groups: {md.num_row_groups}")
    print(f"Columns: {md.num_columns}")

    # Show stats from first row group
    rg = md.row_group(0)
    print(f"\nRow group 0 stats:")
    for i in range(rg.num_columns):
        col = rg.column(i)
        stats = col.statistics
        if stats:
            print(f"  {col.path_in_schema}: min={stats.min}, max={stats.max}, nulls={stats.null_count}")


def benchmark_kontra():
    """Benchmark Kontra's min_rows check."""
    import kontra
    from kontra import rules

    print("\n" + "="*60)
    print("KONTRA: min_rows check")
    print("="*60)

    # Warm up
    kontra.validate(PARQUET_FILE, rules=[rules.min_rows(100)])

    # Benchmark
    times = []
    for i in range(5):
        start = time.time()
        result = kontra.validate(PARQUET_FILE, rules=[rules.min_rows(100)])
        elapsed = time.time() - start
        times.append(elapsed)
        print(f"  Run {i+1}: {elapsed*1000:.1f}ms - passed={result.passed}")

    avg = sum(times) / len(times)
    print(f"\n  Average: {avg*1000:.1f}ms")
    return avg


def benchmark_pandera():
    """Benchmark Pandera's equivalent check."""
    import pandera as pa
    import pandera.polars as pap

    print("\n" + "="*60)
    print("PANDERA: min_rows equivalent (dataframe check)")
    print("="*60)

    # Pandera doesn't have built-in min_rows, so we just validate column exists
    # The point is: Pandera MUST load the data first
    schema = pap.DataFrameSchema(
        columns={"id": pap.Column(int)},
    )

    # Warm up
    df = pl.read_parquet(PARQUET_FILE)
    schema.validate(df)
    del df

    # Benchmark
    times = []
    for i in range(5):
        start = time.time()
        df = pl.read_parquet(PARQUET_FILE)
        load_time = time.time() - start

        start2 = time.time()
        schema.validate(df)
        validate_time = time.time() - start2

        total = load_time + validate_time
        times.append(total)
        print(f"  Run {i+1}: {total*1000:.1f}ms (load={load_time*1000:.1f}ms, validate={validate_time*1000:.1f}ms)")
        del df

    avg = sum(times) / len(times)
    print(f"\n  Average: {avg*1000:.1f}ms")
    return avg


def benchmark_pandera_lazy():
    """Benchmark Pandera with LazyFrame (still needs to scan for len)."""
    import pandera as pa
    import pandera.polars as pap

    print("\n" + "="*60)
    print("PANDERA: with scan_parquet (lazy)")
    print("="*60)

    schema = pap.DataFrameSchema(
        columns={"id": pap.Column(int)},
    )

    # Benchmark
    times = []
    for i in range(5):
        start = time.time()
        # scan_parquet is lazy, but Pandera will collect it
        lf = pl.scan_parquet(PARQUET_FILE)
        df = lf.collect()  # Pandera needs eager DataFrame
        load_time = time.time() - start

        start2 = time.time()
        schema.validate(df)
        validate_time = time.time() - start2

        total = load_time + validate_time
        times.append(total)
        print(f"  Run {i+1}: {total*1000:.1f}ms (load={load_time*1000:.1f}ms, validate={validate_time*1000:.1f}ms)")
        del df

    avg = sum(times) / len(times)
    print(f"\n  Average: {avg*1000:.1f}ms")
    return avg


def benchmark_raw_metadata():
    """Show how fast pure metadata read is."""
    print("\n" + "="*60)
    print("RAW: PyArrow metadata read only")
    print("="*60)

    times = []
    for i in range(5):
        start = time.time()
        pf = pq.ParquetFile(PARQUET_FILE)
        num_rows = pf.metadata.num_rows
        elapsed = time.time() - start
        times.append(elapsed)
        print(f"  Run {i+1}: {elapsed*1000:.2f}ms - rows={num_rows:,}")

    avg = sum(times) / len(times)
    print(f"\n  Average: {avg*1000:.2f}ms")
    return avg


def prove_kontra_uses_metadata():
    """Prove Kontra uses metadata by showing the preplan decisions."""
    from kontra.preplan.planner import preplan_single_parquet

    print("\n" + "="*60)
    print("PROOF: Kontra preplan decisions from metadata")
    print("="*60)

    # min_rows can be answered from metadata alone
    predicates = [
        ("rule_1", "id", ">=", 0),  # All ids are >= 0
        ("rule_2", "id", "not_null", None),  # No nulls in id
    ]

    preplan = preplan_single_parquet(
        PARQUET_FILE,
        required_columns=["id"],
        predicates=predicates,
    )

    print(f"\nPreplan result (metadata only, no data scan):")
    print(f"  Total row groups: {preplan.stats['rg_total']}")
    print(f"  Row groups to scan: {preplan.stats['rg_kept']}")
    print(f"  Total rows (from metadata): {preplan.stats['total_rows']:,}")

    print(f"\n  Rule decisions:")
    for rule_id, decision in preplan.rule_decisions.items():
        print(f"    {rule_id}: {decision}")

    # Now demonstrate with not_null check
    print("\n" + "-"*40)
    print("Testing not_null rule (uses null_count from metadata):")

    predicates_null = [
        ("not_null_id", "id", "not_null", None),
    ]
    preplan2 = preplan_single_parquet(
        PARQUET_FILE,
        required_columns=["id"],
        predicates=predicates_null,
    )
    print(f"  not_null_id decision: {preplan2.rule_decisions.get('not_null_id')}")
    print(f"  (This came from metadata null_count=0, NOT from scanning data)")


if __name__ == "__main__":
    create_test_file()
    show_parquet_metadata()

    prove_kontra_uses_metadata()

    t_raw = benchmark_raw_metadata()
    t_kontra = benchmark_kontra()
    t_pandera = benchmark_pandera()

    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)
    print(f"Raw metadata read:  {t_raw*1000:>8.1f}ms")
    print(f"Kontra min_rows:    {t_kontra*1000:>8.1f}ms")
    print(f"Pandera equivalent: {t_pandera*1000:>8.1f}ms")
    print(f"\nKontra is {t_pandera/t_kontra:.0f}x faster")
    print(f"Kontra overhead vs raw: {(t_kontra - t_raw)*1000:.1f}ms")
