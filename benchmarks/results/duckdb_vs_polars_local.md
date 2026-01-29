# DuckDB vs Polars Benchmark Results

## Warm Execution (excludes import time)

| Rows | Rule Type | DuckDB (ms) | Polars (ms) | Winner | Speedup |
|------|-----------|-------------|-------------|--------|---------|
| 1,000 | not_null | 6.69 | 0.02 | polars | 326.7x |
| 1,000 | unique | 6.34 | 0.13 | polars | 50.0x |
| 1,000 | range | 7.21 | 0.09 | polars | 80.8x |
| 1,000 | regex | 6.48 | 0.12 | polars | 52.0x |
| 1,000 | allowed_values | 6.05 | 0.10 | polars | 58.4x |
| 1,000 | mixed_5_rules | 7.27 | 0.23 | polars | 31.2x |
| 10,000 | not_null | 6.39 | 0.02 | polars | 361.2x |
| 10,000 | unique | 6.55 | 0.33 | polars | 20.0x |
| 10,000 | range | 6.00 | 0.09 | polars | 63.8x |
| 10,000 | regex | 7.21 | 0.31 | polars | 22.9x |
| 10,000 | allowed_values | 5.96 | 0.20 | polars | 29.4x |
| 10,000 | mixed_5_rules | 7.88 | 0.58 | polars | 13.6x |
| 100,000 | not_null | 6.97 | 0.02 | polars | 409.0x |
| 100,000 | unique | 11.06 | 2.31 | polars | 4.8x |
| 100,000 | range | 7.20 | 0.22 | polars | 32.7x |
| 100,000 | regex | 16.67 | 2.87 | polars | 5.8x |
| 100,000 | allowed_values | 7.58 | 1.32 | polars | 5.7x |
| 100,000 | mixed_5_rules | 13.21 | 3.86 | polars | 3.4x |
| 1,000,000 | not_null | 7.45 | 0.02 | polars | 434.5x |
| 1,000,000 | unique | 24.13 | 41.44 | duckdb | 1.7x |
| 1,000,000 | range | 11.76 | 0.66 | polars | 17.8x |
| 1,000,000 | regex | 43.05 | 26.67 | polars | 1.6x |
| 1,000,000 | allowed_values | 12.19 | 10.61 | polars | 1.1x |
| 1,000,000 | mixed_5_rules | 30.89 | 51.59 | duckdb | 1.7x |

## Analysis

### Break-even Point

DuckDB import overhead: ~100ms

For DuckDB to be worth the import cost, it needs to save >100ms in execution time.

### Recommendations

- If DuckDB is consistently faster: Keep DuckDB for local files
- If Polars is faster or similar: Use Polars-only for local files (save 100ms import)
- If it depends on file size: Set a threshold (e.g., >1M rows → use DuckDB)
