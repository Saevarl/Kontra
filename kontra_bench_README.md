# Kontra + DuckDB Benchmarking Notes

## Recommended Environment Variables (override as needed)

```bash
# DuckDB tuning
export KONTRA_DUCKDB_PROFILE=balanced   # speed|balanced|conservative|auto
export KONTRA_DUCKDB_THREADS=8          # or "auto"
export KONTRA_DUCKDB_MEM=70%            # or absolute like "24GB"
export KONTRA_DUCKDB_TMP=/mnt/ssd/tmp   # fast local SSD recommended
export KONTRA_OBJECT_CACHE=true         # reuse objects in-session

# S3/MinIO tuning for DuckDB
export KONTRA_S3_ENDPOINT=http://minio.your.lan:9000
export KONTRA_S3_REGION=us-east-1
export KONTRA_S3_ACCESS_KEY_ID=...
export KONTRA_S3_SECRET_ACCESS_KEY=...
export KONTRA_S3_URL_STYLE=path         # path or virtual_hosted
export KONTRA_S3_USE_SSL=false          # true if TLS enabled
export KONTRA_S3_VERIFY=false           # true if using valid TLS
export KONTRA_S3_MAX_CONNECTIONS=32     # try 16/32/64 based on NIC
```

> Note: Map these KONTRA_* vars inside Kontra to DuckDB settings:
> - `threads` → `PRAGMA threads`
> - `memory`  → `SET memory_limit='...'`
> - `tmp`     → `SET temp_directory='...'`
> - `object_cache` → `SET enable_object_cache=true`
> - S3 params → `SET s3_endpoint=...`, `SET s3_region=...`, `SET s3_url_style=...`, `SET s3_use_ssl=...`, `SET s3_verify=...`, `SET s3_access_key_id=...`, `SET s3_secret_access_key=...`, `SET s3_max_connections=...`

## Running the benchmark

```bash
chmod +x kontra_bench.sh
./kontra_bench.sh s3://bucket/path/data_10m.parquet path/to/contract.yaml 3
```

This writes `bench_results.csv`. Import it into your notebook/BI tool or run quick stats:

```bash
column -s, -t < bench_results.csv | less -S
```

## Suggested Matrix to Try

- THREADS: 4, 8, 12, 16
- MEM: 60%, 70%, 85%
- S3 MAX CONNECTIONS: 16, 32, 64 (set before running script, re-run per value)
- Pushdown=on, Projection=on
- Repeats: 3–5

For 100M rows over MinIO:
- Prefer Parquet with multiple row groups (128–256MB), avoid too many tiny files.
- Ensure the temp directory is on a fast SSD with enough free space to avoid spills.
- If CSV, enable staging to Parquet and re-run the same matrix.
