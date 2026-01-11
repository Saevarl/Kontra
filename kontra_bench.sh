#!/usr/bin/env bash
# Simple Kontra + DuckDB benchmarking harness
# Usage: ./kontra_bench.sh s3://bucket/path/data.parquet path/to/contract.yaml [repeats]
set -euo pipefail

if [[ $# -lt 2 ]]; then
  echo "Usage: $0 <dataset_uri> <contract_path> [repeats]" >&2
  exit 1
fi

DATASET="$1"
CONTRACT="$2"
REPEATS="${3:-3}"

# Output file
OUT="bench_results.csv"
if [[ ! -f "$OUT" ]]; then
  echo "ts,dataset,threads,memory,tmp,object_cache,s3_max_conns,projection,pushdown,run_idx,duration_s,exit_code" > "$OUT"
fi

# Sensible defaults (override in environment)
: "${KONTRA_DUCKDB_TMP:=/tmp/kontra}"
: "${KONTRA_OBJECT_CACHE:=true}"
: "${KONTRA_S3_MAX_CONNECTIONS:=32}"

mkdir -p "$KONTRA_DUCKDB_TMP"

THREADS_SET=(4 8 12 16)
MEM_SET=("60%" "70%" "85%")
PROJECTION_SET=("on")
PUSHDOWN_SET=("on")

timestamp() { date -u +"%Y-%m-%dT%H:%M:%SZ"; }

for threads in "${THREADS_SET[@]}"; do
  for mem in "${MEM_SET[@]}"; do
    for proj in "${PROJECTION_SET[@]}"; do
      for push in "${PUSHDOWN_SET[@]}"; do
        for ((i=1;i<=REPEATS;i++)); do
          export KONTRA_DUCKDB_THREADS="$threads"
          export KONTRA_DUCKDB_MEM="$mem"
          export KONTRA_DUCKDB_TMP
          export KONTRA_OBJECT_CACHE
          export KONTRA_S3_MAX_CONNECTIONS

          ts="$(timestamp)"
          start_ns=$(date +%s%N)

          # Run Kontra validate
          set +e
          kontra validate "$DATASET" --contract "$CONTRACT" \
            --pushdown="$push" --projection="$proj" --stats >/dev/null 2>&1
          ec=$?
          set -e

          end_ns=$(date +%s%N)
          dur_s=$(python3 - <<PY
start=${start_ns}
end=${end_ns}
print(round((end-start)/1e9, 3))
PY
)

          echo "${ts},${DATASET},${threads},${mem},${KONTRA_DUCKDB_TMP},${KONTRA_OBJECT_CACHE},${KONTRA_S3_MAX_CONNECTIONS},${proj},${push},${i},${dur_s},${ec}" >> "$OUT"
          echo "Ran threads=${threads} mem=${mem} proj=${proj} push=${push} run=${i} -> ${dur_s}s (exit=${ec})"
        done
      done
    done
  done
done

echo ""
echo "Done. Results in ${OUT}"
