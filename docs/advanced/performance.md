# Performance

How Kontra stays fast on large datasets.

## Execution Model

Kontra has two fast paths:

| Path | What happens | When it's used |
|------|--------------|----------------|
| **Preplan** | Proves rules from metadata (no scan) | When metadata is available and sufficient |
| **Pushdown** | Runs batched SQL in the engine | When rules can't be proven from metadata |

If pushdown is unavailable or disabled, Kontra falls back to local execution (Polars).

---

## Benchmarks

These benchmarks compare execution strategies across common backends. The trade-offs are consistent.

Unlike validators that run one query per rule or require loading data into Python, Kontra batches rules and runs them where the data lives.

### Terminology

| Term | Meaning |
|------|---------|
| **Preplan** | Resolve a rule from metadata (no data scan) |
| **Pushdown** | Run a rule as SQL in the engine (DuckDB/Postgres/SQL Server) |
| **Tally** | Controls counting behavior. `tally=False` uses fail-fast checks (EXISTS). `tally=True` forces exact counts (aggregates) and disables preplan for that rule. |

In the tables below:
- **exists** = pushdown with `tally=False`
- **agg** = pushdown with `tally=True`

---

### Test Contracts

```python
from kontra import rules

# Contract A (fail-fast): tally=False everywhere (default)
contract_failfast = [
    rules.not_null("user_id"),
    rules.not_null("email"),
    rules.range("age", min=0, max=150),
    rules.unique("email"),
    rules.allowed_values("status", ["active", "inactive", "pending"]),
    rules.regex("email", r".*@.*"),
    rules.min_rows(1),
]

# Contract B (exact): tally=True everywhere
contract_exact = [
    rules.not_null("user_id", tally=True),
    rules.not_null("email", tally=True),
    rules.range("age", min=0, max=150, tally=True),
    rules.unique("email", tally=True),
    rules.allowed_values("status", [...], tally=True),
    rules.regex("email", r".*@.*", tally=True),
    rules.min_rows(1),  # dataset rules are always exact
]

# Contract C (mixed): exact only where counts matter
contract_mixed = [
    rules.not_null("user_id"),              # fail-fast
    rules.not_null("email", tally=True),    # exact: want null count
    rules.range("age", min=0, max=150),     # fail-fast
    rules.unique("email", tally=True),      # exact: want duplicate count
    rules.allowed_values("status", [...]),  # fail-fast
    rules.regex("email", r".*@.*"),         # fail-fast
    rules.min_rows(1),
]
```

**Mixed is the common case:** keep most rules fail-fast, enable `tally=True` only for the metrics you actually report.

**Rule capabilities (when preplan=on):**
- **Preplan-capable:** `not_null`, `range` (best on Parquet metadata; more limited on Postgres stats)
- **Pushdown-only:** `unique`, `allowed_values`, `regex`, `min_rows`

**Important:** `tally=True` disables preplan for that rule—exact counts require scanning.

---

### PostgreSQL (local) — 5M rows

| Contract | Preplan | Pushdown | Time | What happens |
|----------|---------|----------|------|--------------|
| fail-fast | off | on | **1.0s** | 7 exists |
| fail-fast | on | on | 1.2s | 1 preplan + 6 exists |
| mixed | off | on | 3.2s | 5 exists + 2 agg |
| mixed | on | on | 3.3s | 1 preplan + 4 exists + 2 agg |
| exact | off | on | 5.7s | 7 agg |

---

### S3/MinIO — 5M rows

| Contract | Preplan | Pushdown | Time | What happens |
|----------|---------|----------|------|--------------|
| fail-fast | off | on | 1.6s | 7 exists |
| fail-fast | on | on | **1.4s** | 3 preplan + 4 exists |
| mixed | on | on | 1.4s | 2 preplan + 3 exists + 2 agg |
| exact | off | on | 2.0s | 7 agg |

**Note:** Preplan helps—Parquet metadata proves not_null/range rules without reading row data.

---

### Azure ADLS Gen2 — 5M rows

| Contract | Preplan | Pushdown | Time | What happens |
|----------|---------|----------|------|--------------|
| fail-fast | on | on | **2.5s** | 3 preplan + 4 exists |
| fail-fast | off | on | 3.5s | 7 exists |
| mixed | off | on | 3.2s | 5 exists + 2 agg |
| exact | off | on | 5.1s | 7 agg |
| fail-fast | off | off | 8.5s | full transfer → Polars |

**Note:** Pushdown is 2-3× faster than Polars (avoids 290MB data transfer).

---

## SQL Execution: EXISTS vs Aggregates

Kontra uses two SQL strategies based on tally mode:

### tally=False (fail-fast)

Generates EXISTS queries that stop at first violation:

```sql
SELECT
  EXISTS(SELECT 1 FROM data WHERE "user_id" IS NULL) AS "COL:user_id:not_null",
  EXISTS(SELECT 1 FROM data WHERE "email" IS NULL) AS "COL:email:not_null",
  EXISTS(SELECT 1 FROM data WHERE "status" NOT IN ('active','inactive','pending')) AS "COL:status:allowed_values"
```

**One query, multiple EXISTS checks.** Each EXISTS returns TRUE/FALSE immediately when it finds a match. No counting.

### tally=True (exact counts)

Generates batched aggregate query:

```sql
SELECT
  SUM(CASE WHEN "user_id" IS NULL THEN 1 ELSE 0 END) AS "COL:user_id:not_null",
  SUM(CASE WHEN "email" IS NULL THEN 1 ELSE 0 END) AS "COL:email:not_null",
  COUNT(*) - COUNT(DISTINCT "email") AS "COL:email:unique",
  SUM(CASE WHEN "status" NOT IN ('active','inactive','pending') THEN 1 ELSE 0 END) AS "COL:status:allowed_values",
  COUNT(*) AS "__row_count__"
FROM data;
```

**One query, all rules batched.** Full table scan, exact violation counts.

### Why batching matters

Without batching, N rules typically mean N round-trips:

```
Unbatched:  time ≈ N × (round_trip + per_rule_work)
Batched:    time ≈ 1 × (round_trip + combined_work)
```

As round-trip cost increases (remote databases, object storage, cross-region), batching becomes the difference between "fast" and "painful."

---

## Preplan: Metadata Resolution

Parquet files store statistics in the footer:
- `null_count` per column per row-group
- `min`/`max` values per column per row-group
- `num_rows` total

Kontra reads these stats and resolves rules:

```
not_null(user_id)  →  null_count=0 for all row-groups  →  PASS
range(age, 0, 120) →  min=18, max=95 across all row-groups  →  PASS
```

Preplan only reads footer statistics, so it's often fast even on very large files.

**When preplan helps:**
- Local files (fast metadata access)
- Rules that can be fully resolved from metadata

**When preplan hurts:**
- Remote files (metadata still requires network calls)
- Files with many row-groups (more metadata to read)

**PostgreSQL:** Reads `pg_stats` catalog (populated by `ANALYZE`).

**SQL Server:** Reads `sys.dm_db_stats_histogram` (more limited).

---

## Profile Presets: Behind the Scenes

### scout (metadata only)

Reads only file/database metadata:
- **Parquet:** Row-group stats (null_count, min/max)
- **PostgreSQL:** `pg_stats` catalog
- **SQL Server:** `sys.dm_db_stats_histogram`

No data access. Use for quick recon on large tables.

### scan (default)

Metadata first, then targeted queries:

1. **Null/distinct counts** from metadata (pg_stats, Parquet footer)
2. **Classify columns** by cardinality (low/medium/high)
3. **Numeric stats** via sampled query (TABLESAMPLE SYSTEM)
4. **Top values** for low-cardinality columns only (batched GROUP BY)
5. **High-cardinality** trusts metadata (skips expensive queries)

```sql
-- Numeric stats (sampled, not full scan)
SELECT MIN(age), MAX(age), AVG(age), STDDEV(age)
FROM table TABLESAMPLE SYSTEM(1);

-- Low-cardinality top values (batched)
SELECT status, COUNT(*) FROM table GROUP BY status;
```

Gets rich stats without scanning every row.

### interrogate (full scan)

Full table scan. Gets everything including median, percentiles, and exact distributions.

| Metric | scout | scan | interrogate |
|--------|-------|------|-------------|
| null_count | ✅ | ✅ | ✅ |
| distinct_count | estimated | estimated | exact |
| min/max | ✅ | ✅ | ✅ |
| mean/std | ❌ | sampled | exact |
| median/percentiles | ❌ | ❌ | ✅ |
| top_values | limited | ✅ | ✅ |

---

## Controls

### CLI

```bash
kontra validate contract.yml --preplan off      # skip metadata, use SQL
kontra validate contract.yml --pushdown off     # skip SQL, use Polars
kontra validate contract.yml --tally            # exact counts (aggregates)
kontra validate contract.yml --no-tally         # fail-fast (EXISTS)
kontra validate contract.yml --stats summary    # show execution stats
```

### Python

```python
result = kontra.validate(
    "data.parquet",
    rules=[...],
    preplan="auto",   # "on" | "off" | "auto"
    pushdown="auto",  # "on" | "off" | "auto"
    tally=False,      # True for exact counts
)
```

**Note:** When passing a DataFrame, only Polars executes (no preplan, no pushdown).

---

## Debugging

```bash
kontra validate contract.yml --stats summary --preplan on
```

```
✅ COL:user_id:not_null [metadata]
✅ COL:email:not_null [metadata]
✅ COL:age:range [metadata]
✅ COL:email:unique [sql]
✅ COL:status:allowed_values [sql]
✅ COL:email:regex [sql]
✅ DATASET:min_rows [sql]

Stats  •  rows=5,000,000  duration=1403ms  engine=duckdb+polars
```

Each rule shows which path resolved it: `[metadata]` (preplan) or `[sql]` (pushdown).
