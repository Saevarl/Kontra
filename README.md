# Kontra

**Fast data quality validation for files, databases, and DataFrames.**

Kontra validates data against declarative rules. It stays fast on large datasets by resolving checks from metadata when possible, then running the rest via batched SQL pushdown (DuckDB / PostgreSQL / SQL Server).

```bash
pip install kontra
```

## Quick Start

```python
import kontra
from kontra import rules

result = kontra.validate("users.parquet", rules=[
    rules.not_null("user_id"),
    rules.unique("email"),
    rules.range("age", min=0, max=120),
])

result.passed        # True
result.to_dict()     # Structured output for CI/services
result.to_llm()      # Token-optimized summary for agents
```

DataFrames work too:

```python
result = kontra.validate(df, rules=[...])  # Polars or pandas
```

## CLI

```bash
kontra profile users.parquet --draft > contract.yml
kontra validate contract.yml
```

```
✅ users — PASSED (4 of 4 rules)
  ✅ COL:user_id:not_null [metadata]
  ✅ COL:age:range [metadata]
  ✅ COL:email:unique [sql]
  ✅ COL:status:allowed_values [sql]
```

## Execution

Metadata (preplan) resolves what it can prove. Remaining rules run via SQL pushdown when available, or locally (Polars). Preplan and pushdown are configurable.

## Contracts

Rules can also be defined in YAML:

```yaml
name: users
datasource: users.parquet

rules:
  - name: not_null
    params: { column: user_id }

  - name: unique
    params: { column: email }
    severity: warning

  - name: allowed_values
    params:
      column: status
      values: [active, inactive, pending]

  - name: range
    params: { column: age, min: 0, max: 120 }
```

## What You Get

- **18 built-in rules** for nulls, uniqueness, ranges, regex, freshness, and more ([reference](docs/reference/rules.md))
- **Fast execution**: metadata analysis + batched SQL pushdown
- **Multiple sources**: Parquet, CSV, PostgreSQL, SQL Server, S3, Azure ADLS Gen2
- **Agent-friendly**: structured, token-optimized summaries via `.to_llm()`
- **Debuggable failures**: collect failing rows during validation, fetch more later on demand
- **Track drift**: save runs and compare over time with `kontra diff`

## Fail Fast vs Exact Counts

By default, Kontra runs in fail-fast mode: it stops at the first violation per rule and reports `failed_count: 1` as a lower bound. This enables early termination and metadata-only resolution — large Parquet tables can validate in milliseconds when Parquet statistics are sufficient to prove a rule passes.

When you need exact counts, enable `tally`:

```python
result = kontra.validate("users.parquet", rules=[...], tally=True)
```

Or per-rule in YAML:

```yaml
rules:
  - name: not_null
    params: { column: user_id }
    tally: true      # scan all rows, count all violations
```

Results:
- default (fail fast) → `failed_count: 1` (≥1 violation exists)
- `tally: true` → `failed_count: 23741` (exact)

## Failure Samples

```python
# Collect samples during validation
result = kontra.validate("users.parquet", rules=[...], sample=5)

# Access what was collected
for rule in result.rules:
    if not rule.passed and rule.samples:
        print(rule.rule_id, rule.samples)

# Need more? Fetch on demand
result.sample_failures("COL:user_id:not_null", n=20)
```

## Install Extras

```bash
pip install "kontra[postgres]"     # PostgreSQL
pip install "kontra[sqlserver]"    # SQL Server
pip install "kontra[s3]"           # S3 / MinIO
```

## Documentation

| Doc | Audience |
|-----|----------|
| [Getting Started](docs/getting-started.md) | New users |
| [Python API](docs/python-api.md) | Library users |
| [Rules Reference](docs/reference/rules.md) | All 18 rules |
| [Configuration](docs/reference/config.md) | Project setup |
| [Advanced Topics](docs/advanced/) | Agents, state, performance |
| [Architecture](docs/reference/architecture.md) | Contributors |

## License

MIT
