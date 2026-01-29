# Getting Started

Validate your first dataset in five minutes.

## Install

```bash
pip install kontra
```

For databases and cloud storage:
```bash
pip install "kontra[postgres]"     # PostgreSQL
pip install "kontra[sqlserver]"    # SQL Server
pip install "kontra[s3]"           # S3 / MinIO
```

Azure ADLS Gen2 works out of the box.

## Quick Validation

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
```

Three rules validated against a Parquet file. Each rule reports its execution source:

```python
for r in result.rules:
    print(f"{r.rule_id}: {r.source}")

# COL:user_id:not_null: metadata
# COL:age:range: metadata
# COL:email:unique: sql
```

`metadata` (also called preplan) means Kontra proved the rule from available metadata (Parquet stats, database catalogs) without scanning data. `sql` means it ran as a pushdown query in the active engine (DuckDB, PostgreSQL, or SQL Server). This is why large datasets validate fast.

## Profile First, Then Validate

For unfamiliar data, profile it:

```python
profile = kontra.profile("users.parquet")
print(profile)
```

```
DatasetProfile(users.parquet)
  Rows: 50,000 | Columns: 5
  Columns:
    - user_id: int, 50,000 distinct, [identifier]
    - email: string, 2% null, 49,000 distinct
    - status: string, 3 distinct, [category]
    - age: int, 78 distinct, [measure]
    - created_at: datetime, [timestamp]
```

Then write rules based on what you see:

```python
result = kontra.validate("users.parquet", rules=[
    rules.not_null("user_id"),
    rules.unique("email"),
    rules.allowed_values("status", ["active", "inactive", "pending"]),
    rules.range("age", min=0, max=120),
])
```

## Database Tables

Same API, different source:

```python
result = kontra.validate(
    "postgres://user:pass@localhost:5432/myapp/public.users",
    rules=[
        rules.not_null("user_id"),
        rules.unique("email"),
    ]
)
```

Or bring your own connection:

```python
import psycopg

conn = psycopg.connect(host="localhost", dbname="myapp")
result = kontra.validate(conn, table="public.users", rules=[...])
```

Works with PostgreSQL and SQL Server. See [Configuration](reference/config.md) for named datasources.

## CLI Workflow

Profile and draft work from the CLI too:

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

## Contracts

The `--draft` output is a YAML contract:

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

Edit it, commit it, run it in CI. Kontra exits non-zero if any blocking rule fails. The contract is the source of truth.

### Severity

Rules are `blocking` by default. A single blocking failure means `result.passed` is `False`.

Set `severity: warning` for rules that should be tracked but not block:

```python
result.passed              # Only considers blocking rules
result.blocking_failures   # Rules that block
result.warnings            # Rules with severity: warning
```

Available levels: `blocking`, `warning`, `info`.

### Context

Attach arbitrary metadata to rules. Kontra stores it but doesn't use it for validation:

```yaml
rules:
  - name: not_null
    params: { column: email }
    context:
      owner: data-eng
      fix_hint: "Backfill from user_profiles table"
      pagerduty: email-quality
```

Access it in code:

```python
for rule in result.blocking_failures:
    hint = rule.context.get("fix_hint", "")
    print(f"{rule.rule_id}: {rule.message} → {hint}")
```

Context is carried through to outputs for routing, alerts, dashboards, and agents. Kontra stores it but doesn't interpret it.

## When Validation Fails

```python
result = kontra.validate("users.parquet", rules=[
    rules.not_null("email"),
    rules.allowed_values("status", ["active", "inactive"]),  # "pending" will fail
])

if not result.passed:
    for rule in result.blocking_failures:
        print(f"{rule.rule_id}: {rule.failed_count} violations")
        print(f"  {rule.message}")
```

```
COL:email:not_null: 1 violations
  At least 1 null value found in email
COL:status:allowed_values: 1 violations
  At least 1 row: status value not in ['active', 'inactive']
```

By default Kontra runs fail-fast per rule. Enable `tally=True` for exact counts.

## Two Knobs Worth Knowing

**`tally`**: By default, Kontra stops at the first violation (fail-fast) and reports `failed_count: 1` as a lower bound. When you need exact counts:

```python
result = kontra.validate("users.parquet", rules=[...], tally=True)
# result.rules[0].failed_count is now exact
```

Or per-rule in YAML:

```yaml
rules:
  - name: not_null
    params: { column: user_id }
    tally: true
```

Note: `tally=True` disables metadata resolution for that rule (exact counts require scanning). With `--tally` in the CLI, preplan is disabled for the run.

**`sample`**: Collect failing rows during validation:

```python
result = kontra.validate("users.parquet", rules=[...], sample=5)

for rule in result.rules:
    if not rule.passed:
        print(rule.samples)  # Up to 5 failing rows

# Need more? Fetch on demand
result.sample_failures("COL:email:not_null", n=20)
```

Note: In fail-fast mode (`tally=False`), Kontra stops at the first violation, so you get at most 1 sample per rule. Set `tally=True` to collect more during validation, or use `sample_failures()` to fetch on demand afterward.

## Next Steps

| What you want | Where to go |
|---------------|-------------|
| Full Python API | [Python API](python-api.md) |
| All 18 rules | [Rules Reference](reference/rules.md) |
| Named datasources, environments | [Configuration](reference/config.md) |
| Compare runs over time | [State & Diff](advanced/state-and-diff.md) |
| Integrate with agents | [Agents & Services](advanced/agents-and-llms.md) |
| Execution model details | [Performance](advanced/performance.md) |
