# State & Diff

Track validation history and compare runs over time.

## How It Works

When you run `kontra validate`, results are automatically saved to a state backend. You can then compare runs to detect regressions or improvements.

## CLI Usage

```bash
# Run validation (automatically saves state)
kontra validate contract.yml

# Compare to previous run
kontra diff

# Compare to 7 days ago
kontra diff --since 7d

# Compare to specific date
kontra diff --run 2024-01-15

# Output formats
kontra diff -o json      # CI/CD integration
kontra diff -o llm       # Token-optimized
```

### View Validation History

```bash
# Show all runs for a contract
kontra history contract.yml

# Show only recent runs (last 7 days)
kontra history contract.yml --since 7d

# Show only failed runs
kontra history contract.yml --failed-only

# Output as JSON for processing
kontra history contract.yml -o json

# Limit number of runs shown
kontra history contract.yml --limit 20
```

Output:
```
Diff: users_contract
Comparing: 2024-01-10 14:30 -> 2024-01-12 09:15
==================================================

Overall: PASSED -> FAILED

New Blocking Failures (1)
  - COL:email:not_null (523 violations)

Warning Regressions (1)
  - COL:status:allowed_values: 10 -> 45 (+35)

Resolved (1)
  - COL:age:range
```

## Python API

```python
import kontra

# Compare latest to previous run
diff = kontra.diff("my_contract")

if diff.regressed:
    print("Quality regressed!")
    for rule in diff.new_failures:
        print(f"  NEW: {rule.rule_id}")

if diff.improved:
    print("Quality improved!")
    for rule in diff.resolved:
        print(f"  RESOLVED: {rule.rule_id}")

# Diff properties
diff.has_changes       # bool
diff.improved          # bool
diff.regressed         # bool
diff.before            # run summary
diff.after             # run summary
diff.new_failures      # list of new failures
diff.resolved          # list of resolved failures
diff.count_changes     # list of count changes

# Serialize
diff.to_dict()
diff.to_json()
diff.to_llm()
```

## History Management

```python
# List past runs
runs = kontra.list_runs("my_contract")
for run in runs:
    print(f"{run.timestamp}: {'PASS' if run.passed else 'FAIL'}")

# Get specific run
run = kontra.get_run("my_contract")  # latest
run = kontra.get_run("my_contract", run_id="abc123")

# Check if history exists
if kontra.has_runs("my_contract"):
    diff = kontra.diff("my_contract")
```

## State Backends

Configure in `.kontra/config.yml`:

```yaml
defaults:
  state_backend: "local"  # default

environments:
  production:
    state_backend: postgres://${PGHOST}/${PGDATABASE}

  staging:
    state_backend: s3://${S3_BUCKET}/kontra-state/
```

### Local (Default)

State stored in `.kontra/state/` directory.

```yaml
state_backend: "local"
```

### PostgreSQL

State stored in database tables.

```yaml
state_backend: postgres://${PGHOST}/${PGDATABASE}
```

### S3

State stored in S3 bucket.

```yaml
state_backend: s3://my-bucket/kontra-state/
```

Requires `pip install kontra[s3]` and AWS credentials.

### SQL Server

State stored in database tables.

```yaml
state_backend: mssql://${MSSQL_HOST}/${MSSQL_DATABASE}
```

Or via URI:
```yaml
state_backend: sqlserver://user:password@host:1433/database
```

## Annotations

Annotations provide "memory without authority"—agents and humans can record context about validation runs without affecting Kontra's behavior:

```python
import kontra

# Annotate the latest run
kontra.annotate(
    "users_contract.yml",
    actor_type="agent",
    actor_id="repair-agent-v2",
    annotation_type="resolution",
    summary="Fixed null emails by backfilling from user_profiles",
)

# Annotate a specific rule
kontra.annotate(
    "users_contract.yml",
    rule_id="COL:email:not_null",
    actor_type="human",
    actor_id="alice@example.com",
    annotation_type="false_positive",
    summary="Service accounts are expected to have null emails",
)

# Load run with annotations
result = kontra.get_run_with_annotations("users_contract.yml")
for ann in result.annotations or []:
    print(f"[{ann['annotation_type']}] {ann['summary']}")
```

**Key invariant**: Kontra never reads annotations during validation or diff. They're purely for consumer use.

Annotations are stored in a normalized schema:
- `kontra_annotations` table (PostgreSQL, SQL Server)
- `<run_id>.ann.jsonl` files (local, S3)

## Profile Diff (Scout Diff)

Compare data profiles over time:

```bash
kontra profile data.parquet --save-profile  # save profile
# ... later ...
kontra profile-diff data.parquet            # compare to previous
```

```python
# Python API
profile = kontra.profile(df, save=True)
diff = kontra.profile_diff("data.parquet")

for change in diff.changes:
    print(f"{change.column}.{change.field}: {change.before} -> {change.after}")
```

## Disabling State

```bash
kontra validate contract.yml --no-state
```

```python
result = kontra.validate(df, rules=[...], save=False)
```
