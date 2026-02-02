# Project Setup & History

Initialize a Kontra project, track validation history, and compare runs over time.

---

## Initialize a Project

```bash
kontra init
```

Creates:
- `.kontra/config.yml` - Project configuration
- `contracts/` - Directory for validation contracts

The config file contains documented defaults and examples:

```yaml
version: "1"

defaults:
  preplan: "on"         # on | off
  pushdown: "on"        # on | off
  projection: "on"      # on | off
  output_format: "rich" # rich | json
  state_backend: "local"

datasources: {}
  # prod_db:
  #   type: postgres
  #   host: ${PGHOST}
  #   ...
```

### Named Datasources

Define datasources once in config, reference them everywhere:

```yaml
# .kontra/config.yml
datasources:
  prod_db:
    type: postgres
    host: ${PGHOST}
    user: ${PGUSER}
    password: ${PGPASSWORD}
    database: ${PGDATABASE}
    tables:
      users: public.users
      orders: public.orders

  data_lake:
    type: s3
    bucket: ${S3_BUCKET}
    prefix: warehouse/
    tables:
      events: events.parquet
```

Then use them:

```bash
kontra profile prod_db.users
kontra validate contract.yml --data prod_db.orders
```

```python
result = kontra.validate("prod_db.users", rules=[...])
```

Credentials stay in config (or environment variables). Contracts stay clean and portable.

---

## Validation History

When you run `kontra validate`, results are automatically saved to a state backend. This enables comparing runs over time.

### View History

```bash
# Show all runs for a contract
kontra history contract.yml

# Recent runs only
kontra history contract.yml --since 7d

# Failed runs only
kontra history contract.yml --failed-only

# JSON output
kontra history contract.yml -o json
```

### Compare Runs (Diff)

```bash
# Compare latest to previous
kontra diff contract.yml

# Compare to 7 days ago
kontra diff contract.yml --since 7d

# Compare to specific date
kontra diff contract.yml --run 2024-01-15

# Output formats
kontra diff contract.yml -o json   # CI/CD integration
kontra diff contract.yml -o llm    # Token-optimized
```

### Python API

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

### History API

```python
# List past runs
runs = kontra.list_runs("my_contract")
for run in runs:
    print(f"{run['timestamp']}: {'PASS' if run['passed'] else 'FAIL'}")

# Get specific run
result = kontra.get_run("my_contract")  # latest
result = kontra.get_run("my_contract", run_id="2024-01-15T10:30:00")

# Check if history exists
if kontra.has_runs("my_contract"):
    diff = kontra.diff("my_contract")
```

---

## State Backends

Configure where validation history is stored:

```yaml
# .kontra/config.yml
defaults:
  state_backend: "local"  # default

environments:
  production:
    state_backend: postgres://${PGHOST}/${PGDATABASE}

  staging:
    state_backend: s3://${S3_BUCKET}/kontra-state/
```

### Local (Default)

State stored in `.kontra/state/` directory. No setup required.

```yaml
state_backend: "local"
```

### PostgreSQL

State stored in database tables (`kontra_runs`, `kontra_annotations`).

```yaml
state_backend: postgres://${PGHOST}/${PGDATABASE}
```

### S3

State stored as JSON files in S3.

```yaml
state_backend: s3://my-bucket/kontra-state/
```

Requires `pip install kontra[s3]` and AWS credentials.

### SQL Server

State stored in database tables.

```yaml
state_backend: mssql://${MSSQL_HOST}/${MSSQL_DATABASE}
```

---

## Annotations

Annotations provide "memory without authority"—agents and humans can record context about validation runs without affecting Kontra's behavior.

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

# Query annotations across runs (agent memory)
hints = kontra.get_annotations(
    "users_contract.yml",
    rule_id="COL:email:not_null",
)
for hint in hints:
    print(f"[{hint['annotation_type']}] {hint['summary']}")
```

**Key invariant**: Kontra never reads annotations during validation or diff. They're purely for consumer use.

Annotations are stored in:
- `kontra_annotations` table (PostgreSQL, SQL Server)
- `<run_id>.ann.jsonl` files (local, S3)

---

## Disabling State

Skip saving results to state backend:

```bash
kontra validate contract.yml --no-state
```

```python
result = kontra.validate(df, rules=[...], save=False)
```
