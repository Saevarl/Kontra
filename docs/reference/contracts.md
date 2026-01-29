# Contracts Reference

Contracts are YAML files that define validation rules for a dataset. This doc covers contract structure and rule configuration. For rule parameters and behavior, see [Rules Reference](rules.md).

## Structure

```yaml
name: users
datasource: users.parquet

rules:
  - name: not_null
    params: { column: user_id }

  - name: unique
    params: { column: email }
```

| Field | Required | Description |
|-------|----------|-------------|
| `name` | Yes | Contract identifier (used in history, diff) |
| `datasource` | Yes | File path, URI, or named datasource |
| `description` | No | Human-readable description |
| `rules` | Yes | List of rule definitions |

---

## Rule Definition

```yaml
rules:
  - name: not_null
    params: { column: user_id }
    id: custom_id              # Optional: override auto-generated ID
    severity: blocking         # Optional: blocking | warning | info
    tally: true                # Optional: exact counts vs fail-fast
    context:                   # Optional: consumer-defined metadata
      owner: data-eng
      fix_hint: "Check ETL job"
```

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| `name` | Yes | | Rule type (see [Rules Reference](rules.md)) |
| `params` | Yes | | Rule parameters |
| `id` | No | auto | Custom rule ID |
| `severity` | No | `blocking` | Severity level |
| `tally` | No | inherit | Exact counts vs fail-fast |
| `context` | No | | Arbitrary metadata |

---

## Severity

Severity controls whether a rule failure affects `result.passed`.

```yaml
rules:
  - name: not_null
    params: { column: user_id }
    severity: blocking         # Fails result.passed (default)

  - name: unique
    params: { column: email }
    severity: warning          # Tracked, doesn't fail result.passed

  - name: range
    params: { column: score, min: 0 }
    severity: info             # Informational only
```

| Level | `result.passed` | Use case |
|-------|-----------------|----------|
| `blocking` | Affected | Hard requirements |
| `warning` | Not affected | Monitor, don't block |
| `info` | Not affected | Informational metrics |

Kontra records severity. The CLI maps blocking failures to a non-zero exit code.

Access in code:

```python
result.passed              # Only considers blocking rules
result.blocking_failures   # List of failed blocking rules
result.warnings            # List of failed warning rules
```

---

## Context

Attach arbitrary metadata to rules. Kontra stores it but doesn't interpret it.

```yaml
rules:
  - name: not_null
    params: { column: email }
    context:
      owner: data-eng
      fix_hint: "Backfill from user_profiles table"
      pagerduty: email-quality
      tags: ["critical", "daily"]
      sla_hours: 4
```

Access in code:

```python
for rule in result.blocking_failures:
    owner = rule.context.get("owner", "unknown")
    hint = rule.context.get("fix_hint", "")
    print(f"{rule.rule_id} ({owner}): {hint}")
```

**Use cases:**
- Route alerts to the right team
- Provide fix hints to agents
- Tag rules for dashboards
- Track SLAs

---

## Tally

Controls whether rules count all violations or stop at the first.

```yaml
rules:
  # Inherit from global setting (default)
  - name: not_null
    params: { column: user_id }

  # Force exact count for this rule
  - name: unique
    params: { column: email }
    tally: true

  # Force fail-fast for this rule
  - name: range
    params: { column: age, min: 0 }
    tally: false
```

| Setting | `failed_count` | Speed |
|---------|----------------|-------|
| `tally: true` | Exact | Scans all rows |
| `tally: false` | 1 (means ≥1) | Stops at first violation |

**Precedence:** CLI `--tally`/`--no-tally` > per-rule `tally` > API `tally=` > default (`false`)

**Notes:**
- `tally` only applies to column and cross-column rules. Dataset rules (`min_rows`, `max_rows`, `freshness`, `custom_sql_check`) always return exact counts.
- `tally: true` disables preplan for that rule (exact counts require scanning).

---

## Custom Rule IDs

Rule IDs are auto-generated as `COL:{column}:{rule_name}` or `DATASET:{rule_name}`.

When you have multiple rules with the same auto-generated ID, add explicit `id`:

```yaml
rules:
  # These would both be COL:shipping_date:conditional_not_null
  - name: conditional_not_null
    id: shipped_needs_date
    params: { column: shipping_date, when: "status == 'shipped'" }

  - name: conditional_not_null
    id: delivered_needs_date
    params: { column: shipping_date, when: "status == 'delivered'" }
```

Kontra raises `DuplicateRuleIdError` if IDs collide.

---

## Severity Weights (Optional)

Severity weights are optional numeric weights for each severity level. Kontra carries them but never acts on them.

Configure in `.kontra/config.yml`:

```yaml
severity_weights:
  blocking: 1.0
  warning: 0.5
  info: 0.1
```

When configured, each `RuleResult` includes its weight:

```python
for rule in result.rules:
    print(f"{rule.rule_id}: weight={rule.severity_weight}")
```

`RuleResult.to_llm()` includes the weight:

```
COL:email:not_null: FAIL (≥10 failures)[w=1.0]
```

---

## Quality Score (Optional)

When severity weights are configured, `ValidationResult` computes a quality score:

```python
result.quality_score  # Float 0.0-1.0, or None if weights not configured
```

**Formula:**

```
quality_score = 1.0 - weighted_violation_rate
weighted_violation_rate = Σ(failed_count × weight) / (total_rows × Σ(weights))
```

**Example:**
- 100 rows, 3 rules
- Rule 1: blocking (w=1.0), 10 failures
- Rule 2: warning (w=0.5), 4 failures
- Rule 3: blocking (w=1.0), 0 failures
- Weighted violations: 10×1.0 + 4×0.5 + 0×1.0 = 12
- Max possible: 100 × (1.0 + 0.5 + 1.0) = 250
- Quality score: 1.0 - 12/250 = 0.952

`ValidationResult.to_llm()` includes the score:

```
VALIDATION: test FAILED (100 rows) [score=0.95]
BLOCKING: COL:email:not_null (10)
WARNING: COL:name:unique (4)
PASSED: 1 rules
```

Useful for dashboards, trend tracking, and agents that need a scalar summary beyond binary pass/fail.

---

## Datasource Formats

The `datasource` field accepts:

| Format | Example |
|--------|---------|
| Local file | `users.parquet`, `./data/events.csv` |
| S3 | `s3://bucket/path/file.parquet` |
| Azure ADLS | `abfss://container@account.dfs.core.windows.net/path` |
| PostgreSQL | `postgres://user:pass@host:5432/db/schema.table` |
| SQL Server | `mssql://user:pass@host:1433/db/schema.table` |
| Named datasource | `prod_db.users` (from config) |

See [Configuration](config.md) for named datasources.
