# Kontra Ideas Backlog

Ideas worth exploring but not yet committed to roadmap. Revisit periodically.

---

## Materialization / Data Output

**Status**: Backlog (not immediate priority)

### Quarantine Mode (Minimal)

```bash
kontra validate contract.yml --quarantine ./quarantine/
```

- Outputs failed rows to separate file/partition
- Preserves which rules failed per row
- Enables: `kontra scout quarantine/failed.parquet` for failure analysis

**Insight potential**: Scout on quarantine could reveal:
- Patterns in failures (specific date ranges, user segments)
- Whether failures are random noise or systematic
- Data quality trends (is the quarantine growing?)

### Materialize Mode (Full)

```bash
kontra materialize contract.yml --output clean.parquet
```

- Outputs only rows that passed all row-level rules
- Useful for ML pipelines, data lake tiering

### Technical Notes

- Architecture already supports this (materializers, predicates)
- Row-level rules can filter: `not_null`, `allowed_values`, `regex`, `range`
- Dataset-level rules can't filter: `min_rows`, `max_rows`, `unique`
- Could compose all row predicates into single Polars filter

### Open Questions

1. Output format: Parquet only, or pluggable?
2. Audit trail: Just quarantine rows, or track which rules failed?
3. Streaming: Batch output or generator API for pipelines?

---

## dlt Integration

**Status**: Watching (not convinced yet)

- dlt gaining popularity but mixed real-world experience
- Could be a source connector for dlt pipelines
- Low priority until clear demand

---

## Agentic Features (Evaluated)

**Status**: Roadmapped for v0.3 and v0.4

### Tier 1: High Impact, Low Complexity → v0.3

| Feature | Why | Status |
|---------|-----|--------|
| **Validation State Snapshots** | Enables "what changed?" - fundamental for agents | Roadmapped v0.3 |
| **Scout Diff** | Zero-scan, works on stored summaries | Roadmapped v0.3 |
| **Semantic Failure Summaries** | Structured "why" for LLM reasoning | Roadmapped v0.3 |
| **Rule Severity** | blocking/warning/info workflow control | Roadmapped v0.3 |

### Tier 2: High Impact, Medium Complexity → v0.4

| Feature | Why | Status |
|---------|-----|--------|
| **Contract Mutation Proposals** | The "copilot" feature | Roadmapped v0.4 |
| **Drift Detection** | Proactive alerting before failures | Roadmapped v0.4 |
| **Goal-Directed Validation** | `--only`, `--columns`, `--tier` | Roadmapped v0.4 |
| **Named Checkpoints** | Production baseline comparisons | Roadmapped v0.4 |

### Tier 3: Future / Exploratory

| Feature | Notes |
|---------|-------|
| **Conditional Rules** | Real-world complexity, but adds contract complexity |
| **Causal Hints** | `schema_drift`, `novel_value` - start simple |
| **Semantic Column Evolution** | `identifier → category` detection |
| **Suggested Remediations** | Context-dependent, start simple |

### Skipped (For Now)

- **dlt integration**: Wait for clear demand
- **Natural Language to Contract**: Document pattern, don't build wrapper
- **Auto-remediation**: Too risky without human-in-loop culture

---

## MCP Server

**Status**: Future (not in immediate roadmap)

The state management abstraction (v0.3) makes MCP trivial:
- MCP server just wraps the library
- State backend is configured at MCP level
- Tools: `kontra_validate`, `kontra_scout`, `kontra_diff`

Not building into Kontra core - separate package or user-implemented.

---

## Streaming Validation

**Status**: Future (v0.5+ if demand)

For very large datasets or real-time pipelines:
```python
for batch in validate_streaming("huge.parquet", batch_size=100000):
    if not batch.passed:
        handle_failure(batch)
```

Wait for clear use case before implementing.

---

## Unique Rule Semantic Mismatch

**Status**: Bug/Design Decision

Discovered in tier equivalence testing - `unique` rule has different semantics between SQL and Polars:

- **PostgreSQL SQL**: `COUNT(*) - COUNT(DISTINCT col)` = counts "extra rows beyond unique"
- **Polars**: `is_duplicated().sum()` = counts "all rows participating in duplicates"

For data with user_ids `[0-99, 0, 1, 2]`:
- SQL returns 3 (103 total - 100 distinct = 3 extra)
- Polars returns 6 (0, 1, 2 each appear twice = 6 rows)

### Options

1. **Change Polars to match SQL**: Count extra rows, not all duplicate rows
2. **Change SQL to match Polars**: Use window function to count all duplicate rows
3. **Document as intentional**: Different semantics, user picks based on need

Currently masked because DuckDB doesn't support unique pushdown (falls back to Polars).

---

## Scout → Validate One-Liner

**Status**: Backlog

Convenience API for profiling data, generating rules, and validating in one pass without reading data twice.

### Current State

```python
# Works but reads data twice
result = kontra.validate(df, rules=kontra.suggest_rules(kontra.scout(df)).to_dict())
```

### Proposed Options

1. **Single function**: `kontra.validate_with_suggestions(data, preset="standard")`
2. **Expose DataFrame from profile**: `profile._df` or `profile.dataframe` for reuse
3. **Pipeline builder**: `kontra.pipeline(data).scout().suggest().validate()`

### Technical Notes

- Scout already materializes the DataFrame internally
- Could cache/expose it for subsequent validate call
- Pipeline approach enables lazy chaining

### Open Questions

1. Should suggestions be filtered by confidence before validation?
2. Return both profile and result, or just result?
3. CLI equivalent: `kontra validate --auto-contract data.parquet`?

---

## Pipeline Validation Decorator

**Status**: Backlog (likely roadmap soon)

Inject validation into data pipelines without boilerplate. Inspired by Pandera's `@pa.check_output`.

```python
@kontra.validate(contract="users.yml", on_fail="raise")
def load_users() -> pl.DataFrame:
    return pl.read_parquet("...")

# Or with inline rules
@kontra.validate(rules=[rules.not_null("id"), rules.min_rows(100)])
def fetch_orders() -> pl.DataFrame:
    return db.query("SELECT * FROM orders")
```

### Behavior Options

- `on_fail="raise"` - Raise `ValidationError` on blocking failures
- `on_fail="warn"` - Log warning, return data anyway
- `on_fail="return_result"` - Return `(df, ValidationResult)` tuple

### Implementation

Pure syntactic sugar - decorator wraps function and calls `kontra.validate()` on return value. No engine changes needed.

---

## Class-Based Contracts (KontraModel)

**Status**: Backlog (needs design thought)

Pydantic-style contract definition for developers who prefer Python over YAML.

```python
from kontra import KontraModel, Field

class UserContract(KontraModel):
    user_id: int = Field(rules=[NotNull(), Unique()])
    email: str = Field(rules=[NotNull()])

    class Config:
        datasource = "prod_db.users"

result = UserContract.validate()
```

### Open Questions

1. Build custom or leverage Pydantic? (Don't recreate Pydantic)
2. Could use `pydantic.BaseModel` with custom field validators?
3. How to handle dtype inference from type hints?

### Implementation

Compiles down to existing `RuleSpec` objects. No engine changes needed.

---

*Last updated: 2026-01-15*
