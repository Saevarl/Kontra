# Kontra Roadmap

## Vision

Kontra is a **developer-first data validation engine** that feels like magic. Like Tailscale makes networking "just work" and Claude Code understands context before you explain it, Kontra should make data quality validation effortless, intelligent, and fast.

**Core Principles:**
- **Zero-friction start**: One command from data to validation
- **Intelligent defaults**: Infer what "good" looks like from the data itself
- **Speed over ceremony**: Metadata-first, scan only when necessary
- **Agentic-first**: Built for LLM integration from the ground up
- **Progressive disclosure**: Simple surface, infinite depth
- **Configuration as UX**: Every config surface must be intuitive and consistent

---

## Configuration UX Principles

As Kontra grows, we add more configurable surfaces. Each must follow these principles:

### 1. Sensible Defaults (Zero-Config Works)
```bash
# This should just work, no flags needed
kontra validate contract.yml
kontra scout data.parquet
```

### 2. Progressive Disclosure
```bash
# Simple case is simple
kontra validate contract.yml

# Power user has options
kontra validate contract.yml --pushdown on --stats profile --show-plan
```

### 3. Consistency Across Surfaces
```bash
# Same pattern for all data sources
kontra scout postgres://...
kontra scout s3://bucket/data.parquet
kontra scout ./local/data.csv

# Same pattern for all backends
--state-backend local      # default
--state-backend s3://...
--state-backend postgres://...
```

### 4. Environment Variables for CI/CD
```bash
# Every flag has an env var equivalent
export KONTRA_STATE_BACKEND=postgres://...
export KONTRA_PREPLAN=auto
export KONTRA_PUSHDOWN=auto
```

### 5. Config File for Persistence
```yaml
# .kontra/config.yml - one place for all settings
defaults:
  preplan: auto
  pushdown: auto
  state_backend: local

environments:
  production:
    state_backend: postgres://${PGHOST}/${PGDATABASE}
```

### 6. Errors Suggest Fixes
```
Error: PostgreSQL connection failed

  Could not connect to localhost:5432

  Try:
    export PGHOST=your-host
    export PGUSER=your-user

  Or use full URI:
    postgres://user:pass@host:5432/database/schema.table
```

---

## Current State (v0.1.x) ✅

### Completed
- [x] 10 built-in rules with SQL pushdown
- [x] Three-tier execution: preplan → SQL pushdown → Polars
- [x] Data sources: Parquet, CSV, PostgreSQL, SQL Server
- [x] Scout profiling with lite/standard/deep/llm presets
- [x] Column projection for efficiency
- [x] JSON/Rich/Markdown/LLM output formats
- [x] Execution source tracking (`[metadata]`, `[sql]`, `[polars]`)
- [x] `--dry-run` validation
- [x] `kontra init` magic command
- [x] Better error messages with suggestions
- [x] Documentation (quickstart, rules, architecture)
- [x] SQL utilities consolidation

### Rules
| Rule | Description | SQL Pushdown |
|------|-------------|--------------|
| `not_null` | No NULL values | DuckDB, PG, SQL Server |
| `unique` | No duplicates | PG, SQL Server |
| `min_rows` | Minimum row count | All |
| `max_rows` | Maximum row count | All |
| `allowed_values` | Values in set | PG, SQL Server |
| `freshness` | Data recency | All |
| `range` | Min/max bounds | All |
| `regex` | Pattern matching | All |
| `dtype` | Type checking | Schema only |
| `custom_sql_check` | User SQL | Polars/DuckDB |

---

## v0.2 — Magic Polish

**Goal**: Refine the magic, improve inference

### 0.2.1 Scout Enhancements
- [x] LLM preset (`--preset llm`) - token-optimized output
- [x] LLM output format (`-o llm`) - 6.7x smaller than JSON
- [ ] Improve semantic type detection accuracy
- [ ] Add more patterns: phone, postal code, coordinates

### 0.2.2 Rule Inference Improvements
- [ ] Higher confidence thresholds for `kontra init`
- [ ] `kontra suggest` command for explicit rule suggestions
- [ ] Confidence scores in output

### 0.2.3 Validation Explanations
- [x] Detailed failure explanations for `allowed_values`
- [ ] Extend to all rules (not_null, unique, range, etc.)
- [ ] Structured `failure_mode` field in results

---

## v0.3 — Agentic Foundation 🤖

**Goal**: State management and time-based reasoning for agents

This is the foundational release for agentic workflows. Without state, agents can only see "now". With state, they can reason about "change over time".

### Design Principles

1. **State is implicit** (like git) - no extra flags for normal use
2. **Backends are pluggable** - local, S3, PostgreSQL
3. **Diff is explicit** - `kontra diff` is a first-class command
4. **Library-first** - CLI wraps the library, MCP wraps the library

### 0.3.1 Validation State

**State Object Shape:**
```json
{
  "schema_version": "1.0",
  "contract_fingerprint": "sha256:abc123",
  "dataset_fingerprint": "sha256:def456",
  "run_at": "2024-01-13T10:30:00Z",
  "summary": {
    "passed": true,
    "total_rules": 8,
    "passed_rules": 8,
    "failed_rules": 0,
    "row_count": 1000000
  },
  "rules": [
    {
      "rule_id": "COL:user_id:not_null",
      "passed": true,
      "failed_count": 0,
      "execution_source": "metadata"
    },
    {
      "rule_id": "COL:status:allowed_values",
      "passed": false,
      "failed_count": 42,
      "failure_mode": "novel_category",
      "details": {
        "unexpected_values": [{"value": "archived", "count": 42}]
      }
    }
  ]
}
```

**CLI Usage:**
```bash
# State saved automatically to .kontra/
kontra validate contract.yml

# Explicit state location
kontra validate contract.yml --state-backend s3://bucket/kontra/

# Via environment variable
export KONTRA_STATE_BACKEND=postgres://user:pass@host/db
kontra validate contract.yml
```

### 0.3.2 State Backend Protocol

```python
class StateBackend(Protocol):
    """Pluggable state storage."""

    def save(self, state: ValidationState) -> None:
        """Save a validation state."""
        ...

    def get_latest(self, contract_fp: str) -> Optional[ValidationState]:
        """Get most recent state for a contract."""
        ...

    def get_at(self, contract_fp: str, timestamp: datetime) -> Optional[ValidationState]:
        """Get state at a specific time."""
        ...

    def get_history(self, contract_fp: str, limit: int = 10) -> List[ValidationState]:
        """Get recent history for a contract."""
        ...
```

**Implementations:**
| Backend | Use Case | Storage |
|---------|----------|---------|
| `LocalStore` | Development | `.kontra/state/` |
| `S3Store` | Distributed pipelines | S3 bucket |
| `PostgresStore` | Queryable history | PostgreSQL table |

### 0.3.3 Diff Command

```bash
# Compare to last run
kontra diff

# Compare to specific date
kontra diff --since 7d
kontra diff --run 2024-01-12

# Compare to named checkpoint
kontra diff --checkpoint production
```

**Output:**
```
Diff: 2024-01-13 vs 2024-01-12
Contract: users_contract

Rules:
  COL:status:allowed_values
    - was: PASS
    + now: FAIL (42 violations)
    + failure_mode: novel_category
    + new_values: ["archived"]

  COL:email:not_null
    - violations: 0
    + violations: 15 (+15)

Dataset:
  row_count: 1,000,000 → 1,050,000 (+5%)
```

### 0.3.4 Scout Diff

```bash
# Compare two scout profiles
kontra scout-diff before.json after.json

# Or use stored profiles
kontra scout data.parquet --save-profile
kontra scout-diff --since 7d
```

**Output:**
```
Scout Diff: 2024-01-13 vs 2024-01-12

Schema:
  + new_column: string (added)
  - old_column: removed

Cardinality:
  status: 4 → 6 values (+2)
    + "archived" (new)
    + "unknown" (new)

Null Rates:
  email: 2.0% → 5.0% (+3.0%)  ⚠️
```

### 0.3.5 Semantic Failure Modes

Structured "why this failed" for agent reasoning:

```python
class FailureMode(Enum):
    NOVEL_CATEGORY = "novel_category"      # New values in allowed_values
    NULL_SPIKE = "null_spike"              # Null rate increased
    RANGE_VIOLATION = "range_violation"    # Values outside bounds
    DUPLICATE_EMERGENCE = "duplicate_emergence"  # Uniqueness violated
    SCHEMA_DRIFT = "schema_drift"          # Column type changed
    FRESHNESS_LAG = "freshness_lag"        # Data is stale
```

### 0.3.6 Rule Severity

```yaml
# contract.yml
rules:
  - name: not_null
    params: { column: user_id }
    severity: blocking  # Fails pipeline

  - name: not_null
    params: { column: middle_name }
    severity: warning   # Warns but continues

  - name: freshness
    params: { column: updated_at, max_age: "24h" }
    severity: info      # Logs only
```

**Exit codes:**
- `0`: All rules passed (or only info/warning failures)
- `1`: At least one blocking rule failed
- `2`: Configuration error
- `3`: Runtime error

### 0.3.7 Library API

```python
from kontra import ValidationEngine
from kontra.state import LocalStore, S3Store, PostgresStore

# Default: local state
engine = ValidationEngine("contract.yml")
result = engine.run()

# Explicit backend
engine = ValidationEngine(
    "contract.yml",
    state_store=PostgresStore("postgres://...")
)

# Get state and diff
state = result.to_state()
diff = engine.diff_from_last()

# Programmatic diff
if diff.has_regressions:
    print(f"Regressions: {diff.regressions}")
    print(f"New failures: {diff.new_failures}")
```

---

## v0.3 Execution Plan

### Phase 1: State Foundation (Week 1)

**Goal**: Basic state save/load with local backend

- [ ] Create `ValidationState` dataclass
- [ ] Create `StateBackend` protocol
- [ ] Implement `LocalStore` (`.kontra/state/`)
- [ ] Add `--state-backend` flag to CLI
- [ ] Auto-save state after each validation
- [ ] Add `KONTRA_STATE_BACKEND` env var

**Files:**
```
src/kontra/
├── state/
│   ├── __init__.py
│   ├── types.py          # ValidationState dataclass
│   ├── backends/
│   │   ├── __init__.py
│   │   ├── base.py       # StateBackend protocol
│   │   └── local.py      # LocalStore
│   └── fingerprint.py    # Contract/dataset fingerprinting
```

**Tests:**
- State serialization roundtrip
- LocalStore save/get/history
- Fingerprint stability

### Phase 2: Diff Command (Week 1-2)

**Goal**: `kontra diff` working with local state

- [ ] Implement `ValidationState.diff()` method
- [ ] Create `StateDiff` dataclass
- [ ] Add `kontra diff` command
- [ ] Add `--since`, `--run` flags
- [ ] Rich diff output formatting

**CLI:**
```bash
kontra diff                    # vs last run
kontra diff --since 7d         # vs 7 days ago
kontra diff --run 2024-01-12   # vs specific run
```

### Phase 3: Failure Modes (Week 2)

**Goal**: Structured failure explanations

- [ ] Define `FailureMode` enum
- [ ] Add `failure_mode` to all rule results
- [ ] Extend `allowed_values` details (already done)
- [ ] Add details to `not_null`, `unique`, `range`
- [ ] Include failure_mode in state object

### Phase 4: Rule Severity (Week 2)

**Goal**: Blocking vs warning vs info

- [ ] Add `severity` field to contract schema
- [ ] Update rule factory to parse severity
- [ ] Modify exit code logic based on severity
- [ ] Update reporter to show severity

### Phase 5: Cloud Backends (Week 3)

**Goal**: S3 and PostgreSQL state storage

- [ ] Implement `S3Store`
- [ ] Implement `PostgresStore`
- [ ] Add connection string parsing
- [ ] Integration tests with Docker

**PostgreSQL Schema:**
```sql
CREATE TABLE kontra_state (
    id SERIAL PRIMARY KEY,
    contract_fingerprint TEXT NOT NULL,
    dataset_fingerprint TEXT,
    run_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    passed BOOLEAN NOT NULL,
    total_rules INT,
    failed_rules INT,
    row_count BIGINT,
    state JSONB NOT NULL,

    INDEX idx_contract_time (contract_fingerprint, run_at DESC)
);
```

### Phase 6: Scout Diff (Week 3)

**Goal**: Compare scout profiles over time

- [ ] Add `--save-profile` flag to scout
- [ ] Create `ScoutDiff` class
- [ ] Add `kontra scout-diff` command
- [ ] Integrate with state backends

### Phase 7: Config File (Week 4)

**Goal**: `.kontra/config.yml` for persistent settings

- [ ] Define config schema
- [ ] Load config on CLI startup
- [ ] Support environment variable substitution
- [ ] Add `kontra config` command for viewing/editing

**Config Schema:**
```yaml
# .kontra/config.yml
version: "1.0"

defaults:
  preplan: auto
  pushdown: auto
  projection: on
  state_backend: local

environments:
  production:
    state_backend: postgres://${PGHOST}/${PGDATABASE}
  staging:
    state_backend: s3://bucket/kontra-state/staging/

# Retention policy for local state
retention:
  max_runs: 100
  max_age_days: 30
```

### Phase 8: Documentation & Polish (Week 4)

- [ ] Update quickstart with state examples
- [ ] Add agentic workflow guide
- [ ] Document state backends
- [ ] Add migration guide for v0.2 → v0.3

---

## v0.4 — Agent Power Features

**Goal**: Advanced agentic capabilities

### 0.4.1 Contract Mutation Proposals
```bash
kontra validate contract.yml --propose-fixes

# Output
Proposed contract changes:

  rules:
    - name: allowed_values
      params:
        column: status
-       values: [active, inactive, pending, deleted]
+       values: [active, inactive, pending, deleted, archived]

Apply? [y/N]
```

### 0.4.2 Drift Detection
```bash
kontra drift contract.yml --threshold 0.1

# Alert when violation rate increases >10%
Drift detected:
  COL:email:not_null
    - 7 days ago: 2.0% nulls
    - today: 5.5% nulls
    - trend: +0.5%/day (will breach threshold in 3 days)
```

### 0.4.3 Goal-Directed Validation
```bash
# Only run specific rules
kontra validate contract.yml --only not_null,unique

# Only run rules for specific columns
kontra validate contract.yml --columns user_id,email

# Only run metadata rules (instant)
kontra validate contract.yml --tier metadata
```

### 0.4.4 Checkpoints
```bash
# Save named checkpoint
kontra validate contract.yml --checkpoint production

# Compare to checkpoint
kontra diff --checkpoint production
```

---

## v0.5+ — Future

### More Data Sources
- [ ] Snowflake
- [ ] BigQuery
- [ ] DuckDB (database mode)
- [ ] MySQL
- [ ] SQLite

### Enterprise Features
- [ ] Data catalog integration (DataHub, Amundsen)
- [ ] Lineage-aware validation
- [ ] Multi-contract orchestration

### Experimental
- [ ] Watch mode
- [ ] VS Code extension
- [ ] GitHub Action
- [ ] Anomaly detection rules
- [ ] Materialization / quarantine mode

---

## Release Checklist

### v0.2.0
- [x] LLM preset and output format
- [ ] Improved semantic type detection
- [ ] Validation explanations for all rules
- [ ] `kontra suggest` command

### v0.3.0
- [ ] Validation state snapshots
- [ ] State backend protocol (local, S3, postgres)
- [ ] `kontra diff` command
- [ ] Scout diff
- [ ] Semantic failure modes
- [ ] Rule severity
- [ ] Config file support

### v0.4.0
- [ ] Contract mutation proposals
- [ ] Drift detection
- [ ] Goal-directed validation
- [ ] Named checkpoints

---

## What Makes It Magic?

1. **One command to start**: `kontra init` takes you from zero to validating
2. **Intelligent inference**: We guess what rules you need from your data
3. **Speed**: Metadata-first means instant feedback on most validations
4. **Time-aware**: State management enables "what changed?" reasoning
5. **Agentic-native**: Structured outputs, semantic failures, pluggable backends
6. **Configuration as UX**: Every setting is intuitive, consistent, discoverable

The magic isn't hiding complexity—it's making the right thing easy and the complex thing possible.
