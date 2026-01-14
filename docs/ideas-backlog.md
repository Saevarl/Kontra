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

*Last updated: 2026-01-13*
