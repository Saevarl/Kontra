---
hide:
  - toc
---

# Measure data quality. Keep the decisions yours.

Kontra is a developer-first measurement engine for data contracts. It evaluates
declarative rules against files, databases, and DataFrames, then returns
structured results for humans, CI systems, and agents.

<div class="hero-actions" markdown>
[Get started](getting-started.md){ .md-button .md-button--primary }
[Python API](python-api.md){ .md-button }
[View on GitHub](https://github.com/Saevarl/Kontra){ .md-button }
</div>

```python
import kontra
from kontra import rules

result = kontra.validate("users.parquet", rules=[
    rules.not_null("user_id"),
    rules.unique("email"),
    rules.range("age", min=0, max=120),
])

result.passed
result.to_dict()   # structured output
result.to_llm()    # compact agent context
```

<div class="feature-grid" markdown>

<div class="feature-card" markdown>
### Runs where the data lives

Kontra resolves rules from metadata first, pushes eligible work into SQL, and
uses Polars only for the remaining measurements.
</div>

<div class="feature-card" markdown>
### One result model

CLI, Python, CI, and MCP consumers receive the same deterministic counts,
execution sources, severity metadata, and bounded explanations.
</div>

<div class="feature-card" markdown>
### Designed for composition

Kontra measures. Your application decides what blocks a deployment, opens an
incident, or needs human review.
</div>

</div>

## Choose a path

| If you want to… | Start here |
|---|---|
| Validate a file or table | [Getting started](getting-started.md) |
| Use DataFrames or inline rules | [Python API](python-api.md) |
| Define a reusable contract | [Contracts](reference/contracts.md) |
| Inspect all built-in measurements | [Rules](reference/rules.md) |
| Compare transformation inputs and outputs | [Transformation probes](reference/probes.md) |
| Connect Claude, Codex, Cursor, or another MCP client | [Agents and MCP](advanced/agents-and-llms.md) |
| Understand execution cost and exact counts | [Performance](advanced/performance.md) |

## Supported data sources

Parquet, CSV, PostgreSQL, SQL Server, ClickHouse, S3-compatible object storage,
Azure ADLS Gen2, Polars DataFrames, pandas DataFrames, and Python records all
enter the same measurement API.

```bash
pip install kontra
kontra profile users.parquet --draft > contract.yml
kontra validate contract.yml
```

Kontra is open source under the Apache License 2.0.
