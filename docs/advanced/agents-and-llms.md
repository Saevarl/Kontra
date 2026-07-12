# Agents & LLM Integration

Kontra is designed for programmatic use by LLM agents and services.

## Official MCP Server

Install the server with its PostgreSQL backend dependencies:

```bash
pip install "kontra[mcp-postgres]"
```

The server accepts configured datasource names and trusted contracts from a
single directory. It does not accept arbitrary database URLs, filesystem paths,
inline rules, or SQL from tool calls.

```bash
export KONTRA_CONFIG=/etc/kontra/config.yml
export KONTRA_MCP_CONTRACTS_DIR=/etc/kontra/contracts
export KONTRA_MCP_POSTGRES_URI='postgresql://kontra@db/kontra'
kontra-mcp
```

### Connect it to your coding agent

Most clients launch the server over `stdio` and manage its lifecycle for you.
Point them at `kontra-mcp` through [`uvx`](https://docs.astral.sh/uv/) so no
manual install step is needed.

**Claude Code, Claude Desktop, Cursor** — add to the client's MCP config
(`.mcp.json`, `claude_desktop_config.json`, or Cursor's `mcp.json`):

```json
{
  "mcpServers": {
    "kontra": {
      "command": "uvx",
      "args": ["--from", "kontra[mcp-postgres]", "kontra-mcp"],
      "env": {
        "KONTRA_MCP_POSTGRES_URI": "postgresql://kontra@db/kontra",
        "KONTRA_CONFIG": "/etc/kontra/config.yml",
        "KONTRA_MCP_CONTRACTS_DIR": "/etc/kontra/contracts"
      }
    }
  }
}
```

Claude Code can add the same entry from the CLI:

```bash
claude mcp add kontra \
  --env KONTRA_MCP_POSTGRES_URI=postgresql://kontra@db/kontra \
  --env KONTRA_CONFIG=/etc/kontra/config.yml \
  -- uvx --from "kontra[mcp-postgres]" kontra-mcp
```

**Codex CLI** — add to `~/.codex/config.toml`:

```toml
[mcp_servers.kontra]
command = "uvx"
args = ["--from", "kontra[mcp-postgres]", "kontra-mcp"]

[mcp_servers.kontra.env]
KONTRA_MCP_POSTGRES_URI = "postgresql://kontra@db/kontra"
KONTRA_CONFIG = "/etc/kontra/config.yml"
```

…or register it with the Codex CLI:

```bash
codex mcp add kontra \
  --env KONTRA_MCP_POSTGRES_URI=postgresql://kontra@db/kontra \
  --env KONTRA_CONFIG=/etc/kontra/config.yml \
  -- uvx --from "kontra[mcp-postgres]" kontra-mcp
```

### Tools and resources

The server exposes ten tools and three read-only resources. Every source-bearing
argument must be a configured datasource name; contracts are relative names
confined to the trusted contracts directory. Tool calls never accept a raw data
path, connection URL, inline rule, or SQL statement.

| Tool | Arguments | Returns |
|------|-----------|---------|
| `validate` | `datasource`, `contract`, `env?`, `tally?`, `sample?` | Validation result; the run is persisted |
| `profile` | `datasource`, `preset?`, `columns?`, `sample?`, `save?` | Dataset profile; persisted when `save` is set |
| `validation_history` | `contract`, `limit?`, `since?`, `failed_only?` | Bounded run summaries, newest first |
| `validation_diff` | `contract` | Diff of the two most recent runs |
| `get_validation_run` | `contract`, `run_id?` | One persisted run (latest if no ID); dataset URIs and fingerprints removed |
| `measure_failure_samples` | `datasource`, `contract`, `rule_id`, `n?`, `env?` | Example failing rows for one rule, measured now (not persisted) |
| `profile_history` | `datasource`, `limit?` | Bounded profile history, newest first |
| `profile_diff` | `datasource` | Diff of the two most recent persisted profiles |
| `compare_datasets` | `before`, `after`, `key?`/`before_key?`/`after_key?` | Row and key deltas between two datasources |
| `profile_relationship` | `left`, `right`, `on?`/`left_on?`/`right_on?` | Join cardinality and relational shape of two datasources |

| Resource | Returns |
|----------|---------|
| `kontra://health` | Server configuration/status metadata (backend type only, no credentials) |
| `kontra://rules` | Built-in measurement rules and their parameters |
| `kontra://datasources` | Configured datasource names and tables (no connection details) |

**Transformation probes.** `compare_datasets` and `profile_relationship` measure how two
datasources relate — row and key deltas, join cardinality — for reasoning about JOINs and
deduplication. They never emit raw rows and composite keys are capped at eight columns. Before
materializing, the server compares source metadata against a 100,000-row ceiling. Database row
counts may be catalog estimates, so this is a cost guardrail rather than a hard security boundary.
Override it with `KONTRA_MCP_MAX_PROBE_ROWS`.

**Failure samples.** `measure_failure_samples` runs a fresh measurement and returns example
failing rows for one rule, restricted to rule-relevant columns. Treat that output as live data
leaving your boundary. Persisted history keeps counts, not samples, so this tool always measures
against current data (`"measurement": "current"`). Setting `sample > 0` on `validate` also
returns live failure samples in that immediate result.

### Remote deployments

`stdio` is the default transport. For a remote deployment, use Streamable HTTP:

```bash
kontra-mcp --transport streamable-http --host 127.0.0.1 --port 8000
```

Kontra does not implement HTTP authentication itself. Non-loopback binds
therefore **fail closed**: a host other than `127.0.0.1`/`localhost` is refused
unless you pass `--allow-remote-unauthenticated`. Use that override only when an
authenticating proxy or an isolated network supplies the access boundary.

PostgreSQL stores validation runs and profiles requested with persistence enabled.
Current failure samples and transformation probes are transient. The server does
not make policy decisions, edit contracts, write annotations, recommend fixes,
or expose arbitrary SQL.

Credentials can also come from `DATABASE_URL` or the standard PostgreSQL `PG*`
environment variables. Server responses report only the backend type, never the
connection URI.

---

## Core Functions

### Validate Data

```python
import kontra
from kontra import rules

result = kontra.validate(df, rules=[
    rules.not_null("user_id"),
    rules.unique("email"),
])

result.passed        # bool
result.failed_count  # int
result.total_rows    # int

for rule in result.blocking_failures:
    print(f"{rule.rule_id}: {rule.failed_count} violations")
```

### Profile Data

```python
profile = kontra.profile(df)
print(f"Rows: {profile.row_count}")
for col in profile.columns:
    print(f"{col.name}: {col.dtype}, {col.null_rate:.0%} null")
```

---

## Transformation Probes

Measure transformation effects before and after changes.

### Compare (Before/After)

```python
result = kontra.compare(before_df, after_df, key="user_id")

# Key metrics
result.row_delta           # change in row count
result.duplicated_after    # keys appearing >1x in after
result.dropped             # keys lost in transformation
result.changed_rows        # rows where values differ
```

Use `key=` when the identifying column has the same name on both sides. For a
FK→PK comparison where the sides name it differently, use `before_key=`/`after_key=`:

```python
result = kontra.compare(tickets, orgs, before_key="organization_id", after_key="id")
```

### Profile Relationship (JOIN Structure)

```python
profile = kontra.profile_relationship(left_df, right_df, on="customer_id")

# Key metrics
profile.right_duplicate_keys         # keys appearing >1x in right
profile.right_key_multiplicity_max   # max rows per key in right
profile.left_keys_without_match      # left keys not in right
```

For differently named join keys, use `left_on=`/`right_on=` instead of `on=`
(same naming as pandas' `merge`):

```python
profile = kontra.profile_relationship(tickets, orgs, left_on="organization_id", right_on="id")
```

See [Transformation Probes](../reference/probes.md) for full schemas and all fields.

---

## Token-Optimized Output

All result types have a `.to_llm()` method that returns a compact, token-efficient string:

```python
# Validation result
result = kontra.validate("data.parquet", rules=[...])
print(result.to_llm())
# VALIDATION: my_contract PASSED
# PASSED: 5 rules

# With failures
# VALIDATION: my_contract FAILED
# BLOCKING: COL:email:not_null (523 nulls), COL:status:allowed_values (12 invalid)
# WARNING: COL:age:range (3 out of bounds)
# PASSED: 13 rules

# Profile
profile = kontra.profile("data.parquet")
print(profile.to_llm())
# DATASET: users.parquet (50K rows, 8 cols)
# COLS: user_id(int64,100%,unique), email(str,98%), status(str,100%,3vals), ...

# Compare
result = kontra.compare(before, after, key="order_id")
print(result.to_llm())
# COMPARE: 1000 → 1200 rows (+200)
# key: order_id
# keys: preserved=1000, dropped=0, added=0
# duplicated_keys: 50
# changes: 200 modified, 800 unchanged

# Diff
diff = kontra.diff("my_contract")
print(diff.to_llm())
# DIFF: my_contract 2024-01-10 -> 2024-01-12
# REGRESSION: COL:email:not_null (0 -> 523 nulls)
# RESOLVED: COL:age:range
```

### Output Methods

| Method | Description |
|--------|-------------|
| `.to_dict()` | Nested dictionary |
| `.to_json()` | JSON string |
| `.to_llm()` | Compact string for LLM context |

---

## Available Rules

```python
from kontra import rules

# Column checks
rules.not_null("column")
rules.unique("column")
rules.dtype("column", "int64")
rules.range("column", min=0, max=100)
rules.allowed_values("column", ["a", "b", "c"])
rules.regex("column", r"^[A-Z]{2}\d{4}$")

# Cross-column checks
rules.compare("end_date", "start_date", ">=")
rules.conditional_not_null("shipping_date", when="status == 'shipped'")

# Dataset checks
rules.min_rows(1000)
rules.max_rows(1000000)
```

---

## Service Integration

### Health Check

```python
health = kontra.health()

# {
#     "version": "0.x.x",
#     "status": "ok",
#     "config_found": True,
#     "config_path": "/app/.kontra/config.yml",
#     "rule_count": 18,
#     "rules": ["not_null", "unique", "range", ...]
# }

if health["status"] == "ok":
    print(f"Kontra {health['version']} ready")
```

### Config Path Injection

Services that don't run from a project directory need explicit config:

```python
# Set config path for service use
kontra.set_config("/etc/kontra/config.yml")

# All subsequent calls use this config
result = kontra.validate("prod_db.users", rules=[...])

# Check current setting
path = kontra.get_config_path()

# Reset to auto-discovery
kontra.set_config(None)
```

### Datasource Resolution

```python
# Resolve datasource name to URI
uri = kontra.resolve("users")           # searches all datasources
uri = kontra.resolve("prod_db.users")   # explicit datasource

# List available datasources
sources = kontra.list_datasources()
# {
#     "prod_db": ["users", "orders", "products"],
#     "local_data": ["events", "metrics"],
# }
```

### Rule Discovery

```python
rules_list = kontra.list_rules()

for rule in rules_list:
    print(f"{rule['name']} ({rule['scope']})")
    print(f"  {rule['description']}")
    print(f"  Params: {rule['params']}")

# not_null (column)
#   Fails where column contains NULL values
#   Params: {'column': 'required', 'include_nan': 'optional'}
```

---

## Suggested Rules

When an agent needs to generate validation rules from data:

```python
profile = kontra.profile("data.parquet", preset="interrogate")
suggestions = kontra.draft(profile)

# Filter by confidence
high_confidence = suggestions.filter(min_confidence=0.9)

# Get as dict for validation
rules = high_confidence.to_dict()
result = kontra.validate("data.parquet", rules=rules)
```

**Note:** Suggested rules are heuristic. They reflect observed patterns in the data, not ground truth. Agents should present them as starting points, not authoritative contracts.

---

## Error Handling

```python
from kontra.errors import (
    KontraError,           # base class
    ContractNotFoundError,
    InvalidDataError,
    ConnectionError,
)

try:
    result = kontra.validate("data.parquet", "contract.yml")
except ContractNotFoundError as e:
    return {"error": "contract_not_found", "message": str(e)}
except InvalidDataError as e:
    return {"error": "invalid_data", "message": str(e)}
except RuntimeError as e:
    # File access errors (missing files, permissions)
    return {"error": "data_access_error", "message": str(e)}
except KontraError as e:
    return {"error": "kontra_error", "message": str(e)}
```

---

## Workflow Example

### Transformation Pipeline

1. **Profile relationship** before writing JOIN:
```python
profile = kontra.profile_relationship(orders, customers, on="customer_id")
# Check: profile.right_key_multiplicity_max > 1 means duplicates
```

2. **Write transformation** based on profile insights

3. **Compare** to measure transformation effects:
```python
result = kontra.compare(orders, joined_result, key="order_id")
# Check: result.duplicated_after > 0 means key duplication
```

4. **Validate** final output:
```python
result = kontra.validate(final_df, rules=[
    rules.unique("order_id"),
    rules.not_null("customer_name"),
])
```

### Agent-Callable Function

```python
def validate_data(data_source: str, contract: str) -> dict:
    """Agent-callable validation function."""

    result = kontra.validate(data_source, contract=contract)

    response = {
        "passed": result.passed,
        "total_rows": result.total_rows,
        "summary": result.to_llm(),
    }

    if result.blocking_failures:
        failure = result.blocking_failures[0]
        response["status"] = "blocked"
        response["worst_rule"] = {
            "id": failure.rule_id,
            "message": failure.message,
            "failed_count": failure.failed_count,
            "owner": failure.context.get("owner") if failure.context else None,
        }
    elif result.warnings:
        response["status"] = "warnings"
    else:
        response["status"] = "passed"

    return response
```

### Contracts with Severity and Context

```yaml
rules:
  - name: not_null
    params: { column: user_id }
    severity: blocking
    context:
      owner: data_platform
      fix_hint: User ID is required

  - name: range
    params: { column: age, min: 0 }
    severity: warning
```

See [Rule Context](../reference/contracts.md#context) for details.
