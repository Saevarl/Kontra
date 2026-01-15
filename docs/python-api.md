# Kontra Python API

Complete guide to using Kontra as a Python library.

## Installation

```bash
pip install kontra

# With database support
pip install kontra[postgres]
pip install kontra[sqlserver]
pip install kontra[all]
```

## Quick Start

```python
import kontra
import polars as pl

# Validate a DataFrame
df = pl.read_parquet("data.parquet")
result = kontra.validate(df, "contract.yml")

if result.passed:
    print("All rules passed!")
else:
    print(f"{result.failed_count} rules failed")

# Profile data
profile = kontra.scout(df)
print(profile)
# Profile(users.parquet)
#   Rows: 1,000,000
#   Columns: 12
#   Columns: user_id(int64), email(str), status(str), ...
```


---

## Validation

### Basic Usage

```python
import kontra
import polars as pl
import pandas as pd

# From DataFrame (Polars)
df = pl.read_parquet("data.parquet")
result = kontra.validate(df, "contract.yml")

# From DataFrame (pandas) - auto-converted to Polars
pdf = pd.read_parquet("data.parquet")
result = kontra.validate(pdf, "contract.yml")

# From file path
result = kontra.validate("data.parquet", "contract.yml")

# From named datasource (defined in .kontra/config.yml)
result = kontra.validate("users", "contract.yml")
result = kontra.validate("prod_db.users", "contract.yml")
```

### Inline Rules (No Contract File)

```python
from kontra import rules

# Using helper functions (recommended)
result = kontra.validate(df, rules=[
    rules.not_null("user_id"),
    rules.unique("email"),
    rules.range("age", min=0, max=150),
    rules.allowed_values("status", ["active", "inactive", "pending"], severity="warning"),
    rules.regex("email", r"^[\w.-]+@[\w.-]+\.\w+$"),
    rules.dtype("created_at", "datetime"),
])

# Mix contract and inline rules
result = kontra.validate(df, "base_contract.yml", rules=[
    rules.freshness("updated_at", max_age="24h"),
])

# Dict syntax also supported (matches YAML contract format)
result = kontra.validate(df, rules=[
    {"name": "not_null", "params": {"column": "user_id"}},
    {"name": "unique", "params": {"column": "email"}},
])
```

### Rules Helper Reference

```python
from kontra import rules

# Column rules
rules.not_null("column")
rules.not_null("column", include_nan=True)  # Also catch NaN values
rules.unique("column")
rules.dtype("column", "int64")  # int64, float64, string, datetime, bool
rules.range("column", min=0, max=100)
rules.allowed_values("column", ["a", "b", "c"])
rules.regex("column", r"pattern")

# Dataset rules
rules.min_rows(1000)
rules.max_rows(1000000)
rules.freshness("column", max_age="24h")  # 24h, 7d, 1w

# All rules accept optional severity
rules.not_null("email", severity="warning")  # "blocking" | "warning" | "info"
```

### Validation Options

```python
result = kontra.validate(
    df,
    "contract.yml",

    # Execution control
    preplan="auto",      # "on" | "off" | "auto" (default: "auto")
    pushdown="auto",     # "on" | "off" | "auto" (default: "auto")
    projection=True,     # Column pruning (default: True)
    csv_mode="auto",     # "auto" | "duckdb" | "parquet" (default: "auto")

    # Environment
    env="production",    # Use environment from .kontra/config.yml

    # History
    save=True,           # Save to history (default: True, uses local storage)

    # Diagnostics
    stats="summary",     # "none" | "summary" | "profile" (default: "none")
)
```

### Validation Result

```python
result = kontra.validate(df, "contract.yml")

# Print shows summary
print(result)
# ValidationResult(my_contract) FAILED
#   Total: 10 rules | Passed: 8 | Failed: 2 | Warnings: 1
#   Blocking: COL:email:not_null, COL:status:allowed_values

# Overall status
result.passed          # bool - True if all blocking rules passed
result.dataset         # str - Dataset name/path
result.total_rules     # int - Total rules evaluated
result.passed_count    # int - Rules that passed
result.failed_count    # int - Rules that failed (blocking)
result.warning_count   # int - Rules that failed (warning severity)

# Detailed results
for rule in result.rules:
    rule.rule_id       # str - e.g., "COL:user_id:not_null"
    rule.name          # str - e.g., "not_null"
    rule.passed        # bool
    rule.failed_count  # int - Number of failing rows (see note below)
    rule.message       # str - Human-readable message
    rule.severity      # str - "blocking" | "warning" | "info"
    rule.source        # str - "metadata" | "sql" | "polars"

# Note: When rule.source == "metadata" (preplan), failed_count is 1 for any
# failure (meaning "at least one violation"). For exact counts, use preplan="off".

# Filter results
result.blocking_failures   # List of failed blocking rules
result.warnings           # List of failed warning rules

# Serialization
result.to_dict()          # Dict representation
result.to_json()          # JSON string
result.to_json(indent=2)  # Pretty JSON
result.to_llm()           # Token-optimized string for AI

# Statistics (if stats="summary" or "profile")
result.stats.row_count
result.stats.column_count
result.stats.duration_ms
```

### Dry Run (Validate Contract Only)

```python
# Check contract validity without executing against data
check = kontra.validate(
    df,
    "contract.yml",
    dry_run=True,
)

check.valid            # bool - Contract is valid
check.rules_count      # int - Number of rules
check.errors           # List of validation errors (if any)
check.warnings         # List of warnings (e.g., unknown columns)

# Also works with inline rules
check = kontra.validate(df, rules=[...], dry_run=True)
```

### Execution Plan (Debugging)

```python
# See what would be executed without running
plan = kontra.explain(df, "contract.yml")

plan.preplan_rules     # Rules resolved from metadata
plan.sql_rules         # Rules pushed to SQL
plan.polars_rules      # Rules executed in Polars
plan.required_columns  # Columns needed

# SQL that would be generated
for rule in plan.sql_rules:
    print(f"{rule.rule_id}: {rule.sql}")
```

---

## Scout (Profiling)

### Basic Usage

```python
import kontra

# Profile from DataFrame
profile = kontra.scout(df)

# Profile from file
profile = kontra.scout("data.parquet")

# Profile from named datasource
profile = kontra.scout("users")
profile = kontra.scout("prod_db.users")
```

### Presets

```python
# Lite - Schema and basic counts only (fastest)
profile = kontra.scout(df, preset="lite")

# Standard - Includes distributions, top values (default)
profile = kontra.scout(df, preset="standard")

# Deep - Full analysis with patterns, percentiles
profile = kontra.scout(df, preset="deep")

# LLM - Optimized data collection for AI context (minimal tokens)
profile = kontra.scout(df, preset="llm")
```

**Note on `preset` vs `to_llm()`:**
- `preset="llm"` controls what data is **collected** (minimal, token-optimized)
- `.to_llm()` controls how data is **formatted** (compact string output)

You can combine them:
- `preset="deep"` + `.to_llm()` → Full analysis, compact output
- `preset="llm"` + `.to_json()` → Minimal analysis, full JSON

### Scout Options

```python
profile = kontra.scout(
    df,
    preset="standard",

    # Column filtering
    columns=["user_id", "email", "status"],  # Only these columns

    # Sampling
    sample=10000,        # Sample N rows (default: all)

    # Analysis options
    include_patterns=True,   # Detect email, uuid, phone patterns
    top_n=10,               # Top N frequent values (default: 5)
    percentiles=[25, 50, 75, 90, 95, 99],

    # History
    save=True,              # Save to history for diffing
)
```

### Profile Result

```python
profile = kontra.scout(df)

# Print shows summary
print(profile)
# Profile(users.parquet)
#   Rows: 1,000,000
#   Columns: 12
#   Size: 45.2 MB
#   Columns: user_id(int64,unique), email(str,98%), status(str,4vals), ...

# Dataset level
profile.source         # str - File path or datasource name
profile.row_count      # int
profile.column_count   # int
profile.size_bytes     # int (if available)

# Column profiles
for col in profile.columns:
    col.name           # str
    col.dtype          # str - "int64", "string", "datetime", etc.
    col.null_count     # int
    col.null_pct       # float - 0.0 to 100.0
    col.unique_count   # int
    col.unique_pct     # float

    # Numeric columns
    col.min            # Minimum value
    col.max            # Maximum value
    col.mean           # Mean
    col.std            # Standard deviation
    col.percentiles    # Dict[int, float] - {25: 10.5, 50: 20.0, ...}

    # String columns
    col.min_length     # int
    col.max_length     # int
    col.avg_length     # float
    col.pattern        # str or None - "email", "uuid", "phone", etc.

    # Categorical (low cardinality)
    col.top_values     # List[Tuple[value, count]]
    col.all_values     # List[value] - If distinct count <= threshold

    # Flags
    col.is_unique      # bool - 100% unique
    col.is_nullable    # bool - Has any nulls
    col.is_constant    # bool - Single value

# Serialization
profile.to_dict()
profile.to_json()
profile.to_markdown()
profile.to_llm()       # Token-optimized string for AI
```

### Suggest Rules

```python
# Generate contract rules from profile
profile = kontra.scout(df, preset="deep")
suggestions = kontra.suggest_rules(profile)

# Works with any preset, but deeper presets give better suggestions:
# - lite: not_null, dtype (from schema + null counts)
# - standard: + unique, allowed_values (from top values)
# - deep: + range, regex (from percentiles, patterns)

# Returns list of suggested rules
for rule in suggestions:
    rule.name          # str - Rule type
    rule.params        # dict - Rule parameters
    rule.confidence    # float - 0.0 to 1.0
    rule.reason        # str - Why this rule was suggested

# Export as contract YAML
yaml_str = suggestions.to_yaml()
print(yaml_str)
# name: suggested_contract
# dataset: data.parquet
# rules:
#   - name: not_null
#     params: { column: user_id }
#   - name: unique
#     params: { column: user_id }
#   ...

# Export as JSON (inline rules format)
json_rules = suggestions.to_json()

# Export as dict (usable directly in validate)
rule_dicts = suggestions.to_dict()
result = kontra.validate(df, rules=rule_dicts)

# Save to file
suggestions.save("contracts/users.yml")

# Filter suggestions
high_confidence = suggestions.filter(min_confidence=0.9)
not_null_rules = suggestions.filter(name="not_null")
```

---

## History & Diff

Kontra automatically saves validation and profile runs for comparison over time.

### Validation History

```python
import kontra

# List past validation runs
runs = kontra.list_runs("my_contract")

for run in runs:
    run.id             # str - Unique run ID
    run.timestamp      # datetime
    run.passed         # bool
    run.total_rules    # int
    run.failed_count   # int
    run.dataset        # str
    run.fingerprint    # str - Dataset fingerprint

# Get specific run
run = kontra.get_run("my_contract", run_id="abc123")
run = kontra.get_run("my_contract")  # Latest run

# Get full result from a run
result = run.result    # ValidationResult object

# Check if history exists
if kontra.has_runs("my_contract"):
    ...
```

### Validation Diff

```python
# Compare latest to previous run
diff = kontra.diff("my_contract")

# Compare to specific time
diff = kontra.diff("my_contract", since="7d")      # 7 days ago
diff = kontra.diff("my_contract", since="24h")     # 24 hours ago
diff = kontra.diff("my_contract", since="2024-01-15")  # Specific date

# Compare two specific runs
diff = kontra.diff("my_contract", before="run_abc", after="run_xyz")

# Print shows summary
print(diff)
# Diff(my_contract) REGRESSED
#   2024-01-10 -> 2024-01-12
#   New failures: 2 | Resolved: 1

# Diff result
diff.has_changes       # bool
diff.improved          # bool - Fewer failures than before
diff.regressed         # bool - More failures than before

diff.before            # Run summary (timestamp, passed, counts)
diff.after             # Run summary

diff.new_failures      # Rules that started failing
diff.resolved          # Rules that stopped failing
diff.count_changes     # Rules where failure count changed

# Serialization
diff.to_dict()
diff.to_json()
diff.to_llm()          # Token-optimized for AI context
```

### Profile History

```python
# List past profile runs
profiles = kontra.list_profiles("data.parquet")

# Get specific profile
profile = kontra.get_profile("data.parquet")  # Latest
profile = kontra.get_profile("data.parquet", run_id="xyz")
```

### Profile Diff (Scout Diff)

```python
# Compare profiles over time
diff = kontra.scout_diff("data.parquet")
diff = kontra.scout_diff("data.parquet", since="7d")

diff.has_changes
diff.schema_changes    # Added/removed/changed columns
diff.stats_changes     # Significant statistical changes

for change in diff.changes:
    change.column      # str
    change.field       # str - "null_pct", "unique_count", etc.
    change.before      # Previous value
    change.after       # Current value
    change.delta       # Numeric change
    change.pct_change  # Percentage change
```

---

## Configuration

### Named Datasources

```python
import kontra

# Resolve datasource name to URI (for debugging/inspection)
uri = kontra.resolve("users")           # Searches all datasources
uri = kontra.resolve("prod_db.users")   # Explicit datasource

# List available datasources
sources = kontra.list_datasources()
# {
#     "prod_db": ["users", "orders", "products"],
#     "local_data": ["events", "metrics"],
# }

# Use in validation/scout (resolution happens automatically)
result = kontra.validate("users", "contract.yml")
profile = kontra.scout("prod_db.orders")
```

### Effective Configuration

```python
# Get resolved configuration
config = kontra.config()
config = kontra.config(env="production")

config.preplan         # str - "auto"
config.pushdown        # str - "auto"
config.projection      # bool - True
config.output_format   # str - "rich"
config.state_backend   # str - "local"
```

---

## Service/Agent Support

Functions designed for MCP servers, long-running services, and AI agents.

### Health Check

```python
import kontra

# Verify Kontra is working
health = kontra.health()

health["version"]       # str - e.g., "0.3.0"
health["status"]        # str - "ok" or "config_not_found"
health["config_found"]  # bool
health["config_path"]   # str or None
health["rule_count"]    # int - Number of registered rules
health["rules"]         # list - Available rule names

# Use in health endpoints
if health["status"] == "ok":
    print(f"Kontra {health['version']} ready with {health['rule_count']} rules")
```

### Rule Discovery

```python
# List all available validation rules
rules = kontra.list_rules()

for rule in rules:
    print(f"{rule['name']} ({rule['scope']})")
    print(f"  {rule['description']}")
    print(f"  Params: {rule['params']}")

# Example output:
# not_null (column)
#   Fails where column contains NULL values
#   Params: {'column': 'required'}
# range (column)
#   Fails where column values are outside [min, max] range
#   Params: {'column': 'required', 'min': 'optional', 'max': 'optional'}
```

### Config Path Injection

By default, Kontra discovers config from the current working directory (`.kontra/config.yml`).
For services that don't run from a project directory, set the config path explicitly:

```python
import kontra

# Set config path for service use
kontra.set_config("/etc/kontra/config.yml")

# All subsequent calls use this config
result = kontra.validate(df, rules=[...])
profile = kontra.scout("prod_db.users")

# Check current setting
path = kontra.get_config_path()  # "/etc/kontra/config.yml"

# Reset to auto-discovery
kontra.set_config(None)
```

---

## Output Formats

### JSON

```python
result = kontra.validate(df, "contract.yml")

# Compact JSON
json_str = result.to_json()

# Pretty JSON
json_str = result.to_json(indent=2)

# As dict
data = result.to_dict()
```

### LLM-Optimized

Token-efficient format for AI/LLM context:

```python
# Validation result for LLM
llm_str = result.to_llm()
# VALIDATION: my_contract FAILED
# BLOCKING: COL:email:not_null (523 nulls), COL:status:allowed_values (12 invalid)
# WARNING: COL:age:range (3 out of bounds)
# PASSED: 15 rules

# Profile for LLM
profile = kontra.scout(df)
llm_str = profile.to_llm()
# DATASET: users.parquet (1M rows, 12 cols)
# COLS: user_id(int64,100%,unique), email(str,98%), status(str,100%,4vals), ...

# Diff for LLM
diff = kontra.diff("my_contract")
llm_str = diff.to_llm()
# DIFF: my_contract 2024-01-10 -> 2024-01-12
# REGRESSION: COL:email:not_null (0 -> 523 nulls)
# RESOLVED: COL:age:range
```

### Markdown

```python
profile = kontra.scout(df)
md_str = profile.to_markdown()

# Returns formatted markdown table
```

---

## Error Handling

Kontra raises specific exceptions for different error types:

```python
import kontra
from kontra.errors import (
    KontraError,           # Base class
    ContractError,         # Contract issues
    ContractNotFoundError,
    ContractParseError,
    ContractValidationError,
    DataError,             # Data issues
    DataNotFoundError,
    DataFormatError,
    ConnectionError,       # Database issues
    PostgresConnectionError,
    SqlServerConnectionError,
    S3ConnectionError,
    ConfigError,           # Configuration issues
    ConfigParseError,
    UnknownEnvironmentError,
)

try:
    result = kontra.validate(df, "contract.yml")
except ContractNotFoundError as e:
    print(f"Contract not found: {e}")
except DataNotFoundError as e:
    print(f"Data not found: {e}")
except KontraError as e:
    print(f"Kontra error: {e}")
```

All exceptions include helpful suggestions:

```python
try:
    kontra.validate(df, "missing.yml")
except ContractNotFoundError as e:
    print(e)
    # Contract file not found: missing.yml
    #
    # Try:
    #   - Check the file path is correct
    #   - Ensure the file exists and is readable
    #   - Create a contract at: missing.yml
```

---

## Complete Example

```python
import kontra
from kontra import rules
import polars as pl

# Load data
df = pl.read_parquet("data/users.parquet")

# Profile first (optional)
profile = kontra.scout(df, preset="standard")
print(f"Dataset: {profile.row_count:,} rows, {profile.column_count} columns")

# Generate suggested rules
suggestions = kontra.suggest_rules(profile)
high_conf = suggestions.filter(min_confidence=0.8)
print(f"Suggested {len(high_conf)} rules")

# Save suggestions as contract
high_conf.save("contracts/users.yml")

# Or use inline rules directly
result = kontra.validate(df, rules=[
    rules.not_null("user_id", severity="blocking"),
    rules.unique("user_id", severity="blocking"),
    rules.not_null("email", severity="warning"),
    rules.allowed_values("status", ["active", "inactive", "pending", "deleted"]),
    rules.range("age", min=0, max=150),
])

# Check result
if result.passed:
    print(f"SUCCESS: All {result.total_rules} rules passed")
else:
    print(f"FAILED: {result.failed_count} blocking failures")
    for rule in result.blocking_failures:
        print(f"  - {rule.rule_id}: {rule.message}")

    if result.warnings:
        print(f"WARNINGS: {result.warning_count}")
        for rule in result.warnings:
            print(f"  - {rule.rule_id}: {rule.message}")

# Compare to previous run
if kontra.has_runs("users"):
    diff = kontra.diff("users")
    if diff.regressed:
        print("WARNING: Quality regressed since last run!")
        for failure in diff.new_failures:
            print(f"  NEW: {failure.rule_id}")

# Send to LLM for analysis
llm_context = f"""
{profile.to_llm()}

{result.to_llm()}

{diff.to_llm() if diff else "No previous runs"}
"""
```

---

## API Reference Summary

### Core Functions

| Function | Description |
|----------|-------------|
| `kontra.validate(data, contract, **opts)` | Validate data against contract |
| `kontra.validate(data, rules=[...], **opts)` | Validate with inline rules |
| `kontra.scout(data, **opts)` | Profile data |
| `kontra.suggest_rules(profile)` | Generate rules from profile |
| `kontra.explain(data, contract)` | Show execution plan |
| `kontra.diff(contract, **opts)` | Compare validation runs |
| `kontra.scout_diff(source, **opts)` | Compare profile runs |

### History Functions

| Function | Description |
|----------|-------------|
| `kontra.list_runs(contract)` | List validation runs |
| `kontra.get_run(contract, run_id=None)` | Get specific run |
| `kontra.has_runs(contract)` | Check if history exists |
| `kontra.list_profiles(source)` | List profile runs |
| `kontra.get_profile(source, run_id=None)` | Get specific profile |

### Configuration Functions

| Function | Description |
|----------|-------------|
| `kontra.resolve(name)` | Resolve datasource name to URI |
| `kontra.list_datasources()` | List configured datasources |
| `kontra.config(env=None)` | Get effective configuration |

### Service/Agent Support

| Function | Description |
|----------|-------------|
| `kontra.health()` | Health check (version, config status, rule count) |
| `kontra.list_rules()` | List available rules with descriptions |
| `kontra.set_config(path)` | Set config path for service use |
| `kontra.get_config_path()` | Get current config path override |

### Rules Helpers

| Function | Description |
|----------|-------------|
| `rules.not_null(column, include_nan=False)` | Column has no nulls (optionally NaN) |
| `rules.unique(column)` | Column values are unique |
| `rules.dtype(column, type)` | Column has expected type |
| `rules.range(column, min, max)` | Values within range |
| `rules.allowed_values(column, values)` | Values in allowed set |
| `rules.regex(column, pattern)` | Values match pattern |
| `rules.min_rows(n)` | Dataset has minimum rows |
| `rules.max_rows(n)` | Dataset has maximum rows |
| `rules.freshness(column, max_age)` | Data is recent |

### Result Types

| Type | Key Properties |
|------|----------------|
| `ValidationResult` | `passed`, `rules`, `to_json()`, `to_llm()` |
| `RuleResult` | `rule_id`, `passed`, `failed_count`, `severity` |
| `Profile` | `row_count`, `columns`, `to_json()`, `to_llm()` |
| `ColumnProfile` | `name`, `dtype`, `null_pct`, `unique_count` |
| `Diff` | `has_changes`, `regressed`, `new_failures`, `to_llm()` |
| `Suggestions` | `to_yaml()`, `to_json()`, `save()`, `filter()` |
