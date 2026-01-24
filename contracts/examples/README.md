# Example Contracts

These contracts demonstrate Kontra's features. They reference example data paths - adjust `datasource` to point to your actual data.

## Examples

### 01_basic.yml
Common validation patterns:
- `not_null` / `unique` for primary keys
- `regex` for email format
- `allowed_values` for enums
- `range` for numeric bounds
- `min_rows` for volume checks

### 02_severity_and_context.yml
Production-ready patterns:
- `severity: blocking` - fails the validation
- `severity: warning` - flags but doesn't fail
- `severity: info` - informational only
- `context` - metadata for debugging (owner, fix_hint, tier)

### 03_advanced_rules.yml
Cross-column and conditional validation:
- `compare` - validate column relationships (dates, amounts)
- `conditional_not_null` - require values based on conditions
- `conditional_range` - conditional bounds checking

### 04_with_datasource.yml
Using named datasources from `.kontra/config.yml`:
- Reference tables as `datasource.table`
- Centralize connection strings
- Support for Postgres, SQL Server, S3, local files

## Running Examples

```bash
# Validate with a contract
kontra validate contracts/examples/01_basic.yml

# Override datasource at runtime
kontra validate contracts/examples/01_basic.yml --data path/to/data.parquet
```
