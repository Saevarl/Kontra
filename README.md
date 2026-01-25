# Kontra

**Data quality validation for developers.**

Kontra validates datasets against declarative contracts. Define rules in YAML, run them against Parquet, CSV, PostgreSQL, or SQL Server. Get violation counts back.

```bash
pip install kontra
```

## 30-Second Example

```bash
# Profile your data
kontra profile data.parquet

# Draft a starting contract
kontra profile data.parquet --draft > contract.yml

# Validate
kontra validate contract.yml
```

Output:
```
PASSED - data.parquet (4 rules)
  COL:user_id:not_null      ✓
  COL:email:unique          ✓
  COL:status:allowed_values ✓
  COL:age:range             ✓
```

## What You Write

```yaml
# contract.yml
name: users_quality
datasource: data/users.parquet

rules:
  - name: not_null
    params: { column: user_id }

  - name: unique
    params: { column: email }

  - name: allowed_values
    params:
      column: status
      values: [active, inactive, pending]
```

## What You Get

- **18 built-in rules**: not_null, unique, range, regex, contains, length, freshness, and more
- **Fast execution**: Metadata analysis and SQL pushdown before loading data
- **Multiple sources**: Parquet, CSV, PostgreSQL, SQL Server, S3
- **Python API**: Use as a library with `kontra.validate(df, rules=[...])`
- **State tracking**: Compare runs over time with `kontra diff`

## Documentation

| Doc | Audience |
|-----|----------|
| [Getting Started](docs/getting-started.md) | New users |
| [Python API](docs/python-api.md) | Library users |
| [Rules Reference](docs/reference/rules.md) | Everyone |
| [Configuration](docs/reference/config.md) | Project setup |
| [Advanced Topics](docs/advanced/) | Agents, state, execution model |
| [Architecture](docs/reference/architecture.md) | Contributors |

## License

MIT
