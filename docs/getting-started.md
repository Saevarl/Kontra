# Getting Started

This guide takes you from install to validated dataset.

## Install

```bash
pip install kontra
```

For database support:
```bash
pip install kontra[postgres]    # PostgreSQL
pip install kontra[sqlserver]   # SQL Server
pip install kontra[s3]          # S3
```

## 1. Initialize Project

```bash
kontra init
```

This creates `.kontra/config.yml` and a `contracts/` directory.

## 2. Profile Your Data

```bash
kontra scout data.parquet
```

Output:
```
Dataset: data.parquet
Rows: 50,000 | Columns: 8

Columns:
  user_id     int64    100% non-null, unique
  email       string   98% non-null
  status      string   100% non-null, 3 values: active, inactive, pending
  age         int64    100% non-null, range: [18, 95]
  created_at  datetime 100% non-null
```

## 3. Generate a Contract

```bash
kontra scout data.parquet --suggest-rules > contracts/users.yml
```

Review the generated file and adjust as needed:

```yaml
# contracts/users.yml
name: users_quality
datasource: data.parquet

rules:
  - name: not_null
    params: { column: user_id }

  - name: unique
    params: { column: user_id }

  - name: not_null
    params: { column: email }

  - name: allowed_values
    params:
      column: status
      values: [active, inactive, pending]

  - name: range
    params:
      column: age
      min: 0
      max: 150
```

## 4. Validate

```bash
kontra validate contracts/users.yml
```

Output:
```
PASSED - data.parquet (5 rules)
  COL:user_id:not_null       ✓
  COL:user_id:unique         ✓
  COL:email:not_null         ✓
  COL:status:allowed_values  ✓
  COL:age:range              ✓
```

**If this worked, you're done.** You now have a validated dataset.

---

## When You Need More

| Task | Doc |
|------|-----|
| Use Kontra in Python code | [Python API](python-api.md) |
| Connect to PostgreSQL or SQL Server | [Configuration](reference/config.md) |
| See all available rules | [Rules Reference](reference/rules.md) |
| Compare validation runs over time | [State & Diff](advanced/state-and-diff.md) |
| Integrate with LLM agents | [Agents & Services](advanced/agents-and-llms.md) |
