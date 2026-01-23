# Task: Create User Primary Phone View

## Context

You have two data files:
- `data/users.parquet` - User records
- `data/phones.parquet` - Phone numbers (each user can have multiple phones)

## Goal

Create a transformation that produces `user_with_primary`:
- One row per user
- Includes the user's primary phone number
- Schema: `user_id`, `name`, `primary_phone`

## Success Criteria

Your output must pass validation against `target_contract.yml`:

```python
import kontra

result = kontra.validate(your_output, "target_contract.yml")
assert result.passed
```

## Available Tools

You have access to:
- `polars` for data manipulation
- `kontra` for validation and data quality measurement

## Deliverable

Write Python code that:
1. Loads the source data
2. Transforms it to produce `user_with_primary`
3. Validates the output against the contract
