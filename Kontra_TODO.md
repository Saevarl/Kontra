# Kontra TODO

> Focus: new rules (with SQL pushdown where sensible), custom logic routes, planner/engine hardening, tests, and docs/DX.

---

## 1) New Rules (with pushdown where sensible)

### 1.1 `allowed_values`
**Goal:** Validate a column is within a small enum; SQL pushdown for small sets.

- [ ] **Spec:** `params: { column, values: [..] }`; optional `case_sensitive: false`.
- [ ] **Polars path:** `~pl.col(col).is_in(values)`.
- [ ] **SQL path (DuckDB):** `WHERE col NOT IN (...)`.
- [ ] **Guard:** Only push down if `len(values) <= ENUM_PUSH_CAP` (e.g., 100).
- [ ] **Rule ID:** `COL:<col>:allowed_values`.
- [ ] **Message parity:** `<col> has N value(s) outside allowed set`.
- [ ] **Registry/Planner:** Register in `RuleRegistry`; mark SQL-capable.
- [ ] **Projection:** `required_cols = [col]`.

**Manual contract**
```yaml
dataset: "tests/data/users.parquet"
rules:
  - name: allowed_values
    params: { column: status, values: ["active","inactive","pending"] }
```

**Tests**
- [ ] Unit: inside vs outside; (optional) case sensitivity.
- [ ] Integration: `pushdown=auto` vs `off` → identical results; projection effective.
- [ ] Large enum (> cap) remains Polars; verify planner kept residual.

---

### 1.2 `in_range`
**Goal:** Numeric range check with inclusive/exclusive bounds.

- [ ] **Spec:** `params: { column, min, max, inclusive: true }`.
- [ ] **Polars:** `is_between(min, max, closed="both"|"left"|"right"|"none")`.
- [ ] **SQL:** `WHERE col < min OR col > max` (adjust for exclusivity).
- [ ] **Rule ID:** `COL:<col>:in_range`.
- [ ] **Message:** `<col> outside [min,max] (inclusive)`.
- [ ] **Planner/Projection** wired.

**Manual contract**
```yaml
dataset: "tests/data/users.parquet"
rules:
  - name: in_range
    params: { column: age, min: 13, max: 95, inclusive: true }
```

**Tests**
- [ ] Boundary pass/fail per inclusivity.
- [ ] Parity `auto`/`off`.
- [ ] Mixed numeric types (int/float) behave sensibly.

---

### 1.3 `unique` (SQL pushdown v1.1)
**Goal:** Uniqueness per column.

- [ ] **Polars:** `is_duplicated()` + count duplicates.
- [ ] **SQL:** `GROUP BY col HAVING COUNT(*) > 1` (return violator sample).
- [ ] **Rule ID:** `COL:<col>:unique`.
- [ ] **Message:** `<col> has N duplicate(s)`.
- [ ] **Planner:** mark SQL-capable; projection `required_cols = [col]`.

**Manual contract**
```yaml
dataset: "tests/data/users.parquet"
rules:
  - name: unique
    params: { column: user_id }
```

**Tests**
- [ ] Dataset with injected duplicates.
- [ ] Parity `auto`/`off`; SQL wins on overlap.
- [ ] Performance sanity on large sample (e.g., 10M rows).

---

### 1.4 `freshness`
**Goal:** Ensure latest timestamp not older than a threshold.

- [ ] **Spec:** `params: { column, max_age: "30d" }` (support `h/m/d`).
- [ ] **Polars:** `df[col].max() >= now - timedelta(...)`.
- [ ] **SQL:** `SELECT MAX(col) < now() - INTERVAL ... → fail when true`.
- [ ] **Rule ID:** `COL:<col>:freshness`.
- [ ] **Message:** `<col> max ts is older than 30d`.

**Manual contract**
```yaml
dataset: "tests/data/users.parquet"
rules:
  - name: freshness
    params: { column: last_login, max_age: "30d" }
```

**Tests**
- [ ] Control clock near threshold.
- [ ] Parity `auto`/`off`.

---

### 1.5 `percent_null`
**Goal:** Nulls under a threshold.

- [ ] **Spec:** `params: { column, lte: 0.02 }`.
- [ ] **Polars:** `df[col].is_null().mean() <= p`.
- [ ] **SQL:** `SUM(col IS NULL)::DOUBLE / COUNT(*) <= p`.
- [ ] **Rule ID:** `COL:<col>:percent_null`.
- [ ] **Message:** `<col> null ratio p>N%`.

**Manual contract**
```yaml
dataset: "tests/data/users.parquet"
rules:
  - name: percent_null
    params: { column: email, lte: 0.02 }
```

**Tests**
- [ ] Controlled null injection (0%, 1%, 5%).
- [ ] Parity `auto`/`off`.

---

## 2) Custom Logic Routes

### 2.1 `custom_sql_check` (finalize)

- [ ] Placeholder: `${DATASET}` → bound table/view in DuckDB.
- [ ] Support `sample_limit` for violator capping.
- [ ] Message: `custom_sql_check returned N row(s)`.
- [ ] Planner requires no extra columns.

**Manual contract**
```yaml
dataset: "tests/data/users.parquet"
rules:
  - name: custom_sql_check
    params:
      sql: "SELECT user_id FROM read_parquet('${DATASET}') WHERE balance < 0"
      sample_limit: 50
```

**Tests**
- [ ] Generate negatives → failure.
- [ ] Bad SQL → executor error, engine survives.

---

### 2.2 `python_udf`

- [ ] **Spec:** `params: { fn: "pkg.mod.func", columns: [...], config: {...} }`.
- [ ] Load via `importlib`; call `fn(df, config=...)`.
- [ ] Normalize to `{passed, failed_count, message}`.
- [ ] **Rule ID:** `UDF:<fn>`.
- [ ] Projection = `columns`.

**Manual contract**
```yaml
dataset: "tests/data/users.parquet"
rules:
  - name: python_udf
    params:
      fn: "my_project.qc_rules.must_be_adult"
      columns: ["age","is_premium"]
      config: { min_age: 18 }
```

**Tests**
- [ ] Tiny UDF module in tests; deterministic fail/pass.
- [ ] UDF errors → graceful failure.

---

## 3) Planner / Engine Hardening

### 3.1 Pushdown Capability Flags

- [ ] Each rule advertises: `supports_sql: bool`, `semantics: strict|approx`.
- [ ] Planner pushes only when `supports_sql` and semantics allow.
- [ ] Env flag `KONTRA_PUSH_APPROX=1` allows approximate pushdown.

**Tests**
- [ ] Force approx off → stays residual.
- [ ] Force approx on → goes SQL; parity holds.

---

### 3.2 Violation Sampling

- [ ] Standardize `sample_limit` param.
- [ ] Both SQL and Polars paths respect it.

**Tests**
- [ ] With many failures, output ≤ sample_limit exemplars.

---

## 4) Test Suite Additions

### 4.1 Parity Matrix
- [ ] `pytest.mark.parametrize("pushdown", ["auto","off"])` for new rules.
- [ ] Assert identical `rules_passed/failed`.
- [ ] Verify projection behavior.

### 4.2 Boundary/Value Cases
- [ ] Min/max inclusivity (in_range).
- [ ] Case sensitivity (allowed_values).
- [ ] Datetime thresholds (freshness).

### 4.3 Negative Paths
- [ ] Bad SQL syntax → executor error, engine survives.
- [ ] UDF raises → handled cleanly.
- [ ] Executor crash → fallback to Polars.

### 4.4 Determinism
- [ ] Sort results by `rule_id` for comparison.
- [ ] Stable IDs even when YAML order changes.

---

## 5) Docs & DX

### 5.1 Rule Reference
- [ ] Each rule documented: params, examples, SQL pushdown notes.

### 5.2 Contract Gallery
- [ ] Add `contracts/examples/` with example YAMLs.
- [ ] Include README with sample commands.

### 5.3 CLI Help Polish
- [ ] Add `contra plan` → print compiled plan summary.

---

## 6) “Done” Definition

Each feature is done when:
- Spec settled & documented.
- Polars + SQL paths implemented and tested.
- Planner wired correctly.
- Rule IDs deterministic.
- Parity `auto/off` proven.
- Manual contract validated.
- Docs & example added.
- Bench sanity checked (10M-row scale if needed).

---

## Quick Starter Order
✅ Easy + High Impact:

1. `allowed_values`
2. `in_range`
3. `percent_null`
4. `freshness`
5. `unique` pushdown
6. `custom_sql_check` hardening
7. `python_udf` (Polars-only, DX unlock)
