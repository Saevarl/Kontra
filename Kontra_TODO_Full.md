# Kontra TODO (Full Consolidated)

> Combines actionable features, CSV pushdown handling, and all remaining gaps from the latest project status brief.

---

## 0) Final Engine Stabilization (from Status Brief)

### 0.1 Projection Correctness Tests
- [ ] **pushdown=on:** required=9, loaded=6, avail=19
- [ ] **pushdown=off:** required=9, loaded=9, avail=19
- [ ] All rules pushed down → Polars path skipped, total width/rows stable.
- [ ] No rules pushed → `auto` vs `off` identical results.
- [ ] CSV + S3 schema peeking returns correct columns.
- [ ] Deterministic merge order, SQL overrides overlaps.

### 0.2 Executor Failure Safety
- [ ] SQL executor exceptions fall back to Polars gracefully.
- [ ] Failures in compile/execute/introspect surface as warnings, not crashes.

### 0.3 Deprecation Nudge
- [ ] `--sql-engine none` prints: “⚠️  Deprecated; use --pushdown off.”
- [ ] Tested in CLI (one‑time yellow line).

### 0.4 Coverage Block (JSON only)
- [ ] Add optional coverage object:
```json
"coverage": {
  "rules_total": 9,
  "rules_sql": 3, "rules_failed_sql": 2,
  "rules_polars": 6, "rules_failed_polars": 1,
  "validated_columns": ["…"]
}
```
- [ ] Excluded from Rich output.
- [ ] Shown in JSON reporter for CI dashboards.

### 0.5 Message Parity
- [ ] SQL messages normalized to Polars wording (e.g. “contains null values”).

### 0.6 Docs Update
- [ ] README section “How pushdown works” diagram.
- [ ] Explain req/loaded/avail metrics in one‑liner.
- [ ] Confirm CLI label shows: `engine=duckdb+polars (pushdown: on)`.

---

## 1) New Rules (with pushdown where sensible)

*(same as earlier file — unchanged for brevity)*

Includes:
- 1.1 `allowed_values`
- 1.2 `in_range`
- 1.3 `unique`
- 1.4 `freshness`
- 1.5 `percent_null`

Each with Polars + SQL path, planner wiring, parity tests, and manual contracts.

---

## 2) Custom Logic Routes

- 2.1 `custom_sql_check` (finalize placeholders, sampling)
- 2.2 `python_udf` (import‑path based)

Both implemented & tested per earlier checklist.

---

## 3) Planner / Engine Hardening

### 3.1 Pushdown Capability Flags
- [ ] Add `supports_sql`, `semantics` attributes to rules.
- [ ] Planner only pushes strict semantics (opt‑in for approx).
- [ ] Env: `KONTRA_PUSH_APPROX=1` to enable approximate.

### 3.2 Violation Sampling
- [ ] Standardize `sample_limit` for both SQL + Polars.
- [ ] Cap violation rows in all reporters.

---

## 4) Test Suite Additions

### 4.1 Parity Matrix
- [ ] Parametrize `pushdown` in tests: auto/off.
- [ ] Assert identical pass/fail counts and stats.

### 4.2 Boundary & Value Tests
- [ ] Min/max inclusivity.
- [ ] Case sensitivity for `allowed_values`.
- [ ] Datetime thresholds for `freshness`.

### 4.3 Negative Path Tests
- [ ] Bad SQL syntax → executor error surfaced, engine survives.
- [ ] UDF exception → handled cleanly.
- [ ] Executor boom → fallback to Polars confirmed.

### 4.4 Determinism
- [ ] Sort results by `rule_id` for test comparisons.
- [ ] Rule ID stable across YAML order changes.

---

## 5) Docs & Developer Experience

### 5.1 Rule Reference
- [ ] Each rule documented: params, examples, pushdown notes.

### 5.2 Contracts Gallery
- [ ] `contracts/examples/` folder with YAMLs.
- [ ] Include README with “Try this” CLI samples.

### 5.3 CLI & Reporting
- [ ] Add `contra plan` → prints compiled plan summary.
- [ ] Add coverage block display in `--json` output only.

---

## 6) “Done” Definition

A feature/rule is done when:
- Spec documented and approved.
- Polars + SQL paths implemented & tested.
- Planner wiring verified.
- Rule IDs deterministic, messages consistent.
- Parity proven (`auto` vs `off`).
- Manual contract validated locally.
- Docs and examples updated.
- Optional perf sanity (10M rows) passes.

---

## 7) CSV + SQL Pushdown Handling

**Goal:** Ensure CSV sources (local/S3) behave like Parquet under SQL pushdown.

### Implementation
- [ ] Add executor logic for both `read_csv_auto()` and CSV→Parquet staging (via Polars).
- [ ] Env var: `KONTRA_SQL_CSV_MODE=auto|duckdb|parquet`.
- [ ] Default = `auto`: try DuckDB direct, fallback to staging.
- [ ] Use `${TABLE}` placeholder for format‑agnostic SQL.
- [ ] Projection respected for all modes.

### Tests
- [ ] Local CSV → DuckDB direct works with `custom_sql_check`.
- [ ] Remote CSV (S3/HTTP) → ensure `httpfs` loaded, creds honored.
- [ ] Fallback: `KONTRA_SQL_CSV_MODE=parquet` → temp parquet created.
- [ ] Projection → only required columns selected.
- [ ] Error handling → bad path/creds: `on_error: fail|warn|skip`.
- [ ] Large CSV → Parquet staging faster, results identical.
- [ ] Determinism → identical results across CSV and staged paths.

**Manual contract**
```yaml
dataset: "s3://bucket/users.csv"
rules:
  - name: custom_sql_check
    params:
      sql: |
        SELECT user_id FROM ${TABLE}
        WHERE is_premium AND TRY_CAST(age AS INTEGER) < 18
      on_error: "fail"
```

---

## 8) Quick Starter Order

1. ✅ `allowed_values`
2. ✅ `in_range`
3. ✅ `percent_null`
4. ✅ `freshness`
5. ✅ `unique` pushdown
6. ✅ `custom_sql_check` hardening
7. ✅ `python_udf` (Polars-only)
8. ✅ CSV pushdown coverage
9. ✅ Engine freeze tests + coverage + deprecation

---

**After all items complete:**  
Cut `v0.1.1`, mark `engine.py` stable (“don’t-touch”), and treat new features as registry additions only.
