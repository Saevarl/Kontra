# Kontra Roadmap

## Completed

### v0.1 — Core Engine ✅
- 10 built-in validation rules
- Three-tier execution: preplan → SQL pushdown → Polars
- Data sources: Parquet, CSV, PostgreSQL, SQL Server, S3
- CLI: `kontra validate`, `kontra scout`, `kontra init`
- Rich terminal output with JSON/Markdown options

### v0.2 — Performance ✅
- SQL pushdown to DuckDB (Parquet/CSV)
- SQL pushdown to PostgreSQL
- SQL pushdown to SQL Server
- Parquet metadata preplan (min/max/null_count)
- Column projection (load only needed columns)
- Scout presets: lite, standard, deep, llm

### v0.3 — State & Python API ✅ (Current)
- **State persistence**: local, S3, PostgreSQL backends
- **Diff commands**: `kontra diff`, `kontra scout-diff`
- **Config file**: `.kontra/config.yml` with environments
- **Named datasources**: `prod_db.users` references
- **Python API**: `kontra.validate()`, `kontra.scout()`
- **Inline rules**: `rules.not_null()`, `rules.unique()`, etc.
- **Rule severity**: blocking, warning, info
- **Failure modes**: structured "why this failed" data

---

## Planned

### v0.4 — Cross-Column Rules & dbt Integration

**Cross-column validation** (table stakes)
```yaml
# Compare two columns
- name: compare
  params: { left: end_date, right: start_date, op: ">=" }

# Conditional not-null
- name: conditional_not_null
  params: { column: shipping_date, when: "status == 'shipped'" }
```

**dbt integration** - Separate package `kontra-dbt`
- [ ] Run contracts as dbt post-hooks
- [ ] Auto-discover models → datasources
- [ ] `dbt run` triggers validation

### v0.5 — Profile Storage & Custom SQL Pushdown

**Database persistence for Scout profiles**
- [ ] PostgreSQL backend for profile storage
- [ ] S3 backend for profile storage
- [ ] Profile retention policies

**Custom SQL pushdown**
- [ ] Push `custom_sql_check` directly to source database
- [ ] Auto-detect source type, execute SQL in-place
- [ ] Fallback to DuckDB+Polars for file sources

### v0.6 — Agent Power Features

**Contract mutation proposals**
```bash
kontra validate contract.yml --propose-fixes
# Suggests: add "archived" to allowed_values
```

**Drift detection**
```bash
kontra drift contract.yml --threshold 0.1
# Alert when violation rate trends upward
```

**Goal-directed validation**
```bash
kontra validate contract.yml --only not_null,unique
kontra validate contract.yml --columns user_id,email
```

### v0.7 — Integrations

- [ ] More data sources: Snowflake, BigQuery, MySQL
- [ ] Observability: OpenTelemetry, Prometheus metrics
- [ ] CI/CD: GitHub Action

---

## Future Ideas

- **Referential integrity** - `foreign_key` rule checking IDs exist in reference tables
- **Statistical range rule** - Explicit baseline_mean/std bounds (deterministic, not ML)
- **Schema evolution in Scout** - Enhance `scout_diff` to surface schema changes
- **Sampling for large datasets** - Opt-in for billion-row tables
- **Airflow/Dagster operators** - Convenience wrappers (Python API already works)
- **OpenLineage/DataHub** - Enterprise lineage integration
- **Web UI** - Dashboard for validation history (optional, keep core lightweight)
- **VS Code extension** - Inline contract editing

## Not Doing

These are explicitly out of scope for Kontra:

- **Alerting (Slack/PagerDuty)** - "Kontra measures, consumers decide." Use exit codes + CI/CD.
- **ML-based anomaly detection** - Non-deterministic, breaks "same inputs → same outputs"
- **Great Expectations migration** - Complex mapping, not worth the maintenance burden

---

## Design Principles

1. **Zero-friction start**: `kontra init` from data to validation
2. **Intelligent defaults**: Infer rules from data itself
3. **Speed over ceremony**: Metadata-first, scan only when necessary
4. **Agentic-first**: Built for LLM integration (`.to_llm()` methods)
5. **Progressive disclosure**: Simple surface, infinite depth
