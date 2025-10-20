# Kontra – MVP (v1.0) Scope Definition (v1.2)

**Purpose:**  
This document defines the minimum feature set required for Kontra to deliver immediate, production-grade value to data engineering teams.  
v1.0 focuses on **robustness, performance, and developer trust**, not breadth. It proves the architectural foundation through real-world scale and reliability.

---

## 1. Core Validation Engine

### ✅ Scope
- **Architecture Completeness:**  
  - Implements the Rule Registry, Factory, and Validation Engine architecture.  
  - Supports single-pass, streaming validation via Polars.  
  - Provides deterministic execution (identical input → identical output).

- **Rule Execution Model:**  
  - Lazy, vectorized evaluation through Polars.  
  - SQL Pushdown support for database connectors.  
  - Supports hierarchical rules: column-level and dataset-level.

- **Supported Rules (v1.0):**

| Category | Rule | Description |
|-----------|------|-------------|
| **Column** | `not_null` | Ensures no null values. |
|  | `unique` | Ensures uniqueness per column. |
|  | `allowed_values` | Restricts to defined enumerations. |
|  | `dtype` | Checks column type (string, int, float, date). |
|  | `regex` | Validates values against a regex pattern. |
| **Dataset** | `min_rows` | Enforces a minimum record count. |
|  | `max_rows` | Enforces a maximum record count. |
|  | `custom_sql_check` | Executes user-specified DuckDB SQL logic. |

> These rules cover 80% of early adopter data validation needs while testing the core registry and query planning architecture.

### 🚫 Out of Scope
- Cross-table or relational integrity rules.  
- Conditional or dependency-based rules (`if / then`).  

---

## 2. Supported Connectors & Data Formats

### ✅ Scope
- **Local Filesystem**
  - Parquet (`.parquet`)
  - CSV (`.csv`)

- **Cloud Storage**
  - Amazon S3 (Parquet, CSV via streaming and predicate pushdown)

- **Databases (SQL Pushdown)**
  - PostgreSQL  
  - Snowflake

### 🧩 Technical Goals
- Predicate pushdown for Parquet.  
- Streamed loading of large datasets (20GB+).  
- Secure credential loading from environment variables or `.env` files.  

### 🚫 Deferred
- Google Cloud Storage (GCS), Azure Blob.  
- BigQuery and Databricks connectors.  
- Incremental validation for partitioned data.

---

## 3. Performance & Scale Targets

Performance is a **release gate** — all targets must be met before GA.

| Metric | Target | Environment |
|---------|---------|--------------|
| Dataset Size | 100M rows, 10 columns | Parquet in S3 |
| Runtime | ≤ 3 minutes | GitHub Actions runner |
| Memory | ≤ 2GB RAM | Streaming mode |
| Rule Count | ≥ 10 rules | No degradation |

### 🧪 Benchmark Suite
A reproducible benchmark suite will measure:
- CPU utilization  
- Polars execution time  
- SQL pushdown efficiency  
- Parquet bytes scanned vs total size

> Performance validation is part of CI; benchmark regression must fail the build.

---

## 4. Remediation & Actions

### ✅ Scope
- **QuarantineAction**
  - Writes failing rows to local or S3 storage.  
  - Schema: original columns + `_error_reason`, `_violated_rule_id`.  
  - Idempotent and deterministic.  

- **AlertAction (Slack)**  
  - Sends summary to a Slack webhook channel.  
  - Message includes failing rule IDs, counts, and DLQ links.

### 🚫 Deferred
- **AsanaAction / JiraAction** — project management integration.  
- **ReplayAction** — reprocessing quarantined datasets.

### 🔒 Quality Targets
- 100% exception-safe.  
- Retry logic with exponential backoff for transient errors.  
- DLQ integrity validated via schema checksum.

---

## 5. Integrations & Developer Experience

### ✅ Scope
- **CLI**
  - `Kontra validate` fully implemented.  
  - Supports `--data`, `--no-actions`, `--output-format json`.  
  - Rich output + deterministic JSON mode.  
  - Exit codes standardized (0 success, 1 validation fail, 2+ system error).

- **Python SDK**
  ```python
  from Kontra import validate

  result = validate("Kontract.yml", data="s3://lake/users.parquet")
  if not result.passed:
      result.quarantine()
  ```

- **Dagster Integration (dagster-Kontra)**  
  - Native `@op` wrapping validation.  
  - Returns `AssetCheckResult`.  
  - Supports parameterized dataset Kontracts.

### 🚫 Deferred
- AirflowOperator, dlt Transformer, Airbyte Docker integration (target v1.1–v1.2).

### 💡 Quality Gates
- Full CLI UX spec implemented.  
- Type-safe SDK with Pydantic models.  
- 90%+ unit test coverage on engine and SDK.

---

## 6. Intelligent Tooling (Deferred to v1.1)

| Feature | Description |
|----------|--------------|
| `Kontra infer` | Profile a dataset to generate draft Kontracts. |
| `Kontra docs` | Generate Markdown documentation from Kontracts. |
| `Generative Data Fixer` | Suggest remediation logic using LLMs. |

> These will build upon the v1.0 engine foundation once performance and reliability are proven.

---

## 7. Release Criteria

A v1.0 release candidate must satisfy all criteria below.

| Category | Requirement |
|-----------|--------------|
| **Reliability** | All CLI and SDK paths tested end-to-end. |
| **Determinism** | Repeated runs produce identical outputs. |
| **Performance** | Meets benchmark targets under load. |
| **Documentation** | README, CLI help, Quickstart complete. |
| **Test Coverage** | ≥ 90% for core modules. |
| **Compatibility** | Python 3.10+, Linux/macOS. |
| **Security** | No plaintext secrets, ephemeral credential loading. |
| **CI/CD Readiness** | JSON output validated against schema 1.0. |

---

## 8. Post-v1.0 Roadmap

| Version | Theme | Focus |
|----------|--------|--------|
| **v1.1** | Operationalization | Jira/Asana, Replay Workflow, Incremental Validation |
| **v1.2** | Intelligence | `Kontra infer`, `Kontra docs`, Generative Fixer |
| **v1.3** | Enterprise | BigQuery, Databricks, GCS |
| **v1.4** | Visualization | Self-hosted UI and report dashboard |

---

## ✅ Summary

**Kontra v1.0** establishes a production-grade, performance-first validation foundation.  
It is fast, deterministic, and developer-focused — providing confidence in data quality at scale.

All future innovation (AI remediation, live dashboards, enterprise integrations) builds upon this core.

