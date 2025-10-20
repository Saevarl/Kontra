# Kontra – Product Requirements Document (PRD v1.0)

**Purpose:**  
This PRD defines the functional, technical, and experiential requirements for Kontra v1.0 — a developer-first, high-performance data quality engine designed to validate datasets against declarative Kontracts.

---

## 1. Overview

### **Mission**
Ensure data reliability from prototype to production through a fast, extensible, and developer-centric validation engine.

### **Objectives**
- Validate large datasets efficiently (100M+ rows).  
- Deliver a frictionless developer experience via CLI and SDK.  
- Provide robust extensibility through a modular rule and plugin system.  
- Integrate seamlessly with modern orchestration and data workflows.

### **Non-Goals**
- Not a transformation framework (e.g., dbt, Spark).  
- Not an ETL tool (e.g., Fivetran, Airbyte).  
- Not a full workflow manager — integrates into existing ones.

---

## 2. Target Users

| Persona | Description | Primary Needs |
|----------|--------------|----------------|
| **Data Engineers** | Build and maintain data pipelines. | Automate data validation and failure handling. |
| **Analytics Engineers** | Work with dbt, Snowflake, and Dagster. | Validate outputs before publishing metrics. |
| **Data Quality Leads** | Own governance and trust. | Centralize validation and compliance. |

---

## 3. Product Philosophy (Kontra Tenets)

1. **Performance is Paramount** — Validation should never be the bottleneck.  
2. **Developer Experience is the Product** — If it’s not delightful in the terminal or SDK, it’s not done.  
3. **Extensibility is by Design** — Every component is pluggable and replaceable.  

---

## 4. Core Features (MVP Scope)

### **1. Core Validation Engine**
- Declarative `contract.yml` defining dataset expectations.  
- Rule-based architecture: modular `Rule` objects (e.g., `not_null`, `unique`).  
- Registry pattern for discovery and instantiation.  
- Polars-based execution with optional DuckDB SQL pushdown.

**Acceptance Criteria:**
- Engine validates dataset end-to-end with a single call.  
- Failing rules produce consistent, human-readable and JSON outputs.  
- Deterministic output for identical runs.  

---

### **2. Supported Rules (v1.0)**

| Category | Rule | Description |
|-----------|------|-------------|
| **Column** | `not_null` | Ensures no null values. |
|  | `unique` | Ensures unique column values. |
|  | `allowed_values` | Restricts to predefined values. |
|  | `dtype` | Validates expected type (int, str, date). |
|  | `regex` | Validates against regex patterns. |
| **Dataset** | `min_rows` | Ensures dataset exceeds a minimum size. |
|  | `max_rows` | Ensures dataset does not exceed a max size. |
|  | `custom_sql_check` | Executes user-specified SQL in DuckDB context. |

---

### **3. Data Connectors (v1.0)**

| Source | Formats | Capabilities |
|---------|----------|---------------|
| Local Filesystem | CSV, Parquet | Full validation, lazy streaming. |
| Amazon S3 | CSV, Parquet | Lazy loading, predicate pushdown. |
| PostgreSQL | Table | SQL pushdown. |
| Snowflake | Table | SQL pushdown. |

---

### **4. Actions & Remediation**

| Action | Description | v1.0 Scope |
|---------|--------------|------------|
| `QuarantineAction` | Writes failing rows to local or S3. | ✅ |
| `AlertAction` (Slack) | Sends summary to Slack webhook. | ✅ |
| `AsanaAction`, `JiraAction` | Create project tickets for failures. | ❌ (v1.1) |
| `ReplayAction` | Reprocess quarantined data. | ❌ (v1.1) |

**Acceptance Criteria:**
- Actions execute sequentially, never crash engine on failure.  
- DLQ schema includes `_error_reason` and `_violated_rule_id`.  
- Retry mechanism with exponential backoff for transient errors.

---

### **5. CLI & SDK**

#### **CLI**
- `Kontra validate [OPTIONS] CONTRACT_PATH`
- Flags: `--data`, `--output-format`, `--no-actions`, `--fail-fast`, `--verbose`
- Rich (human-readable) and JSON outputs.  
- Exit codes standardized (0 success, 1 validation failure, 2+ error).

#### **SDK**
```python
from Kontra import validate
result = validate("contract.yml", data="s3://bucket/file.parquet")
if not result.passed:
    result.quarantine()
```

**Acceptance Criteria:**
- CLI and SDK produce identical results for same Kontract.  
- JSON schema validated with version `1.0`.  
- Supports integration into Dagster `AssetCheckResult` pipeline.  

---

### **6. Performance & Scale**

| Metric | Target | Validation Context |
|---------|---------|--------------------|
| Dataset | 100M rows / 10 cols | Parquet from S3 |
| Runtime | < 3 minutes | GitHub Actions runner |
| Memory | < 2GB RAM | Streaming mode |
| Rules | 10 active | No degradation |

---

## 5. Out of Scope (v1.0)

- LLM-assisted remediation (“Generative Fixer”)  
- `kontra infer` (contract inference)  
- `kontra docs` (documentation generator)  
- Non-AWS connectors (GCS, BigQuery, Azure)  
- GUI or web dashboard

---

## 6. Technical Constraints

- Language: Python 3.10+  
- Core dependencies: Polars, DuckDB, Pydantic, Rich, Typer  
- No persistent state: all operations are stateless and idempotent.  
- Credentials loaded securely via environment variables.

---

## 7. Performance Benchmarks

Benchmarks must be reproducible through the `/benchmarks` suite.  
Includes metrics for runtime, memory footprint, and I/O read efficiency.

---

## 8. Testing & QA Requirements

| Category | Target |
|-----------|--------|
| Unit Test Coverage | ≥ 90% for core engine, CLI, SDK |
| Integration Tests | Required for connectors and actions |
| Contract Validation | Schema consistency enforced by Pydantic |
| Performance Tests | Must meet target runtime and memory |
| Determinism Tests | Identical runs produce identical results |

---

## 9. Release Criteria

| Category | Requirement |
|-----------|--------------|
| **Reliability** | All error paths tested end-to-end |
| **Performance** | Meets all benchmark targets |
| **Documentation** | CLI help + Quickstart + README |
| **CI/CD Ready** | JSON outputs schema-validated |
| **Security** | No plaintext secrets or local config |

---

## 10. Roadmap Beyond v1.0

| Version | Theme | Focus |
|----------|--------|--------|
| **v1.1** | Operationalization | Asana/Jira actions, Replay workflow |
| **v1.2** | Intelligence | `infer`, `docs`, Generative Fixer |
| **v1.3** | Scale | BigQuery, Databricks, GCS |
| **v1.4** | UI & Analytics | Dashboard and historical run analysis |

---

### ✅ Summary

Kontra v1.0 delivers a **complete, production-grade validation engine** built for data practitioners.  
It is **fast, composable, and CI-native**, laying the foundation for future intelligence, collaboration, and enterprise integrations.

