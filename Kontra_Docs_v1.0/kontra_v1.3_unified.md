# 🧩 Kontra Unified Product Document (v1.3)

**Purpose:**  
This document unifies Kontra’s Product Requirements (PRD), Technical Architecture (TAD), CLI/UX Specification, and MVP Scope into a single reference.  
It defines Kontra’s evolved mission, system design, developer experience, and roadmap as of v1.3.

---

## 1. Overview

### 🧭 Mission
Empower developers, data engineers, and data scientists to **trust and control data quality effortlessly**.  
Kontra provides a **fast, flexible, and developer-centric validation engine** that integrates into modern data workflows — from local CSV/Parquet to cloud, SQL, and DLT pipelines.

### 🎯 Objectives
- Validate large datasets efficiently (100M+ rows).  
- Deliver a frictionless CLI and Python SDK.  
- Enable modular extensibility (rules, connectors, actions).  
- Support both validation and valid-row extraction.  
- Offer smart contract inference (`kontra infer`).  
- Optimize automatically via intelligent tuning profiles.  

### 🚫 Non-Goals
- Not a transformation or ETL system (e.g., dbt, Fivetran).  
- Not a workflow orchestrator — integrates into existing ones.

---

## 2. Target Users

| Persona | Description | Primary Needs |
|----------|--------------|---------------|
| **Data Engineers** | Build and maintain data pipelines | Automated data validation and CI/CD integration |
| **ML Engineers** | Develop training pipelines | Filter valid and consistent data |
| **Data Scientists** | Explore and prepare datasets | Quick local validation and filtering |
| **Data Quality Leads** | Govern trust and compliance | Centralized, auditable validation process |

---

## 3. Product Philosophy (Kontra Tenets)

1. **Performance is Paramount**  
   Hybrid execution (SQL pushdown + Polars backend) scales efficiently beyond 100M rows.  
2. **Developer Experience is the Product**  
   Intuitive CLI, ergonomic SDK, and reproducible outputs.  
3. **Extensibility by Design**  
   Rules, connectors, and actions are all modular and pluggable.  
4. **Flexibility First**  
   Validate, infer, extract, or stream valid rows — all within one engine.  

---

## 4. Core System Architecture (TAD Summary)

### 4.1 Layered Design

| Layer | Purpose |
|--------|----------|
| **CLI / SDK** | Unified UX for data practitioners. |
| **Configuration Layer** | Reads settings from env, config, and CLI; merges with Auto-Tune. |
| **Validation Core** | Compiles and executes rule plans (pushdown + residual). |
| **Connector Layer** | Handles data I/O: local, S3, ADLS, PostgreSQL. |
| **Remediation Layer** | Post-validation actions (Slack, quarantine, Asana). |
| **Reporting Layer** | JSON + human reports for pipelines and CI. |

---

### 4.2 Hybrid Execution Model
- **Pushdown Path:** Executes SQL-friendly rules (not_null, unique) via DuckDB or PostgreSQL.  
- **Residual Path:** Executes complex rules via Polars backend.  
- **Planner:** Determines execution strategy per rule.  
- **Projection:** Optional column-level optimization to minimize I/O.

---

### 4.3 Concurrency & Fault Tolerance
- Parallel rule evaluation via thread pools or async routines.  
- Deterministic execution ordering for reproducibility.  
- Structured JSON logs, OpenTelemetry-compatible.  

---

### 4.4 Configuration & Performance Tuning

#### 🔧 DuckDB Profiles
Profiles define optimal concurrency and memory allocation:

| Profile | Use Case | Threads | Memory | Temp Dir | Notes |
|----------|-----------|----------|----------|----------|-------|
| **speed** | Local SSD benchmarks | up to 16 | 85% RAM | fast tmp | High throughput |
| **balanced (default)** | General workloads | up to 8 | 70% RAM | auto tmp | Best stability |
| **conservative** | Shared/CI envs | up to 4 | 50% RAM | system tmp | Minimal footprint |
| **auto** | Auto-tuned dynamically | adaptive | adaptive | auto | Caches best config |

#### ⚙️ Auto-Tune Mode
- Runs lightweight micro-benchmarks on representative samples.  
- Tests concurrency/memory combos and selects the best.  
- Persists results in `~/.kontra/autotune.json`.  
- Logged in every report for reproducibility.  

Example log:
```json
"duckdb": {
  "profile": "auto",
  "threads": 12,
  "memory": "24GB",
  "tmp": "/mnt/ssd/tmp"
}
```

---

### 4.5 Extensibility Framework
- **Rules Registry:** Declarative rule loading via YAML/Pydantic.  
- **Connector Registry:** Local, S3, PostgreSQL, ADLS.  
- **Action Registry:** Slack, Asana, Quarantine, custom handlers.  

---

### 4.6 Future Extensions
- Caching layer for staged parquet reuse.  
- Interactive TUI dashboards.  
- Incremental and real-time validation.  
- DLT integration for streaming valid rows.  

---

## 5. CLI & UX Specification

### 5.1 Design Principles
Clarity, consistency, machine-readability, and speed.  
All CLI outputs mirror the Python SDK API.

---

### 5.2 Command Reference

| Command | Description |
|----------|-------------|
| `kontra validate` | Validate dataset against a Kontract |
| `kontra infer` | Infer a draft Kontract from dataset |
| `kontra extract` | Stream or write valid rows only |
| `kontra docs` | Generate human + JSON documentation |
| `kontra tune` | Run DuckDB Auto-Tune benchmark |
| `kontra replay` | Reprocess quarantined or invalid rows |

---

### 5.3 Example Usage

```bash
kontra validate data.parquet --pushdown --projection
kontra infer s3://bucket/dataset.csv --sample 0.05
kontra validate data.csv --duckdb.profile=auto --stats
kontra extract data.parquet --only-valid --out valid.parquet
kontra tune
```

---

### 5.4 Output Schema

```json
{
  "summary": {
    "status": "PASS",
    "duration_ms": 9820
  },
  "rules": [
    {"name": "not_null", "status": "PASS"},
    {"name": "unique", "status": "FAIL", "message": "duplicate emails"}
  ],
  "duckdb": {"threads": 12, "memory": "24GB", "tmp": "/mnt/ssd/tmp"}
}
```

---

### 5.5 UX Enhancements
- `--explain`: Display compiled rule plan.  
- `--explain-config`: Print detected system and tuning decisions.  
- `--stats`: Summarize performance and coverage.  
- Structured JSON for CI/CD.  
- Exit codes: `0` PASS, `1` FAIL, `2` ERROR.

---

## 6. MVP (v1.3) Scope Definition

### 6.1 Core Validation Engine
Implements:
- Rule Registry, Factory, Engine orchestration.  
- Built-in rules: `not_null`, `unique`, `dtype`, `regex`, `min_rows`, `max_rows`, `allowed_values`, `custom_sql_check`.  
- Deterministic hybrid planner (pushdown + residual).  

---

### 6.2 Connectors
- Local (CSV, Parquet).  
- Cloud: S3, PostgreSQL, ADLS.  
- CSV → Parquet staging via `csv_mode=auto`.

---

### 6.3 Performance Targets
| Metric | Target |
|--------|---------|
| Runtime | ≤3 min for 100M rows |
| Memory | ≤2GB typical |
| Speed gain | Auto-tune vs manual ±5% |
| Determinism | identical outputs for identical inputs |

---

### 6.4 Remediation
- **QuarantineAction:** Save invalid rows.  
- **SlackAction:** Send notifications.  
- **AsanaAction:** Task creation (optional, future SaaS layer).  

---

### 6.5 Integrations
- CLI + SDK parity.  
- **DLThub** source compatibility (stream valid rows).  
- **Dagster** integration for pipeline orchestration.  

---

## 7. Current Project Status (v0.1.1 Candidate)

### ✅ Achieved vs MVP
| Area | MVP Goal | Current Status | Notes |
|------|-----------|----------------|-------|
| **Core Engine** | Stable hybrid planner | ✅ Achieved | pushdown + residual validated |
| **Rules** | Base rule set | ✅ Complete | dtype, regex, unique, etc. tested |
| **Performance** | 100M rows, <3 min | ⚙️ Partial | 5M dataset benchmarks show 8–11s |
| **CLI** | Validate command | ✅ Stable | tested with stats + projection flags |
| **Infer** | Smart schema inference | 🧩 In Progress | planned for v1.1 |
| **SQL Support** | PostgreSQL pushdown | 🚧 Planned | DuckDB fully stable |
| **Connectors** | Local + S3 | ✅ Done | ADLS planned |
| **Actions** | Quarantine + Slack | ⚙️ Partial | Asana future |
| **Auto-Tune** | Profiled DuckDB configs | 🧠 Designed | prototype next |
| **Testing** | Deterministic CI suite | ✅ Passing | ~30 integration tests |
| **Docs** | CLI + dev docs | ⚙️ Draft | updating with infer + tune |

### 🧪 Performance Summary
| Dataset | Pushdown | Projection | Duration | Notes |
|----------|-----------|-------------|-----------|--------|
| users_5m.parquet | On | On | ~10–11 s | optimal path |
| users_5m.csv | Auto | On | ~35 s | consistent correctness |
| users_5m.csv | Auto | Off | ~38 s | baseline |
| users_5m.csv (staged) | On | On | ~36 s | matches parquet |

Pushdown + projection yield ~10% faster runtime; all tests deterministic and CI-stable.

### ⚠️ Known Gaps
- No caching or profiling yet.  
- SQL support limited to DuckDB.  
- Auto-Tune implementation incomplete.  
- No ML/DS filtering utility mode yet.  

---

## 8. Roadmap (2025)

| Version | Theme | Focus |
|----------|--------|--------|
| v1.1 | Flexibility | SQL pushdown (Postgres), infer command |
| v1.2 | Intelligence | Auto contract suggestion, generative fixer |
| v1.3 | Scale | ADLS connector, caching, profiling |
| v1.4 | Ecosystem | DLThub integration, SaaS actions, dashboards |

---

## 9. Success Metrics
- <3 min validation at 100M+ rows.  
- ≥95% rule coverage under pushdown.  
- ≥80% adoption by DEs within pilot teams.  
- Consistent benchmark reproducibility.  

---

## 10. Strategic North Star
Kontra becomes the **“data linter” for pipelines and ML workflows** —  
as simple as `pytest`, as fast as DuckDB, and as flexible as Polars.  
It’s the default tool developers reach for when they need **confidence in their data**.

---

✅ **Kontra v1.3 Summary**  
- Hybrid SQL + Polars architecture validated.  
- Auto-Tune performance configuration system introduced.  
- Infer command and SQL connector planned.  
- Expanded rule set and connector ecosystem in progress.  
- Fully open-source, developer-first, and built for scale.
