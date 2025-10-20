# Contra – Technical Architecture Document (TAD v1.1)

**Purpose:**  
This document defines the high-level architecture of the Contra Data Quality Engine.  
It translates Contra’s product tenets — *Performance is Paramount, Developer Experience is the Product, and Extensibility by Design* — into a concrete, maintainable technical system.

---

## 1. System Overview

Contra is a **modular, layered system**. Each component has a single, well-defined responsibility and communicates via explicit interfaces.  
The overall design philosophy mirrors the UNIX ethos: *small, composable parts that can be easily replaced or extended*.

### 1.1 Major Components

#### **CLI / SDK Layer**
- **Purpose:** The thin, user-facing boundary.  
- **Responsibilities:**  
  - Command dispatch, argument parsing, and global exception handling.  
  - Delegates work to internal APIs without holding business logic.  
  - Provides stable entrypoints for developers and orchestrators (Dagster, Airflow).  
- **Key Interfaces:**  
  - `contra.cli` (Click or Typer-based)
  - `contra.validate(contract_path: str, data: str)` (Python SDK entrypoint)

> _Design Note:_ The CLI should remain stateless and purely declarative — it should prepare configuration and invoke the engine without maintaining global state.

---

#### **Configuration Layer (models)**
- **Purpose:** Declaratively define the dataset contract and translate YAML into a validated object graph.
- **Implementation:** Pydantic models (`Contract`, `DataSource`, `RuleSpec`, `ActionSpec`).
- **Responsibilities:**  
  - Parse and validate `contract.yml`.  
  - Normalize schema and enforce schema-level constraints (e.g., rule uniqueness, mutually exclusive fields).  
  - Securely load credentials via environment variables or `.env` files — never from disk.  
  - Generate stable hashes or fingerprints for contract reproducibility.

> _Improvement:_ Include **contract versioning** (`contract_version` field) and **checksum validation** to support CI/CD reproducibility.

---

#### **Validation Core (engine, rules, factories)**
- **Engine:**  
  - Central orchestrator managing validation flow.  
  - Coordinates rule resolution, connector initialization, query planning, and result aggregation.  
  - Stateless and deterministic — given a contract and dataset, it must always produce identical results.

- **Rules:**  
  - Each subclass of `BaseRule` implements a single check via a uniform interface:
    ```python
    class BaseRule:
        def build_query(self, frame: pl.LazyFrame) -> pl.LazyFrame: ...
        def evaluate(self, results: pl.DataFrame) -> ValidationResult: ...
    ```
  - Rules operate at one of three scopes: **row**, **column**, or **dataset**.

- **Factories & Registry:**  
  - `RuleFactory`: Discovers, validates, and instantiates rule objects from contract specs.  
  - `RuleRegistry`: Global dictionary mapping rule keys → class references, populated dynamically at import time.  

> _Improvement:_ Add a **RuleExecutionPlan** abstraction — a DAG or list of logical rule operations compiled into a single Polars query. This opens the door for optimization (e.g., predicate coalescing, pushdown grouping).

---

#### **Connector Layer (connectors)**
- **Purpose:** Abstraction over data sources.  
- **Responsibilities:**  
  - Read from local files, S3/GCS, or databases and expose a **Polars DataFrame or LazyFrame**.  
  - Handle efficient streaming, column pruning, and predicate pushdown.  
  - Must implement a simple standard interface:
    ```python
    class BaseConnector:
        def load(self) -> pl.LazyFrame: ...
    ```
- **Examples:** `S3Connector`, `LocalFileConnector`, `PostgresConnector`, `SnowflakeConnector`.

> _Improvement:_ Define a **ConnectorCapabilities** enum (e.g., supports_pushdown, supports_sampling) so the engine can optimize execution plans automatically.

---

#### **Remediation Layer (actions)**
- **Purpose:** Execute workflow-level consequences on validation failure.  
- **Responsibilities:**  
  - Handle actions such as quarantine, alerting, ticket creation, or replay initiation.  
  - Actions are configured per-rule or per-contract.  
  - Implement a base interface:
    ```python
    class BaseAction:
        def execute(self, summary: ValidationSummary): ...
    ```
- **Examples:** `QuarantineAction`, `JiraAction`, `AsanaAction`, `ReplayAction`.

> _Improvement:_ Introduce a **transactional action context**, ensuring atomic execution when multiple actions (e.g., quarantine + ticket) are chained.

---

#### **Reporting Layer (reporters)**
- **Purpose:** Present results to users or downstream systems.  
- **Responsibilities:**  
  - Aggregate `ValidationResult` objects into a final `ValidationSummary`.  
  - Output in formats like console (Rich), JSON (for CI), or Markdown (for Living Docs).  
  - Optionally send metrics to Prometheus or OpenTelemetry for observability.

> _Improvement:_ Add a **ReporterRegistry** similar to the rule registry — enabling plugins for new output formats (Slack, HTML, Datadog).

---

## 2. Data Flow Example: CLI Validation of a Cloud File

This sequence diagram illustrates the most common end-to-end path.

1. **Invocation:**  
   `contra validate --contract contract.yml`
2. **Configuration:**  
   CLI parses arguments → loads YAML → instantiates `Contract` (validated via Pydantic).  
   Credentials are loaded securely from environment variables.
3. **Engine Setup:**  
   CLI constructs a `ValidationEngine(contract)` and calls `engine.run()`.
4. **Connector Load:**  
   The engine uses `ConnectorFactory` to instantiate the appropriate connector (e.g., `S3Connector`).  
   `connector.load()` returns a `Polars.LazyFrame`.
5. **Rule Resolution:**  
   The `RuleFactory` converts each rule spec in the contract into a `Rule` object.  
   The engine builds a **RuleExecutionPlan** (a combined Polars query).
6. **Execution:**  
   The plan executes via `.collect(streaming=True)`.  
   Polars streams the dataset, performing all checks in one pass with minimal memory.
7. **Analysis & Actions:**  
   Results are analyzed. On failure:
   - `QuarantineAction` writes failing rows to a configured DLQ (schema: original columns + `_error_reason`, `_violated_rule_id`).  
   - `AsanaAction` creates a ticket summarizing the failure and linking to DLQ.
8. **Reporting:**  
   Engine returns a `ValidationSummary`.  
   The CLI invokes the `RichReporter`, prints formatted output, and exits with a non-zero status if validation failed.

---

## 3. Plugin Lifecycle & Registry Pattern

The plugin system enables distributed extensibility without touching the core engine.

### 3.1 Interface
A plugin is a standard Python package exposing one or more `BaseRule` subclasses, optionally with actions or reporters.

### 3.2 Registration
Each rule is annotated with a decorator:
```python
@register_rule("not_null")
class NotNullRule(BaseRule): ...
```

### 3.3 Discovery
At startup, Contra imports:
- All built-in `contra.rules` modules.
- Any installed `contra_*` packages discoverable via entry points:
  ```python
  entry_points(group="contra.plugins")
  ```

This import triggers decorator execution, populating a global `RULE_REGISTRY` mapping.

### 3.4 Instantiation
When a rule key in the contract (e.g., `not_null`) is encountered, the `RuleFactory` looks it up in `RULE_REGISTRY`, instantiates it, and injects the relevant context.

> _Improvement:_ Add **namespacing** for rules (`namespace.rule_key`) to avoid collisions between external plugins.

---

## 4. Concurrency, Observability, and Fault Tolerance

**Concurrency:**  
- The engine supports parallel rule evaluation across independent datasets or partitions via thread or async pools (Ray/Dask optional).  
- Internal rule execution remains deterministic — concurrency is a runtime optimization, not a behavioral change.

**Observability:**  
- Emit structured logs (JSON) and metrics (e.g., rule execution time, failure counts).  
- Optional integration with OpenTelemetry for distributed tracing.

**Fault Boundaries:**  
- The engine isolates failures by component:
  - Rule-level failures are caught and reported, never crash the engine.  
  - Connector or Action exceptions bubble up with structured diagnostics.  
- All transient errors (network, permission) include retry policies with exponential backoff.

---

## 5. Architectural Goals (v1.0)

| Goal | Description |
|------|--------------|
| Deterministic Execution | Identical inputs → identical outputs. |
| Streaming Validation | Handle 10GB+ datasets with <2GB memory. |
| Plugin Extensibility | Add rules/actions without core modification. |
| Secure Credentials | No secrets persisted to disk. |
| CI/CD Friendly | JSON output + exit codes for orchestration. |
| Developer Delight | Clear, minimal API and human-readable errors. |

---

## 6. Future Extensions

- **Rule Execution Optimizer:** Merge compatible predicates for fewer passes.  
- **Interactive Report Viewer:** Rich TUI (text-based UI) for browsing validation failures.  
- **Incremental Validation Mode:** Only validate changed partitions since last run.  
- **Streaming Connectors:** Support real-time event validation (Kafka, Pub/Sub).

---

### ✅ Summary

This architecture ensures Contra remains:
- **Composable:** Each component replaceable via well-defined interfaces.  
- **Extensible:** Rules, actions, and reporters are plugin-driven.  
- **Performant:** Built natively on Rust-backed compute (Polars/DuckDB).  
- **Observable:** Transparent in behavior and diagnostics.  
- **Developer-Centric:** CLI and SDK form a cohesive, ergonomic user experience.
