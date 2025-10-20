# Kontra – CLI and UX Specification (v1.2)

**Purpose:**  
This document defines the user experience and interface for the Kontra Command-Line Interface (CLI) and SDK.  
It aims to deliver a tool that is *simple for humans, structured for machines* — balancing developer delight with operational rigor.

---

## 1. Design Philosophy

- **Clarity:** Every command and output must be self-explanatory.  
- **Consistency:** Commands, flags, and messages follow standard conventions across subcommands.  
- **Machine-Readability:** Structured JSON output enables CI/CD integration.  
- **Safety:** Defaults should favor non-destructive operations.  
- **Delight:** Interactive color, alignment, and clear error design for developer trust.

---

## 2. Command Hierarchy

Kontra organizes its CLI around **four core verbs** — each representing a user workflow.

| Command | Description | Example |
|----------|--------------|----------|
| `contra validate` | Validate data against a declarative contract. | `contra validate contracts/users.yml` |
| `contra infer` | Infer a draft contract from existing data. | `contra infer s3://data/users.parquet` |
| `contra docs` | Generate living documentation from a contract. | `contra docs contracts/users.yml` |
| `contra replay` | Reprocess quarantined data after fixes. | `contra replay s3://dlq/users/2025-10-17/` |

---

## 3. `contra validate` — Core Command

This is the **primary entrypoint** for running validation checks.

### **Usage**
```bash
contra validate [OPTIONS] CONTRACT_PATH
```

### **Arguments**
- `CONTRACT_PATH` *(required)* — Path or URI to a `contract.yml` file.

### **Options**
| Flag | Description |
|------|--------------|
| `--data PATH_OR_URI` | Optional override for the data location defined in the contract. Useful for local or ad-hoc runs. |
| `--output-format [rich|json|yaml]` | Selects output format. Defaults to `rich`. |
| `--no-actions` | Runs validation without triggering remediation actions. |
| `--fail-fast` | Stops after the first rule failure. |
| `--rule RULE_ID` | Runs a specific rule or pattern (e.g., `COL:email:*`). |
| `--verbose` | Prints debug details including execution plans. |
| `--version` | Displays the current CLI version. |
| `--help` | Displays contextual help. |

---

## 4. Example User Experience

### ✅ **Success Scenario**
```bash
$ contra validate contracts/users.yml

[2025-10-17 22:02:00] INFO: Validation started for dataset: raw.users
[2025-10-17 22:02:05] INFO: Source: s3://my-lake/data/users_2025-10-17.parquet
[2025-10-17 22:03:15] INFO: Evaluated 12 rules in 70.2s (streaming mode)
[2025-10-17 22:03:15] INFO: All rules passed.

✅ PASS — dataset conforms to contract
```

### ❌ **Failure Scenario**
```bash
$ contra validate contracts/users.yml

[2025-10-17 22:02:00] INFO: Validation started for dataset: raw.users
[2025-10-17 22:02:05] INFO: Source: s3://my-lake/data/users_2025-10-17.parquet
[2025-10-17 22:03:15] ERROR: 2 of 12 rules failed.

❌ FAIL — dataset violates the contract

                       Failure Summary
┏━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━┓
┃ Rule ID          ┃ Message                      ┃ Count    ┃
┡━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━┩
│ COL:user_id:uniq │ user_id has 152 duplicate(s) │ 152      │
│ COL:email:format │ email has 84 invalid email(s)│ 84       │
└──────────────────┴──────────────────────────────┴──────────┘

[2025-10-17 22:03:18] INFO: Executing remediation actions...
[2025-10-17 22:03:25] INFO: QuarantineAction: Quarantined 236 rows → s3://my-lake/quarantine/users/
[2025-10-17 22:03:26] INFO: SlackAction: Sent alert to #data-quality-alerts
[2025-10-17 22:03:27] INFO: Validation complete. Exiting with status code 1.
```

> **UX Design Notes:**  
> - Aligned columns for readability.  
> - Timestamped logs (UTC, ISO format).  
> - Context-aware emoji summary (`✅ PASS` / `❌ FAIL`).  
> - Verbosity auto-adjusts when run in CI vs terminal.

---

## 5. Exit Codes

| Code | Meaning | Description |
|-------|----------|-------------|
| `0` | SUCCESS | All rules passed. |
| `1` | VALIDATION_FAILED | One or more rules failed. |
| `2` | CONFIG_ERROR | Invalid or unreadable `contract.yml`. |
| `3` | CONNECTION_ERROR | Data source not accessible. |
| `4` | RUNTIME_ERROR | Internal or plugin error. |

---

## 6. JSON Output Schema

Machine-readable output for automation (CI/CD, Airflow, Dagster).

### **Example JSON**
```json
{
  "schema_version": "1.0",
  "dataset_name": "raw.users",
  "timestamp_utc": "2025-10-17T22:03:15Z",
  "engine_version": "0.3.0",
  "validation_passed": false,
  "statistics": {
    "execution_time_seconds": 75.2,
    "rows_evaluated": 100000000,
    "rules_total": 12,
    "rules_passed": 10,
    "rules_failed": 2
  },
  "results": [
    {
      "rule_id": "COL:user_id:unique",
      "passed": false,
      "message": "user_id has 152 duplicate(s)",
      "failed_count": 152,
      "severity": "ERROR",
      "action_executed": ["quarantine"]
    },
    {
      "rule_id": "COL:email:format_email",
      "passed": false,
      "message": "email has 84 invalid email(s)",
      "failed_count": 84,
      "severity": "ERROR",
      "action_executed": ["slack_alert"]
    }
  ],
  "quarantine": {
    "location": "s3://my-lake/quarantine/users/2025-10-17/",
    "rows_quarantined": 236
  }
}
```

### **Schema Guarantees**
- Versioned: `schema_version` must increment on breaking change.  
- Deterministic: Field order is stable across runs.  
- Extensible: Unknown fields ignored by parsers.  
- Typed: All timestamps are ISO 8601 UTC.

---

## 7. Developer Experience Enhancements

| Feature | Description |
|----------|--------------|
| **Autocomplete** | `contra --install-completion` for Bash/Zsh/Fish. |
| **Color & Formatting** | Rich terminal colors, auto-disabled in CI. |
| **Progress Feedback** | Spinner or percentage during large validations. |
| **Dry Run Mode** | `--no-actions` simulates remediation actions. |
| **Telemetry (Opt-in)** | Usage stats enabled via `CONTRA_TELEMETRY=1`. |
| **Context Awareness** | Auto-detect CI environments; switch to JSON output. |

---

## 8. Error Handling Principles

1. Errors must be **clear and actionable**.  
2. Default to **structured messages**, not stack traces.  
3. Example format:
   ```text
   ERROR: Failed to connect to s3://data/users.parquet
   Hint: Check your AWS credentials (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
   ```
4. Debug mode (`--verbose`) shows traceback and internal context.

---

## 9. Future Command Concepts

| Command | Purpose |
|----------|----------|
| `contra plan` | Preview validation plan (rules, execution strategy). |
| `contra profile` | Performance diagnostics and benchmarks. |
| `contra explain` | Display SQL/Polars plan for a rule. |
| `contra secrets` | Manage secret providers (future). |

---

## ✅ Summary

The Kontra CLI delivers:
- A clean, consistent developer experience.  
- Full parity between CLI and SDK outputs.  
- Deterministic behavior for automation.  
- Human-readable elegance for daily workflow.  

> “The best CLI tools feel invisible — you remember what they *did*, not how they made you fight to use them.”

