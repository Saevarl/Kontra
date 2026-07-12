# CLI Reference

The command line uses the same contracts, datasource resolution, execution
tiers, and state backends as the Python API.

```bash
kontra --help
kontra <command> --help
```

## Commands

| Command | Purpose |
|---|---|
| `kontra init` | Create `.kontra/config.yml` |
| `kontra profile SOURCE` | Measure schema and column statistics |
| `kontra validate CONTRACT` | Execute contract rules |
| `kontra history CONTRACT` | List persisted validation runs |
| `kontra diff [CONTRACT]` | Compare validation runs |
| `kontra profile-diff SOURCE` | Compare persisted profiles |
| `kontra config [show\|path]` | Inspect configuration |

## Validate

```bash
kontra validate contract.yml [OPTIONS]
```

| Option | Values | Meaning |
|---|---|---|
| `--data` | source | Override the contract datasource |
| `-o, --output-format` | `rich`, `json` | Select output |
| `--stats` | `none`, `summary`, `profile` | Attach execution statistics |
| `--preplan` | `on`, `off` | Enable metadata resolution |
| `--pushdown` | `on`, `off` | Enable SQL execution |
| `--tally / --no-tally` | boolean override | Exact counts or fail-fast detection |
| `--projection` | `on`, `off` | Load only required columns |
| `--csv-mode` | `auto`, `duckdb`, `parquet` | Select CSV execution strategy |
| `-e, --env` | environment | Apply a named config overlay |
| `--dry-run` | flag | Parse and compile without reading data |
| `--explain` | flag | Show tier assignment without execution |
| `--show-plan` | flag | Print generated SQL for debugging |
| `--explain-preplan` | flag | Print metadata decisions |
| `--state-backend` | backend URI | Override state storage |
| `--no-state` | flag | Do not persist this run |
| `--storage-options` | JSON | Pass cloud storage options |
| `--only` | comma-separated rules/IDs | Run a rule subset |
| `--columns` | comma-separated columns | Run rules touching a column subset |
| `-v, --verbose` | flag | Show detailed errors |

Filtered runs are not persisted because partial results would corrupt history.

## Profile

```bash
kontra profile SOURCE [OPTIONS]
```

| Option | Values | Meaning |
|---|---|---|
| `-o, --output-format` | `rich`, `json`, `markdown`, `llm` | Select output |
| `-p, --preset` | `scout`, `scan`, `interrogate` | Select profiling depth |
| `-s, --sample` | row count | Profile a sample |
| `-c, --columns` | comma-separated columns | Restrict columns |
| `-l, --list-values-threshold` | integer | List complete low-cardinality values |
| `-t, --top-n` | integer | Limit frequent-value output |
| `--include-patterns` | flag | Detect string patterns |
| `--draft` | flag | Emit a draft contract |
| `--save-profile` | flag | Persist the profile |
| `-e, --env` | environment | Apply a config overlay |
| `--storage-options` | JSON | Pass cloud storage options |

## History and diff

```bash
kontra history contract.yml --since 7d --limit 20 --failed-only
kontra diff contract.yml --since 7d -o llm
kontra profile-diff users.parquet --since 7d
```

History accepts `table` or `json` output. Validation diff accepts `rich`, `json`,
or token-optimized `llm` output. See [History and diff](../advanced/state-and-diff.md)
for backend configuration and result semantics.

## Exit codes

| Code | Meaning |
|---|---|
| `0` | All blocking rules passed |
| `1` | At least one blocking rule failed |
| `2` | Contract, configuration, or data-source error |
| `3` | Unexpected runtime or connection error |
