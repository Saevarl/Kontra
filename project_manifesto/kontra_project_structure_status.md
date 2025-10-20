# Kontra Project Structure — Status Map

**Legend** - ✅ implemented/working  
- 🟨 present but partial/stub  
- ❌ not implemented yet / placeholder

> Status reflects the current code we have discussed/landed in this session. If a file exists in your repo but isn't wired or is a stub, it's marked 🟨. Adjust as you fill things in.

```text
Kontra/
├─ README.md                                             🟨
├─ LICENSE                                               🟨
├─ CHANGELOG.md                                          ❌
├─ CONTRIBUTING.md                                       ❌
├─ CODE_OF_CONDUCT.md                                    ❌
├─ SECURITY.md                                           ❌
├─ .gitignore                                            ✅
├─ .editorconfig                                         🟨
├─ .env.example                                          🟨
├─ Makefile                                              🟨
├─ pyproject.toml                                        ✅
├─ ruff.toml                                             🟨
├─ mypy.ini                                              🟨
├─ pytest.ini                                            🟨
├─ .pre-commit-config.yaml                               🟨
├─ mkdocs.yml                                            ❌
├─ docker/
│  ├─ Dockerfile                                         ❌
│  └─ docker-compose.yml                                 ❌
├─ .github/
│  └─ workflows/
│     ├─ ci.yml                                          ❌
│     ├─ release.yml                                     ❌
│     └─ docs.yml                                        ❌
├─ docs/
│  ├─ index.md                                           ❌
│  ├─ quickstart.md                                      ❌
│  ├─ concepts/
│  │  ├─ contracts.md                                    ❌
│  │  ├─ rules.md                                        ❌
│  │  ├─ connectors.md                                   ❌
│  │  ├─ actions.md                                      ❌
│  │  └─ reporting.md                                    ❌
│  ├─ guides/
│  │  ├─ authoring-contracts.md                          ❌
│  │  ├─ ci-cd.md                                        ❌
│  │  └─ performance-tuning.md                           ❌
│  └─ references/
│     ├─ cli.md                                          ❌
│     ├─ sdk.md                                          ❌
│     └─ json-schema.md                                  ❌
├─ examples/
│  ├─ contracts/
│  │  ├─ users.yml                                       🟨
│  │  └─ sales.yml                                       ❌
│  ├─ data/
│  │  ├─ users.parquet                                   🟨
│  │  └─ users.csv                                       🟨
│  └─ pipelines/
│     └─ dagster_example.py                              ❌
├─ benchmarks/
│  ├─ README.md                                          ❌
│  ├─ datasets/                                          ❌
│  ├─ cases/
│  │  └─ parquet_100m_10c.yaml                           ❌
│  └─ run_benchmarks.py                                  ❌
├─ scripts/
│  ├─ generate_fake_data.py                              ❌
│  ├─ synthesize_users.py                                ✅
│  ├─ validate_local.sh                                  🟨
│  └─ release_notes.py                                   ❌
├─ schemas/
│  ├─ contract.schema.json                               ❌
│  └─ validation_output.schema.json                      ❌
├─ src/
│  └─ Kontra/
│     ├─ __init__.py                                     ✅
│     ├─ version.py                                      ✅
│     ├─ exceptions.py                                   🟨
│     ├─ types.py                                        ❌
│     ├─ utils/
│     │  ├─ __init__.py                                  ✅
│     │  ├─ logging.py                                   🟨
│     │  ├─ hashing.py                                   ❌
│     │  ├─ time.py                                      ✅
│     │  ├─ env.py                                       🟨
│     │  └─ io.py                                        ❌
│     ├─ observability/
│     │  ├─ __init__.py                                  ❌
│     │  └─ otel.py                                      ❌
│     ├─ cli/
│     │  ├─ __init__.py                                  ✅
│     │  └─ main.py                                      ✅  (validate command, stats, exit codes)
│     ├─ sdk/
│     │  ├─ __init__.py                                  ❌
│     │  └─ api.py                                       ❌
│     ├─ config/
│     │  ├─ __init__.py                                  ✅
│     │  ├─ loader.py                                    ✅  (local + s3)
│     │  ├─ models.py                                    ✅
│     │  └─ validators.py                                ❌
│     ├─ engine/
│     │  ├─ __init__.py                                  ✅
│     │  ├─ engine.py                                    ✅  (orchestrator + stats)
│     │  ├─ execution_plan.py                            ✅  (v2 compile/prune/execute)
│     │  ├─ result.py                                    ❌
│     │  ├─ stats.py                                     ✅
│     │  └─ planner/
│     │     ├─ __init__.py                               ✅
│     │     ├─ optimizer.py                              ❌
│     │     └─ predicates.py                             ✅
│     ├─ rules/
│     │  ├─ __init__.py                                  ✅
│     │  ├─ base.py                                      ✅  (required_columns hook)
│     │  ├─ registry.py                                  ✅
│     │  ├─ factory.py                                   ✅  (stable rule_id policy)
│     │  └─ builtin/
│     │     ├─ __init__.py                               ✅
│     │     ├─ not_null.py                               ✅  (predicate)
│     │     ├─ unique.py                                 ✅  (predicate)
│     │     ├─ allowed_values.py                         ✅  (predicate)
│     │     ├─ dtype.py                                  ✅  (strict, fallback + required_columns)
│     │     ├─ regex.py                                  ✅  (predicate; contains(pattern))
│     │     ├─ min_rows.py                               ✅
│     │     ├─ max_rows.py                               ✅
│     │     └─ custom_sql_check.py                       ✅  (fallback)
│     ├─ connectors/
│     │  ├─ __init__.py                                  ✅
│     │  ├─ base.py                                      ✅  (load signature)
│     │  ├─ factory.py                                   ✅
│     │  ├─ filesystem.py                                ✅  (projection-aware + fallbacks)
│     │  ├─ s3.py                                        ✅  (storage_options + fallbacks)
│     │  ├─ postgres.py                                  ❌
│     │  ├─ snowflake.py                                 ❌
│     │  └─ utils/
│     │     ├─ __init__.py                               ❌
│     │     ├─ parquet.py                                ❌
│     │     └─ sql.py                                    ❌
│     ├─ actions/
│     │  ├─ __init__.py                                  ❌
│     │  ├─ base.py                                      ❌
│     │  ├─ quarantine.py                                ❌
│     │  └─ slack_alert.py                               ❌
│     └─ reporters/
│        ├─ __init__.py                                  ✅
│        ├─ base.py                                      🟨
│        ├─ rich_reporter.py                             ✅
│        ├─ json_reporter.py                             ❌
│        └─ yaml_reporter.py                             ❌
├─ integrations/
│  └─ dagster-Kontra/
│     ├─ README.md                                       ❌
│     ├─ pyproject.toml                                  ❌
│     └─ src/
│        └─ dagster_Kontra/
│           ├─ __init__.py                               ❌
│           └─ op.py                                     ❌
└─ tests/
   ├─ conftest.py                                        🟨
   ├─ unit/
   │  ├─ test_config_models.py                           🟨
   │  ├─ test_engine_core.py                             🟨
   │  ├─ test_rule_registry.py                           🟨
   │  ├─ test_builtin_rules.py                           🟨
   │  ├─ test_connectors.py                              🟨
   │  ├─ test_actions.py                                 ❌
   │  ├─ test_reporters.py                               ❌
   │  └─ test_cli.py                                     🟨
   └─ integration/
      ├─ test_validate_parquet_s3.py                     🟨
      ├─ test_validate_postgres_pushdown.py              ❌
      ├─ test_quarantine_s3.py                           ❌
      └─ test_slack_alert.py                             ❌