"""End-to-end PostgreSQL persistence coverage for the official MCP service."""

import kontra

from kontra.mcp.service import KontraMCPService, MCPSettings


def test_mcp_validate_profile_history_and_diff(postgres_container, tmp_path):
    config_path = tmp_path / "config.yml"
    config_path.write_text(
        """
version: "1"
datasources:
  mcp_test:
    type: postgres
    host: localhost
    port: 5433
    user: kontra
    password: kontra_test
    database: kontra_test
    tables:
      users: public.users
""",
        encoding="utf-8",
    )
    contract_path = tmp_path / "mcp_users.yml"
    contract_path.write_text(
        """
name: mcp_postgres_users
rules:
  - name: min_rows
    params:
      threshold: 1
  - name: not_null
    params:
      column: user_id
""",
        encoding="utf-8",
    )

    settings = MCPSettings(
        "postgres://kontra:kontra_test@localhost:5433/kontra_test",
        tmp_path,
        config_path,
    )
    service = KontraMCPService(settings)
    try:
        first = service.validate("mcp_test.users", "mcp_users")
        second = service.validate("mcp_test.users", "mcp_users")
        profile = service.profile("mcp_test.users", preset="scout")
        service.profile("mcp_test.users", preset="scout")
        run = service.get_validation_run("mcp_users")
        samples = service.measure_failure_samples(
            "mcp_test.users", "mcp_users", "COL:user_id:not_null", n=2
        )
        comparison = service.compare_datasets(
            "mcp_test.users", "mcp_test.users", key="user_id"
        )
        relationship = service.profile_relationship(
            "mcp_test.users", "mcp_test.users", on="user_id"
        )

        assert first["passed"] is True
        assert second["passed"] is True
        assert profile["dataset"]["row_count"] > 0
        assert len(service.validation_history("mcp_users")) >= 2
        assert service.validation_diff("mcp_users")["has_changes"] is False
        assert run is not None
        assert "dataset_uri" not in run
        assert "contract_fingerprint" not in run
        assert "dataset_fingerprint" not in run
        assert samples["measurement"] == "current"
        assert samples["historical_run_id"] is None
        assert len(service.profile_history("mcp_test.users")) >= 2
        assert service.profile_diff("mcp_test.users") is not None
        assert comparison["row_stats"]["delta"] == 0
        assert comparison["samples"] == {
            "duplicated_keys": [],
            "dropped_keys": [],
            "changed_rows": [],
        }
        assert relationship["coverage"]["left_keys_without_match"] == 0
        assert relationship["samples"] == {
            "left_keys_without_match": [],
            "right_keys_without_match": [],
            "right_keys_with_multiple_rows": [],
        }
    finally:
        service.close()
        kontra.set_config(None)
