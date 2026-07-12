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

        assert first["passed"] is True
        assert second["passed"] is True
        assert profile["dataset"]["row_count"] > 0
        assert len(service.validation_history("mcp_users")) >= 2
        assert service.validation_diff("mcp_users")["has_changes"] is False
        assert len(service.profile_history("mcp_test.users")) >= 1
    finally:
        service.close()
        kontra.set_config(None)
