import asyncio
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

mcp = pytest.importorskip("mcp")

from kontra.mcp.server import _SafeServiceProxy, _sanitize_error_message, create_server
from kontra.mcp.service import MCPSettings


def test_server_exposes_bounded_tool_schemas_without_context_parameter():
    server = create_server(MCPSettings("postgres://", Path("contracts").resolve()))
    tools = {tool.name: tool for tool in asyncio.run(server.list_tools())}

    assert set(tools) == {
        "validate",
        "profile",
        "validation_history",
        "validation_diff",
        "get_validation_run",
        "measure_failure_samples",
        "profile_history",
        "profile_diff",
        "compare_datasets",
        "profile_relationship",
    }
    assert "ctx" not in tools["validate"].inputSchema["properties"]
    assert tools["validate"].inputSchema["required"] == ["datasource", "contract"]
    assert tools["profile"].inputSchema["required"] == ["datasource"]
    assert "sample_limit" not in tools["compare_datasets"].inputSchema["properties"]
    assert "sample_limit" not in tools["profile_relationship"].inputSchema["properties"]


class _FakeService:
    def health(self):
        return {"status": "ok"}

    def list_rules(self):
        return [{"name": "not_null"}]

    def list_datasources(self):
        return {"warehouse": ["users"]}


def test_server_lists_and_reads_discovery_resources():
    server = create_server(MCPSettings("postgres://", Path("contracts").resolve()))
    fake_context = SimpleNamespace(
        request_context=SimpleNamespace(lifespan_context=_FakeService())
    )
    server.get_context = lambda: fake_context

    resources = asyncio.run(server.list_resources())
    templates = asyncio.run(server.list_resource_templates())

    assert {str(resource.uri) for resource in resources} == {
        "kontra://health",
        "kontra://rules",
        "kontra://datasources",
    }
    assert templates == []

    expected = {
        "kontra://health": {"status": "ok"},
        "kontra://rules": [{"name": "not_null"}],
        "kontra://datasources": {"warehouse": ["users"]},
    }
    for uri, payload in expected.items():
        contents = asyncio.run(server.read_resource(uri))
        assert json.loads(contents[0].content) == payload


def test_http_bind_settings_are_explicit():
    server = create_server(
        MCPSettings("postgres://", Path("contracts").resolve()),
        host="0.0.0.0",
        port=9123,
        allow_remote_unauthenticated=True,
    )
    assert server.settings.host == "0.0.0.0"
    assert server.settings.port == 9123


def test_server_factory_refuses_remote_unauthenticated_bind():
    with pytest.raises(ValueError, match="Refusing an unauthenticated"):
        create_server(
            MCPSettings("postgres://", Path("contracts").resolve()),
            host="0.0.0.0",
        )


def test_error_sanitizer_masks_uri_and_query_credentials():
    message = (
        "failed postgres://agent:p@ssword@db/internal "
        "and https://blob/path?sig=secret-signature&token=secret-token"
    )
    sanitized = _sanitize_error_message(message)

    assert "p@ssword" not in sanitized
    assert "secret-signature" not in sanitized
    assert "secret-token" not in sanitized
    assert "postgres://agent:***@db/internal" in sanitized


def test_safe_service_proxy_never_returns_unsanitized_exception():
    class FailingService:
        def health(self):
            raise IOError("postgres://user:password@db/private?token=abc")

    with pytest.raises(ValueError) as error:
        _SafeServiceProxy(FailingService()).health()

    assert "password" not in str(error.value)
    assert "token=abc" not in str(error.value)
