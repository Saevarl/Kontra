from pathlib import Path
import threading
import time

import pytest

from kontra.mcp.cli import build_parser, main
from kontra.mcp.service import KontraMCPService, MCPSettings


def _service(tmp_path: Path) -> KontraMCPService:
    service = object.__new__(KontraMCPService)
    service.settings = MCPSettings("postgres://", tmp_path)
    service.list_datasources = lambda: {"warehouse": ["users", "orders"]}
    return service


def test_settings_require_postgres(monkeypatch):
    for name in (
        "KONTRA_MCP_POSTGRES_URI",
        "DATABASE_URL",
        "PGDATABASE",
    ):
        monkeypatch.delenv(name, raising=False)

    with pytest.raises(ValueError, match="PostgreSQL is required"):
        MCPSettings.from_env()


def test_settings_reject_non_postgres_backend(monkeypatch):
    monkeypatch.setenv("KONTRA_MCP_POSTGRES_URI", "sqlite:///state.db")

    with pytest.raises(ValueError, match="PostgreSQL URI"):
        MCPSettings.from_env()


def test_datasource_boundary_rejects_uri_and_unknown_name(tmp_path):
    service = _service(tmp_path)

    with pytest.raises(ValueError, match="Only configured"):
        service._require_datasource("postgres://secret@example/db/public.users")
    with pytest.raises(ValueError, match="Unknown configured"):
        service._require_datasource("warehouse.missing")

    service._require_datasource("warehouse.users")


def test_contract_boundary_resolves_only_inside_root(tmp_path):
    contract = tmp_path / "users.yml"
    contract.write_text("name: users\nrules: []\n", encoding="utf-8")
    service = _service(tmp_path)

    assert service._contract_path("users") == contract
    with pytest.raises(ValueError, match="inside"):
        service._contract_path("../private.yml")


def test_settings_normalize_programmatic_paths(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    settings = MCPSettings("postgres://", Path("contracts"), Path("config.yml"))

    assert settings.contracts_dir == (tmp_path / "contracts").resolve()
    assert settings.config_path == (tmp_path / "config.yml").resolve()


@pytest.mark.parametrize("value", [0, 101, True, "5"])
def test_history_limit_is_bounded(value):
    with pytest.raises(ValueError, match="limit"):
        KontraMCPService._limit(value)


def test_since_parser_accepts_duration_and_rejects_invalid():
    assert KontraMCPService._parse_since("24h") is not None
    assert KontraMCPService._parse_since("7d") is not None
    with pytest.raises(ValueError, match="since"):
        KontraMCPService._parse_since("tomorrow-ish")


def test_cli_defaults_to_stdio():
    args = build_parser().parse_args([])
    assert args.transport == "stdio"
    assert args.host == "127.0.0.1"
    assert args.port == 8000


def test_cli_refuses_remote_unauthenticated_http():
    with pytest.raises(SystemExit, match="Refusing an unauthenticated"):
        main(["--transport", "streamable-http", "--host", "0.0.0.0"])


def test_history_calls_serialize_shared_postgres_connection(tmp_path):
    contract = tmp_path / "users.yml"
    contract.write_text("name: users\nrules: []\n", encoding="utf-8")
    service = _service(tmp_path)
    service._store_lock = threading.RLock()

    class SlowStore:
        def __init__(self):
            self.active = 0
            self.max_active = 0
            self.guard = threading.Lock()

        def get_run_summaries(self, *args, **kwargs):
            with self.guard:
                self.active += 1
                self.max_active = max(self.max_active, self.active)
            time.sleep(0.03)
            with self.guard:
                self.active -= 1
            return []

    store = SlowStore()
    service._state_store = store
    threads = [
        threading.Thread(target=service.validation_history, args=("users",))
        for _ in range(2)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert store.max_active == 1
