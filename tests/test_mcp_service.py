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


def test_probe_preflight_rejects_large_materialization(tmp_path, monkeypatch):
    import kontra

    service = _service(tmp_path)
    service.settings = MCPSettings("postgres://", tmp_path, max_probe_rows=10)
    profile = type("Profile", (), {"row_count": 11, "row_count_estimated": False})()
    monkeypatch.setattr(kontra, "profile", lambda *args, **kwargs: profile)

    with pytest.raises(ValueError, match="materialization limit"):
        service._enforce_probe_row_limit("warehouse.users")


def test_probe_key_bounds_and_configured_sources(tmp_path):
    service = _service(tmp_path)

    service._validate_probe_inputs(
        "warehouse.users", "warehouse.orders", ["id"], None, None
    )
    with pytest.raises(ValueError, match="between 1 and 8"):
        service._validate_probe_inputs(
            "warehouse.users", "warehouse.orders", [], None, None
        )
    with pytest.raises(ValueError, match="Unknown configured"):
        service._validate_probe_inputs(
            "warehouse.users", "missing.users", "id", None, None
        )


@pytest.mark.parametrize("run_id", ["0", "-1", "latest", "1.2", "1" * 20])
def test_validation_run_rejects_unscoped_or_ambiguous_ids(tmp_path, run_id):
    contract = tmp_path / "users.yml"
    contract.write_text("name: users\nrules: []\n", encoding="utf-8")
    service = _service(tmp_path)
    service._store_lock = threading.RLock()
    service._state_store = type("Store", (), {"get_history": lambda *args, **kwargs: []})()

    with pytest.raises(ValueError, match="positive numeric"):
        service.get_validation_run("users", run_id=run_id)


def test_failure_samples_request_only_rule_relevant_columns(tmp_path, monkeypatch):
    import kontra

    contract = tmp_path / "users.yml"
    contract.write_text("name: users\nrules: []\n", encoding="utf-8")
    service = _service(tmp_path)
    service._store_lock = threading.RLock()
    service._enforce_probe_row_limit = lambda *args: None
    captured = {}

    rule = type(
        "Rule",
        (),
        {"rule_id": "COL:id:not_null", "passed": False, "failed_count": 1, "tally": True},
    )()
    samples = type("Samples", (), {"to_dict": lambda self: [{"id": None}]})()
    result = type(
        "Result",
        (),
        {
            "rules": [rule],
            "sample_failures": lambda self, *args, **kwargs: samples,
        },
    )()

    def fake_validate(*args, **kwargs):
        captured.update(kwargs)
        return result

    monkeypatch.setattr(kontra, "validate", fake_validate)
    payload = service.measure_failure_samples(
        "warehouse.users", "users", "COL:id:not_null"
    )

    assert captured["sample_columns"] == "relevant"
    assert payload["sample_columns"] == "relevant"
    assert payload["samples"] == [{"id": None}]


def test_profile_returns_and_stores_only_the_configured_alias(tmp_path, monkeypatch):
    import importlib
    import kontra

    service = _service(tmp_path)
    service._store_lock = threading.RLock()
    saved = []
    service._profile_store = type(
        "Store", (), {"save": lambda self, state: saved.append(state)}
    )()
    profile = type(
        "Profile",
        (),
        {
            "source_uri": "mssql://sa:secret@private-db/app/dbo.users",
            "profiled_at": "2026-07-14T00:00:00Z",
            "to_dict": lambda self: {
                "source_uri": self.source_uri,
                "dataset": {"row_count": 5},
            },
        },
    )()

    def fake_state(result):
        stored_profile = type("StoredProfile", (), {"source_uri": result.source_uri})()
        return type(
            "State",
            (),
            {"source_uri": result.source_uri, "profile": stored_profile},
        )()

    monkeypatch.setattr(kontra, "profile", lambda *args, **kwargs: profile)
    store_module = importlib.import_module("kontra.scout.store")
    monkeypatch.setattr(store_module, "create_profile_state", fake_state)

    payload = service.profile("warehouse.users")

    assert payload["source_uri"] == "warehouse.users"
    assert saved[0].source_uri == "warehouse.users"
    assert saved[0].profile.source_uri == "warehouse.users"


def test_health_does_not_expose_config_path(tmp_path, monkeypatch):
    import kontra

    service = _service(tmp_path)
    monkeypatch.setattr(
        kontra,
        "health",
        lambda: {"status": "ok", "config_path": "/srv/private/.kontra/config.yml"},
    )

    payload = service.health()

    assert "config_path" not in payload
