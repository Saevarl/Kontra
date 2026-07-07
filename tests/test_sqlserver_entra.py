# tests/test_sqlserver_entra.py
"""
Tests for Entra ID (Azure AD) authentication on the native SQL Server path,
plus the pymssql/pyodbc paramstyle compatibility shim.

These tests run WITHOUT a SQL Server (no container). A fake ``pyodbc`` module is
injected via ``sys.modules`` so we can assert the exact ODBC connection-string
shape emitted per auth mode, and confirm actionable errors when the driver or
pyodbc itself is missing.

Covers Azure SQL Database and Azure SQL Managed Instance (MI) host shapes.
"""

from __future__ import annotations

import sys
import types

import pytest

from kontra.connectors import sqlserver as ss
from kontra.connectors.sqlserver import SqlServerConnectionParams
from kontra.connectors.db_utils import execute_with_params


# --------------------------------------------------------------------------- #
# Fakes
# --------------------------------------------------------------------------- #


class _FakePyodbcError(Exception):
    pass


def _make_fake_pyodbc(drivers, capture):
    """Build a fake pyodbc module capturing the connection string."""
    mod = types.ModuleType("pyodbc")

    def _drivers():
        return list(drivers)

    def _connect(conn_str, *args, **kwargs):
        capture["conn_str"] = conn_str
        capture["args"] = args
        capture["kwargs"] = kwargs
        return object()  # opaque connection

    mod.drivers = _drivers
    mod.connect = _connect
    mod.Error = _FakePyodbcError
    return mod


@pytest.fixture
def fake_pyodbc(monkeypatch):
    """Inject a fake pyodbc exposing a modern driver; capture connect() input."""
    capture: dict = {}
    drivers = [
        "SQL Server",
        "ODBC Driver 17 for SQL Server",
        "ODBC Driver 18 for SQL Server",
    ]
    mod = _make_fake_pyodbc(drivers, capture)
    monkeypatch.setitem(sys.modules, "pyodbc", mod)
    return capture


def _params(**over):
    base = dict(
        host="myserver.database.windows.net",
        port=1433,
        user="sa",
        password=None,
        database="mydb",
        schema="dbo",
        table="orders",
    )
    base.update(over)
    return SqlServerConnectionParams(**base)


# --------------------------------------------------------------------------- #
# Connection-string shape per auth mode
# --------------------------------------------------------------------------- #


class TestEntraConnectionString:
    def test_entra_mi_system_assigned(self, fake_pyodbc):
        p = _params(auth="entra_mi")
        ss.get_connection(p)
        assert fake_pyodbc["conn_str"] == (
            "Driver={ODBC Driver 18 for SQL Server};"
            "Server=myserver.database.windows.net,1433;"
            "Database=mydb;"
            "Authentication=ActiveDirectoryMsi;"
            "Encrypt=yes;"
            "TrustServerCertificate=no;"
        )

    def test_entra_mi_user_assigned_client_id(self, fake_pyodbc):
        p = _params(auth="entra_mi", client_id="11111111-2222-3333")
        ss.get_connection(p)
        assert fake_pyodbc["conn_str"].endswith(
            "TrustServerCertificate=no;UID=11111111-2222-3333;"
        )
        assert "Authentication=ActiveDirectoryMsi;" in fake_pyodbc["conn_str"]

    def test_entra_default(self, fake_pyodbc):
        p = _params(auth="entra_default")
        ss.get_connection(p)
        assert fake_pyodbc["conn_str"] == (
            "Driver={ODBC Driver 18 for SQL Server};"
            "Server=myserver.database.windows.net,1433;"
            "Database=mydb;"
            "Authentication=ActiveDirectoryDefault;"
            "Encrypt=yes;"
            "TrustServerCertificate=no;"
        )

    def test_entra_service_principal(self, fake_pyodbc):
        p = _params(
            auth="entra_service_principal",
            client_id="app-id",
            client_secret="secret-value",
            tenant_id="tenant-guid",
        )
        ss.get_connection(p)
        cs = fake_pyodbc["conn_str"]
        assert "Authentication=ActiveDirectoryServicePrincipal;" in cs
        assert cs.endswith("UID=app-id;PWD=secret-value;")
        # tenant_id is carried but NOT injected (msodbcsql18 has no such keyword)
        assert "tenant" not in cs.lower()

    def test_entra_interactive(self, fake_pyodbc):
        p = _params(auth="entra_interactive")
        ss.get_connection(p)
        assert "Authentication=ActiveDirectoryInteractive;" in fake_pyodbc["conn_str"]

    def test_managed_instance_public_endpoint_host_and_port(self, fake_pyodbc):
        # MI public endpoint: <mi>.<zone>.database.windows.net on port 3342
        p = _params(
            host="mymi.abcd1234.database.windows.net",
            port=3342,
            database="sales",
            auth="entra_default",
        )
        ss.get_connection(p)
        assert (
            "Server=mymi.abcd1234.database.windows.net,3342;"
            in fake_pyodbc["conn_str"]
        )
        # Encrypt mandatory for MI
        assert "Encrypt=yes;" in fake_pyodbc["conn_str"]

    def test_encrypt_yes_always_present(self, fake_pyodbc):
        for mode in (
            "entra_default",
            "entra_mi",
            "entra_service_principal",
            "entra_interactive",
        ):
            p = _params(auth=mode, client_id="a", client_secret="b")
            ss.get_connection(p)
            assert "Encrypt=yes;" in fake_pyodbc["conn_str"]
            assert "TrustServerCertificate=no;" in fake_pyodbc["conn_str"]

    def test_semicolon_in_value_is_brace_escaped(self, fake_pyodbc):
        # A value with a ';' must be brace-escaped so it can't inject a keyword.
        p = _params(
            auth="entra_service_principal",
            client_id="app",
            client_secret="pa;ss}word",
        )
        ss.get_connection(p)
        cs = fake_pyodbc["conn_str"]
        # '}' doubled, whole value wrapped in braces
        assert "PWD={pa;ss}}word};" in cs

    def test_newest_driver_selected(self, monkeypatch):
        capture: dict = {}
        mod = _make_fake_pyodbc(
            [
                "ODBC Driver 13 for SQL Server",
                "ODBC Driver 18 for SQL Server",
                "ODBC Driver 17 for SQL Server",
            ],
            capture,
        )
        monkeypatch.setitem(sys.modules, "pyodbc", mod)
        ss.get_connection(_params(auth="entra_default"))
        assert "Driver={ODBC Driver 18 for SQL Server};" in capture["conn_str"]


# --------------------------------------------------------------------------- #
# Actionable errors
# --------------------------------------------------------------------------- #


class TestEntraErrors:
    def test_pyodbc_missing_is_actionable(self, monkeypatch):
        # Ensure pyodbc import fails.
        monkeypatch.setitem(sys.modules, "pyodbc", None)
        with pytest.raises(ImportError) as exc:
            ss.get_connection(_params(auth="entra_mi"))
        msg = str(exc.value)
        assert "kontra[sqlserver-entra]" in msg
        assert "msodbcsql18" in msg

    def test_no_driver_installed_is_actionable(self, monkeypatch):
        capture: dict = {}
        mod = _make_fake_pyodbc(["SQL Server", "PostgreSQL Unicode"], capture)
        monkeypatch.setitem(sys.modules, "pyodbc", mod)
        with pytest.raises(RuntimeError) as exc:
            ss.get_connection(_params(auth="entra_default"))
        msg = str(exc.value)
        assert "ODBC Driver" in msg
        assert "msodbcsql18" in msg

    def test_connect_failure_wrapped(self, monkeypatch):
        capture: dict = {}
        mod = _make_fake_pyodbc(["ODBC Driver 18 for SQL Server"], capture)

        def _boom(conn_str, *a, **k):
            raise mod.Error("login failed")

        mod.connect = _boom
        monkeypatch.setitem(sys.modules, "pyodbc", mod)
        with pytest.raises(ConnectionError) as exc:
            ss.get_connection(_params(auth="entra_mi", client_id="cid"))
        assert "Entra ID" in str(exc.value)
        assert "cid" in str(exc.value)


# --------------------------------------------------------------------------- #
# Auth resolution priority
# --------------------------------------------------------------------------- #


class TestAuthResolution:
    def test_uri_query_beats_env(self, monkeypatch):
        monkeypatch.setenv("MSSQL_AUTH", "entra_default")
        p = ss.resolve_connection_params(
            "mssql://h:1433/db/dbo.t?auth=entra_interactive"
        )
        assert p.auth == "entra_interactive"

    def test_env_fallback(self, monkeypatch):
        monkeypatch.setenv("MSSQL_AUTH", "entra_mi")
        monkeypatch.setenv("AZURE_CLIENT_ID", "envcid")
        p = ss.resolve_connection_params("mssql://h:1433/db/dbo.t")
        assert p.auth == "entra_mi"
        assert p.client_id == "envcid"

    def test_default_is_sql(self, monkeypatch):
        monkeypatch.delenv("MSSQL_AUTH", raising=False)
        p = ss.resolve_connection_params("mssql://h:1433/db/dbo.t")
        assert p.auth == "sql"

    def test_client_id_uri_beats_env(self, monkeypatch):
        monkeypatch.setenv("AZURE_CLIENT_ID", "envcid")
        p = ss.resolve_connection_params(
            "mssql://h:1433/db/dbo.t?auth=entra_mi&client_id=uricid"
        )
        assert p.client_id == "uricid"

    def test_invalid_auth_value_raises_listing_allowed(self):
        with pytest.raises(ValueError) as exc:
            ss.resolve_connection_params("mssql://h:1433/db/dbo.t?auth=bogus")
        msg = str(exc.value)
        assert "bogus" in msg
        for allowed in (
            "sql",
            "entra_default",
            "entra_mi",
            "entra_service_principal",
            "entra_interactive",
        ):
            assert allowed in msg

    def test_config_bakes_auth_into_uri(self):
        from kontra.config.settings import KontraConfig, resolve_datasource

        cfg = KontraConfig.model_validate(
            {
                "version": "1",
                "datasources": {
                    "azmi": {
                        "type": "mssql",
                        "host": "mymi.abcd.database.windows.net",
                        "database": "sales",
                        "auth": "entra_default",
                        "tables": {"orders": "dbo.orders"},
                    }
                },
            }
        )
        uri = resolve_datasource("azmi.orders", cfg)
        assert uri == (
            "mssql://mymi.abcd.database.windows.net:1433/sales/dbo.orders"
            "?auth=entra_default"
        )
        # And it round-trips through resolution.
        p = ss.resolve_connection_params(uri)
        assert p.auth == "entra_default"

    def test_config_sql_auth_unchanged(self):
        from kontra.config.settings import KontraConfig, resolve_datasource

        cfg = KontraConfig.model_validate(
            {
                "version": "1",
                "datasources": {
                    "db": {
                        "type": "mssql",
                        "host": "h",
                        "user": "sa",
                        "password": "pw",
                        "database": "d",
                        "tables": {"o": "dbo.o"},
                    }
                },
            }
        )
        assert resolve_datasource("db.o", cfg) == "mssql://sa:pw@h:1433/d/dbo.o"


# --------------------------------------------------------------------------- #
# auth="sql" regression: must still call pymssql identically
# --------------------------------------------------------------------------- #


class TestSqlAuthRegression:
    def test_sql_auth_calls_pymssql_with_identical_kwargs(self, monkeypatch):
        captured: dict = {}
        fake = types.ModuleType("pymssql")

        class _OpErr(Exception):
            pass

        def _connect(**kwargs):
            captured.update(kwargs)
            return object()

        fake.connect = _connect
        fake.OperationalError = _OpErr
        monkeypatch.setitem(sys.modules, "pymssql", fake)

        p = _params(host="localhost", database="mydb", user="sa", password="Secret1")
        ss.get_connection(p)  # default auth="sql"
        assert captured == {
            "server": "localhost",
            "port": 1433,
            "user": "sa",
            "password": "Secret1",
            "database": "mydb",
        }

    def test_sql_auth_does_not_touch_pyodbc(self, monkeypatch):
        # If pyodbc is broken/absent, auth="sql" must not care.
        monkeypatch.setitem(sys.modules, "pyodbc", None)
        fake = types.ModuleType("pymssql")
        fake.connect = lambda **k: object()
        fake.OperationalError = Exception
        monkeypatch.setitem(sys.modules, "pymssql", fake)
        # Should not raise.
        ss.get_connection(_params(auth="sql"))


# --------------------------------------------------------------------------- #
# Paramstyle shim
# --------------------------------------------------------------------------- #


class _FakeCursor:
    """Cursor whose module name we can control to mimic pymssql / pyodbc."""

    def __init__(self):
        self.executed = None
        self.params = None

    def execute(self, sql, params=None):
        self.executed = sql
        self.params = params


def _cursor_from_module(module_name):
    cur = _FakeCursor()
    # Reassign __module__ on the type via a subclass in a fake module.
    cur.__class__ = type(
        "Cursor", (_FakeCursor,), {"__module__": module_name}
    )
    return cur


class TestParamstyleShim:
    def test_pymssql_cursor_keeps_percent_s(self):
        cur = _cursor_from_module("pymssql")
        execute_with_params(cur, "SELECT * FROM t WHERE a = %s", ("x",))
        assert cur.executed == "SELECT * FROM t WHERE a = %s"
        assert cur.params == ("x",)

    def test_pyodbc_cursor_gets_question_mark(self):
        cur = _cursor_from_module("pyodbc")
        execute_with_params(cur, "SELECT * FROM t WHERE a = %s AND b = %s", ("x", "y"))
        assert cur.executed == "SELECT * FROM t WHERE a = ? AND b = ?"
        assert cur.params == ("x", "y")

    def test_params_none_passes_through(self):
        cur = _cursor_from_module("pyodbc")
        execute_with_params(cur, "SELECT COUNT(*) FROM t")
        assert cur.executed == "SELECT COUNT(*) FROM t"
        assert cur.params is None

    def test_psycopg_cursor_untouched(self):
        # postgres uses %s natively; shim must not rewrite it.
        cur = _cursor_from_module("psycopg")
        execute_with_params(cur, "SELECT * FROM t WHERE a = %s", ("x",))
        assert cur.executed == "SELECT * FROM t WHERE a = %s"

    def test_pyodbc_submodule_detected(self):
        cur = _cursor_from_module("pyodbc.something")
        execute_with_params(cur, "WHERE a = %s", ("x",))
        assert cur.executed == "WHERE a = ?"
