# src/kontra/state/backends/sqlserver.py
"""
SQL Server state storage with normalized schema (v0.5).

Schema:
    kontra_runs - Run-level metadata
    kontra_rule_results - Per-rule results (references kontra_runs)
    kontra_annotations - Append-only annotations (references runs/rules)

Queries and row mapping live in sql_base; this module holds the SQL Server
connection handling, DDL and dialect details.
"""

from __future__ import annotations

import json
import logging
import os
import struct
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from types import ModuleType
from typing import Any, Dict
from urllib.parse import parse_qs, urlparse

from .base import StateBackend
from .sql_base import SQLStateBackend

_logger = logging.getLogger(__name__)

# Lazy-loaded pyodbc exception types (pyodbc may not be installed)
_PyodbcError = None


def _get_db_error():
    """Get the pyodbc base error class, lazy-loaded."""
    global _PyodbcError
    if _PyodbcError is None:
        try:
            import pyodbc
            _PyodbcError = pyodbc.Error
        except ImportError:
            _PyodbcError = Exception  # Fallback
    return _PyodbcError


# SQL Server caps a statement at 2100 parameters; IN lists are chunked below it.
_MAX_IN_PARAMS = 2000

# pyodbc has no built-in conversion for DATETIMEOFFSET (ODBC type -155) and
# raises on every read of run_at/created_at, so decode SQL Server's
# SQL_SS_TIMESTAMPOFFSET_STRUCT ourselves.
_SQL_SS_TIMESTAMPOFFSET = -155


def _datetimeoffset_to_datetime(value: bytes | None) -> datetime | None:
    if value is None:
        return None
    year, month, day, hour, minute, second, nanos, tz_hour, tz_minute = struct.unpack(
        "<6hI2h", value
    )
    return datetime(
        year, month, day, hour, minute, second, nanos // 1000,
        tzinfo=timezone(timedelta(hours=tz_hour, minutes=tz_minute)),
    )


class SQLServerStore(SQLStateBackend):
    """
    SQL Server database state storage backend with normalized schema.

    Uses pyodbc for database access. Automatically creates
    the required tables if they don't exist.

    URI format: mssql://user:pass@host:port/database
                sqlserver://user:pass@host:port/database

    Also supports environment variables:
        MSSQL_HOST, MSSQL_PORT, MSSQL_USER, MSSQL_PASSWORD, MSSQL_DATABASE
    """

    # DDL for creating tables (SQL Server syntax)
    CREATE_TABLES_SQL = """
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='kontra_runs' AND xtype='U')
    CREATE TABLE kontra_runs (
        id INT IDENTITY(1,1) PRIMARY KEY,

        -- Identity
        contract_fingerprint NVARCHAR(255) NOT NULL,
        contract_name NVARCHAR(255) NOT NULL,
        dataset_fingerprint NVARCHAR(255),
        dataset_name NVARCHAR(500),

        -- Timing
        run_at DATETIMEOFFSET NOT NULL,
        duration_ms INT,

        -- Summary
        passed BIT NOT NULL,
        total_rows BIGINT,
        total_rules INT NOT NULL,
        passed_rules INT NOT NULL,
        failed_rules INT NOT NULL,

        -- By severity
        blocking_failures INT NOT NULL DEFAULT 0,
        warning_failures INT NOT NULL DEFAULT 0,
        info_failures INT NOT NULL DEFAULT 0,

        -- Execution metadata
        execution_stats NVARCHAR(MAX),  -- JSON string

        -- Schema version
        schema_version NVARCHAR(50) NOT NULL DEFAULT '2.0',
        engine_version NVARCHAR(50)
    );

    IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name='idx_kontra_runs_contract_time')
    CREATE INDEX idx_kontra_runs_contract_time
        ON kontra_runs (contract_fingerprint, run_at DESC);

    IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name='idx_kontra_runs_passed')
    CREATE INDEX idx_kontra_runs_passed
        ON kontra_runs (contract_fingerprint, passed, run_at DESC);

    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='kontra_rule_results' AND xtype='U')
    CREATE TABLE kontra_rule_results (
        id INT IDENTITY(1,1) PRIMARY KEY,
        run_id INT NOT NULL REFERENCES kontra_runs(id) ON DELETE CASCADE,

        -- Rule identity
        rule_id NVARCHAR(255) NOT NULL,
        rule_name NVARCHAR(100) NOT NULL,

        -- Result
        passed BIT NOT NULL,
        failed_count BIGINT NOT NULL DEFAULT 0,

        -- Metadata
        severity NVARCHAR(20) NOT NULL,
        message NVARCHAR(MAX),
        column_name NVARCHAR(255),
        execution_source NVARCHAR(50),

        -- Variable structure
        failure_mode NVARCHAR(100),
        details NVARCHAR(MAX),   -- JSON string
        context NVARCHAR(MAX),   -- JSON string
        samples NVARCHAR(MAX)    -- JSON string
    );

    IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name='idx_kontra_rule_results_run')
    CREATE INDEX idx_kontra_rule_results_run
        ON kontra_rule_results (run_id);

    IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name='idx_kontra_rule_results_rule_id')
    CREATE INDEX idx_kontra_rule_results_rule_id
        ON kontra_rule_results (rule_id, run_id DESC);

    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='kontra_annotations' AND xtype='U')
    CREATE TABLE kontra_annotations (
        id INT IDENTITY(1,1) PRIMARY KEY,

        -- What this annotates. SQL Server rejects a second cascade path
        -- (runs -> rule_results -> annotations, error 1785), so only run_id
        -- cascades. Deleting a run still removes its rule-level annotations.
        run_id INT NOT NULL REFERENCES kontra_runs(id) ON DELETE CASCADE,
        rule_result_id INT REFERENCES kontra_rule_results(id),

        -- Who created it
        actor_type NVARCHAR(50) NOT NULL,
        actor_id NVARCHAR(255) NOT NULL,

        -- What it says
        annotation_type NVARCHAR(100) NOT NULL,
        summary NVARCHAR(MAX) NOT NULL,
        payload NVARCHAR(MAX),  -- JSON string

        -- When
        created_at DATETIMEOFFSET NOT NULL DEFAULT SYSDATETIMEOFFSET()
    );

    IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name='idx_kontra_annotations_run')
    CREATE INDEX idx_kontra_annotations_run
        ON kontra_annotations (run_id);

    IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name='idx_kontra_annotations_time')
    CREATE INDEX idx_kontra_annotations_time
        ON kontra_annotations (created_at DESC);
    """

    _PARAM = "?"
    _DISPLAY_NAME = "SQL Server"
    _CONNECT_HELP = (
        "Set environment variables:\n"
        "  export MSSQL_HOST=localhost\n"
        "  export MSSQL_PORT=1433\n"
        "  export MSSQL_USER=your_user\n"
        "  export MSSQL_PASSWORD=your_password\n"
        "  export MSSQL_DATABASE=your_database\n\n"
        "Or use full URI:\n"
        "  mssql://user:pass@host:1433/database"
    )
    _logger = _logger
    _db_error = staticmethod(_get_db_error)

    # The cross-run annotation query has not been implemented for SQL Server;
    # keep StateBackend's empty default rather than change results here.
    get_annotations_for_contract = StateBackend.get_annotations_for_contract

    @staticmethod
    def _parse_connection_params(uri: str) -> Dict[str, Any]:
        """
        Parse SQL Server connection parameters from URI and environment.

        Priority: URI values > MSSQL_XXX env vars > defaults
        """
        parsed = urlparse(uri)

        # Start with defaults
        params: Dict[str, Any] = {
            "server": "localhost",
            "port": 1433,
            "user": None,
            "password": None,
            "database": None,
            "driver": "{ODBC Driver 17 for SQL Server}",
        }

        # Layer 1: Environment variables
        if os.getenv("MSSQL_HOST"):
            params["server"] = os.getenv("MSSQL_HOST")
        if os.getenv("MSSQL_PORT"):
            params["port"] = int(os.getenv("MSSQL_PORT"))
        if os.getenv("MSSQL_USER"):
            params["user"] = os.getenv("MSSQL_USER")
        if os.getenv("MSSQL_PASSWORD"):
            params["password"] = os.getenv("MSSQL_PASSWORD")
        if os.getenv("MSSQL_DATABASE"):
            params["database"] = os.getenv("MSSQL_DATABASE")
        if os.getenv("MSSQL_DRIVER"):
            params["driver"] = os.getenv("MSSQL_DRIVER")

        # Layer 2: Explicit URI values (highest priority)
        if parsed.hostname:
            params["server"] = parsed.hostname
        if parsed.port:
            params["port"] = parsed.port
        if parsed.username:
            params["user"] = parsed.username
        if parsed.password:
            params["password"] = parsed.password
        if parsed.path and parsed.path != "/":
            params["database"] = parsed.path.strip("/").split("/")[0]

        query_params = parse_qs(parsed.query)
        for key, values in query_params.items():
            if values:
                params[key] = values[0]

        return params

    def _import_driver(self) -> ModuleType:
        try:
            import pyodbc
        except ImportError as e:
            raise RuntimeError(
                "SQL Server state backend requires 'pyodbc'. "
                "Install with: pip install pyodbc"
            ) from e
        return pyodbc

    def _connect(self, driver: ModuleType) -> Any:
        # Build connection string
        conn_str_parts = [
            f"DRIVER={self._conn_params['driver']}",
            f"SERVER={self._conn_params['server']},{self._conn_params['port']}",
        ]
        if self._conn_params.get("database"):
            conn_str_parts.append(f"DATABASE={self._conn_params['database']}")
        if self._conn_params.get("user"):
            conn_str_parts.append(f"UID={self._conn_params['user']}")
        if self._conn_params.get("password"):
            conn_str_parts.append(f"PWD={self._conn_params['password']}")

        conn_str = ";".join(conn_str_parts)

        conn = driver.connect(conn_str)
        conn.add_output_converter(_SQL_SS_TIMESTAMPOFFSET, _datetimeoffset_to_datetime)
        return conn

    def _ddl_statements(self) -> list[str]:
        # Execute each statement separately (SQL Server doesn't like batches with CREATE)
        return [st.strip() for st in self.CREATE_TABLES_SQL.split(";") if st.strip()]

    @contextmanager
    def _cursor(self, conn: Any) -> Iterator[Any]:
        # pyodbc's own cursor context manager commits on exit; transactions
        # are committed or rolled back explicitly, so only close here.
        cursor = conn.cursor()
        try:
            yield cursor
        finally:
            cursor.close()

    def _insert_returning_id(self, table: str, columns: Sequence[str]) -> str:
        return (
            f"INSERT INTO {table} ({', '.join(columns)}) "
            f"OUTPUT INSERTED.id VALUES ({self._placeholders(len(columns))})"
        )

    def _select_top(
        self,
        columns: str,
        body: str,
        params: tuple[Any, ...],
        limit: int | None = None,
    ) -> tuple[str, tuple[Any, ...]]:
        if limit is None:
            return f"SELECT TOP 1 {columns} {body}", params
        return f"SELECT TOP (?) {columns} {body}", (limit, *params)

    def _in_clauses(
        self, column: str, values: Sequence[Any]
    ) -> Iterator[tuple[str, tuple[Any, ...]]]:
        for start in range(0, len(values), _MAX_IN_PARAMS):
            chunk = tuple(values[start:start + _MAX_IN_PARAMS])
            yield f"{column} IN ({self._placeholders(len(chunk))})", chunk

    @staticmethod
    def _decode_json(value: Any) -> Any:
        # JSON is stored as NVARCHAR text
        if not value:
            return None
        try:
            return json.loads(value)
        except (json.JSONDecodeError, ValueError):
            return None  # Malformed JSON, use default

    def __repr__(self) -> str:
        server = self._conn_params.get("server", "?")
        database = self._conn_params.get("database", "?")
        return f"SQLServerStore(server={server}, database={database})"
