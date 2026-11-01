# src/kontra/state/backends/postgres.py
"""
PostgreSQL state storage with normalized schema (v0.5).

Schema:
    kontra_runs - Run-level metadata
    kontra_rule_results - Per-rule results (references kontra_runs)
    kontra_annotations - Append-only annotations (references runs/rules)

Queries and row mapping live in sql_base; this module holds the PostgreSQL
connection handling, DDL and dialect details.
"""

from __future__ import annotations

import logging
import os
from collections.abc import Iterator, Sequence
from types import ModuleType
from typing import Any, Dict
from urllib.parse import parse_qs, urlparse

from .sql_base import SQLStateBackend

_logger = logging.getLogger(__name__)

# Lazy-loaded psycopg exception types (psycopg may not be installed)
_PsycopgError = None


def _get_db_error():
    """Get the psycopg base error class, lazy-loaded."""
    global _PsycopgError
    if _PsycopgError is None:
        try:
            import psycopg
            _PsycopgError = psycopg.Error
        except ImportError:
            _PsycopgError = Exception  # Fallback
    return _PsycopgError


class PostgresStore(SQLStateBackend):
    """
    PostgreSQL database state storage backend with normalized schema.

    Uses psycopg3 (psycopg) for database access. Automatically creates
    the required tables if they don't exist.

    URI format: postgres://user:pass@host:port/database
                postgresql://user:pass@host:port/database

    Also supports standard PostgreSQL environment variables:
        PGHOST, PGPORT, PGUSER, PGPASSWORD, PGDATABASE
    """

    # Legacy table for migration detection
    LEGACY_TABLE = "kontra_state"

    CREATE_TABLES_SQL = """
    -- Run-level metadata
    CREATE TABLE IF NOT EXISTS kontra_runs (
        id SERIAL PRIMARY KEY,

        -- Identity
        contract_fingerprint TEXT NOT NULL,
        contract_name TEXT NOT NULL,
        dataset_fingerprint TEXT,
        dataset_name TEXT,

        -- Timing
        run_at TIMESTAMPTZ NOT NULL,
        duration_ms INT,

        -- Summary
        passed BOOLEAN NOT NULL,
        total_rows BIGINT,
        total_rules INT NOT NULL,
        passed_rules INT NOT NULL,
        failed_rules INT NOT NULL,

        -- By severity
        blocking_failures INT NOT NULL DEFAULT 0,
        warning_failures INT NOT NULL DEFAULT 0,
        info_failures INT NOT NULL DEFAULT 0,

        -- Execution metadata
        execution_stats JSONB,

        -- Schema version
        schema_version TEXT NOT NULL DEFAULT '2.0',
        engine_version TEXT
    );

    CREATE INDEX IF NOT EXISTS idx_kontra_runs_contract_time
        ON kontra_runs (contract_fingerprint, run_at DESC);

    CREATE INDEX IF NOT EXISTS idx_kontra_runs_passed
        ON kontra_runs (contract_fingerprint, passed, run_at DESC);

    -- Per-rule results
    CREATE TABLE IF NOT EXISTS kontra_rule_results (
        id SERIAL PRIMARY KEY,
        run_id INT NOT NULL REFERENCES kontra_runs(id) ON DELETE CASCADE,

        -- Rule identity
        rule_id TEXT NOT NULL,
        rule_name TEXT NOT NULL,

        -- Result
        passed BOOLEAN NOT NULL,
        failed_count BIGINT NOT NULL DEFAULT 0,

        -- Metadata
        severity TEXT NOT NULL,
        message TEXT,
        column_name TEXT,
        execution_source TEXT,

        -- Variable structure
        failure_mode TEXT,
        details JSONB,
        context JSONB,
        samples JSONB
    );

    CREATE INDEX IF NOT EXISTS idx_kontra_rule_results_run
        ON kontra_rule_results (run_id);

    CREATE INDEX IF NOT EXISTS idx_kontra_rule_results_rule_id
        ON kontra_rule_results (rule_id, run_id DESC);

    -- Annotations (append-only)
    CREATE TABLE IF NOT EXISTS kontra_annotations (
        id SERIAL PRIMARY KEY,

        -- What this annotates
        run_id INT NOT NULL REFERENCES kontra_runs(id) ON DELETE CASCADE,
        rule_result_id INT REFERENCES kontra_rule_results(id) ON DELETE CASCADE,

        -- Who created it
        actor_type TEXT NOT NULL,
        actor_id TEXT NOT NULL,

        -- What it says
        annotation_type TEXT NOT NULL,
        summary TEXT NOT NULL,
        payload JSONB,

        -- When
        created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );

    CREATE INDEX IF NOT EXISTS idx_kontra_annotations_run
        ON kontra_annotations (run_id);

    CREATE INDEX IF NOT EXISTS idx_kontra_annotations_rule
        ON kontra_annotations (rule_result_id)
        WHERE rule_result_id IS NOT NULL;

    CREATE INDEX IF NOT EXISTS idx_kontra_annotations_time
        ON kontra_annotations (created_at DESC);
    """

    _PARAM = "%s"
    _DISPLAY_NAME = "PostgreSQL"
    _CONNECT_HELP = (
        "Set environment variables:\n"
        "  export PGHOST=localhost\n"
        "  export PGPORT=5432\n"
        "  export PGUSER=your_user\n"
        "  export PGPASSWORD=your_password\n"
        "  export PGDATABASE=your_database\n\n"
        "Or use full URI:\n"
        "  postgres://user:pass@host:5432/database"
    )
    _logger = _logger
    _db_error = staticmethod(_get_db_error)

    @staticmethod
    def _parse_connection_params(uri: str) -> Dict[str, Any]:
        """
        Parse PostgreSQL connection parameters from URI and environment.

        Priority: URI values > DATABASE_URL > PGXXX env vars > defaults
        """
        parsed = urlparse(uri)

        # Start with defaults
        params: Dict[str, Any] = {
            "host": "localhost",
            "port": 5432,
            "user": os.getenv("USER", "postgres"),
            "password": None,
            "dbname": None,
        }

        # Layer 1: Standard PGXXX environment variables
        if os.getenv("PGHOST"):
            params["host"] = os.getenv("PGHOST")
        if os.getenv("PGPORT"):
            params["port"] = int(os.getenv("PGPORT"))
        if os.getenv("PGUSER"):
            params["user"] = os.getenv("PGUSER")
        if os.getenv("PGPASSWORD"):
            params["password"] = os.getenv("PGPASSWORD")
        if os.getenv("PGDATABASE"):
            params["dbname"] = os.getenv("PGDATABASE")

        # Layer 2: DATABASE_URL (common in PaaS)
        database_url = os.getenv("DATABASE_URL")
        if database_url:
            db_parsed = urlparse(database_url)
            if db_parsed.hostname:
                params["host"] = db_parsed.hostname
            if db_parsed.port:
                params["port"] = db_parsed.port
            if db_parsed.username:
                params["user"] = db_parsed.username
            if db_parsed.password:
                params["password"] = db_parsed.password
            if db_parsed.path and db_parsed.path != "/":
                params["dbname"] = db_parsed.path.strip("/").split("/")[0]

        # Layer 3: Explicit URI values (highest priority)
        if parsed.hostname:
            params["host"] = parsed.hostname
        if parsed.port:
            params["port"] = parsed.port
        if parsed.username:
            params["user"] = parsed.username
        if parsed.password:
            params["password"] = parsed.password
        if parsed.path and parsed.path != "/":
            params["dbname"] = parsed.path.strip("/").split("/")[0]

        query_params = parse_qs(parsed.query)
        for key, values in query_params.items():
            if values:
                params[key] = values[0]

        return params

    def _import_driver(self) -> ModuleType:
        try:
            import psycopg
        except ImportError as e:
            raise RuntimeError(
                "PostgreSQL state backend requires 'psycopg'. "
                "Install with: pip install psycopg[binary]"
            ) from e
        return psycopg

    def _connect(self, driver: ModuleType) -> Any:
        conn_str = f"host={self._conn_params['host']} port={self._conn_params['port']}"
        if self._conn_params.get("user"):
            conn_str += f" user={self._conn_params['user']}"
        if self._conn_params.get("password"):
            conn_str += f" password={self._conn_params['password']}"
        if self._conn_params.get("dbname"):
            conn_str += f" dbname={self._conn_params['dbname']}"
        return driver.connect(conn_str)

    def _insert_returning_id(self, table: str, columns: Sequence[str]) -> str:
        return (
            f"INSERT INTO {table} ({', '.join(columns)}) "
            f"VALUES ({self._placeholders(len(columns))}) RETURNING id"
        )

    def _select_top(
        self,
        columns: str,
        body: str,
        params: tuple[Any, ...],
        limit: int | None = None,
    ) -> tuple[str, tuple[Any, ...]]:
        if limit is None:
            return f"SELECT {columns} {body} LIMIT 1", params
        return f"SELECT {columns} {body} LIMIT %s", (*params, limit)

    def _in_clauses(
        self, column: str, values: Sequence[Any]
    ) -> Iterator[tuple[str, tuple[Any, ...]]]:
        # One array parameter, whatever the number of values
        yield f"{column} = ANY(%s)", (list(values),)

    @staticmethod
    def _decode_json(value: Any) -> Any:
        # JSONB columns arrive already decoded
        return value

    def __repr__(self) -> str:
        host = self._conn_params.get("host", "?")
        dbname = self._conn_params.get("dbname", "?")
        return f"PostgresStore(host={host}, dbname={dbname})"
