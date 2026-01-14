# src/kontra/state/backends/postgres.py
"""
PostgreSQL state storage.

Stores validation states in a PostgreSQL database table.

Schema:
    CREATE TABLE kontra_state (
        id SERIAL PRIMARY KEY,
        contract_fingerprint TEXT NOT NULL,
        dataset_fingerprint TEXT,
        contract_name TEXT NOT NULL,
        run_at TIMESTAMPTZ NOT NULL,
        passed BOOLEAN NOT NULL,
        total_rules INT NOT NULL,
        passed_rules INT NOT NULL,
        failed_rules INT NOT NULL,
        blocking_failures INT DEFAULT 0,
        warning_failures INT DEFAULT 0,
        info_failures INT DEFAULT 0,
        row_count BIGINT,
        state JSONB NOT NULL
    );

    CREATE INDEX idx_kontra_state_contract_time
        ON kontra_state (contract_fingerprint, run_at DESC);
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse, parse_qs

from .base import StateBackend
from kontra.state.types import ValidationState


class PostgresStore(StateBackend):
    """
    PostgreSQL database state storage backend.

    Uses psycopg3 (psycopg) for database access. Automatically creates
    the required table if it doesn't exist.

    URI format: postgres://user:pass@host:port/database
                postgresql://user:pass@host:port/database

    Also supports standard PostgreSQL environment variables:
        PGHOST, PGPORT, PGUSER, PGPASSWORD, PGDATABASE
    """

    TABLE_NAME = "kontra_state"

    CREATE_TABLE_SQL = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        id SERIAL PRIMARY KEY,
        contract_fingerprint TEXT NOT NULL,
        dataset_fingerprint TEXT,
        contract_name TEXT NOT NULL,
        run_at TIMESTAMPTZ NOT NULL,
        passed BOOLEAN NOT NULL,
        total_rules INT NOT NULL,
        passed_rules INT NOT NULL,
        failed_rules INT NOT NULL,
        blocking_failures INT DEFAULT 0,
        warning_failures INT DEFAULT 0,
        info_failures INT DEFAULT 0,
        row_count BIGINT,
        state JSONB NOT NULL
    );

    CREATE INDEX IF NOT EXISTS idx_kontra_state_contract_time
        ON {TABLE_NAME} (contract_fingerprint, run_at DESC);
    """

    def __init__(self, uri: str):
        """
        Initialize the PostgreSQL store.

        Args:
            uri: PostgreSQL connection URI

        The URI can be a full connection string or just the scheme,
        with connection details from environment variables.
        """
        self.uri = uri
        self._conn_params = self._parse_connection_params(uri)
        self._conn = None
        self._table_created = False

    @staticmethod
    def _parse_connection_params(uri: str) -> Dict[str, Any]:
        """
        Parse PostgreSQL connection parameters from URI and environment.

        Priority: URI values > PGXXX env vars > defaults
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

        # Parse query parameters
        query_params = parse_qs(parsed.query)
        for key, values in query_params.items():
            if values:
                params[key] = values[0]

        return params

    def _get_conn(self):
        """Get or create the database connection."""
        if self._conn is not None:
            return self._conn

        try:
            import psycopg
        except ImportError as e:
            raise RuntimeError(
                "PostgreSQL state backend requires 'psycopg'. "
                "Install with: pip install psycopg[binary]"
            ) from e

        # Build connection string
        conn_str = f"host={self._conn_params['host']} port={self._conn_params['port']}"
        if self._conn_params.get("user"):
            conn_str += f" user={self._conn_params['user']}"
        if self._conn_params.get("password"):
            conn_str += f" password={self._conn_params['password']}"
        if self._conn_params.get("dbname"):
            conn_str += f" dbname={self._conn_params['dbname']}"

        try:
            self._conn = psycopg.connect(conn_str)
            self._ensure_table()
        except Exception as e:
            raise ConnectionError(
                f"Failed to connect to PostgreSQL: {e}\n\n"
                "Set environment variables:\n"
                "  export PGHOST=localhost\n"
                "  export PGPORT=5432\n"
                "  export PGUSER=your_user\n"
                "  export PGPASSWORD=your_password\n"
                "  export PGDATABASE=your_database\n\n"
                "Or use full URI:\n"
                "  postgres://user:pass@host:5432/database"
            ) from e

        return self._conn

    def _ensure_table(self) -> None:
        """Create the state table if it doesn't exist."""
        if self._table_created:
            return

        conn = self._conn
        with conn.cursor() as cur:
            cur.execute(self.CREATE_TABLE_SQL)
        conn.commit()
        self._table_created = True

    def save(self, state: ValidationState) -> None:
        """Save a validation state to the database."""
        conn = self._get_conn()

        sql = f"""
        INSERT INTO {self.TABLE_NAME} (
            contract_fingerprint,
            dataset_fingerprint,
            contract_name,
            run_at,
            passed,
            total_rules,
            passed_rules,
            failed_rules,
            blocking_failures,
            warning_failures,
            info_failures,
            row_count,
            state
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """

        try:
            with conn.cursor() as cur:
                cur.execute(sql, (
                    state.contract_fingerprint,
                    state.dataset_fingerprint,
                    state.contract_name,
                    state.run_at,
                    state.summary.passed,
                    state.summary.total_rules,
                    state.summary.passed_rules,
                    state.summary.failed_rules,
                    state.summary.blocking_failures,
                    state.summary.warning_failures,
                    state.summary.info_failures,
                    state.summary.row_count,
                    json.dumps(state.to_dict()),
                ))
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise IOError(f"Failed to save state to PostgreSQL: {e}") from e

    def get_latest(self, contract_fingerprint: str) -> Optional[ValidationState]:
        """Get the most recent state for a contract."""
        conn = self._get_conn()

        sql = f"""
        SELECT state FROM {self.TABLE_NAME}
        WHERE contract_fingerprint = %s
        ORDER BY run_at DESC
        LIMIT 1
        """

        try:
            with conn.cursor() as cur:
                cur.execute(sql, (contract_fingerprint,))
                row = cur.fetchone()
                if row:
                    return ValidationState.from_dict(row[0])
                return None
        except Exception:
            return None

    def get_history(
        self,
        contract_fingerprint: str,
        limit: int = 10,
    ) -> List[ValidationState]:
        """Get recent history for a contract, newest first."""
        conn = self._get_conn()

        sql = f"""
        SELECT state FROM {self.TABLE_NAME}
        WHERE contract_fingerprint = %s
        ORDER BY run_at DESC
        LIMIT %s
        """

        try:
            with conn.cursor() as cur:
                cur.execute(sql, (contract_fingerprint, limit))
                rows = cur.fetchall()
                return [ValidationState.from_dict(row[0]) for row in rows]
        except Exception:
            return []

    def get_at(
        self,
        contract_fingerprint: str,
        timestamp: datetime,
    ) -> Optional[ValidationState]:
        """Get state at or before a specific timestamp."""
        conn = self._get_conn()

        sql = f"""
        SELECT state FROM {self.TABLE_NAME}
        WHERE contract_fingerprint = %s AND run_at <= %s
        ORDER BY run_at DESC
        LIMIT 1
        """

        try:
            with conn.cursor() as cur:
                cur.execute(sql, (contract_fingerprint, timestamp))
                row = cur.fetchone()
                if row:
                    return ValidationState.from_dict(row[0])
                return None
        except Exception:
            return None

    def delete_old(
        self,
        contract_fingerprint: str,
        keep_count: int = 100,
    ) -> int:
        """Delete old states, keeping the most recent ones."""
        conn = self._get_conn()

        # Get IDs to keep
        sql_keep = f"""
        SELECT id FROM {self.TABLE_NAME}
        WHERE contract_fingerprint = %s
        ORDER BY run_at DESC
        LIMIT %s
        """

        sql_delete = f"""
        DELETE FROM {self.TABLE_NAME}
        WHERE contract_fingerprint = %s
        AND id NOT IN (
            SELECT id FROM {self.TABLE_NAME}
            WHERE contract_fingerprint = %s
            ORDER BY run_at DESC
            LIMIT %s
        )
        """

        try:
            with conn.cursor() as cur:
                cur.execute(sql_delete, (contract_fingerprint, contract_fingerprint, keep_count))
                deleted = cur.rowcount
            conn.commit()
            return deleted
        except Exception:
            conn.rollback()
            return 0

    def list_contracts(self) -> List[str]:
        """List all contract fingerprints with stored state."""
        conn = self._get_conn()

        sql = f"""
        SELECT DISTINCT contract_fingerprint FROM {self.TABLE_NAME}
        ORDER BY contract_fingerprint
        """

        try:
            with conn.cursor() as cur:
                cur.execute(sql)
                rows = cur.fetchall()
                return [row[0] for row in rows]
        except Exception:
            return []

    def clear(self, contract_fingerprint: Optional[str] = None) -> int:
        """
        Clear stored states.

        Args:
            contract_fingerprint: If provided, only clear this contract's states.
                                 If None, clear all states.

        Returns:
            Number of state rows deleted.
        """
        conn = self._get_conn()

        try:
            with conn.cursor() as cur:
                if contract_fingerprint:
                    cur.execute(
                        f"DELETE FROM {self.TABLE_NAME} WHERE contract_fingerprint = %s",
                        (contract_fingerprint,)
                    )
                else:
                    cur.execute(f"DELETE FROM {self.TABLE_NAME}")
                deleted = cur.rowcount
            conn.commit()
            return deleted
        except Exception:
            conn.rollback()
            return 0

    def close(self) -> None:
        """Close the database connection."""
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def __repr__(self) -> str:
        host = self._conn_params.get("host", "?")
        dbname = self._conn_params.get("dbname", "?")
        return f"PostgresStore(host={host}, dbname={dbname})"

    def __del__(self):
        self.close()
