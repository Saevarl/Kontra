# src/kontra/state/backends/sqlserver.py
"""
SQL Server state storage with normalized schema (v0.5).

Schema:
    kontra_runs - Run-level metadata
    kontra_rule_results - Per-rule results (references kontra_runs)
    kontra_annotations - Append-only annotations (references runs/rules)
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse, parse_qs

from .base import StateBackend

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
from kontra.state.types import (
    Annotation,
    RuleState,
    StateSummary,
    ValidationState,
)


class SQLServerStore(StateBackend):
    """
    SQL Server database state storage backend with normalized schema.

    Uses pyodbc for database access. Automatically creates
    the required tables if they don't exist.

    URI format: mssql://user:pass@host:port/database
                sqlserver://user:pass@host:port/database

    Also supports environment variables:
        MSSQL_HOST, MSSQL_PORT, MSSQL_USER, MSSQL_PASSWORD, MSSQL_DATABASE
    """

    # Table names
    RUNS_TABLE = "kontra_runs"
    RULE_RESULTS_TABLE = "kontra_rule_results"
    ANNOTATIONS_TABLE = "kontra_annotations"

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

        -- What this annotates
        run_id INT NOT NULL REFERENCES kontra_runs(id) ON DELETE CASCADE,
        rule_result_id INT REFERENCES kontra_rule_results(id) ON DELETE CASCADE,

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

    def __init__(self, uri: str):
        """
        Initialize the SQL Server store.

        Args:
            uri: SQL Server connection URI

        The URI can be a full connection string or just the scheme,
        with connection details from environment variables.
        """
        self.uri = uri
        self._conn_params = self._parse_connection_params(uri)
        self._conn = None
        self._tables_created = False

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
            import pyodbc
        except ImportError as e:
            raise RuntimeError(
                "SQL Server state backend requires 'pyodbc'. "
                "Install with: pip install pyodbc"
            ) from e

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

        try:
            self._conn = pyodbc.connect(conn_str)
            self._ensure_tables()
        except Exception as e:
            raise ConnectionError(
                f"Failed to connect to SQL Server: {e}\n\n"
                "Set environment variables:\n"
                "  export MSSQL_HOST=localhost\n"
                "  export MSSQL_PORT=1433\n"
                "  export MSSQL_USER=your_user\n"
                "  export MSSQL_PASSWORD=your_password\n"
                "  export MSSQL_DATABASE=your_database\n\n"
                "Or use full URI:\n"
                "  mssql://user:pass@host:1433/database"
            ) from e

        return self._conn

    def _ensure_tables(self) -> None:
        """Create the state tables if they don't exist."""
        if self._tables_created:
            return

        conn = self._conn
        cursor = conn.cursor()
        # Execute each statement separately (SQL Server doesn't like batches with CREATE)
        for statement in self.CREATE_TABLES_SQL.split(";"):
            statement = statement.strip()
            if statement:
                cursor.execute(statement)
        conn.commit()
        self._tables_created = True

    def save(self, state: ValidationState) -> None:
        """Save a validation state to the database (normalized)."""
        conn = self._get_conn()

        # Insert run
        run_sql = f"""
        INSERT INTO {self.RUNS_TABLE} (
            contract_fingerprint,
            contract_name,
            dataset_fingerprint,
            dataset_name,
            run_at,
            duration_ms,
            passed,
            total_rows,
            total_rules,
            passed_rules,
            failed_rules,
            blocking_failures,
            warning_failures,
            info_failures,
            schema_version,
            engine_version
        ) OUTPUT INSERTED.id VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
        """

        # Insert rule result
        rule_sql = f"""
        INSERT INTO {self.RULE_RESULTS_TABLE} (
            run_id,
            rule_id,
            rule_name,
            passed,
            failed_count,
            severity,
            message,
            column_name,
            execution_source,
            failure_mode,
            details,
            context,
            samples
        ) OUTPUT INSERTED.id VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
        """

        try:
            cursor = conn.cursor()

            # Insert run
            cursor.execute(run_sql, (
                state.contract_fingerprint,
                state.contract_name,
                state.dataset_fingerprint,
                state.dataset_uri,
                state.run_at,
                state.duration_ms,
                state.summary.passed,
                state.summary.row_count,
                state.summary.total_rules,
                state.summary.passed_rules,
                state.summary.failed_rules,
                state.summary.blocking_failures,
                state.summary.warning_failures,
                state.summary.info_failures,
                state.schema_version,
                state.engine_version,
            ))
            run_id = cursor.fetchone()[0]

            # Insert rule results
            for rule in state.rules:
                cursor.execute(rule_sql, (
                    run_id,
                    rule.rule_id,
                    rule.rule_name,
                    rule.passed,
                    rule.failed_count,
                    rule.severity,
                    rule.message,
                    rule.column,
                    rule.execution_source,
                    rule.failure_mode,
                    json.dumps(rule.details) if rule.details else None,
                    None,  # context
                    None,  # samples
                ))

            conn.commit()

            # Update state with assigned ID
            state.id = run_id

        except Exception as e:
            conn.rollback()
            raise IOError(f"Failed to save state to SQL Server: {e}") from e

    def _build_state_from_rows(
        self,
        run_row: tuple,
        rule_rows: List[tuple],
    ) -> ValidationState:
        """Build a ValidationState from database rows."""
        # Parse run row (note: pyodbc returns in order, not named)
        (
            run_id, contract_fingerprint, contract_name, dataset_fingerprint,
            dataset_name, run_at, duration_ms, passed, total_rows, total_rules,
            passed_rules, failed_rules, blocking_failures, warning_failures,
            info_failures, execution_stats, schema_version, engine_version
        ) = run_row

        # Build summary
        summary = StateSummary(
            passed=bool(passed),
            total_rules=total_rules,
            passed_rules=passed_rules,
            failed_rules=failed_rules,
            row_count=total_rows,
            blocking_failures=blocking_failures,
            warning_failures=warning_failures,
            info_failures=info_failures,
        )

        # Build rules
        rules = []
        for rule_row in rule_rows:
            (
                rule_result_id, _, rule_id, rule_name, rule_passed,
                failed_count, severity, message, column_name, execution_source,
                failure_mode, details, context, samples
            ) = rule_row

            # Parse details from JSON string
            parsed_details = None
            if details:
                try:
                    parsed_details = json.loads(details)
                except (json.JSONDecodeError, ValueError):
                    pass  # Malformed JSON, use default

            rule = RuleState(
                rule_id=rule_id,
                rule_name=rule_name,
                passed=bool(rule_passed),
                failed_count=failed_count,
                execution_source=execution_source or "unknown",
                severity=severity,
                failure_mode=failure_mode,
                details=parsed_details,
                message=message,
                column=column_name,
                id=rule_result_id,
            )
            rules.append(rule)

        return ValidationState(
            id=run_id,
            contract_fingerprint=contract_fingerprint,
            dataset_fingerprint=dataset_fingerprint,
            contract_name=contract_name,
            dataset_uri=dataset_name or "",
            run_at=run_at if isinstance(run_at, datetime) else datetime.now(timezone.utc),
            summary=summary,
            rules=rules,
            schema_version=schema_version or "2.0",
            engine_version=engine_version or "unknown",
            duration_ms=duration_ms,
        )

    def get_latest(self, contract_fingerprint: str) -> Optional[ValidationState]:
        """Get the most recent state for a contract."""
        conn = self._get_conn()

        run_sql = f"""
        SELECT TOP 1 id, contract_fingerprint, contract_name, dataset_fingerprint,
               dataset_name, run_at, duration_ms, passed, total_rows, total_rules,
               passed_rules, failed_rules, blocking_failures, warning_failures,
               info_failures, execution_stats, schema_version, engine_version
        FROM {self.RUNS_TABLE}
        WHERE contract_fingerprint = ?
        ORDER BY run_at DESC
        """

        rule_sql = f"""
        SELECT id, run_id, rule_id, rule_name, passed, failed_count,
               severity, message, column_name, execution_source,
               failure_mode, details, context, samples
        FROM {self.RULE_RESULTS_TABLE}
        WHERE run_id = ?
        ORDER BY id
        """

        try:
            cursor = conn.cursor()
            cursor.execute(run_sql, (contract_fingerprint,))
            run_row = cursor.fetchone()
            if not run_row:
                return None

            run_id = run_row[0]
            cursor.execute(rule_sql, (run_id,))
            rule_rows = cursor.fetchall()

            return self._build_state_from_rows(run_row, rule_rows)
        except _get_db_error() as e:
            _logger.debug(f"Database error getting latest state: {e}")
            return None

    def get_history(
        self,
        contract_fingerprint: str,
        limit: int = 10,
    ) -> List[ValidationState]:
        """Get recent history for a contract, newest first."""
        conn = self._get_conn()

        run_sql = f"""
        SELECT TOP (?) id, contract_fingerprint, contract_name, dataset_fingerprint,
               dataset_name, run_at, duration_ms, passed, total_rows, total_rules,
               passed_rules, failed_rules, blocking_failures, warning_failures,
               info_failures, execution_stats, schema_version, engine_version
        FROM {self.RUNS_TABLE}
        WHERE contract_fingerprint = ?
        ORDER BY run_at DESC
        """

        rule_sql = f"""
        SELECT id, run_id, rule_id, rule_name, passed, failed_count,
               severity, message, column_name, execution_source,
               failure_mode, details, context, samples
        FROM {self.RULE_RESULTS_TABLE}
        WHERE run_id IN (?)
        ORDER BY run_id, id
        """

        try:
            cursor = conn.cursor()
            cursor.execute(run_sql, (limit, contract_fingerprint))
            run_rows = cursor.fetchall()
            if not run_rows:
                return []

            # Get all rule results (one query per run for simplicity)
            states = []
            rule_sql_single = f"""
            SELECT id, run_id, rule_id, rule_name, passed, failed_count,
                   severity, message, column_name, execution_source,
                   failure_mode, details, context, samples
            FROM {self.RULE_RESULTS_TABLE}
            WHERE run_id = ?
            ORDER BY id
            """
            for run_row in run_rows:
                run_id = run_row[0]
                cursor.execute(rule_sql_single, (run_id,))
                rule_rows = cursor.fetchall()
                state = self._build_state_from_rows(run_row, rule_rows)
                states.append(state)

            return states
        except _get_db_error() as e:
            _logger.debug(f"Database error getting history: {e}")
            return []

    def get_at(
        self,
        contract_fingerprint: str,
        timestamp: datetime,
    ) -> Optional[ValidationState]:
        """Get state at or before a specific timestamp."""
        conn = self._get_conn()

        run_sql = f"""
        SELECT TOP 1 id, contract_fingerprint, contract_name, dataset_fingerprint,
               dataset_name, run_at, duration_ms, passed, total_rows, total_rules,
               passed_rules, failed_rules, blocking_failures, warning_failures,
               info_failures, execution_stats, schema_version, engine_version
        FROM {self.RUNS_TABLE}
        WHERE contract_fingerprint = ? AND run_at <= ?
        ORDER BY run_at DESC
        """

        rule_sql = f"""
        SELECT id, run_id, rule_id, rule_name, passed, failed_count,
               severity, message, column_name, execution_source,
               failure_mode, details, context, samples
        FROM {self.RULE_RESULTS_TABLE}
        WHERE run_id = ?
        ORDER BY id
        """

        try:
            cursor = conn.cursor()
            cursor.execute(run_sql, (contract_fingerprint, timestamp))
            run_row = cursor.fetchone()
            if not run_row:
                return None

            run_id = run_row[0]
            cursor.execute(rule_sql, (run_id,))
            rule_rows = cursor.fetchall()

            return self._build_state_from_rows(run_row, rule_rows)
        except _get_db_error() as e:
            _logger.debug(f"Database error getting state at timestamp: {e}")
            return None

    def delete_old(
        self,
        contract_fingerprint: str,
        keep_count: int = 100,
    ) -> int:
        """Delete old states, keeping the most recent ones."""
        conn = self._get_conn()

        # Delete runs not in the top keep_count (cascade deletes rule_results)
        sql_delete = f"""
        DELETE FROM {self.RUNS_TABLE}
        WHERE contract_fingerprint = ?
        AND id NOT IN (
            SELECT TOP (?) id FROM {self.RUNS_TABLE}
            WHERE contract_fingerprint = ?
            ORDER BY run_at DESC
        )
        """

        try:
            cursor = conn.cursor()
            cursor.execute(sql_delete, (contract_fingerprint, keep_count, contract_fingerprint))
            deleted = cursor.rowcount
            conn.commit()
            return deleted
        except _get_db_error() as e:
            _logger.warning(f"Database error deleting old states: {e}")
            conn.rollback()
            return 0

    def list_contracts(self) -> List[str]:
        """List all contract fingerprints with stored state."""
        conn = self._get_conn()

        sql = f"""
        SELECT DISTINCT contract_fingerprint FROM {self.RUNS_TABLE}
        ORDER BY contract_fingerprint
        """

        try:
            cursor = conn.cursor()
            cursor.execute(sql)
            rows = cursor.fetchall()
            return [row[0] for row in rows]
        except _get_db_error() as e:
            _logger.debug(f"Database error listing contracts: {e}")
            return []

    def clear(self, contract_fingerprint: Optional[str] = None) -> int:
        """
        Clear stored states.

        Args:
            contract_fingerprint: If provided, only clear this contract's states.
                                 If None, clear all states.

        Returns:
            Number of run rows deleted (rule_results cascade).
        """
        conn = self._get_conn()

        try:
            cursor = conn.cursor()
            if contract_fingerprint:
                cursor.execute(
                    f"DELETE FROM {self.RUNS_TABLE} WHERE contract_fingerprint = ?",
                    (contract_fingerprint,)
                )
            else:
                cursor.execute(f"DELETE FROM {self.RUNS_TABLE}")
            deleted = cursor.rowcount
            conn.commit()
            return deleted
        except _get_db_error() as e:
            _logger.warning(f"Database error clearing states: {e}")
            conn.rollback()
            return 0

    # -------------------------------------------------------------------------
    # Annotation Methods
    # -------------------------------------------------------------------------

    def save_annotation(self, annotation: Annotation) -> int:
        """Save an annotation (append-only)."""
        conn = self._get_conn()

        sql = f"""
        INSERT INTO {self.ANNOTATIONS_TABLE} (
            run_id, rule_result_id, actor_type, actor_id,
            annotation_type, summary, payload, created_at
        ) OUTPUT INSERTED.id VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?
        )
        """

        try:
            cursor = conn.cursor()
            cursor.execute(sql, (
                annotation.run_id,
                annotation.rule_result_id,
                annotation.actor_type,
                annotation.actor_id,
                annotation.annotation_type,
                annotation.summary,
                json.dumps(annotation.payload) if annotation.payload else None,
                annotation.created_at or datetime.now(timezone.utc),
            ))
            annotation_id = cursor.fetchone()[0]
            conn.commit()

            annotation.id = annotation_id
            return annotation_id
        except Exception as e:
            conn.rollback()
            raise IOError(f"Failed to save annotation: {e}") from e

    def get_annotations(
        self,
        run_id: int,
        rule_result_id: Optional[int] = None,
    ) -> List[Annotation]:
        """Get annotations for a run or specific rule result."""
        conn = self._get_conn()

        if rule_result_id is not None:
            sql = f"""
            SELECT id, run_id, rule_result_id, actor_type, actor_id,
                   annotation_type, summary, payload, created_at
            FROM {self.ANNOTATIONS_TABLE}
            WHERE run_id = ? AND rule_result_id = ?
            ORDER BY created_at DESC
            """
            params = (run_id, rule_result_id)
        else:
            sql = f"""
            SELECT id, run_id, rule_result_id, actor_type, actor_id,
                   annotation_type, summary, payload, created_at
            FROM {self.ANNOTATIONS_TABLE}
            WHERE run_id = ?
            ORDER BY created_at DESC
            """
            params = (run_id,)

        try:
            cursor = conn.cursor()
            cursor.execute(sql, params)
            rows = cursor.fetchall()

            annotations = []
            for row in rows:
                (
                    ann_id, run_id_val, rule_result_id_val, actor_type, actor_id,
                    annotation_type, summary, payload, created_at
                ) = row

                # Parse payload from JSON string
                parsed_payload = None
                if payload:
                    try:
                        parsed_payload = json.loads(payload)
                    except (json.JSONDecodeError, ValueError):
                        pass

                annotation = Annotation(
                    id=ann_id,
                    run_id=run_id_val,
                    rule_result_id=rule_result_id_val,
                    actor_type=actor_type,
                    actor_id=actor_id,
                    annotation_type=annotation_type,
                    summary=summary,
                    payload=parsed_payload,
                    created_at=created_at if isinstance(created_at, datetime) else None,
                )
                annotations.append(annotation)
            return annotations
        except _get_db_error() as e:
            _logger.debug(f"Database error getting annotations: {e}")
            return []

    def get_run_with_annotations(
        self,
        contract_fingerprint: str,
        run_id: Optional[int] = None,
    ) -> Optional[ValidationState]:
        """Get a validation state with its annotations loaded."""
        conn = self._get_conn()

        # Get the run
        if run_id is not None:
            run_sql = f"""
            SELECT TOP 1 id, contract_fingerprint, contract_name, dataset_fingerprint,
                   dataset_name, run_at, duration_ms, passed, total_rows, total_rules,
                   passed_rules, failed_rules, blocking_failures, warning_failures,
                   info_failures, execution_stats, schema_version, engine_version
            FROM {self.RUNS_TABLE}
            WHERE id = ? AND contract_fingerprint = ?
            """
            run_params = (run_id, contract_fingerprint)
        else:
            run_sql = f"""
            SELECT TOP 1 id, contract_fingerprint, contract_name, dataset_fingerprint,
                   dataset_name, run_at, duration_ms, passed, total_rows, total_rules,
                   passed_rules, failed_rules, blocking_failures, warning_failures,
                   info_failures, execution_stats, schema_version, engine_version
            FROM {self.RUNS_TABLE}
            WHERE contract_fingerprint = ?
            ORDER BY run_at DESC
            """
            run_params = (contract_fingerprint,)

        rule_sql = f"""
        SELECT id, run_id, rule_id, rule_name, passed, failed_count,
               severity, message, column_name, execution_source,
               failure_mode, details, context, samples
        FROM {self.RULE_RESULTS_TABLE}
        WHERE run_id = ?
        ORDER BY id
        """

        ann_sql = f"""
        SELECT id, run_id, rule_result_id, actor_type, actor_id,
               annotation_type, summary, payload, created_at
        FROM {self.ANNOTATIONS_TABLE}
        WHERE run_id = ?
        ORDER BY created_at DESC
        """

        try:
            cursor = conn.cursor()
            cursor.execute(run_sql, run_params)
            run_row = cursor.fetchone()
            if not run_row:
                return None

            actual_run_id = run_row[0]

            # Get rules
            cursor.execute(rule_sql, (actual_run_id,))
            rule_rows = cursor.fetchall()

            # Get annotations
            cursor.execute(ann_sql, (actual_run_id,))
            ann_rows = cursor.fetchall()

            # Build state
            state = self._build_state_from_rows(run_row, rule_rows)

            # Build annotations list
            annotations = []
            for row in ann_rows:
                (
                    ann_id, run_id_val, rule_result_id_val, actor_type, actor_id,
                    annotation_type, summary, payload, created_at
                ) = row

                parsed_payload = None
                if payload:
                    try:
                        parsed_payload = json.loads(payload)
                    except (json.JSONDecodeError, ValueError):
                        pass

                annotations.append(Annotation(
                    id=ann_id,
                    run_id=run_id_val,
                    rule_result_id=rule_result_id_val,
                    actor_type=actor_type,
                    actor_id=actor_id,
                    annotation_type=annotation_type,
                    summary=summary,
                    payload=parsed_payload,
                    created_at=created_at if isinstance(created_at, datetime) else None,
                ))

            self._attach_annotations_to_state(state, annotations)
            return state
        except _get_db_error() as e:
            _logger.debug(f"Database error getting run with annotations: {e}")
            return None

    def get_history_with_annotations(
        self,
        contract_fingerprint: str,
        limit: int = 10,
    ) -> List[ValidationState]:
        """Get recent history with annotations loaded."""
        # Get history first
        states = self.get_history(contract_fingerprint, limit=limit)
        if not states:
            return []

        conn = self._get_conn()
        run_ids = [s.id for s in states if s.id is not None]

        if not run_ids:
            for state in states:
                state.annotations = []
                for rule in state.rules:
                    rule.annotations = []
            return states

        # Build IN clause (SQL Server style)
        placeholders = ",".join("?" * len(run_ids))
        ann_sql = f"""
        SELECT id, run_id, rule_result_id, actor_type, actor_id,
               annotation_type, summary, payload, created_at
        FROM {self.ANNOTATIONS_TABLE}
        WHERE run_id IN ({placeholders})
        ORDER BY created_at DESC
        """

        try:
            cursor = conn.cursor()
            cursor.execute(ann_sql, run_ids)
            ann_rows = cursor.fetchall()

            # Build annotations index
            annotations_index: Dict[int, Dict[Optional[int], List[Annotation]]] = {}

            for row in ann_rows:
                (
                    ann_id, run_id_val, rule_result_id_val, actor_type, actor_id,
                    annotation_type, summary, payload, created_at
                ) = row

                parsed_payload = None
                if payload:
                    try:
                        parsed_payload = json.loads(payload)
                    except (json.JSONDecodeError, ValueError):
                        pass

                annotation = Annotation(
                    id=ann_id,
                    run_id=run_id_val,
                    rule_result_id=rule_result_id_val,
                    actor_type=actor_type,
                    actor_id=actor_id,
                    annotation_type=annotation_type,
                    summary=summary,
                    payload=parsed_payload,
                    created_at=created_at if isinstance(created_at, datetime) else None,
                )

                if run_id_val not in annotations_index:
                    annotations_index[run_id_val] = {}
                annotations_index[run_id_val].setdefault(rule_result_id_val, []).append(annotation)

            # Attach to states
            for state in states:
                if state.id is not None and state.id in annotations_index:
                    run_anns = annotations_index[state.id]
                    state.annotations = run_anns.get(None, [])
                    for rule in state.rules:
                        if rule.id is not None:
                            rule.annotations = run_anns.get(rule.id, [])
                        else:
                            rule.annotations = []
                else:
                    state.annotations = []
                    for rule in state.rules:
                        rule.annotations = []

            return states
        except _get_db_error() as e:
            _logger.debug(f"Database error loading annotations for history: {e}")
            for state in states:
                state.annotations = []
                for rule in state.rules:
                    rule.annotations = []
            return states

    def close(self) -> None:
        """Close the database connection."""
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def __repr__(self) -> str:
        server = self._conn_params.get("server", "?")
        database = self._conn_params.get("database", "?")
        return f"SQLServerStore(server={server}, database={database})"

    def __del__(self):
        self.close()
