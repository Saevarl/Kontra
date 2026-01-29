# src/kontra/state/backends/postgres.py
"""
PostgreSQL state storage with normalized schema (v0.5).

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
from kontra.state.types import (
    Annotation,
    RuleState,
    StateSummary,
    ValidationState,
)


class PostgresStore(StateBackend):
    """
    PostgreSQL database state storage backend with normalized schema.

    Uses psycopg3 (psycopg) for database access. Automatically creates
    the required tables if they don't exist.

    URI format: postgres://user:pass@host:port/database
                postgresql://user:pass@host:port/database

    Also supports standard PostgreSQL environment variables:
        PGHOST, PGPORT, PGUSER, PGPASSWORD, PGDATABASE
    """

    # Table names
    RUNS_TABLE = "kontra_runs"
    RULE_RESULTS_TABLE = "kontra_rule_results"
    ANNOTATIONS_TABLE = "kontra_annotations"

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
        self._tables_created = False

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
            self._ensure_tables()
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

    def _ensure_tables(self) -> None:
        """Create the state tables if they don't exist."""
        if self._tables_created:
            return

        conn = self._conn
        with conn.cursor() as cur:
            cur.execute(self.CREATE_TABLES_SQL)
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
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        ) RETURNING id
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
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        ) RETURNING id
        """

        try:
            with conn.cursor() as cur:
                # Insert run
                cur.execute(run_sql, (
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
                run_id = cur.fetchone()[0]

                # Insert rule results
                for rule in state.rules:
                    cur.execute(rule_sql, (
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
                        None,  # context - not stored in RuleState currently
                        None,  # samples - not stored in state currently
                    ))

            conn.commit()

            # Update state with assigned ID
            state.id = run_id

        except Exception as e:
            conn.rollback()
            raise IOError(f"Failed to save state to PostgreSQL: {e}") from e

    def _build_state_from_rows(
        self,
        run_row: tuple,
        rule_rows: List[tuple],
    ) -> ValidationState:
        """Build a ValidationState from database rows."""
        # Parse run row
        (
            run_id, contract_fingerprint, contract_name, dataset_fingerprint,
            dataset_name, run_at, duration_ms, passed, total_rows, total_rules,
            passed_rules, failed_rules, blocking_failures, warning_failures,
            info_failures, execution_stats, schema_version, engine_version
        ) = run_row

        # Build summary
        summary = StateSummary(
            passed=passed,
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

            rule = RuleState(
                rule_id=rule_id,
                rule_name=rule_name,
                passed=rule_passed,
                failed_count=failed_count,
                execution_source=execution_source or "unknown",
                severity=severity,
                failure_mode=failure_mode,
                details=details,
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
            run_at=run_at,
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
        SELECT id, contract_fingerprint, contract_name, dataset_fingerprint,
               dataset_name, run_at, duration_ms, passed, total_rows, total_rules,
               passed_rules, failed_rules, blocking_failures, warning_failures,
               info_failures, execution_stats, schema_version, engine_version
        FROM {self.RUNS_TABLE}
        WHERE contract_fingerprint = %s
        ORDER BY run_at DESC
        LIMIT 1
        """

        rule_sql = f"""
        SELECT id, run_id, rule_id, rule_name, passed, failed_count,
               severity, message, column_name, execution_source,
               failure_mode, details, context, samples
        FROM {self.RULE_RESULTS_TABLE}
        WHERE run_id = %s
        ORDER BY id
        """

        try:
            with conn.cursor() as cur:
                cur.execute(run_sql, (contract_fingerprint,))
                run_row = cur.fetchone()
                if not run_row:
                    return None

                run_id = run_row[0]
                cur.execute(rule_sql, (run_id,))
                rule_rows = cur.fetchall()

                return self._build_state_from_rows(run_row, rule_rows)
        except _get_db_error() as e:
            _logger.debug(f"Database error getting latest state for {contract_fingerprint}: {e}")
            return None

    def get_history(
        self,
        contract_fingerprint: str,
        limit: int = 10,
    ) -> List[ValidationState]:
        """Get recent history for a contract, newest first."""
        conn = self._get_conn()

        run_sql = f"""
        SELECT id, contract_fingerprint, contract_name, dataset_fingerprint,
               dataset_name, run_at, duration_ms, passed, total_rows, total_rules,
               passed_rules, failed_rules, blocking_failures, warning_failures,
               info_failures, execution_stats, schema_version, engine_version
        FROM {self.RUNS_TABLE}
        WHERE contract_fingerprint = %s
        ORDER BY run_at DESC
        LIMIT %s
        """

        rule_sql = f"""
        SELECT id, run_id, rule_id, rule_name, passed, failed_count,
               severity, message, column_name, execution_source,
               failure_mode, details, context, samples
        FROM {self.RULE_RESULTS_TABLE}
        WHERE run_id = ANY(%s)
        ORDER BY run_id, id
        """

        try:
            with conn.cursor() as cur:
                cur.execute(run_sql, (contract_fingerprint, limit))
                run_rows = cur.fetchall()
                if not run_rows:
                    return []

                # Get all rule results in one query
                run_ids = [row[0] for row in run_rows]
                cur.execute(rule_sql, (run_ids,))
                all_rule_rows = cur.fetchall()

                # Group rule rows by run_id
                rules_by_run: Dict[int, List[tuple]] = {}
                for rule_row in all_rule_rows:
                    run_id = rule_row[1]
                    rules_by_run.setdefault(run_id, []).append(rule_row)

                # Build states
                states = []
                for run_row in run_rows:
                    run_id = run_row[0]
                    rule_rows = rules_by_run.get(run_id, [])
                    state = self._build_state_from_rows(run_row, rule_rows)
                    states.append(state)

                return states
        except _get_db_error() as e:
            _logger.debug(f"Database error getting history for {contract_fingerprint}: {e}")
            return []

    def get_at(
        self,
        contract_fingerprint: str,
        timestamp: datetime,
    ) -> Optional[ValidationState]:
        """Get state at or before a specific timestamp."""
        conn = self._get_conn()

        run_sql = f"""
        SELECT id, contract_fingerprint, contract_name, dataset_fingerprint,
               dataset_name, run_at, duration_ms, passed, total_rows, total_rules,
               passed_rules, failed_rules, blocking_failures, warning_failures,
               info_failures, execution_stats, schema_version, engine_version
        FROM {self.RUNS_TABLE}
        WHERE contract_fingerprint = %s AND run_at <= %s
        ORDER BY run_at DESC
        LIMIT 1
        """

        rule_sql = f"""
        SELECT id, run_id, rule_id, rule_name, passed, failed_count,
               severity, message, column_name, execution_source,
               failure_mode, details, context, samples
        FROM {self.RULE_RESULTS_TABLE}
        WHERE run_id = %s
        ORDER BY id
        """

        try:
            with conn.cursor() as cur:
                cur.execute(run_sql, (contract_fingerprint, timestamp))
                run_row = cur.fetchone()
                if not run_row:
                    return None

                run_id = run_row[0]
                cur.execute(rule_sql, (run_id,))
                rule_rows = cur.fetchall()

                return self._build_state_from_rows(run_row, rule_rows)
        except _get_db_error() as e:
            _logger.debug(f"Database error getting state at timestamp for {contract_fingerprint}: {e}")
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
        WHERE contract_fingerprint = %s
        AND id NOT IN (
            SELECT id FROM {self.RUNS_TABLE}
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
        except _get_db_error() as e:
            _logger.warning(f"Database error deleting old states for {contract_fingerprint}: {e}")
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
            with conn.cursor() as cur:
                cur.execute(sql)
                rows = cur.fetchall()
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
            with conn.cursor() as cur:
                if contract_fingerprint:
                    cur.execute(
                        f"DELETE FROM {self.RUNS_TABLE} WHERE contract_fingerprint = %s",
                        (contract_fingerprint,)
                    )
                else:
                    cur.execute(f"DELETE FROM {self.RUNS_TABLE}")
                deleted = cur.rowcount
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
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s
        ) RETURNING id
        """

        try:
            with conn.cursor() as cur:
                cur.execute(sql, (
                    annotation.run_id,
                    annotation.rule_result_id,
                    annotation.actor_type,
                    annotation.actor_id,
                    annotation.annotation_type,
                    annotation.summary,
                    json.dumps(annotation.payload) if annotation.payload else None,
                    annotation.created_at or datetime.now(timezone.utc),
                ))
                annotation_id = cur.fetchone()[0]
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
            WHERE run_id = %s AND rule_result_id = %s
            ORDER BY created_at DESC
            """
            params = (run_id, rule_result_id)
        else:
            sql = f"""
            SELECT id, run_id, rule_result_id, actor_type, actor_id,
                   annotation_type, summary, payload, created_at
            FROM {self.ANNOTATIONS_TABLE}
            WHERE run_id = %s
            ORDER BY created_at DESC
            """
            params = (run_id,)

        try:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                rows = cur.fetchall()

                annotations = []
                for row in rows:
                    (
                        ann_id, run_id, rule_result_id, actor_type, actor_id,
                        annotation_type, summary, payload, created_at
                    ) = row
                    annotation = Annotation(
                        id=ann_id,
                        run_id=run_id,
                        rule_result_id=rule_result_id,
                        actor_type=actor_type,
                        actor_id=actor_id,
                        annotation_type=annotation_type,
                        summary=summary,
                        payload=payload,
                        created_at=created_at,
                    )
                    annotations.append(annotation)
                return annotations
        except _get_db_error() as e:
            _logger.debug(f"Database error getting annotations for run {run_id}: {e}")
            return []

    def get_annotations_for_contract(
        self,
        contract_fingerprint: str,
        rule_id: Optional[str] = None,
        annotation_type: Optional[str] = None,
        limit: int = 20,
    ) -> List[Annotation]:
        """Get annotations across all runs for a contract."""
        conn = self._get_conn()

        # Build the query with JOINs to get rule_id
        # We join runs to filter by contract, and rule_results to get rule_id
        sql = f"""
        SELECT
            a.id, a.run_id, a.rule_result_id, a.actor_type, a.actor_id,
            a.annotation_type, a.summary, a.payload, a.created_at,
            rr.rule_id
        FROM {self.ANNOTATIONS_TABLE} a
        JOIN {self.RUNS_TABLE} r ON a.run_id = r.id
        LEFT JOIN {self.RULE_RESULTS_TABLE} rr ON a.rule_result_id = rr.id
        WHERE r.contract_fingerprint = %s
        """
        params: List[Any] = [contract_fingerprint]

        if rule_id is not None:
            sql += " AND rr.rule_id = %s"
            params.append(rule_id)

        if annotation_type is not None:
            sql += " AND a.annotation_type = %s"
            params.append(annotation_type)

        sql += " ORDER BY a.created_at DESC LIMIT %s"
        params.append(limit)

        try:
            with conn.cursor() as cur:
                cur.execute(sql, tuple(params))
                rows = cur.fetchall()

                annotations = []
                for row in rows:
                    (
                        ann_id, run_id, rule_result_id, actor_type, actor_id,
                        ann_type, summary, payload, created_at, rule_id_val
                    ) = row
                    annotation = Annotation(
                        id=ann_id,
                        run_id=run_id,
                        rule_result_id=rule_result_id,
                        rule_id=rule_id_val,
                        actor_type=actor_type,
                        actor_id=actor_id,
                        annotation_type=ann_type,
                        summary=summary,
                        payload=payload,
                        created_at=created_at,
                    )
                    annotations.append(annotation)
                return annotations
        except _get_db_error() as e:
            _logger.debug(f"Database error getting annotations for contract {contract_fingerprint}: {e}")
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
            SELECT id, contract_fingerprint, contract_name, dataset_fingerprint,
                   dataset_name, run_at, duration_ms, passed, total_rows, total_rules,
                   passed_rules, failed_rules, blocking_failures, warning_failures,
                   info_failures, execution_stats, schema_version, engine_version
            FROM {self.RUNS_TABLE}
            WHERE id = %s AND contract_fingerprint = %s
            """
            run_params = (run_id, contract_fingerprint)
        else:
            run_sql = f"""
            SELECT id, contract_fingerprint, contract_name, dataset_fingerprint,
                   dataset_name, run_at, duration_ms, passed, total_rows, total_rules,
                   passed_rules, failed_rules, blocking_failures, warning_failures,
                   info_failures, execution_stats, schema_version, engine_version
            FROM {self.RUNS_TABLE}
            WHERE contract_fingerprint = %s
            ORDER BY run_at DESC
            LIMIT 1
            """
            run_params = (contract_fingerprint,)

        rule_sql = f"""
        SELECT id, run_id, rule_id, rule_name, passed, failed_count,
               severity, message, column_name, execution_source,
               failure_mode, details, context, samples
        FROM {self.RULE_RESULTS_TABLE}
        WHERE run_id = %s
        ORDER BY id
        """

        ann_sql = f"""
        SELECT id, run_id, rule_result_id, actor_type, actor_id,
               annotation_type, summary, payload, created_at
        FROM {self.ANNOTATIONS_TABLE}
        WHERE run_id = %s
        ORDER BY created_at DESC
        """

        try:
            with conn.cursor() as cur:
                cur.execute(run_sql, run_params)
                run_row = cur.fetchone()
                if not run_row:
                    return None

                actual_run_id = run_row[0]

                # Get rules
                cur.execute(rule_sql, (actual_run_id,))
                rule_rows = cur.fetchall()

                # Get annotations
                cur.execute(ann_sql, (actual_run_id,))
                ann_rows = cur.fetchall()

                # Build state
                state = self._build_state_from_rows(run_row, rule_rows)

                # Build annotations list
                annotations = []
                for row in ann_rows:
                    (
                        ann_id, run_id_val, rule_result_id, actor_type, actor_id,
                        annotation_type, summary, payload, created_at
                    ) = row
                    annotations.append(Annotation(
                        id=ann_id,
                        run_id=run_id_val,
                        rule_result_id=rule_result_id,
                        actor_type=actor_type,
                        actor_id=actor_id,
                        annotation_type=annotation_type,
                        summary=summary,
                        payload=payload,
                        created_at=created_at,
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
        # For efficiency, we load history without annotations first,
        # then load annotations in batch
        states = self.get_history(contract_fingerprint, limit=limit)
        if not states:
            return []

        conn = self._get_conn()
        run_ids = [s.id for s in states if s.id is not None]

        if not run_ids:
            # No IDs, just return empty annotations
            for state in states:
                state.annotations = []
                for rule in state.rules:
                    rule.annotations = []
            return states

        ann_sql = f"""
        SELECT id, run_id, rule_result_id, actor_type, actor_id,
               annotation_type, summary, payload, created_at
        FROM {self.ANNOTATIONS_TABLE}
        WHERE run_id = ANY(%s)
        ORDER BY created_at DESC
        """

        try:
            with conn.cursor() as cur:
                cur.execute(ann_sql, (run_ids,))
                ann_rows = cur.fetchall()

            # Build annotations index
            # Key: (run_id, rule_result_id or None)
            annotations_index: Dict[int, Dict[Optional[int], List[Annotation]]] = {}

            for row in ann_rows:
                (
                    ann_id, run_id, rule_result_id, actor_type, actor_id,
                    annotation_type, summary, payload, created_at
                ) = row
                annotation = Annotation(
                    id=ann_id,
                    run_id=run_id,
                    rule_result_id=rule_result_id,
                    actor_type=actor_type,
                    actor_id=actor_id,
                    annotation_type=annotation_type,
                    summary=summary,
                    payload=payload,
                    created_at=created_at,
                )

                if run_id not in annotations_index:
                    annotations_index[run_id] = {}
                annotations_index[run_id].setdefault(rule_result_id, []).append(annotation)

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
            # On error, return states without annotations
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
        host = self._conn_params.get("host", "?")
        dbname = self._conn_params.get("dbname", "?")
        return f"PostgresStore(host={host}, dbname={dbname})"

    def __del__(self):
        self.close()
