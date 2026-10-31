# src/kontra/state/backends/sql_base.py
"""
Shared implementation of the normalized SQL state schema (v0.5).

Schema:
    kontra_runs - Run-level metadata
    kontra_rule_results - Per-rule results (references kontra_runs)
    kontra_annotations - Append-only annotations (references runs/rules)

PostgresStore and SQLServerStore store the same rows in the same tables and
run the same queries. Subclasses provide the connection, the DDL, and the
pieces of SQL that differ between dialects: placeholder style, LIMIT vs TOP,
RETURNING vs OUTPUT, how a list of run ids is bound, and how JSON columns
come back from the driver.

Driver imports stay inside the subclasses' methods so importing kontra never
loads psycopg or pyodbc.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Iterator, Sequence
from datetime import datetime, timezone
from types import ModuleType
from typing import Any

from kontra.state.types import (
    Annotation,
    RuleState,
    StateSummary,
    ValidationState,
)

from .base import StateBackend

_RUN_COLUMNS = """id, contract_fingerprint, contract_name, dataset_fingerprint,
       dataset_name, run_at, duration_ms, passed, total_rows, total_rules,
       passed_rules, failed_rules, blocking_failures, warning_failures,
       info_failures, execution_stats, schema_version, engine_version"""

_RULE_COLUMNS = """id, run_id, rule_id, rule_name, passed, failed_count,
       severity, message, column_name, execution_source,
       failure_mode, details, context, samples"""

_ANNOTATION_COLUMNS = """id, run_id, rule_result_id, actor_type, actor_id,
       annotation_type, summary, payload, created_at"""

_RUN_INSERT_COLUMNS = (
    "contract_fingerprint",
    "contract_name",
    "dataset_fingerprint",
    "dataset_name",
    "run_at",
    "duration_ms",
    "passed",
    "total_rows",
    "total_rules",
    "passed_rules",
    "failed_rules",
    "blocking_failures",
    "warning_failures",
    "info_failures",
    "schema_version",
    "engine_version",
)

_RULE_INSERT_COLUMNS = (
    "run_id",
    "rule_id",
    "rule_name",
    "passed",
    "failed_count",
    "severity",
    "message",
    "column_name",
    "execution_source",
    "failure_mode",
    "details",
    "context",
    "samples",
)

_ANNOTATION_INSERT_COLUMNS = (
    "run_id",
    "rule_result_id",
    "actor_type",
    "actor_id",
    "annotation_type",
    "summary",
    "payload",
    "created_at",
)


class SQLStateBackend(StateBackend):
    """
    State backend over the normalized kontra_runs / kontra_rule_results /
    kontra_annotations tables. Not used directly; see PostgresStore and
    SQLServerStore.
    """

    # Table names
    RUNS_TABLE = "kontra_runs"
    RULE_RESULTS_TABLE = "kontra_rule_results"
    ANNOTATIONS_TABLE = "kontra_annotations"

    CREATE_TABLES_SQL = ""

    # --- Dialect settings (overridden by subclasses) ---
    _PARAM = "%s"  # DB-API placeholder
    _DISPLAY_NAME = ""  # Used in error messages
    _CONNECT_HELP = ""  # Appended to connection errors
    _logger = logging.getLogger(__name__)

    def __init__(self, uri: str):
        """
        Initialize the store.

        Args:
            uri: Database connection URI

        The URI can be a full connection string or just the scheme,
        with connection details from environment variables.
        """
        self.uri = uri
        self._conn_params = self._parse_connection_params(uri)
        self._conn = None
        self._tables_created = False

    # -------------------------------------------------------------------------
    # Dialect hooks
    # -------------------------------------------------------------------------

    @staticmethod
    def _parse_connection_params(uri: str) -> dict[str, Any]:
        raise NotImplementedError

    def _import_driver(self) -> ModuleType:
        """Import the DB-API driver, raising RuntimeError if it is missing."""
        raise NotImplementedError

    def _connect(self, driver: ModuleType) -> Any:
        """Open a new connection with the driver from _import_driver()."""
        raise NotImplementedError

    @staticmethod
    def _db_error() -> type:
        """The driver's base error class, caught on read paths."""
        raise NotImplementedError

    def _ddl_statements(self) -> list[str]:
        """CREATE_TABLES_SQL as the statements to execute, in order."""
        return [self.CREATE_TABLES_SQL]

    def _cursor(self, conn: Any) -> Any:
        """A cursor usable as a context manager that only closes on exit."""
        return conn.cursor()

    def _insert_returning_id(self, table: str, columns: Sequence[str]) -> str:
        """INSERT statement whose result set is the new row's id."""
        raise NotImplementedError

    def _select_top(
        self,
        columns: str,
        body: str,
        params: tuple[Any, ...],
        limit: int | None = None,
    ) -> tuple[str, tuple[Any, ...]]:
        """
        Limit ``SELECT {columns} {body}`` to the first rows of its ORDER BY.

        With ``limit=None`` the query returns a single row; otherwise ``limit``
        is bound as a parameter. Returns the SQL and its full parameter tuple.
        """
        raise NotImplementedError

    def _in_clauses(
        self, column: str, values: Sequence[Any]
    ) -> Iterator[tuple[str, tuple[Any, ...]]]:
        """
        Yield ``(condition, params)`` pairs that together match ``column``
        against every value. Dialects with a parameter limit yield several.
        """
        raise NotImplementedError

    @staticmethod
    def _decode_json(value: Any) -> Any:
        """Python value of a JSON column as returned by the driver."""
        raise NotImplementedError

    # -------------------------------------------------------------------------
    # Connection
    # -------------------------------------------------------------------------

    def _get_conn(self):
        """Get or create the database connection."""
        if self._conn is not None:
            return self._conn

        driver = self._import_driver()
        try:
            self._conn = self._connect(driver)
            self._ensure_tables()
        except Exception as e:
            raise ConnectionError(
                f"Failed to connect to {self._DISPLAY_NAME}: {e}\n\n{self._CONNECT_HELP}"
            ) from e

        return self._conn

    def _ensure_tables(self) -> None:
        """Create the state tables if they don't exist."""
        if self._tables_created:
            return

        conn = self._conn
        with self._cursor(conn) as cur:
            for statement in self._ddl_statements():
                cur.execute(statement)
        conn.commit()
        self._tables_created = True

    def close(self) -> None:
        """Close the database connection."""
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def __del__(self):
        self.close()

    # -------------------------------------------------------------------------
    # Row mapping
    # -------------------------------------------------------------------------

    def _placeholders(self, n: int) -> str:
        return ", ".join([self._PARAM] * n)

    def _build_state_from_rows(
        self,
        run_row: tuple,
        rule_rows: list[tuple],
    ) -> ValidationState:
        """Build a ValidationState from database rows."""
        (
            run_id, contract_fingerprint, contract_name, dataset_fingerprint,
            dataset_name, run_at, duration_ms, passed, total_rows, total_rules,
            passed_rules, failed_rules, blocking_failures, warning_failures,
            info_failures, execution_stats, schema_version, engine_version
        ) = run_row

        # bool(): SQL Server BIT comes back as int from some drivers
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

        rules = []
        for rule_row in rule_rows:
            (
                rule_result_id, _, rule_id, rule_name, rule_passed,
                failed_count, severity, message, column_name, execution_source,
                failure_mode, details, context, samples
            ) = rule_row

            rules.append(RuleState(
                rule_id=rule_id,
                rule_name=rule_name,
                passed=bool(rule_passed),
                failed_count=failed_count,
                execution_source=execution_source or "unknown",
                severity=severity,
                failure_mode=failure_mode,
                details=self._decode_json(details),
                message=message,
                column=column_name,
                id=rule_result_id,
            ))

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

    def _annotation_from_row(
        self, row: tuple, rule_id: str | None = None
    ) -> Annotation:
        (
            ann_id, run_id, rule_result_id, actor_type, actor_id,
            annotation_type, summary, payload, created_at
        ) = row
        return Annotation(
            id=ann_id,
            run_id=run_id,
            rule_result_id=rule_result_id,
            rule_id=rule_id,
            actor_type=actor_type,
            actor_id=actor_id,
            annotation_type=annotation_type,
            summary=summary,
            payload=self._decode_json(payload),
            created_at=created_at if isinstance(created_at, datetime) else None,
        )

    def _fetch_for_runs(
        self,
        cur: Any,
        columns: str,
        table: str,
        run_ids: Sequence[int],
        order_by: str,
    ) -> list[tuple]:
        """Rows of ``table`` belonging to any of ``run_ids``."""
        rows: list[tuple] = []
        for condition, params in self._in_clauses("run_id", run_ids):
            cur.execute(
                f"SELECT {columns} FROM {table} WHERE {condition} ORDER BY {order_by}",
                params,
            )
            rows.extend(cur.fetchall())
        return rows

    def _get_one_state(
        self, conn: Any, run_sql: str, run_params: tuple[Any, ...]
    ) -> ValidationState | None:
        """Run a single-run query and load that run's rule results."""
        p = self._PARAM
        rule_sql = f"""
        SELECT {_RULE_COLUMNS}
        FROM {self.RULE_RESULTS_TABLE}
        WHERE run_id = {p}
        ORDER BY id
        """

        with self._cursor(conn) as cur:
            cur.execute(run_sql, run_params)
            run_row = cur.fetchone()
            if not run_row:
                return None

            cur.execute(rule_sql, (run_row[0],))
            return self._build_state_from_rows(run_row, cur.fetchall())

    # -------------------------------------------------------------------------
    # Runs
    # -------------------------------------------------------------------------

    def save(self, state: ValidationState) -> None:
        """Save a validation state to the database (normalized)."""
        conn = self._get_conn()

        run_sql = self._insert_returning_id(self.RUNS_TABLE, _RUN_INSERT_COLUMNS)
        rule_sql = self._insert_returning_id(self.RULE_RESULTS_TABLE, _RULE_INSERT_COLUMNS)

        try:
            with self._cursor(conn) as cur:
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
            raise IOError(f"Failed to save state to {self._DISPLAY_NAME}: {e}") from e

    def get_latest(self, contract_fingerprint: str) -> ValidationState | None:
        """Get the most recent state for a contract."""
        conn = self._get_conn()
        p = self._PARAM
        run_sql, run_params = self._select_top(
            _RUN_COLUMNS,
            f"""
        FROM {self.RUNS_TABLE}
        WHERE contract_fingerprint = {p}
        ORDER BY run_at DESC
        """,
            (contract_fingerprint,),
        )
        try:
            return self._get_one_state(conn, run_sql, run_params)
        except self._db_error() as e:
            self._logger.debug(f"Database error getting latest state for {contract_fingerprint}: {e}")
            return None

    def get_history(
        self,
        contract_fingerprint: str,
        limit: int = 10,
    ) -> list[ValidationState]:
        """Get recent history for a contract, newest first."""
        conn = self._get_conn()
        p = self._PARAM
        run_sql, run_params = self._select_top(
            _RUN_COLUMNS,
            f"""
        FROM {self.RUNS_TABLE}
        WHERE contract_fingerprint = {p}
        ORDER BY run_at DESC
        """,
            (contract_fingerprint,),
            limit,
        )

        try:
            with self._cursor(conn) as cur:
                cur.execute(run_sql, run_params)
                run_rows = cur.fetchall()
                if not run_rows:
                    return []

                # Load every run's rule results together rather than per run
                run_ids = [row[0] for row in run_rows]
                rules_by_run: dict[int, list[tuple]] = {}
                for rule_row in self._fetch_for_runs(
                    cur, _RULE_COLUMNS, self.RULE_RESULTS_TABLE, run_ids, "run_id, id"
                ):
                    rules_by_run.setdefault(rule_row[1], []).append(rule_row)

                return [
                    self._build_state_from_rows(run_row, rules_by_run.get(run_row[0], []))
                    for run_row in run_rows
                ]
        except self._db_error() as e:
            self._logger.debug(f"Database error getting history for {contract_fingerprint}: {e}")
            return []

    def get_at(
        self,
        contract_fingerprint: str,
        timestamp: datetime,
    ) -> ValidationState | None:
        """Get state at or before a specific timestamp."""
        conn = self._get_conn()
        p = self._PARAM
        run_sql, run_params = self._select_top(
            _RUN_COLUMNS,
            f"""
        FROM {self.RUNS_TABLE}
        WHERE contract_fingerprint = {p} AND run_at <= {p}
        ORDER BY run_at DESC
        """,
            (contract_fingerprint, timestamp),
        )
        try:
            return self._get_one_state(conn, run_sql, run_params)
        except self._db_error() as e:
            self._logger.debug(
                f"Database error getting state at timestamp for {contract_fingerprint}: {e}"
            )
            return None

    def delete_old(
        self,
        contract_fingerprint: str,
        keep_count: int = 100,
    ) -> int:
        """Delete old states, keeping the most recent ones."""
        conn = self._get_conn()
        p = self._PARAM

        # Delete runs not in the top keep_count (cascade deletes rule_results)
        keep_sql, keep_params = self._select_top(
            "id",
            f"""FROM {self.RUNS_TABLE}
            WHERE contract_fingerprint = {p}
            ORDER BY run_at DESC""",
            (contract_fingerprint,),
            keep_count,
        )
        sql_delete = f"""
        DELETE FROM {self.RUNS_TABLE}
        WHERE contract_fingerprint = {p}
        AND id NOT IN (
            {keep_sql}
        )
        """

        try:
            with self._cursor(conn) as cur:
                cur.execute(sql_delete, (contract_fingerprint, *keep_params))
                deleted = cur.rowcount
            conn.commit()
            return deleted
        except self._db_error() as e:
            self._logger.warning(
                f"Database error deleting old states for {contract_fingerprint}: {e}"
            )
            conn.rollback()
            return 0

    def list_contracts(self) -> list[str]:
        """List all contract fingerprints with stored state."""
        conn = self._get_conn()

        sql = f"""
        SELECT DISTINCT contract_fingerprint FROM {self.RUNS_TABLE}
        ORDER BY contract_fingerprint
        """

        try:
            with self._cursor(conn) as cur:
                cur.execute(sql)
                return [row[0] for row in cur.fetchall()]
        except self._db_error() as e:
            self._logger.debug(f"Database error listing contracts: {e}")
            return []

    def clear(self, contract_fingerprint: str | None = None) -> int:
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
            with self._cursor(conn) as cur:
                if contract_fingerprint:
                    cur.execute(
                        f"DELETE FROM {self.RUNS_TABLE} WHERE contract_fingerprint = {self._PARAM}",
                        (contract_fingerprint,)
                    )
                else:
                    cur.execute(f"DELETE FROM {self.RUNS_TABLE}")
                deleted = cur.rowcount
            conn.commit()
            return deleted
        except self._db_error() as e:
            self._logger.warning(f"Database error clearing states: {e}")
            conn.rollback()
            return 0

    # -------------------------------------------------------------------------
    # Annotation Methods
    # -------------------------------------------------------------------------

    def save_annotation(self, annotation: Annotation) -> int:
        """Save an annotation (append-only)."""
        conn = self._get_conn()

        sql = self._insert_returning_id(self.ANNOTATIONS_TABLE, _ANNOTATION_INSERT_COLUMNS)

        try:
            with self._cursor(conn) as cur:
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
        rule_result_id: int | None = None,
    ) -> list[Annotation]:
        """Get annotations for a run or specific rule result."""
        conn = self._get_conn()
        p = self._PARAM

        where = f"run_id = {p}"
        params: tuple[Any, ...] = (run_id,)
        if rule_result_id is not None:
            where += f" AND rule_result_id = {p}"
            params = (run_id, rule_result_id)

        sql = f"""
        SELECT {_ANNOTATION_COLUMNS}
        FROM {self.ANNOTATIONS_TABLE}
        WHERE {where}
        ORDER BY created_at DESC
        """

        try:
            with self._cursor(conn) as cur:
                cur.execute(sql, params)
                return [self._annotation_from_row(row) for row in cur.fetchall()]
        except self._db_error() as e:
            self._logger.debug(f"Database error getting annotations for run {run_id}: {e}")
            return []

    def get_annotations_for_contract(
        self,
        contract_fingerprint: str,
        rule_id: str | None = None,
        annotation_type: str | None = None,
        limit: int = 20,
    ) -> list[Annotation]:
        """Get annotations across all runs for a contract."""
        conn = self._get_conn()
        p = self._PARAM

        # Join runs to filter by contract, and rule_results to get rule_id
        body = f"""
        FROM {self.ANNOTATIONS_TABLE} a
        JOIN {self.RUNS_TABLE} r ON a.run_id = r.id
        LEFT JOIN {self.RULE_RESULTS_TABLE} rr ON a.rule_result_id = rr.id
        WHERE r.contract_fingerprint = {p}
        """
        params: list[Any] = [contract_fingerprint]

        if rule_id is not None:
            body += f" AND rr.rule_id = {p}"
            params.append(rule_id)

        if annotation_type is not None:
            body += f" AND a.annotation_type = {p}"
            params.append(annotation_type)

        body += " ORDER BY a.created_at DESC"

        sql, sql_params = self._select_top(
            """
            a.id, a.run_id, a.rule_result_id, a.actor_type, a.actor_id,
            a.annotation_type, a.summary, a.payload, a.created_at,
            rr.rule_id""",
            body,
            tuple(params),
            limit,
        )

        try:
            with self._cursor(conn) as cur:
                cur.execute(sql, sql_params)
                return [
                    self._annotation_from_row(row[:9], rule_id=row[9])
                    for row in cur.fetchall()
                ]
        except self._db_error() as e:
            self._logger.debug(
                f"Database error getting annotations for contract {contract_fingerprint}: {e}"
            )
            return []

    def get_run_with_annotations(
        self,
        contract_fingerprint: str,
        run_id: int | None = None,
    ) -> ValidationState | None:
        """Get a validation state with its annotations loaded."""
        conn = self._get_conn()
        p = self._PARAM

        if run_id is not None:
            run_sql = f"""
            SELECT {_RUN_COLUMNS}
            FROM {self.RUNS_TABLE}
            WHERE id = {p} AND contract_fingerprint = {p}
            """
            run_params: tuple[Any, ...] = (run_id, contract_fingerprint)
        else:
            run_sql, run_params = self._select_top(
                _RUN_COLUMNS,
                f"""
            FROM {self.RUNS_TABLE}
            WHERE contract_fingerprint = {p}
            ORDER BY run_at DESC
            """,
                (contract_fingerprint,),
            )

        ann_sql = f"""
        SELECT {_ANNOTATION_COLUMNS}
        FROM {self.ANNOTATIONS_TABLE}
        WHERE run_id = {p}
        ORDER BY created_at DESC
        """

        try:
            state = self._get_one_state(conn, run_sql, run_params)
            if state is None:
                return None

            with self._cursor(conn) as cur:
                cur.execute(ann_sql, (state.id,))
                annotations = [self._annotation_from_row(row) for row in cur.fetchall()]

            self._attach_annotations_to_state(state, annotations)
            return state
        except self._db_error() as e:
            self._logger.debug(f"Database error getting run with annotations: {e}")
            return None

    def get_history_with_annotations(
        self,
        contract_fingerprint: str,
        limit: int = 10,
    ) -> list[ValidationState]:
        # For efficiency, we load history without annotations first,
        # then load annotations in batch
        states = self.get_history(contract_fingerprint, limit=limit)
        if not states:
            return []

        run_ids = [s.id for s in states if s.id is not None]
        annotations_by_run: dict[int, list[Annotation]] = {}

        if run_ids:
            conn = self._get_conn()
            try:
                with self._cursor(conn) as cur:
                    ann_rows = self._fetch_for_runs(
                        cur, _ANNOTATION_COLUMNS, self.ANNOTATIONS_TABLE, run_ids,
                        "created_at DESC",
                    )
                for row in ann_rows:
                    annotation = self._annotation_from_row(row)
                    annotations_by_run.setdefault(annotation.run_id, []).append(annotation)
            except self._db_error() as e:
                # On error, return states without annotations
                self._logger.debug(f"Database error loading annotations for history: {e}")
                annotations_by_run = {}

        for state in states:
            self._attach_annotations_to_state(state, annotations_by_run.get(state.id, []))
        return states
