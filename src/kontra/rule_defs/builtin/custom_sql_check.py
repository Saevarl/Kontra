from __future__ import annotations
from typing import Dict, Any, Optional, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    import polars as pl

from kontra.rule_defs.base import BaseRule
from kontra.rule_defs.registry import register_rule
from kontra.state.types import FailureMode


@register_rule("custom_sql_check", _builtin=True)
class CustomSQLCheck(BaseRule):
    """
    Custom SQL check rule for flexible validation logic.

    Executes user-provided SQL and counts violations.
    Supports remote execution on PostgreSQL/SQL Server when safe.

    Parameters:
        sql: SQL query that returns rows representing violations.
             Use {table} as placeholder for the table reference.

    Example:
        - name: custom_sql_check
          params:
            sql: "SELECT * FROM {table} WHERE balance < 0 AND status = 'active'"

    Remote Execution:
        When the data source is PostgreSQL or SQL Server, the SQL is validated
        using sqlglot to ensure it's safe (SELECT-only, no dangerous functions).
        If safe, it executes directly on the database. Otherwise, falls back to
        loading data into DuckDB.
    """

    rule_scope = "custom"
    supports_tally = False

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._validation_result: Optional[Any] = None  # Cache validation result

        from kontra.errors import RuleParameterError

        # Warn if sql is missing {table} placeholder (BUG-016)
        # Not a hard error because hardcoded "data" works for local DuckDB execution.
        # The placeholder is required for remote execution (PostgreSQL/SQL Server).
        sql = self.params.get("sql") or self.params.get("query")
        if sql and "{table}" not in sql:
            import warnings
            warnings.warn(
                f"custom_sql_check SQL should use {{table}} placeholder instead of "
                f"hardcoded table names. Without it, remote execution (PostgreSQL/SQL Server) "
                f"will fail. Example: SELECT * FROM {{table}} WHERE balance < 0",
                UserWarning,
                stacklevel=4,
            )

        # Validate threshold is non-negative (BUG-017)
        threshold = self.params.get("threshold")
        if threshold is not None and threshold < 0:
            raise RuleParameterError(
                "custom_sql_check", "threshold",
                f"threshold must be >= 0, got {threshold}"
            )

    def needs_all_columns(self) -> bool:
        # The user's SQL can reference any column; the query runs SELECT * over
        # the frame. Projecting the frame down to other rules' columns would
        # make it error on a missing column, so it needs the full frame.
        return True

    def validate(self, df: pl.DataFrame) -> Dict[str, Any]:
        """Execute SQL check via DuckDB (fallback path)."""
        import duckdb

        from kontra.engine.sql_validator import to_count_query, validate_sql

        # Accept both 'sql' (documented) and 'query' (legacy) parameter names
        query = self.params.get("sql") or self.params.get("query")
        if not query:
            return {
                "rule_id": self.rule_id,
                "passed": False,
                "failed_count": int(df.height),
                "message": "Missing 'sql' parameter",
            }

        # Substitute {table} placeholder with the registered table name
        query = query.replace("{table}", "data")

        try:
            # Validate SQL is safe before execution (blocks read_csv, read_parquet, etc.)
            validation = validate_sql(query, dialect="duckdb")
            if not validation.is_safe:
                raise ValueError(f"SQL validation failed: {validation.reason}")

            # Transform to COUNT(*) query for efficiency
            success, count_query = to_count_query(query, dialect="duckdb")
            if not success:
                raise ValueError(f"Failed to transform SQL: {count_query}")

            # Use DuckDB's native Polars support (zero-copy)
            con = duckdb.connect()
            con.register("data", df)
            result = con.execute(count_query).fetchone()

            if result is None or len(result) < 1:
                raise ValueError("Query returned no result")

            failed_count = int(result[0]) if result[0] is not None else 0
            threshold = self.params.get("threshold", 0)
            passed = failed_count <= threshold

            res: Dict[str, Any] = {
                "rule_id": self.rule_id,
                "passed": passed,
                "failed_count": failed_count,
                "message": "Passed" if passed else f"Custom SQL check failed for {failed_count} rows (threshold: {threshold})",
            }

            if failed_count > 0:
                res["failure_mode"] = str(FailureMode.CUSTOM_CHECK_FAILED)
                res["details"] = {
                    "query": query,
                    "failed_row_count": failed_count,
                }

            return res
        except duckdb.Error as e:
            return {
                "rule_id": self.rule_id,
                "passed": False,
                "failed_count": int(df.height),
                "message": f"SQL execution failed: {e}",
            }
        except ValueError as e:
            return {
                "rule_id": self.rule_id,
                "passed": False,
                "failed_count": int(df.height),
                "message": f"Validation error: {e}",
            }

    def compile_predicate(self):
        return None  # fallback-only

    # -------------------------------------------------------------------------
    # Remote Execution Support
    # -------------------------------------------------------------------------

    def supports_remote_execution(self, dialect: str) -> Tuple[bool, str]:
        """
        Check if this rule can be executed directly on a remote database.

        Uses sqlglot to validate the SQL is safe (SELECT-only, no side effects).

        Args:
            dialect: Database dialect ("postgres", "sqlserver")

        Returns:
            Tuple of (is_supported, reason)
        """
        from kontra.engine.sql_validator import validate_sql

        query = self.params.get("sql") or self.params.get("query")
        if not query:
            return False, "Missing SQL parameter"

        # Remove {table} placeholder for validation (it will be replaced later)
        # Use a dummy table name for parsing
        test_sql = query.replace("{table}", "dummy_table")

        result = validate_sql(test_sql, dialect=dialect)

        if result.is_safe:
            self._validation_result = result
            return True, "SQL validated as safe for remote execution"
        else:
            return False, result.reason or "SQL validation failed"

    def get_remote_sql(
        self,
        schema: str,
        table: str,
        dialect: str,
    ) -> str:
        """
        Get the SQL query formatted for remote execution.

        Args:
            schema: Database schema (e.g., "public", "dbo")
            table: Table name
            dialect: Database dialect ("postgres", "sqlserver")

        Returns:
            SQL query with {table} replaced with proper table reference
        """
        from kontra.engine.sql_validator import replace_table_placeholder

        query = self.params.get("sql") or self.params.get("query")
        if not query:
            raise ValueError("Missing SQL parameter")

        return replace_table_placeholder(
            sql=query,
            schema=schema,
            table=table,
            dialect=dialect,
        )

    def to_sql_spec(self) -> Optional[Dict[str, Any]]:
        """
        Return SQL specification for the executor.

        This is used by SQL executors to determine if/how to execute remotely.
        """
        query = self.params.get("sql") or self.params.get("query")
        if not query:
            return None

        return {
            "kind": "custom_sql_check",
            "rule_id": self.rule_id,
            "sql": query,
            "params": self.params,
        }
