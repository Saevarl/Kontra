# src/kontra/api/sampling.py
"""
Sampling subsystem for ValidationResult.

This module contains all the logic for collecting sample failure rows,
extracted from ValidationResult to reduce complexity and improve testability.

The main components are:
- SamplingContext: Immutable context for sampling operations
- SamplingOrchestrator: Coordinates sampling across different data sources
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple, Union, TYPE_CHECKING

if TYPE_CHECKING:
    import polars as pl

_logger = logging.getLogger(__name__)


# --- Sampling reason constants ---

class SampleReason:
    """Constants for why samples may be unavailable."""

    UNAVAILABLE_METADATA = "unavailable_from_metadata"  # Preplan tier - knows existence, not location
    UNAVAILABLE_PASSED = "rule_passed"  # No failures to sample
    UNAVAILABLE_UNSUPPORTED = "rule_unsupported"  # dtype, min_rows, etc. - no row-level samples
    TRUNCATED_BUDGET = "budget_exhausted"  # Global budget hit
    TRUNCATED_LIMIT = "per_rule_limit"  # Per-rule cap hit


# --- Unique rule sampling helpers ---


def _is_unique_rule(rule: Any) -> bool:
    """Check if a rule is a unique rule."""
    return getattr(rule, "name", None) == "unique"


def _filter_samples_polars(
    source: Any,  # pl.DataFrame or pl.LazyFrame
    rule: Any,
    predicate: Any,
    n: int,
) -> Any:  # pl.DataFrame
    """
    Filter samples with special handling for unique rule.

    Works with both DataFrame and LazyFrame sources. Adds row_index,
    and for unique rules adds _duplicate_count sorted by worst offenders.

    Args:
        source: Polars DataFrame or LazyFrame
        rule: Rule object (used to detect unique rule)
        predicate: Polars expression for filtering
        n: Maximum rows to return

    Returns:
        Polars DataFrame with filtered samples
    """
    import polars as pl

    # Convert DataFrame to LazyFrame if needed
    if isinstance(source, pl.DataFrame):
        lf = source.lazy()
    else:
        lf = source

    lf = lf.with_row_index("row_index")

    # Special case: unique rule - add duplicate count, sort by worst offenders
    if _is_unique_rule(rule):
        column = rule.params.get("column")
        return (
            lf.with_columns(
                pl.col(column).count().over(column).alias("_duplicate_count")
            )
            .filter(predicate)
            .sort("_duplicate_count", descending=True)
            .head(n)
            .collect()
        )
    return lf.filter(predicate).head(n).collect()


def _build_unique_sample_query_sql(
    table: str,
    column: str,
    n: int,
    dialect: str,
) -> str:
    """
    Build SQL query for sampling unique rule violations.

    Returns query that finds duplicate values, orders by worst offenders,
    and includes _duplicate_count and row_index.

    Args:
        table: Fully qualified table name
        column: Column being checked for uniqueness
        n: Maximum rows to return
        dialect: SQL dialect ("postgres", "mssql")

    Returns:
        SQL query string
    """
    col = f'"{column}"'

    if dialect == "mssql":
        return f"""
            SELECT t.*, dup._duplicate_count,
                   ROW_NUMBER() OVER (ORDER BY dup._duplicate_count DESC) - 1 AS row_index
            FROM {table} t
            JOIN (
                SELECT {col}, COUNT(*) as _duplicate_count
                FROM {table}
                GROUP BY {col}
                HAVING COUNT(*) > 1
            ) dup ON t.{col} = dup.{col}
            ORDER BY dup._duplicate_count DESC
            OFFSET 0 ROWS FETCH FIRST {n} ROWS ONLY
        """
    return f"""
        SELECT t.*, dup._duplicate_count,
               ROW_NUMBER() OVER (ORDER BY dup._duplicate_count DESC) - 1 AS row_index
        FROM {table} t
        JOIN (
            SELECT {col}, COUNT(*) as _duplicate_count
            FROM {table}
            GROUP BY {col}
            HAVING COUNT(*) > 1
        ) dup ON t.{col} = dup.{col}
        ORDER BY dup._duplicate_count DESC
        LIMIT {n}
    """


# --- Data structures ---


@dataclass
class SamplingContext:
    """
    Immutable context for sampling operations.

    Holds all the information needed to perform sampling without
    coupling to ValidationResult internals.
    """

    data_source: Any  # DatasetHandle | DataFrame | str
    rule_objects: List[Any]  # Rule instances for predicates
    cached_data: Optional["pl.DataFrame"] = None  # Pre-loaded data if available


# --- Main orchestrator ---


class SamplingOrchestrator:
    """
    Orchestrates sampling operations across different data sources.

    Supports:
    - Local Parquet files (Polars scan)
    - Remote Parquet (S3/Azure via DuckDB)
    - PostgreSQL databases
    - SQL Server databases
    - In-memory DataFrames
    """

    def __init__(self, ctx: SamplingContext):
        """
        Initialize the sampling orchestrator.

        Args:
            ctx: SamplingContext with data source and rule objects
        """
        self._ctx = ctx
        self._rule_map = {
            getattr(r, "rule_id", None): r for r in (ctx.rule_objects or [])
        }

    def can_sample_source(self) -> bool:
        """
        Check if the data source supports sampling.

        File-based sources (Parquet, CSV, S3) can always be sampled.
        Database sources need a live connection or db_params.

        Returns:
            True if sampling is possible, False otherwise.
        """
        import polars as pl

        source = self._ctx.data_source

        if source is None:
            return False

        # DataFrame - always sampleable
        if isinstance(source, pl.DataFrame):
            return True

        # String path - file based, always sampleable
        if isinstance(source, str):
            return True

        # DatasetHandle - check scheme and connection
        if hasattr(source, "scheme"):
            scheme = getattr(source, "scheme", None)

            # File-based schemes - always sampleable
            if scheme in (None, "file") or (hasattr(source, "uri") and source.uri):
                uri = getattr(source, "uri", "")
                if uri.lower().endswith((".parquet", ".csv")) or uri.startswith("s3://"):
                    return True

            # BYOC or database with connection - check if connection exists
            if hasattr(source, "external_conn") and source.external_conn is not None:
                return True

            # Database with db_params - we can create a connection for sampling
            if hasattr(source, "db_params") and source.db_params is not None:
                return True

            # Database without connection or db_params - can't sample
            if scheme in ("postgres", "postgresql", "mssql"):
                return False

        return True  # Default to sampleable

    def resolve_sample_columns(
        self,
        sample_columns: Optional[Union[List[str], str]],
        rule_obj: Any,
    ) -> Optional[List[str]]:
        """
        Resolve sample_columns to a list of column names.

        Args:
            sample_columns: None (all), list of names, or "relevant"
            rule_obj: Rule object for "relevant" mode

        Returns:
            List of column names to include, or None for all columns
        """
        if sample_columns is None:
            return None

        if isinstance(sample_columns, list):
            return sample_columns

        if sample_columns == "relevant":
            # Get columns from rule's required_columns() if available
            cols = set()
            if hasattr(rule_obj, "required_columns"):
                cols.update(rule_obj.required_columns())

            # Also check params for column names (required_columns() may be incomplete)
            if hasattr(rule_obj, "params"):
                params = rule_obj.params
                if "column" in params:
                    cols.add(params["column"])
                if "left" in params:
                    cols.add(params["left"])
                if "right" in params:
                    cols.add(params["right"])
                if "when_column" in params:
                    cols.add(params["when_column"])

            return list(cols) if cols else None

        # Comma-separated string: "email,user_id" -> ["email", "user_id"]
        if isinstance(sample_columns, str):
            return [c.strip() for c in sample_columns.split(",") if c.strip()]

        # Unknown value - return all columns
        return None

    def apply_column_projection(
        self,
        df: Any,
        columns: Optional[List[str]],
    ) -> List[Dict[str, Any]]:
        """
        Apply column projection to a DataFrame before converting to dicts.

        Always includes row_index if present.

        Args:
            df: Polars DataFrame
            columns: Columns to include (None = all)

        Returns:
            List of row dicts
        """
        if columns is None:
            return df.to_dicts()

        # Always include row_index and _duplicate_count if present
        cols_to_select = set(columns)
        if "row_index" in df.columns:
            cols_to_select.add("row_index")
        if "_duplicate_count" in df.columns:
            cols_to_select.add("_duplicate_count")

        # Only select columns that exist in the DataFrame
        available_cols = set(df.columns)
        cols_to_select = cols_to_select & available_cols

        if not cols_to_select:
            return df.to_dicts()

        return df.select(sorted(cols_to_select)).to_dicts()

    def collect_samples_for_rule(
        self,
        rule_obj: Any,
        predicate: Any,
        n: int,
        columns: Optional[List[str]] = None,
    ) -> Tuple[List[Dict[str, Any]], str]:
        """
        Collect sample rows for a single rule.

        Uses the existing sampling infrastructure (SQL pushdown, Parquet predicate, etc.)

        Args:
            rule_obj: Rule object
            predicate: Polars expression for filtering
            n: Number of samples to collect
            columns: Columns to include (None = all)

        Returns:
            Tuple of (samples list, source string "sql" or "polars")
        """
        import polars as pl

        source = self._ctx.data_source

        if source is None:
            return [], "polars"

        # Reuse existing loading/filtering logic
        df, load_source = self._load_data_for_sampling(rule_obj, n)

        # If data was already filtered by SQL/DuckDB, just apply column projection
        if load_source == "sql":
            result_df = df.head(n)
            return self.apply_column_projection(result_df, columns), "sql"

        # For Polars path, filter with predicate (unique rule handled by helper)
        result_df = _filter_samples_polars(df, rule_obj, predicate, n)

        # For unique rule, always include _duplicate_count in projection
        if _is_unique_rule(rule_obj) and columns is not None:
            columns = list(columns) + ["_duplicate_count"]

        return self.apply_column_projection(result_df, columns), "polars"

    def _load_data_for_sampling(
        self, rule: Any = None, n: int = 5
    ) -> Tuple["pl.DataFrame", str]:
        """
        Load data from the stored data source for sample_failures().

        For database sources with rules that support SQL filters,
        pushes the filter to SQL for performance.

        Returns:
            Tuple of (DataFrame, source) where source is "sql" or "polars"
        """
        import polars as pl

        source = self._ctx.data_source

        if source is None:
            raise RuntimeError("No data source available")

        # String path/URI
        if isinstance(source, str):
            # Try to load as file with predicate pushdown for Parquet
            if source.lower().endswith(".parquet") or source.startswith("s3://"):
                return self._load_parquet_with_filter(source, rule, n)
            elif source.lower().endswith(".csv"):
                return pl.read_csv(source), "polars"
            # Try parquet first, then CSV
            try:
                return self._load_parquet_with_filter(source, rule, n)
            except (OSError, ValueError) as parquet_err:
                try:
                    return pl.read_csv(source), "polars"
                except (OSError, ValueError) as csv_err:
                    raise RuntimeError(f"Cannot load data from: {source} (parquet: {parquet_err}, csv: {csv_err})")

        # Polars DataFrame (was passed directly)
        if isinstance(source, pl.DataFrame):
            return source, "polars"

        # DatasetHandle (BYOC or parsed URI)
        if hasattr(source, "scheme") and hasattr(source, "uri"):
            # It's a DatasetHandle
            handle = source

            # Check for BYOC (external connection)
            if handle.scheme == "byoc" or (handle.external_conn is not None):
                conn = handle.external_conn
                if conn is None:
                    raise RuntimeError(
                        "Database connection is closed. "
                        "For BYOC, keep the connection open until done with sample_failures()."
                    )
                table = getattr(handle, "table_ref", None) or handle.path
                dialect = handle.dialect or "postgres"
                return self._query_db_with_filter(conn, table, rule, n, dialect), "sql"

            elif handle.scheme in ("postgres", "postgresql"):
                # PostgreSQL via URI
                conn = getattr(handle, "external_conn", None)
                owns_conn = False
                if conn is None and handle.db_params is not None:
                    # Create connection from stored params
                    try:
                        from kontra.connectors.postgres import get_connection
                        conn = get_connection(handle.db_params)
                        owns_conn = True
                    except Exception as e:
                        raise RuntimeError(
                            f"Failed to connect to PostgreSQL for sampling: {e}"
                        ) from e
                if conn is None:
                    raise RuntimeError(
                        "Database connection is not available. "
                        "For URI-based connections, sample_failures() requires re-connection."
                    )
                table = getattr(handle, "table_ref", None) or handle.path
                try:
                    return self._query_db_with_filter(conn, table, rule, n, "postgres"), "sql"
                finally:
                    if owns_conn:
                        conn.close()

            elif handle.scheme == "mssql":
                # SQL Server
                conn = getattr(handle, "external_conn", None)
                owns_conn = False
                if conn is None and handle.db_params is not None:
                    # Create connection from stored params
                    try:
                        from kontra.connectors.sqlserver import get_connection
                        conn = get_connection(handle.db_params)
                        owns_conn = True
                    except Exception as e:
                        raise RuntimeError(
                            f"Failed to connect to SQL Server for sampling: {e}"
                        ) from e
                if conn is None:
                    raise RuntimeError(
                        "Database connection is not available."
                    )
                table = getattr(handle, "table_ref", None) or handle.path
                try:
                    return self._query_db_with_filter(conn, table, rule, n, "mssql"), "sql"
                finally:
                    if owns_conn:
                        conn.close()

            elif handle.scheme in ("file", None) or (handle.uri and not handle.scheme):
                # File-based (local)
                uri = handle.uri
                if uri.lower().endswith(".parquet"):
                    return self._load_parquet_with_filter(uri, rule, n)
                elif uri.lower().endswith(".csv"):
                    return pl.read_csv(uri), "polars"
                return self._load_parquet_with_filter(uri, rule, n)

            elif handle.scheme in ("s3", "abfss", "abfs", "az"):
                # S3 or Azure - use DuckDB for cloud parquet
                return self._load_cloud_parquet_with_filter(handle, rule, n)

        raise RuntimeError(f"Unsupported data source type: {type(source)}")

    def _query_db_with_filter(
        self,
        conn: Any,
        table: str,
        rule: Any,
        n: int,
        dialect: str,
    ) -> "pl.DataFrame":
        """
        Query database with SQL filter if rule supports it.

        Uses the rule's to_sql_filter() method to push the filter to SQL,
        avoiding loading the entire table.
        """
        import polars as pl

        sql_filter = None

        # Special case: unique rule needs subquery with table name
        if _is_unique_rule(rule):
            column = rule.params.get("column")
            if column:
                query = _build_unique_sample_query_sql(table, column, n, dialect)
                return pl.read_database(query, conn)

        if rule is not None and hasattr(rule, "to_sql_filter"):
            sql_filter = rule.to_sql_filter(dialect)

        if sql_filter:
            # Build query with filter and row number
            # ROW_NUMBER() gives us the original row index
            if dialect == "mssql":
                # SQL Server syntax
                query = f"""
                    SELECT *, ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1 AS row_index
                    FROM {table}
                    WHERE {sql_filter}
                    ORDER BY (SELECT NULL)
                    OFFSET 0 ROWS FETCH FIRST {n} ROWS ONLY
                """
            else:
                # PostgreSQL / DuckDB syntax
                query = f"""
                    SELECT *, ROW_NUMBER() OVER () - 1 AS row_index
                    FROM {table}
                    WHERE {sql_filter}
                    LIMIT {n}
                """
            return pl.read_database(query, conn)
        # Fall back to loading all data (rule doesn't support SQL filter)
        return pl.read_database(f"SELECT * FROM {table}", conn)

    def _load_parquet_with_filter(
        self,
        path: str,
        rule: Any,
        n: int,
    ) -> Tuple["pl.DataFrame", str]:
        """
        Load Parquet file with predicate pushdown for performance.

        - S3/remote files: Uses DuckDB SQL pushdown (doesn't download whole file)
        - Local files: Uses Polars scan_parquet (efficient for local)

        Returns:
            Tuple of (DataFrame, source) where source is "sql" or "polars"
        """
        import polars as pl

        # Check if this is a remote file (S3, HTTP)
        is_remote = path.startswith("s3://") or path.startswith("http://") or path.startswith("https://")

        # For remote files, use DuckDB SQL pushdown (much faster - doesn't download whole file)
        if is_remote and rule is not None and hasattr(rule, "to_sql_filter"):
            sql_filter = rule.to_sql_filter("duckdb")
            if sql_filter:
                try:
                    return self._query_parquet_with_duckdb(path, sql_filter, n), "sql"
                except (ImportError, OSError, ValueError) as e:
                    _logger.debug(f"DuckDB query failed, falling back to Polars: {e}")
                    pass  # Fall through to Polars

        # For local files, just return the raw data - caller will filter
        # (Don't filter here to avoid double-filtering in collect_samples_for_rule)
        return pl.read_parquet(path), "polars"

    def _load_cloud_parquet_with_filter(
        self,
        handle: Any,
        rule: Any,
        n: int,
    ) -> Tuple["pl.DataFrame", str]:
        """
        Load cloud Parquet file (S3/Azure) with predicate pushdown.

        Uses handle.fs_opts for credentials instead of environment variables.
        """
        import duckdb
        import polars as pl

        path = handle.uri
        fs_opts = handle.fs_opts or {}

        con = duckdb.connect()

        try:
            # Configure S3 credentials from fs_opts (keys are s3_* prefixed per handle.py)
            if handle.scheme == "s3":
                con.execute("INSTALL httpfs; LOAD httpfs;")
                if fs_opts.get("s3_access_key_id"):
                    con.execute(f"SET s3_access_key_id='{fs_opts['s3_access_key_id']}';")
                if fs_opts.get("s3_secret_access_key"):
                    con.execute(f"SET s3_secret_access_key='{fs_opts['s3_secret_access_key']}';")
                if fs_opts.get("s3_endpoint"):
                    raw_endpoint = fs_opts["s3_endpoint"]
                    endpoint = raw_endpoint.replace("http://", "").replace("https://", "")
                    con.execute(f"SET s3_endpoint='{endpoint}';")
                    # Check if SSL should be disabled (http:// means no SSL)
                    if raw_endpoint.startswith("http://"):
                        con.execute("SET s3_use_ssl=false;")
                    # Path-style access for custom endpoints (MinIO, etc.)
                    con.execute("SET s3_url_style='path';")
                if fs_opts.get("s3_region"):
                    con.execute(f"SET s3_region='{fs_opts['s3_region']}';")
                if fs_opts.get("s3_use_ssl") == "false":
                    con.execute("SET s3_use_ssl=false;")
                if fs_opts.get("s3_url_style"):
                    con.execute(f"SET s3_url_style='{fs_opts['s3_url_style']}';")

            # Configure Azure credentials from fs_opts
            elif handle.scheme in ("abfss", "abfs", "az"):
                if fs_opts.get("azure_account_name"):
                    con.execute(f"SET azure_storage_account_name='{fs_opts['azure_account_name']}';")
                if fs_opts.get("azure_account_key"):
                    from kontra.connectors.uri_utils import validate_azure_account_key

                    validate_azure_account_key(fs_opts["azure_account_key"])
                    con.execute(f"SET azure_storage_account_key='{fs_opts['azure_account_key']}';")
                from kontra.connectors.uri_utils import azure_transport_option

                transport = azure_transport_option(fs_opts)
                if transport:
                    try:
                        con.execute(f"SET azure_transport_option_type='{transport}';")
                    except duckdb.Error:
                        pass  # older azure extensions lack the option; keep default

            # Escape path for SQL
            escaped_path = path.replace("'", "''")

            # Check if rule supports SQL filter
            sql_filter = None
            if rule is not None and hasattr(rule, "to_sql_filter"):
                sql_filter = rule.to_sql_filter("duckdb")

            if sql_filter:
                # Use SQL filter with pushdown
                query = f"""
                    SELECT *, ROW_NUMBER() OVER () - 1 AS row_index
                    FROM read_parquet('{escaped_path}')
                    WHERE {sql_filter}
                    LIMIT {n}
                """
            else:
                # No filter - just load with limit
                query = f"""
                    SELECT *, ROW_NUMBER() OVER () - 1 AS row_index
                    FROM read_parquet('{escaped_path}')
                    LIMIT {n}
                """

            result = con.execute(query).pl()
            return result, "sql"

        finally:
            con.close()

    def _query_parquet_with_duckdb(
        self,
        path: str,
        sql_filter: str,
        n: int,
        columns: Optional[List[str]] = None,
    ) -> "pl.DataFrame":
        """
        Query Parquet file using DuckDB with SQL filter.

        Much faster than Polars for S3 files because DuckDB pushes
        the filter and LIMIT to the row group level.
        """
        import duckdb
        import os

        con = duckdb.connect()

        # Configure S3 if needed
        if path.startswith("s3://"):
            con.execute("INSTALL httpfs; LOAD httpfs;")
            if os.environ.get("AWS_ACCESS_KEY_ID"):
                con.execute(f"SET s3_access_key_id='{os.environ['AWS_ACCESS_KEY_ID']}';")
            if os.environ.get("AWS_SECRET_ACCESS_KEY"):
                con.execute(f"SET s3_secret_access_key='{os.environ['AWS_SECRET_ACCESS_KEY']}';")
            if os.environ.get("AWS_ENDPOINT_URL"):
                endpoint = os.environ["AWS_ENDPOINT_URL"].replace("http://", "").replace("https://", "")
                con.execute(f"SET s3_endpoint='{endpoint}';")
                con.execute("SET s3_use_ssl=false;")
                con.execute("SET s3_url_style='path';")
            if os.environ.get("AWS_REGION"):
                con.execute(f"SET s3_region='{os.environ['AWS_REGION']}';")

        # Escape path for SQL
        escaped_path = path.replace("'", "''")

        # Build column list (with projection if specified)
        if columns:
            col_list = ", ".join(f'"{c}"' for c in columns)
        else:
            col_list = "*"

        # Build query with filter and row number
        query = f"""
            SELECT {col_list}, ROW_NUMBER() OVER () - 1 AS row_index
            FROM read_parquet('{escaped_path}')
            WHERE {sql_filter}
            LIMIT {n}
        """

        try:
            result = con.execute(query).pl()
        finally:
            con.close()
        return result

    def batch_sample_parquet_duckdb(
        self,
        path: str,
        rules_to_sample: List[Tuple[str, str, int, Optional[List[str]]]],
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Batch sample multiple rules from Parquet using a single DuckDB query.

        Args:
            path: Parquet file path or S3 URI
            rules_to_sample: List of (rule_id, sql_filter, limit, columns)

        Returns:
            Dict mapping rule_id to list of sample dicts
        """
        import duckdb
        import os

        if not rules_to_sample:
            return {}

        con = duckdb.connect()

        # Configure S3 if needed
        if path.startswith("s3://"):
            con.execute("INSTALL httpfs; LOAD httpfs;")
            if os.environ.get("AWS_ACCESS_KEY_ID"):
                con.execute(f"SET s3_access_key_id='{os.environ['AWS_ACCESS_KEY_ID']}';")
            if os.environ.get("AWS_SECRET_ACCESS_KEY"):
                con.execute(f"SET s3_secret_access_key='{os.environ['AWS_SECRET_ACCESS_KEY']}';")
            if os.environ.get("AWS_ENDPOINT_URL"):
                endpoint = os.environ["AWS_ENDPOINT_URL"].replace("http://", "").replace("https://", "")
                con.execute(f"SET s3_endpoint='{endpoint}';")
                con.execute("SET s3_use_ssl=false;")
                con.execute("SET s3_url_style='path';")
            if os.environ.get("AWS_REGION"):
                con.execute(f"SET s3_region='{os.environ['AWS_REGION']}';")

        escaped_path = path.replace("'", "''")

        # Collect all columns needed across all rules
        all_columns: set = set()
        for rule_id, sql_filter, limit, columns in rules_to_sample:
            if columns:
                all_columns.update(columns)

        # If any rule needs all columns, use *
        needs_all = any(cols is None for _, _, _, cols in rules_to_sample)

        if needs_all or not all_columns:
            col_list = "*"
        else:
            col_list = ", ".join(f'"{c}"' for c in sorted(all_columns))

        # Build UNION ALL query - one subquery per rule (wrapped in parens for DuckDB)
        subqueries = []
        for rule_id, sql_filter, limit, columns in rules_to_sample:
            escaped_rule_id = rule_id.replace("'", "''")
            subquery = f"""(
                SELECT '{escaped_rule_id}' AS _rule_id, {col_list}, ROW_NUMBER() OVER () - 1 AS row_index
                FROM read_parquet('{escaped_path}')
                WHERE {sql_filter}
                LIMIT {limit}
            )"""
            subqueries.append(subquery)

        query = " UNION ALL ".join(subqueries)

        try:
            result_df = con.execute(query).pl()
        finally:
            con.close()

        # Distribute results to each rule
        results: Dict[str, List[Dict[str, Any]]] = {rule_id: [] for rule_id, _, _, _ in rules_to_sample}

        for row in result_df.to_dicts():
            rule_id = row.pop("_rule_id")
            if rule_id in results:
                results[rule_id].append(row)

        return results

    def batch_sample_db(
        self,
        conn: Any,
        table: str,
        rules_to_sample: List[Tuple[str, str, int, Optional[List[str]]]],
        dialect: str,
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Batch sample multiple rules from database using a single query.

        Args:
            conn: Database connection
            table: Table name (with schema if needed)
            rules_to_sample: List of (rule_id, sql_filter, limit, columns)
            dialect: "postgres" or "mssql"

        Returns:
            Dict mapping rule_id to list of sample dicts
        """
        import polars as pl

        if not rules_to_sample:
            return {}

        # Collect all columns needed across all rules
        all_columns: set = set()
        for rule_id, sql_filter, limit, columns in rules_to_sample:
            if columns:
                all_columns.update(columns)

        needs_all = any(cols is None for _, _, _, cols in rules_to_sample)

        if dialect == "mssql":
            # SQL Server syntax
            if needs_all or not all_columns:
                col_list = "*"
            else:
                col_list = ", ".join(f"[{c}]" for c in sorted(all_columns))

            subqueries = []
            for rule_id, sql_filter, limit, columns in rules_to_sample:
                escaped_rule_id = rule_id.replace("'", "''")
                subquery = f"""(
                    SELECT TOP {limit} '{escaped_rule_id}' AS _rule_id, {col_list},
                           ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1 AS row_index
                    FROM {table}
                    WHERE {sql_filter}
                )"""
                subqueries.append(subquery)
        else:
            # PostgreSQL syntax
            if needs_all or not all_columns:
                col_list = "*"
            else:
                col_list = ", ".join(f'"{c}"' for c in sorted(all_columns))

            subqueries = []
            for rule_id, sql_filter, limit, columns in rules_to_sample:
                escaped_rule_id = rule_id.replace("'", "''")
                subquery = f"""(
                    SELECT '{escaped_rule_id}' AS _rule_id, {col_list},
                           ROW_NUMBER() OVER () - 1 AS row_index
                    FROM {table}
                    WHERE {sql_filter}
                    LIMIT {limit}
                )"""
                subqueries.append(subquery)

        query = " UNION ALL ".join(subqueries)

        result_df = pl.read_database(query, conn)

        # Distribute results to each rule
        results: Dict[str, List[Dict[str, Any]]] = {rule_id: [] for rule_id, _, _, _ in rules_to_sample}

        for row in result_df.to_dicts():
            rule_id = row.pop("_rule_id")
            if rule_id in results:
                results[rule_id].append(row)

        return results

    def perform_eager_sampling(
        self,
        rule_results: List[Any],  # List[RuleResult]
        per_rule_cap: int,
        global_budget: int,
        sample_columns: Optional[Union[List[str], str]] = None,
    ) -> None:
        """
        Populate samples for each rule (eager sampling).

        Uses batched SQL queries when possible (1 query for all rules).
        Falls back to per-rule Polars sampling when SQL not supported.

        Args:
            rule_results: List of RuleResult objects to populate
            per_rule_cap: Max samples per rule
            global_budget: Total samples across all rules
            sample_columns: Columns to include (None=all, list=specific, "relevant"=rule columns)
        """
        import polars as pl

        # Sort rules by failed_count descending (worst offenders first)
        sorted_rules = sorted(
            rule_results,
            key=lambda r: r.failed_count if not r.passed else 0,
            reverse=True,
        )

        remaining_budget = global_budget

        # Phase 1: Collect rules that can use SQL batching
        sql_rules: List[Tuple[Any, Any, int, Optional[List[str]]]] = []  # (rule_result, rule_obj, n, columns)
        polars_rules: List[Tuple[Any, Any, Any, int, Optional[List[str]]]] = []  # (rule_result, rule_obj, predicate, n, columns)

        for rule_result in sorted_rules:
            # Handle passing rules
            if rule_result.passed:
                rule_result.samples = []
                rule_result.samples_reason = SampleReason.UNAVAILABLE_PASSED
                continue

            # Check budget
            if remaining_budget <= 0:
                rule_result.samples = None
                rule_result.samples_reason = SampleReason.TRUNCATED_BUDGET
                rule_result.samples_truncated = True
                continue

            # Get corresponding rule object
            rule_obj = self._rule_map.get(rule_result.rule_id)
            if rule_obj is None:
                rule_result.samples = None
                rule_result.samples_reason = SampleReason.UNAVAILABLE_UNSUPPORTED
                continue

            # Check if rule was resolved via metadata (preplan)
            if rule_result.source == "metadata":
                if not self.can_sample_source():
                    rule_result.samples = None
                    rule_result.samples_reason = SampleReason.UNAVAILABLE_METADATA
                    continue

            # Calculate samples to get for this rule
            n = min(per_rule_cap, remaining_budget)
            remaining_budget -= n  # Reserve budget

            # Determine columns to include
            cols_to_include = self.resolve_sample_columns(sample_columns, rule_obj)

            # Check if rule supports SQL filter (for batching)
            # Note: to_sql_filter can return None to indicate no SQL support for this dialect
            sql_filter_available = False
            if hasattr(rule_obj, "to_sql_filter"):
                # Probe to see if SQL filter is actually available
                sql_filter = rule_obj.to_sql_filter("duckdb")
                if sql_filter is not None:
                    sql_rules.append((rule_result, rule_obj, n, cols_to_include))
                    sql_filter_available = True

            # Fall back to sample_predicate/compile_predicate if SQL not available
            if not sql_filter_available:
                # Check sample_predicate() first (for rules like unique that have
                # different counting vs sampling semantics)
                pred_obj = None
                if hasattr(rule_obj, "sample_predicate"):
                    pred_obj = rule_obj.sample_predicate()
                if pred_obj is None and hasattr(rule_obj, "compile_predicate"):
                    pred_obj = rule_obj.compile_predicate()

                if pred_obj is not None:
                    polars_rules.append((rule_result, rule_obj, pred_obj.expr, n, cols_to_include))
                else:
                    rule_result.samples = None
                    rule_result.samples_reason = SampleReason.UNAVAILABLE_UNSUPPORTED

        # Phase 2: Execute batched SQL sampling if applicable
        if sql_rules:
            self._execute_batched_sql_sampling(sql_rules, per_rule_cap)

        # Phase 3: Execute per-rule Polars sampling for remaining rules
        for rule_result, rule_obj, predicate, n, cols_to_include in polars_rules:
            try:
                samples, samples_source = self.collect_samples_for_rule(rule_obj, predicate, n, cols_to_include)
                rule_result.samples = samples
                rule_result.samples_source = samples_source

                if len(samples) == per_rule_cap and rule_result.failed_count > per_rule_cap:
                    rule_result.samples_truncated = True
                    rule_result.samples_reason = SampleReason.TRUNCATED_LIMIT

            except Exception as e:
                # Sampling is best-effort: record the error on the result
                _logger.debug("Polars sampling failed for %s: %s", rule_result.rule_id, e)
                rule_result.samples = None
                rule_result.samples_reason = f"error: {str(e)[:50]}"

    def _classify_sampling_source(
        self, source: Any
    ) -> Tuple[str, bool, Optional[str], bool, Any, Optional[str], bool]:
        """
        Classify the data source for batched SQL sampling.

        Returns (dialect, is_remote_file, parquet_path, is_database,
        db_conn, db_table, owns_connection). Local files report neither
        remote-file nor database and fall through to Polars sampling.
        `owns_connection` is True when we opened db_conn ourselves and
        must close it after sampling.
        """
        if isinstance(source, str):
            if source.startswith(("s3://", "http://", "https://")):
                # Remote file - use DuckDB for efficient sampling
                return "duckdb", True, source, False, None, None, False
            return "duckdb", False, None, False, None, None, False

        if hasattr(source, "scheme"):
            handle = source
            if handle.scheme in ("postgres", "postgresql"):
                db_conn, owns = self._db_connection_for_sampling(handle, "postgres")
                db_table = handle.table_ref or (
                    f'"{handle.db_params.schema}"."{handle.db_params.table}"'
                    if handle.db_params else None
                )
                return "postgres", False, None, True, db_conn, db_table, owns
            if handle.scheme == "mssql":
                db_conn, owns = self._db_connection_for_sampling(handle, "mssql")
                db_table = handle.table_ref or (
                    f"[{handle.db_params.schema}].[{handle.db_params.table}]"
                    if handle.db_params else None
                )
                return "mssql", False, None, True, db_conn, db_table, owns
            if handle.scheme == "s3":
                return "duckdb", True, handle.uri, False, None, None, False

        return "duckdb", False, None, False, None, None, False

    def _db_connection_for_sampling(
        self, handle: Any, flavor: str
    ) -> Tuple[Any, bool]:
        """
        Connection for sampling: the handle's external connection, or a
        fresh one built from db_params (second element True = caller must
        close it). Returns (None, False) when neither is available.
        """
        db_conn = getattr(handle, "external_conn", None)
        if db_conn is not None or handle.db_params is None:
            return db_conn, False

        display = "PostgreSQL" if flavor == "postgres" else "SQL Server"
        try:
            if flavor == "postgres":
                from kontra.connectors.postgres import get_connection
            else:
                from kontra.connectors.sqlserver import get_connection
            return get_connection(handle.db_params), True
        except ImportError as e:
            _logger.debug(f"{display} driver not available: {e}")
        except (OSError, ConnectionError) as e:
            _logger.debug(f"Could not connect to {display} for sampling: {e}")
        return None, False

    def _execute_batched_sql_sampling(
        self,
        sql_rules: List[Tuple[Any, Any, int, Optional[List[str]]]],
        per_rule_cap: int,
    ) -> None:
        """
        Execute batched SQL sampling for rules that support to_sql_filter().

        Builds a single UNION ALL query for all rules, executes once,
        and distributes results.
        """
        source = self._ctx.data_source
        if source is None:
            return

        (
            dialect,
            is_remote_file,
            parquet_path,
            is_database,
            db_conn,
            db_table,
            owns_connection,
        ) = self._classify_sampling_source(source)

        # Build list of (rule_id, sql_filter, limit, columns) for batching
        rules_to_sample: List[Tuple[str, str, int, Optional[List[str]]]] = []

        for rule_result, rule_obj, n, cols_to_include in sql_rules:
            sql_filter = rule_obj.to_sql_filter(dialect)
            if sql_filter:
                rules_to_sample.append((rule_result.rule_id, sql_filter, n, cols_to_include))
            else:
                # Rule doesn't support this dialect, mark as unsupported
                rule_result.samples = None
                rule_result.samples_reason = SampleReason.UNAVAILABLE_UNSUPPORTED

        if not rules_to_sample:
            return

        # Execute batched query
        try:
            if is_remote_file and parquet_path:
                # S3/remote: Use DuckDB batched sampling (much faster than Polars)
                results = self.batch_sample_parquet_duckdb(parquet_path, rules_to_sample)
                samples_source = "sql"
            elif is_database and db_conn and db_table:
                results = self.batch_sample_db(db_conn, db_table, rules_to_sample, dialect)
                samples_source = "sql"
            else:
                # Fall back to per-rule sampling
                results = {}
                samples_source = "polars"
                for rule_result, rule_obj, n, cols_to_include in sql_rules:
                    if hasattr(rule_obj, "compile_predicate"):
                        pred_obj = rule_obj.compile_predicate()
                        if pred_obj:
                            try:
                                samples, src = self.collect_samples_for_rule(rule_obj, pred_obj.expr, n, cols_to_include)
                                results[rule_result.rule_id] = samples
                                samples_source = src
                            except (ValueError, TypeError, KeyError, OSError) as e:
                                _logger.debug(f"Could not collect samples for rule {rule_result.rule_id}: {e}")
                                results[rule_result.rule_id] = []

            # Distribute results to rule_result objects
            for rule_result, rule_obj, n, cols_to_include in sql_rules:
                samples = results.get(rule_result.rule_id, [])
                rule_result.samples = samples
                rule_result.samples_source = samples_source

                if len(samples) == n and rule_result.failed_count > n:
                    rule_result.samples_truncated = True
                    rule_result.samples_reason = SampleReason.TRUNCATED_LIMIT

        except Exception as e:
            # Batched sampling is best-effort: record the error on every rule
            _logger.debug("Batched SQL sampling failed: %s", e)
            for rule_result, rule_obj, n, cols_to_include in sql_rules:
                rule_result.samples = None
                rule_result.samples_reason = f"error: {str(e)[:50]}"

        finally:
            # Close connection if we created it
            if owns_connection and db_conn is not None:
                try:
                    db_conn.close()
                except (OSError, AttributeError):
                    pass  # Connection cleanup is best-effort

    def sample_failures_for_rule(
        self,
        rule_id: str,
        rule_result: Any,  # RuleResult
        n: int = 5,
        upgrade_tier: bool = False,
    ) -> List[Dict[str, Any]]:
        """
        Get sample rows that failed a specific rule.

        Args:
            rule_id: The rule ID to get failures for
            rule_result: The RuleResult object
            n: Number of sample rows to return
            upgrade_tier: If True, re-execute rules resolved via metadata tier

        Returns:
            List of failing row dicts

        Raises:
            ValueError: If rule doesn't support row-level samples
            RuntimeError: If data source is unavailable
        """
        import polars as pl

        # Cap n at 100
        n = min(n, 100)

        # Find the rule object
        rule_obj = self._rule_map.get(rule_id)
        if rule_obj is None:
            raise ValueError(f"Rule object not found for: {rule_id}")

        # Get the failure predicate
        # Check sample_predicate() first (used by rules like unique that have
        # different counting vs sampling semantics), then fall back to compile_predicate()
        predicate = None
        if hasattr(rule_obj, "sample_predicate"):
            pred_obj = rule_obj.sample_predicate()
            if pred_obj is not None:
                predicate = pred_obj.expr
        if predicate is None and hasattr(rule_obj, "compile_predicate"):
            pred_obj = rule_obj.compile_predicate()
            if pred_obj is not None:
                predicate = pred_obj.expr

        if predicate is None:
            raise ValueError(
                f"Rule '{rule_obj.name}' does not support row-level samples. "
                "Dataset-level rules (min_rows, max_rows, freshness, etc.) "
                "cannot identify specific failing rows."
            )

        # Load data based on source type
        # Try SQL pushdown for database sources
        df, load_source = self._load_data_for_sampling(rule_obj, n)

        # For non-database sources (or if SQL filter wasn't available),
        # we need to filter with Polars
        if load_source != "sql":
            # Filter to failing rows, add index, limit (unique rule handled by helper)
            try:
                failing = _filter_samples_polars(df, rule_obj, predicate, n).to_dicts()
            except Exception as e:
                raise RuntimeError(f"Failed to query failing rows: {e}") from e
        else:
            # SQL pushdown already applied filter and added row index
            failing = df.head(n).to_dicts()

        return failing
