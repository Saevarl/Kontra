# src/kontra/scout/backends/sqlserver_backend.py
"""
SQL Server backend for Scout profiler.

Uses system metadata views for efficient profiling.
"""

from __future__ import annotations

import logging
import os
from typing import Any, Dict, List, Optional, Tuple

from kontra.connectors.handle import DatasetHandle
from kontra.connectors.sqlserver import SqlServerConnectionParams, get_connection
from kontra.scout.dtype_mapping import normalize_dtype

_logger = logging.getLogger(__name__)

# Lazy-loaded pyodbc exception (pyodbc may not be installed)
_PyodbcError = None

def _get_db_error():
    """Get the pyodbc base error class, lazy-loaded."""
    global _PyodbcError
    if _PyodbcError is None:
        try:
            import pyodbc
            _PyodbcError = pyodbc.Error
        except ImportError:
            _PyodbcError = Exception
    return _PyodbcError


class SqlServerBackend:
    """
    SQL Server-based profiler backend.

    Features:
    - Uses sys.dm_db_partition_stats for row count estimates
    - SQL aggregation for profiling
    - Dialect-aware SQL (PERCENTILE_CONT instead of MEDIAN)
    """

    def __init__(
        self,
        handle: DatasetHandle,
        *,
        sample_size: Optional[int] = None,
    ):
        if not handle.db_params:
            raise ValueError("SQL Server handle missing db_params")

        self.handle = handle
        self.params: SqlServerConnectionParams = handle.db_params
        self.sample_size = sample_size
        self._conn = None
        self._schema: Optional[List[Tuple[str, str]]] = None

    def connect(self) -> None:
        """Establish connection to SQL Server."""
        self._conn = get_connection(self.params)

    def close(self) -> None:
        """Close the connection."""
        if self._conn:
            self._conn.close()
            self._conn = None

    def get_schema(self) -> List[Tuple[str, str]]:
        """Return [(column_name, raw_type), ...]"""
        if self._schema is not None:
            return self._schema

        cursor = self._conn.cursor()
        cursor.execute(
            """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
            """,
            (self.params.schema, self.params.table),
        )
        self._schema = [(row[0], row[1]) for row in cursor.fetchall()]
        return self._schema

    def get_row_count(self) -> int:
        """
        Get row count.

        For large tables, uses sys.dm_db_partition_stats estimate first (fast).
        Falls back to COUNT(*) for accuracy.
        """
        cursor = self._conn.cursor()

        # Try partition stats estimate first (instant, no scan)
        cursor.execute(
            """
            SELECT SUM(row_count) AS row_estimate
            FROM sys.dm_db_partition_stats ps
            JOIN sys.objects o ON ps.object_id = o.object_id
            JOIN sys.schemas s ON o.schema_id = s.schema_id
            WHERE s.name = %s AND o.name = %s AND ps.index_id IN (0, 1)
            """,
            (self.params.schema, self.params.table),
        )
        row = cursor.fetchone()
        estimate = int(row[0]) if row and row[0] else 0

        # If estimate is 0 or negative (stats not updated), use COUNT
        if estimate <= 0:
            cursor.execute(f"SELECT COUNT(*) FROM {self._qualified_table()}")
            row = cursor.fetchone()
            return int(row[0]) if row else 0

        # If sample_size is set, we need exact count for accuracy
        if self.sample_size:
            cursor.execute(f"SELECT COUNT(*) FROM {self._qualified_table()}")
            row = cursor.fetchone()
            return int(row[0]) if row else 0

        # Use estimate for large tables
        if os.getenv("KONTRA_VERBOSE"):
            print(f"[INFO] sys.dm_db_partition_stats estimate: {estimate} rows")
        return estimate

    def get_estimated_size_bytes(self) -> Optional[int]:
        """Estimate size from sys.dm_db_partition_stats."""
        try:
            cursor = self._conn.cursor()
            cursor.execute(
                """
                SELECT SUM(used_page_count) * 8 * 1024 AS size_bytes
                FROM sys.dm_db_partition_stats ps
                JOIN sys.objects o ON ps.object_id = o.object_id
                JOIN sys.schemas s ON o.schema_id = s.schema_id
                WHERE s.name = %s AND o.name = %s
                """,
                (self.params.schema, self.params.table),
            )
            row = cursor.fetchone()
            return int(row[0]) if row and row[0] else None
        except _get_db_error() as e:
            _logger.debug(f"Could not get table size: {e}")
            return None

    def execute_stats_query(self, exprs: List[str]) -> Dict[str, Any]:
        """Execute aggregation query."""
        if not exprs:
            return {}

        # Build query with optional sampling
        table = self._qualified_table()
        if self.sample_size:
            # SQL Server sampling: TABLESAMPLE ROWS
            sql = f"""
                SELECT {', '.join(exprs)}
                FROM {table}
                TABLESAMPLE ({self.sample_size} ROWS)
            """
        else:
            sql = f"SELECT {', '.join(exprs)} FROM {table}"

        cursor = self._conn.cursor()
        cursor.execute(sql)
        row = cursor.fetchone()
        col_names = [desc[0] for desc in cursor.description]
        return dict(zip(col_names, row)) if row else {}

    def fetch_top_values(self, column: str, limit: int) -> List[Tuple[Any, int]]:
        """Fetch top N most frequent values."""
        col = self.esc_ident(column)
        table = self._qualified_table()
        sql = f"""
            SELECT TOP {limit} {col} AS val, COUNT(*) AS cnt
            FROM {table}
            WHERE {col} IS NOT NULL
            GROUP BY {col}
            ORDER BY cnt DESC
        """
        try:
            cursor = self._conn.cursor()
            cursor.execute(sql)
            return [(r[0], int(r[1])) for r in cursor.fetchall()]
        except _get_db_error() as e:
            _logger.debug(f"Query error fetching top values for {column}: {e}")
            return []

    def fetch_distinct_values(self, column: str) -> List[Any]:
        """Fetch all distinct values."""
        col = self.esc_ident(column)
        table = self._qualified_table()
        sql = f"""
            SELECT DISTINCT {col}
            FROM {table}
            WHERE {col} IS NOT NULL
            ORDER BY {col}
        """
        try:
            cursor = self._conn.cursor()
            cursor.execute(sql)
            return [r[0] for r in cursor.fetchall()]
        except _get_db_error() as e:
            _logger.debug(f"Query error fetching distinct values for {column}: {e}")
            return []

    def fetch_sample_values(self, column: str, limit: int) -> List[Any]:
        """Fetch sample values."""
        col = self.esc_ident(column)
        table = self._qualified_table()
        sql = f"""
            SELECT TOP {limit} {col}
            FROM {table}
            WHERE {col} IS NOT NULL
        """
        try:
            cursor = self._conn.cursor()
            cursor.execute(sql)
            return [r[0] for r in cursor.fetchall() if r[0] is not None]
        except _get_db_error() as e:
            _logger.debug(f"Query error fetching sample values for {column}: {e}")
            return []

    def esc_ident(self, name: str) -> str:
        """Escape identifier for SQL Server."""
        return "[" + name.replace("]", "]]") + "]"

    @property
    def source_format(self) -> str:
        """Return source format."""
        return "sqlserver"

    # ----------------------------- Internal methods -----------------------------

    def _qualified_table(self) -> str:
        """Return schema.table identifier."""
        return f"{self.esc_ident(self.params.schema)}.{self.esc_ident(self.params.table)}"

    def _get_object_id(self) -> Optional[int]:
        """Get the object_id for the table."""
        cursor = self._conn.cursor()
        cursor.execute(
            """
            SELECT o.object_id
            FROM sys.objects o
            JOIN sys.schemas s ON o.schema_id = s.schema_id
            WHERE s.name = %s AND o.name = %s
            """,
            (self.params.schema, self.params.table),
        )
        row = cursor.fetchone()
        return int(row[0]) if row else None

    def supports_metadata_only(self) -> bool:
        """Check if this backend supports metadata-only profiling."""
        return True

    def profile_metadata_only(
        self, schema: List[Tuple[str, str]], row_count: int
    ) -> Dict[str, Dict[str, Any]]:
        """
        Profile columns using SQL Server metadata (minimal table access).

        SQL Server doesn't store null_frac like PostgreSQL. We use:
        - sys.dm_db_stats_histogram for distinct count estimates
        - sys.columns for basic column info

        Note: For null counts, we fall back to a sampled query since
        SQL Server metadata doesn't include null statistics directly.
        """
        cursor = self._conn.cursor()
        object_id = self._get_object_id()

        if not object_id:
            # Fallback: return empty metadata
            return {col_name: {"null_count": 0, "distinct_count": 0} for col_name, _ in schema}

        # Get stats for each column from sys.dm_db_stats_histogram
        # This gives us distinct count estimates
        stats_info: Dict[str, Dict[str, Any]] = {}

        for col_name, raw_type in schema:
            stats_info[col_name] = {
                "null_count": 0,
                "distinct_count": 0,
                "is_estimate": True,
            }

        # Query column statistics
        try:
            cursor.execute(
                """
                SELECT
                    c.name AS column_name,
                    s.stats_id,
                    sp.rows,
                    sp.rows_sampled,
                    sp.modification_counter
                FROM sys.stats s
                JOIN sys.stats_columns sc ON s.object_id = sc.object_id AND s.stats_id = sc.stats_id
                JOIN sys.columns c ON sc.object_id = c.object_id AND sc.column_id = c.column_id
                CROSS APPLY sys.dm_db_stats_properties(s.object_id, s.stats_id) sp
                WHERE s.object_id = %s AND sc.stats_column_id = 1
                """,
                (object_id,),
            )
            for row in cursor.fetchall():
                col_name = row[0]
                if col_name in stats_info:
                    stats_info[col_name]["rows"] = row[2]
                    stats_info[col_name]["rows_sampled"] = row[3]
        except _get_db_error() as e:
            _logger.debug(f"Could not get stats properties: {e}")

        # Get distinct counts from histogram
        try:
            cursor.execute(
                """
                SELECT
                    c.name AS column_name,
                    SUM(h.distinct_range_rows) + COUNT(*) AS distinct_estimate
                FROM sys.stats s
                JOIN sys.stats_columns sc ON s.object_id = sc.object_id AND s.stats_id = sc.stats_id
                JOIN sys.columns c ON sc.object_id = c.object_id AND sc.column_id = c.column_id
                CROSS APPLY sys.dm_db_stats_histogram(s.object_id, s.stats_id) h
                WHERE s.object_id = %s AND sc.stats_column_id = 1
                GROUP BY c.name
                """,
                (object_id,),
            )
            for row in cursor.fetchall():
                col_name = row[0]
                if col_name in stats_info:
                    stats_info[col_name]["distinct_count"] = int(row[1]) if row[1] else 0
        except _get_db_error() as e:
            # dm_db_stats_histogram might not be available (requires SQL Server 2016 SP1 CU2+)
            _logger.debug(f"Could not get stats histogram (may require SQL Server 2016+): {e}")

        # Fallback: For columns without stats, query COUNT(DISTINCT) directly
        # This handles columns without indexes/statistics
        cols_without_stats = [
            col_name for col_name, _ in schema
            if stats_info.get(col_name, {}).get("distinct_count", 0) == 0
        ]
        if cols_without_stats:
            try:
                # Batch COUNT(DISTINCT) for all columns without stats
                distinct_exprs = [
                    f"COUNT(DISTINCT {self.esc_ident(col)}) AS [{col}_distinct]"
                    for col in cols_without_stats
                ]
                table = self._qualified_table()
                sql = f"SELECT {', '.join(distinct_exprs)} FROM {table}"
                cursor.execute(sql)
                row = cursor.fetchone()
                if row:
                    for i, col_name in enumerate(cols_without_stats):
                        stats_info[col_name]["distinct_count"] = int(row[i] or 0)
                        stats_info[col_name]["is_estimate"] = False
            except _get_db_error() as e:
                _logger.debug(f"Could not get distinct counts: {e}")

        # For null counts, SQL Server doesn't store null statistics in metadata
        # For small tables, just do full count; for large tables, use sampling
        try:
            null_exprs = []
            for col_name, _ in schema:
                c = self.esc_ident(col_name)
                null_exprs.append(
                    f"SUM(CASE WHEN {c} IS NULL THEN 1 ELSE 0 END) AS [{col_name}_nulls]"
                )

            table = self._qualified_table()

            # For small tables (< 100K rows), full count is fast enough
            if row_count < 100000:
                sql = f"SELECT {', '.join(null_exprs)} FROM {table}"
                cursor.execute(sql)
                row = cursor.fetchone()
                if row:
                    for i, (col_name, _) in enumerate(schema):
                        stats_info[col_name]["null_count"] = int(row[i] or 0)
                        stats_info[col_name]["is_estimate"] = False
            else:
                # For large tables, use 1% sample and extrapolate
                sql = f"""
                    SELECT {', '.join(null_exprs)}
                    FROM {table}
                    TABLESAMPLE (1 PERCENT)
                """
                cursor.execute(sql)
                row = cursor.fetchone()
                if row:
                    for i, (col_name, _) in enumerate(schema):
                        sample_nulls = row[i] or 0
                        stats_info[col_name]["null_count"] = int(sample_nulls * 100)
                        stats_info[col_name]["is_estimate"] = True
        except _get_db_error() as e:
            _logger.debug(f"Could not get null counts: {e}")

        return stats_info

    def get_table_freshness(self) -> Dict[str, Any]:
        """
        Get table statistics freshness from sys.dm_db_stats_properties.

        Returns dict with:
        - modification_counter: rows modified since last stats update
        - rows: row count from stats
        - last_updated: timestamp of last stats update
        - stale_ratio: modification_counter / rows
        - is_fresh: True if stale_ratio < 0.2
        """
        cursor = self._conn.cursor()
        object_id = self._get_object_id()

        if not object_id:
            return {
                "modification_counter": 0,
                "rows": 0,
                "last_updated": None,
                "stale_ratio": 1.0,
                "is_fresh": False,
            }

        try:
            cursor.execute(
                """
                SELECT TOP 1
                    sp.last_updated,
                    sp.modification_counter,
                    sp.rows
                FROM sys.stats s
                CROSS APPLY sys.dm_db_stats_properties(s.object_id, s.stats_id) sp
                WHERE s.object_id = %s
                ORDER BY sp.last_updated DESC
                """,
                (object_id,),
            )
            row = cursor.fetchone()

            if not row:
                return {
                    "modification_counter": 0,
                    "rows": 0,
                    "last_updated": None,
                    "stale_ratio": 1.0,
                    "is_fresh": False,
                }

            last_updated = row[0]
            modification_counter = row[1] or 0
            rows = row[2] or 0

            stale_ratio = modification_counter / max(rows, 1) if rows > 0 else 1.0

            return {
                "modification_counter": modification_counter,
                "rows": rows,
                "last_updated": last_updated,
                "stale_ratio": stale_ratio,
                "is_fresh": stale_ratio < 0.2,
            }
        except _get_db_error() as e:
            _logger.debug(f"Could not get table freshness: {e}")
            return {
                "modification_counter": 0,
                "rows": 0,
                "last_updated": None,
                "stale_ratio": 1.0,
                "is_fresh": False,
            }

    def supports_strategic_standard(self) -> bool:
        """Check if this backend supports strategic standard profiling."""
        return True

    def execute_sampled_stats_query(
        self, exprs: List[str], sample_pct: float = 1.0
    ) -> Dict[str, Any]:
        """
        Execute aggregation query with TABLESAMPLE (block sampling).

        SQL Server's TABLESAMPLE works at the page level, so for small tables
        low percentages may return 0 rows. We fall back to full table scan
        for tables under 10K rows or if sampling returns no data.

        Args:
            exprs: List of SQL expressions to compute
            sample_pct: Percentage to sample (default 1%)

        Returns:
            Dict of expression alias -> value
        """
        if not exprs:
            return {}

        table = self._qualified_table()

        # For small tables, TABLESAMPLE may return 0 rows at low percentages
        # Use a minimum of 10% for tables under 10K rows
        row_count = self.get_row_count()
        if row_count < 10000:
            # Skip sampling for small tables - just do full scan
            return self.execute_stats_query(exprs)

        sql = f"""
            SELECT {', '.join(exprs)}
            FROM {table}
            TABLESAMPLE ({sample_pct} PERCENT)
        """

        try:
            cursor = self._conn.cursor()
            cursor.execute(sql)
            row = cursor.fetchone()
            col_names = [desc[0] for desc in cursor.description]
            result = dict(zip(col_names, row)) if row else {}

            # Check if we got data - if all values are None, fall back to full scan
            if all(v is None for v in result.values()):
                return self.execute_stats_query(exprs)

            return result
        except _get_db_error() as e:
            # Fall back to full query if TABLESAMPLE fails
            _logger.debug(f"TABLESAMPLE query failed, falling back to full query: {e}")
            return self.execute_stats_query(exprs)

    def fetch_low_cardinality_values_batched(
        self, columns: List[str]
    ) -> Dict[str, List[Tuple[Any, int]]]:
        """
        Fetch value distributions for multiple low-cardinality columns in one query.

        Uses UNION ALL to batch multiple GROUP BY queries into a single round-trip.
        """
        if not columns:
            return {}

        table = self._qualified_table()
        parts = []
        for col in columns:
            c = self.esc_ident(col)
            parts.append(f"""
                SELECT '{col}' AS col_name, CAST({c} AS NVARCHAR(MAX)) AS val, COUNT(*) AS cnt
                FROM {table}
                WHERE {c} IS NOT NULL
                GROUP BY {c}
            """)

        sql = " UNION ALL ".join(parts) + " ORDER BY col_name, cnt DESC"

        result: Dict[str, List[Tuple[Any, int]]] = {col: [] for col in columns}
        try:
            cursor = self._conn.cursor()
            cursor.execute(sql)
            for row in cursor.fetchall():
                col_name, val, cnt = row
                if col_name in result:
                    result[col_name].append((val, int(cnt)))
        except _get_db_error() as e:
            _logger.debug(f"Query error fetching low cardinality values: {e}")

        return result

    def classify_columns(
        self, schema: List[Tuple[str, str]], row_count: int
    ) -> Dict[str, Dict[str, Any]]:
        """
        Classify columns based on histogram metadata for strategic profiling.

        Classification rules:
        - low: distinct < 20 → fetch all via GROUP BY
        - medium: distinct 20-10000 → sample for top values
        - high: distinct > 10000 → trust histogram only
        """
        # First get metadata
        metadata = self.profile_metadata_only(schema, row_count)

        result = {}
        for col_name, raw_type in schema:
            col_meta = metadata.get(col_name, {})
            distinct_count = col_meta.get("distinct_count", 0)

            # If we don't have distinct count, estimate from row_count
            if distinct_count == 0:
                distinct_count = row_count  # Assume high cardinality

            # Classify cardinality
            if distinct_count < 20:
                cardinality = "low"
                strategy = "group_by"
            elif distinct_count <= 10000:
                cardinality = "medium"
                strategy = "sample"
            else:
                cardinality = "high"
                strategy = "metadata_only"

            result[col_name] = {
                "cardinality": cardinality,
                "distinct_count": distinct_count,
                "strategy": strategy,
                "dtype": normalize_dtype(raw_type),
            }

        return result


def normalize_sqlserver_type(raw_type: str) -> str:
    """
    Normalize a SQL Server type to a simplified type name.

    This is an alias for the shared normalize_dtype function.
    """
    return normalize_dtype(raw_type)
