# src/kontra/scout/backends/sqlserver_backend.py
"""
SQL Server backend for Scout profiler.

Uses system metadata views for efficient profiling.
"""

from __future__ import annotations

import os
from typing import Any, Dict, List, Optional, Tuple

from kontra.connectors.handle import DatasetHandle
from kontra.connectors.sqlserver import SqlServerConnectionParams, get_connection
from kontra.scout.dtype_mapping import normalize_dtype


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
        except Exception:
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
        except Exception:
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
        except Exception:
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
        except Exception:
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


def normalize_sqlserver_type(raw_type: str) -> str:
    """
    Normalize a SQL Server type to a simplified type name.

    This is an alias for the shared normalize_dtype function.
    """
    return normalize_dtype(raw_type)
