# src/kontra/scout/backends/postgres_backend.py
"""
PostgreSQL backend for Scout profiler.

Uses pg_stats for efficient metadata queries and standard SQL for profiling.
"""

from __future__ import annotations

import os
from typing import Any, Dict, List, Optional, Tuple

from kontra.connectors.handle import DatasetHandle
from kontra.connectors.postgres import PostgresConnectionParams, get_connection
from kontra.scout.dtype_mapping import normalize_dtype


class PostgreSQLBackend:
    """
    PostgreSQL-based profiler backend.

    Features:
    - Uses pg_stats for row count estimates (lite preset)
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
            raise ValueError("PostgreSQL handle missing db_params")

        self.handle = handle
        self.params: PostgresConnectionParams = handle.db_params
        self.sample_size = sample_size
        self._conn = None
        self._pg_stats: Optional[Dict[str, Dict[str, Any]]] = None
        self._schema: Optional[List[Tuple[str, str]]] = None

    def connect(self) -> None:
        """Establish connection to PostgreSQL."""
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

        with self._conn.cursor() as cur:
            cur.execute(
                """
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
                """,
                (self.params.schema, self.params.table),
            )
            self._schema = [(row[0], row[1]) for row in cur.fetchall()]
            return self._schema

    def get_row_count(self) -> int:
        """
        Get row count.

        For large tables, uses pg_class estimate first (fast).
        Falls back to COUNT(*) for accuracy.
        """
        # Try pg_class estimate first (instant, no scan)
        with self._conn.cursor() as cur:
            cur.execute(
                """
                SELECT reltuples::bigint
                FROM pg_class
                WHERE relname = %s
                  AND relnamespace = %s::regnamespace
                """,
                (self.params.table, self.params.schema),
            )
            row = cur.fetchone()
            estimate = row[0] if row else 0

            # If estimate is 0 or negative (stats not updated), use COUNT
            if estimate <= 0:
                cur.execute(f"SELECT COUNT(*) FROM {self._qualified_table()}")
                row = cur.fetchone()
                return int(row[0]) if row else 0

            # If sample_size is set, we need exact count for accuracy
            if self.sample_size:
                cur.execute(f"SELECT COUNT(*) FROM {self._qualified_table()}")
                row = cur.fetchone()
                return int(row[0]) if row else 0

            # Use estimate for large tables
            if os.getenv("KONTRA_VERBOSE"):
                print(f"[INFO] pg_class estimate: {estimate} rows")
            return int(estimate)

    def get_estimated_size_bytes(self) -> Optional[int]:
        """Estimate size from pg_class."""
        try:
            with self._conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT pg_total_relation_size(%s::regclass)
                    """,
                    (f"{self.params.schema}.{self.params.table}",),
                )
                row = cur.fetchone()
                return int(row[0]) if row else None
        except Exception:
            return None

    def execute_stats_query(self, exprs: List[str]) -> Dict[str, Any]:
        """Execute aggregation query."""
        if not exprs:
            return {}

        # Build query with optional sampling
        table = self._qualified_table()
        if self.sample_size:
            # PostgreSQL sampling: TABLESAMPLE or random() limit
            sql = f"""
                SELECT {', '.join(exprs)}
                FROM {table}
                TABLESAMPLE BERNOULLI (
                    LEAST(100, {self.sample_size} * 100.0 / NULLIF(
                        (SELECT reltuples FROM pg_class WHERE relname = '{self.params.table}'
                         AND relnamespace = '{self.params.schema}'::regnamespace), 0
                    ))
                )
            """
        else:
            sql = f"SELECT {', '.join(exprs)} FROM {table}"

        with self._conn.cursor() as cur:
            cur.execute(sql)
            row = cur.fetchone()
            col_names = [desc[0] for desc in cur.description]
            return dict(zip(col_names, row)) if row else {}

    def fetch_top_values(self, column: str, limit: int) -> List[Tuple[Any, int]]:
        """Fetch top N most frequent values."""
        col = self.esc_ident(column)
        table = self._qualified_table()
        sql = f"""
            SELECT {col} AS val, COUNT(*) AS cnt
            FROM {table}
            WHERE {col} IS NOT NULL
            GROUP BY {col}
            ORDER BY cnt DESC
            LIMIT {limit}
        """
        try:
            with self._conn.cursor() as cur:
                cur.execute(sql)
                return [(r[0], int(r[1])) for r in cur.fetchall()]
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
            with self._conn.cursor() as cur:
                cur.execute(sql)
                return [r[0] for r in cur.fetchall()]
        except Exception:
            return []

    def fetch_sample_values(self, column: str, limit: int) -> List[Any]:
        """Fetch sample values."""
        col = self.esc_ident(column)
        table = self._qualified_table()
        sql = f"""
            SELECT {col}
            FROM {table}
            WHERE {col} IS NOT NULL
            LIMIT {limit}
        """
        try:
            with self._conn.cursor() as cur:
                cur.execute(sql)
                return [r[0] for r in cur.fetchall() if r[0] is not None]
        except Exception:
            return []

    def esc_ident(self, name: str) -> str:
        """Escape identifier for PostgreSQL."""
        return '"' + name.replace('"', '""') + '"'

    @property
    def source_format(self) -> str:
        """Return source format."""
        return "postgres"

    # ----------------------------- Internal methods -----------------------------

    def _qualified_table(self) -> str:
        """Return schema.table identifier."""
        return f"{self.esc_ident(self.params.schema)}.{self.esc_ident(self.params.table)}"

    def _get_pg_stats(self) -> Dict[str, Dict[str, Any]]:
        """Fetch and cache pg_stats."""
        if self._pg_stats is not None:
            return self._pg_stats

        with self._conn.cursor() as cur:
            cur.execute(
                """
                SELECT attname, null_frac, n_distinct,
                       most_common_vals::text, most_common_freqs::text
                FROM pg_stats
                WHERE schemaname = %s AND tablename = %s
                """,
                (self.params.schema, self.params.table),
            )
            self._pg_stats = {}
            for row in cur.fetchall():
                self._pg_stats[row[0]] = {
                    "null_frac": row[1],
                    "n_distinct": row[2],
                    "most_common_vals": row[3],
                    "most_common_freqs": row[4],
                }
            return self._pg_stats


def normalize_pg_type(raw_type: str) -> str:
    """
    Normalize a PostgreSQL type to a simplified type name.

    This is an alias for the shared normalize_dtype function.
    """
    return normalize_dtype(raw_type)
