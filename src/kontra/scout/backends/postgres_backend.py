# src/kontra/scout/backends/postgres_backend.py
"""
PostgreSQL backend for Scout profiler.

Uses pg_stats for efficient metadata queries and standard SQL for profiling.
"""

from __future__ import annotations

import logging
import os
from typing import Any, Dict, List, Optional, Tuple

from kontra.connectors.handle import DatasetHandle
from kontra.connectors.postgres import PostgresConnectionParams, get_connection
from kontra.scout.dtype_mapping import normalize_dtype

_logger = logging.getLogger(__name__)

# Lazy-loaded psycopg exception (psycopg may not be installed)
_PsycopgError = None

def _get_db_error():
    """Get the psycopg base error class, lazy-loaded."""
    global _PsycopgError
    if _PsycopgError is None:
        try:
            import psycopg
            _PsycopgError = psycopg.Error
        except ImportError:
            _PsycopgError = Exception
    return _PsycopgError


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
            with self._conn.cursor() as cur:
                cur.execute(sql)
                return [r[0] for r in cur.fetchall()]
        except _get_db_error() as e:
            _logger.debug(f"Query error fetching distinct values for {column}: {e}")
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
        except _get_db_error() as e:
            _logger.debug(f"Query error fetching sample values for {column}: {e}")
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

    def supports_metadata_only(self) -> bool:
        """Check if this backend supports metadata-only profiling."""
        return True

    def profile_metadata_only(self, schema: List[Tuple[str, str]], row_count: int) -> Dict[str, Dict[str, Any]]:
        """
        Profile columns using only pg_stats metadata (no table scan).

        Returns dict mapping column_name -> {null_count, distinct_count, ...}

        This is used for the 'lite' preset to achieve near-instant profiling.
        Note: Values are estimates based on PostgreSQL statistics, not exact counts.
        """
        pg_stats = self._get_pg_stats()
        result = {}

        for col_name, raw_type in schema:
            col_stats = pg_stats.get(col_name, {})

            # null_frac is fraction of nulls (0.0 to 1.0)
            null_frac = col_stats.get("null_frac", 0.0) or 0.0
            null_count = int(row_count * null_frac)

            # n_distinct interpretation:
            # - Positive: exact count of distinct values
            # - Negative: fraction of rows that are distinct (multiply by row_count)
            # - 0 or missing: unknown
            n_distinct = col_stats.get("n_distinct", 0) or 0
            if n_distinct > 0:
                distinct_count = int(n_distinct)
            elif n_distinct < 0:
                # Negative means fraction: -0.5 means 50% of rows are distinct
                distinct_count = int(abs(n_distinct) * row_count)
            else:
                # Unknown - estimate from null_frac (non-null rows)
                distinct_count = int(row_count * (1 - null_frac))

            # Parse most_common_vals if available (for low cardinality detection)
            mcv_raw = col_stats.get("most_common_vals")
            most_common_vals = None
            if mcv_raw:
                # pg_stats returns array as text: {val1,val2,...}
                try:
                    # Remove braces and split
                    if mcv_raw.startswith("{") and mcv_raw.endswith("}"):
                        vals = mcv_raw[1:-1].split(",")
                        most_common_vals = [v.strip().strip('"') for v in vals if v.strip()]
                except (ValueError, AttributeError):
                    pass  # Malformed pg_stats value

            result[col_name] = {
                "null_count": null_count,
                "distinct_count": distinct_count,
                "null_frac": null_frac,
                "n_distinct_raw": n_distinct,
                "most_common_vals": most_common_vals,
                "is_estimate": True,  # Flag that these are estimates
            }

        return result


    def get_table_freshness(self) -> Dict[str, Any]:
        """
        Get table statistics freshness from pg_stat_user_tables.

        Returns dict with:
        - n_live_tup: estimated live rows
        - n_mod_since_analyze: rows modified since last ANALYZE
        - last_analyze: timestamp of last manual ANALYZE
        - last_autoanalyze: timestamp of last auto ANALYZE
        - stale_ratio: n_mod_since_analyze / n_live_tup (0.0 = fresh, 1.0 = very stale)
        - is_fresh: True if stale_ratio < 0.2
        """
        with self._conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    n_live_tup,
                    n_mod_since_analyze,
                    last_analyze,
                    last_autoanalyze
                FROM pg_stat_user_tables
                WHERE schemaname = %s AND relname = %s
                """,
                (self.params.schema, self.params.table),
            )
            row = cur.fetchone()

            if not row:
                return {
                    "n_live_tup": 0,
                    "n_mod_since_analyze": 0,
                    "last_analyze": None,
                    "last_autoanalyze": None,
                    "stale_ratio": 1.0,
                    "is_fresh": False,
                }

            n_live_tup = row[0] or 0
            n_mod_since_analyze = row[1] or 0
            last_analyze = row[2]
            last_autoanalyze = row[3]

            # Calculate staleness ratio
            stale_ratio = (
                n_mod_since_analyze / max(n_live_tup, 1)
                if n_live_tup > 0
                else 1.0
            )

            return {
                "n_live_tup": n_live_tup,
                "n_mod_since_analyze": n_mod_since_analyze,
                "last_analyze": last_analyze,
                "last_autoanalyze": last_autoanalyze,
                "stale_ratio": stale_ratio,
                "is_fresh": stale_ratio < 0.2,
            }

    def supports_strategic_standard(self) -> bool:
        """Check if this backend supports strategic standard profiling."""
        return True

    def execute_sampled_stats_query(
        self, exprs: List[str], sample_pct: float = 1.0
    ) -> Dict[str, Any]:
        """
        Execute aggregation query with TABLESAMPLE SYSTEM (block sampling).

        Unlike BERNOULLI which scans the entire table, SYSTEM samples
        at the block level - much faster for large tables.

        Args:
            exprs: List of SQL expressions to compute
            sample_pct: Percentage of blocks to sample (default 1%)

        Returns:
            Dict of expression alias -> value
        """
        if not exprs:
            return {}

        table = self._qualified_table()
        # SYSTEM samples blocks, not rows - much faster than BERNOULLI
        sql = f"""
            SELECT {', '.join(exprs)}
            FROM {table}
            TABLESAMPLE SYSTEM ({sample_pct})
        """

        try:
            with self._conn.cursor() as cur:
                cur.execute(sql)
                row = cur.fetchone()
                col_names = [desc[0] for desc in cur.description]
                result = dict(zip(col_names, row)) if row else {}

                # If TABLESAMPLE returned empty (all NULLs), fall back to full query
                # This happens for small tables where 1% sampling returns 0 rows
                if result and all(v is None for v in result.values()):
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

        Args:
            columns: List of column names to profile

        Returns:
            Dict mapping column_name -> [(value, count), ...]
        """
        if not columns:
            return {}

        table = self._qualified_table()
        parts = []
        for col in columns:
            c = self.esc_ident(col)
            # Cast to text for uniformity, include column name for identification
            parts.append(f"""
                SELECT '{col}' AS col_name, {c}::text AS val, COUNT(*) AS cnt
                FROM {table}
                WHERE {c} IS NOT NULL
                GROUP BY {c}
            """)

        sql = " UNION ALL ".join(parts) + " ORDER BY col_name, cnt DESC"

        result: Dict[str, List[Tuple[Any, int]]] = {col: [] for col in columns}
        try:
            with self._conn.cursor() as cur:
                cur.execute(sql)
                for row in cur.fetchall():
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
        Classify columns based on pg_stats metadata for strategic profiling.

        Returns dict mapping column_name -> {
            "cardinality": "low" | "medium" | "high",
            "n_distinct": raw n_distinct value,
            "estimated_distinct": estimated distinct count,
            "strategy": "group_by" | "sample" | "metadata_only"
        }

        Classification rules:
        - low: n_distinct < 20 → fetch all via GROUP BY
        - medium: n_distinct 20-10000 → sample for top values
        - high: n_distinct > 10000 → trust metadata MCVs only
        """
        pg_stats = self._get_pg_stats()
        result = {}

        for col_name, raw_type in schema:
            col_stats = pg_stats.get(col_name, {})
            n_distinct = col_stats.get("n_distinct", 0) or 0

            # Calculate estimated distinct count
            if n_distinct > 0:
                estimated_distinct = int(n_distinct)
            elif n_distinct < 0:
                estimated_distinct = int(abs(n_distinct) * row_count)
            else:
                estimated_distinct = row_count  # Unknown, assume high

            # Classify cardinality
            if estimated_distinct < 20:
                cardinality = "low"
                strategy = "group_by"
            elif estimated_distinct <= 10000:
                cardinality = "medium"
                strategy = "sample"
            else:
                cardinality = "high"
                strategy = "metadata_only"

            result[col_name] = {
                "cardinality": cardinality,
                "n_distinct": n_distinct,
                "estimated_distinct": estimated_distinct,
                "strategy": strategy,
                "dtype": normalize_dtype(raw_type),
            }

        return result


def normalize_pg_type(raw_type: str) -> str:
    """
    Normalize a PostgreSQL type to a simplified type name.

    This is an alias for the shared normalize_dtype function.
    """
    return normalize_dtype(raw_type)
