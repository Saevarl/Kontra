# src/kontra/scout/backends/base.py
"""
ProfilerBackend protocol - abstract interface for Scout data source adapters.

Each backend implements SQL-based profiling for a specific data source type.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Protocol, Tuple


class ProfilerBackend(Protocol):
    """
    Protocol for Scout profiler backends.

    A backend provides methods to:
    - Connect to the data source
    - Get schema information
    - Execute aggregation queries
    - Fetch values for low-cardinality columns

    Implementations:
    - DuckDBBackend: Parquet, CSV (local + S3)
    - PostgreSQLBackend: PostgreSQL tables
    """

    def connect(self) -> None:
        """Establish connection to the data source."""
        ...

    def close(self) -> None:
        """Close the connection and clean up resources."""
        ...

    def get_schema(self) -> List[Tuple[str, str]]:
        """
        Return schema as [(column_name, raw_type), ...].

        The raw_type is the native type string from the data source.
        """
        ...

    def get_row_count(self) -> int:
        """Return total row count (may use metadata optimization)."""
        ...

    def get_estimated_size_bytes(self) -> Optional[int]:
        """Return estimated size in bytes (if available)."""
        ...

    def execute_stats_query(self, exprs: List[str]) -> Dict[str, Any]:
        """
        Execute a single aggregation query with multiple expressions.

        Args:
            exprs: List of SQL expressions like "COUNT(*) AS total"

        Returns:
            Dict mapping column aliases to values.
        """
        ...

    def fetch_top_values(
        self, column: str, limit: int
    ) -> List[Tuple[Any, int]]:
        """
        Fetch top N most frequent values for a column.

        Args:
            column: Column name
            limit: Maximum number of values to return

        Returns:
            List of (value, count) tuples ordered by count descending.
        """
        ...

    def fetch_distinct_values(self, column: str) -> List[Any]:
        """
        Fetch all distinct values for a low-cardinality column.

        Args:
            column: Column name

        Returns:
            List of distinct values, ordered.
        """
        ...

    def fetch_sample_values(self, column: str, limit: int) -> List[Any]:
        """
        Fetch a sample of values for pattern detection.

        Args:
            column: Column name
            limit: Maximum number of values to return

        Returns:
            List of sample values.
        """
        ...

    def esc_ident(self, name: str) -> str:
        """Escape an identifier (column/table name) for this backend's SQL dialect."""
        ...

    @property
    def source_format(self) -> str:
        """Return the source format identifier (e.g., 'parquet', 'csv', 'postgres')."""
        ...
