# src/kontra/scout/backends/clickhouse_backend.py
"""
ClickHouse backend for Scout profiler.

ClickHouse is a columnar OLAP engine: aggregates are cheap and data transfer is
the expensive part, so this backend pushes *every* statistic down to the server
and only ever ships scalars back. Two schema facts are free and exact:

  - ``system.parts`` gives the live row count with no data scan.
  - ``system.columns.type`` tells us nullability; a column that is NOT
    ``Nullable(T)`` cannot hold a NULL, so its null_count is provably 0 without
    touching a single row.

The connection layer (DBAPI-shaped shim over ``clickhouse-connect``) is provided
by :mod:`kontra.connectors.clickhouse`; this backend never imports the driver
directly, keeping ``import kontra`` free of heavy dependencies.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Tuple

from kontra.connectors.handle import DatasetHandle
from kontra.engine.sql_ir import esc_ident as _esc_ident, lit_str as _lit_str
from kontra.logging import get_logger

_logger = get_logger(__name__)

# Lazy-loaded clickhouse-connect error classes (driver may not be installed at
# import time; it must never be imported when `import kontra` runs).
_ch_errors: Optional[Tuple[type, ...]] = None


def _get_db_errors() -> Tuple[type, ...]:
    """Return the clickhouse-connect base error classes, lazy-loaded."""
    global _ch_errors
    if _ch_errors is None:
        try:
            from clickhouse_connect.driver.exceptions import ClickHouseError, Error
            _ch_errors = (ClickHouseError, Error)
        except ImportError:
            _ch_errors = (Exception,)
    return _ch_errors


# --------------------------------------------------------------------------- #
# SQL dialect adaptation
#
# The profiler builds generic aggregate expressions (see
# ScoutProfiler._build_column_agg_exprs). Two of them use ANSI SQL that
# ClickHouse does not speak. We rewrite ONLY those two shapes here; everything
# else (COUNT, COUNT(DISTINCT), MIN/MAX/AVG, STDDEV, CASE, CAST) is valid
# ClickHouse as-is. We must NOT change the profiler, so the adaptation lives
# entirely in this backend.
# --------------------------------------------------------------------------- #

# PERCENTILE_CONT(<frac>) WITHIN GROUP (ORDER BY <inner>)  ->  quantileExact(<frac>)(<inner>)
# ClickHouse has no WITHIN GROUP; quantileExact is the exact quantile aggregate.
_PERCENTILE_RE = re.compile(
    r"PERCENTILE_CONT\(\s*([0-9.]+)\s*\)\s+WITHIN\s+GROUP\s*\(\s*ORDER\s+BY\s+(.+?)\s*\)",
    re.IGNORECASE,
)

# LENGTH(  ->  lengthUTF8(   (character count, not bytes — matches profiler intent
# for string length stats; length() would count bytes for multi-byte strings).
_LENGTH_RE = re.compile(r"\bLENGTH\s*\(", re.IGNORECASE)

# ... AS FLOAT)  ->  ... AS Nullable(Float64))
# The profiler emits CAST(LENGTH(col) AS FLOAT) for AVG(length). ClickHouse's
# plain Float is NON-nullable and rejects the NULL produced by lengthUTF8() over
# a nullable column ("Cannot convert NULL value to non-Nullable type"), so cast
# to Nullable(Float64) instead. FLOAT only appears inside this CAST shape.
_CAST_FLOAT_RE = re.compile(r"\bAS\s+FLOAT\s*\)", re.IGNORECASE)


def _adapt_expr(expr: str) -> str:
    """Rewrite an ANSI aggregate expression into equivalent ClickHouse SQL."""
    expr = _PERCENTILE_RE.sub(
        lambda m: f"quantileExact({m.group(1)})({m.group(2)})", expr
    )
    expr = _LENGTH_RE.sub("lengthUTF8(", expr)
    expr = _CAST_FLOAT_RE.sub("AS Nullable(Float64))", expr)
    return expr


class ClickHouseBackend:
    """
    ClickHouse-based profiler backend.

    Features:
    - Exact row count from ``system.parts`` (no scan)
    - Nullability from ``system.columns`` (non-Nullable => null_count 0, no scan)
    - Single-pass aggregate query for all column stats (maximal pushdown)
    - Metadata-fast ``scout`` path exploiting the non-Nullable guarantee
    """

    def __init__(
        self,
        handle: DatasetHandle,
        *,
        sample_size: Optional[int] = None,
    ):
        self.handle = handle
        self.sample_size = sample_size
        self._conn = None
        self._schema: Optional[List[Tuple[str, str]]] = None
        self._raw_types: Dict[str, str] = {}
        # ClickHouse row count is always exact (system.parts / count()), so this
        # stays False. Present for interface parity with PostgreSQLBackend, which
        # the profiler reads via getattr for provenance flagging.
        self.row_count_estimated: bool = False

        self._database, self._table = self._resolve_db_and_table(handle)

    # ------------------------------------------------------------------ #
    # Connection lifecycle
    # ------------------------------------------------------------------ #

    def connect(self) -> None:
        """Open a connection (context manager entered manually, closed in close())."""
        from kontra.connectors.db_utils import get_connection_ctx

        self._conn_ctx = get_connection_ctx(self.handle, "clickhouse")
        self._conn = self._conn_ctx.__enter__()

    def close(self) -> None:
        if self._conn is not None:
            self._conn_ctx.__exit__(None, None, None)
            self._conn = None
            self._conn_ctx = None

    # ------------------------------------------------------------------ #
    # Schema / row count / size
    # ------------------------------------------------------------------ #

    def get_schema(self) -> List[Tuple[str, str]]:
        """Return [(column_name, raw_type), ...] from system.columns (no scan)."""
        if self._schema is not None:
            return self._schema

        sql = (
            "SELECT name, type FROM system.columns "
            f"WHERE database = {self._lit(self._database)} "
            f"AND table = {self._lit(self._table)} "
            "ORDER BY position"
        )
        cur = self._conn.cursor()
        try:
            cur.execute(sql)
            rows = cur.fetchall()
        finally:
            cur.close()

        self._schema = [(r[0], r[1]) for r in rows]
        self._raw_types = {name: raw for name, raw in self._schema}
        return self._schema

    def get_row_count(self) -> int:
        """
        Exact row count. Uses ``system.parts`` (no scan) and falls back to
        ``count()`` when parts report nothing (e.g. non-MergeTree engines, views).
        """
        parts_sql = (
            "SELECT sum(rows) FROM system.parts "
            f"WHERE database = {self._lit(self._database)} "
            f"AND table = {self._lit(self._table)} AND active"
        )
        cur = self._conn.cursor()
        try:
            cur.execute(parts_sql)
            row = cur.fetchone()
            count = int(row[0]) if row and row[0] is not None else 0
            if count > 0:
                return count
            # Fallback: engines without system.parts stats (Log, views, ...).
            cur.execute(f"SELECT count() FROM {self._qualified_table()}")
            row = cur.fetchone()
            return int(row[0]) if row and row[0] is not None else 0
        finally:
            cur.close()

    def get_estimated_size_bytes(self) -> Optional[int]:
        """Compressed on-disk size from ``system.parts`` (no scan)."""
        sql = (
            "SELECT sum(bytes_on_disk) FROM system.parts "
            f"WHERE database = {self._lit(self._database)} "
            f"AND table = {self._lit(self._table)} AND active"
        )
        cur = self._conn.cursor()
        try:
            cur.execute(sql)
            row = cur.fetchone()
            return int(row[0]) if row and row[0] is not None else None
        except _get_db_errors() as e:
            _logger.debug(f"Could not get table size: {e}")
            return None
        finally:
            cur.close()

    # ------------------------------------------------------------------ #
    # Aggregate profiling (maximal pushdown)
    # ------------------------------------------------------------------ #

    def execute_stats_query(self, exprs: List[str]) -> Dict[str, Any]:
        """
        Run all column aggregates in a single query and return {alias: value}.

        The profiler passes generic ANSI-ish expressions; ClickHouse-incompatible
        shapes (PERCENTILE_CONT ... WITHIN GROUP, LENGTH) are rewritten via
        ``_adapt_expr``. All computation happens server-side; only scalars return.
        """
        if not exprs:
            return {}

        adapted = [_adapt_expr(e) for e in exprs]
        select = ", ".join(adapted)

        if self.sample_size:
            # ClickHouse SAMPLE requires a sampling key in the table's ORDER BY,
            # which we cannot assume exists. Use a bounded subquery instead: the
            # aggregates run server-side over the first N rows (a head sample, not
            # random). The profiler flags these results as estimates.
            sql = (
                f"SELECT {select} FROM "
                f"(SELECT * FROM {self._qualified_table()} LIMIT {int(self.sample_size)})"
            )
        else:
            sql = f"SELECT {select} FROM {self._qualified_table()}"

        cur = self._conn.cursor()
        try:
            cur.execute(sql)
            row = cur.fetchone()
            col_names = [desc[0] for desc in cur.description]
            return dict(zip(col_names, row)) if row else {}
        finally:
            cur.close()

    def fetch_top_values(self, column: str, limit: int) -> List[Tuple[Any, int]]:
        """Top N most frequent non-null values, computed server-side."""
        col = self.esc_ident(column)
        sql = (
            f"SELECT {col} AS val, count() AS cnt "
            f"FROM {self._qualified_table()} "
            f"WHERE {col} IS NOT NULL "
            f"GROUP BY {col} ORDER BY cnt DESC LIMIT {int(limit)}"
        )
        cur = self._conn.cursor()
        try:
            cur.execute(sql)
            return [(r[0], int(r[1])) for r in cur.fetchall()]
        except _get_db_errors() as e:
            _logger.debug(f"Query error fetching top values for {column}: {e}")
            return []
        finally:
            cur.close()

    def fetch_distinct_values(self, column: str) -> List[Any]:
        """All distinct non-null values (used for low-cardinality columns)."""
        col = self.esc_ident(column)
        sql = (
            f"SELECT DISTINCT {col} FROM {self._qualified_table()} "
            f"WHERE {col} IS NOT NULL ORDER BY {col}"
        )
        cur = self._conn.cursor()
        try:
            cur.execute(sql)
            return [r[0] for r in cur.fetchall()]
        except _get_db_errors() as e:
            _logger.debug(f"Query error fetching distinct values for {column}: {e}")
            return []
        finally:
            cur.close()

    def fetch_sample_values(self, column: str, limit: int) -> List[Any]:
        """A bounded sample of non-null values (used for pattern detection)."""
        col = self.esc_ident(column)
        sql = (
            f"SELECT {col} FROM {self._qualified_table()} "
            f"WHERE {col} IS NOT NULL LIMIT {int(limit)}"
        )
        cur = self._conn.cursor()
        try:
            cur.execute(sql)
            return [r[0] for r in cur.fetchall() if r[0] is not None]
        except _get_db_errors() as e:
            _logger.debug(f"Query error fetching sample values for {column}: {e}")
            return []
        finally:
            cur.close()

    def esc_ident(self, name: str) -> str:
        """Escape an identifier for ClickHouse (backtick quoting)."""
        return _esc_ident(name, "clickhouse")

    @property
    def source_format(self) -> str:
        return "clickhouse"

    # ------------------------------------------------------------------ #
    # Metadata-only fast path (scout preset)
    # ------------------------------------------------------------------ #

    def supports_metadata_only(self) -> bool:
        """ClickHouse supports a fast exact path for the scout preset."""
        return True

    def profile_metadata_only(
        self, schema: List[Tuple[str, str]], row_count: int
    ) -> Dict[str, Dict[str, Any]]:
        """
        Fast profiling for the ``scout`` preset.

        Exploits the non-Nullable schema guarantee: any column not declared
        ``Nullable(...)`` has exactly zero nulls, decided from metadata with no
        scan. For the remaining exact stats (null_count of nullable columns,
        distinct_count of every column, and an exact same-moment COUNT(*)) we
        issue a single aggregate query — cheap on ClickHouse and returning exact
        values, so nothing is flagged as an estimate.
        """
        if self._raw_types:
            raw_types = self._raw_types
        else:
            raw_types = {name: raw for name, raw in schema}

        exprs: List[str] = ["count() AS `__total_rows__`"]
        for col_name, _raw in schema:
            c = self.esc_ident(col_name)
            raw = raw_types.get(col_name, "")
            if _is_nullable_type(raw):
                exprs.append(f"countIf({c} IS NULL) AS {self.esc_ident(f'__null__{col_name}')}")
            # distinct is exact and cheap; compute for every column
            exprs.append(f"uniqExact({c}) AS {self.esc_ident(f'__distinct__{col_name}')}")

        sql = f"SELECT {', '.join(exprs)} FROM {self._qualified_table()}"
        cur = self._conn.cursor()
        try:
            cur.execute(sql)
            row = cur.fetchone()
            names = [desc[0] for desc in cur.description]
            agg = dict(zip(names, row)) if row else {}
        finally:
            cur.close()

        exact_rows = agg.get("__total_rows__")
        result: Dict[str, Dict[str, Any]] = {}
        for col_name, _raw in schema:
            raw = raw_types.get(col_name, "")
            if _is_nullable_type(raw):
                null_count = int(agg.get(f"__null__{col_name}", 0) or 0)
            else:
                # Non-Nullable column cannot contain NULL — proven from schema.
                null_count = 0
            distinct_count = int(agg.get(f"__distinct__{col_name}", 0) or 0)

            result[col_name] = {
                "null_count": null_count,
                "distinct_count": distinct_count,
                # Everything here is exact, not estimated.
                "is_estimate": False,
                "null_count_estimated": False,
                "distinct_count_estimated": False,
                # Same-moment exact row count for the profiler to adopt.
                "exact_row_count": int(exact_rows) if exact_rows is not None else None,
            }

        return result

    # ------------------------------------------------------------------ #
    # Internal helpers
    # ------------------------------------------------------------------ #

    def _qualified_table(self) -> str:
        """Return `database`.`table` (or just `table` when no database is set)."""
        t = self.esc_ident(self._table)
        return f"{self.esc_ident(self._database)}.{t}" if self._database else t

    def _lit(self, value: str) -> str:
        return _lit_str(value, "clickhouse")

    @staticmethod
    def _resolve_db_and_table(handle: DatasetHandle) -> Tuple[str, str]:
        """Resolve (database, table) for both URI and BYOC handles."""
        if handle.scheme == "byoc" and handle.external_conn is not None:
            if not handle.table_ref:
                raise ValueError("BYOC ClickHouse handle missing table_ref")
            from kontra.connectors.detection import parse_table_reference

            db, _schema, table = parse_table_reference(handle.table_ref)
            return db or "", table
        if handle.db_params is not None:
            params = handle.db_params
            return params.database, params.table
        raise ValueError("ClickHouse handle missing db_params or external_conn")


def _is_nullable_type(raw_type: str) -> bool:
    """True if a ClickHouse type string denotes a nullable column."""
    t = raw_type.strip().lower()
    # LowCardinality(Nullable(String)) is also nullable.
    if t.startswith("lowcardinality("):
        t = t[len("lowcardinality("):-1].strip()
    return t.startswith("nullable(")


# --------------------------------------------------------------------------- #
# Type normalization
# --------------------------------------------------------------------------- #

def normalize_clickhouse_type(raw: str) -> str:
    """
    Normalize a ClickHouse type string to Kontra's dtype family name.

    Unwraps ``Nullable(...)`` and ``LowCardinality(...)`` wrappers, then maps the
    inner physical type. Returns one of: "int", "float", "string", "bool",
    "date", "datetime", "binary", or "unknown".

    Examples:
        >>> normalize_clickhouse_type("Nullable(Int32)")
        'int'
        >>> normalize_clickhouse_type("LowCardinality(String)")
        'string'
        >>> normalize_clickhouse_type("Decimal(10, 2)")
        'float'
    """
    t = raw.strip()
    # Unwrap Nullable(...) / LowCardinality(...) (possibly nested) case-insensitively.
    lowered = t.lower()
    changed = True
    while changed:
        changed = False
        for wrapper in ("nullable(", "lowcardinality("):
            if lowered.startswith(wrapper) and t.endswith(")"):
                t = t[len(wrapper):-1].strip()
                lowered = t.lower()
                changed = True

    base = lowered.split("(")[0].strip()

    # Integer family (signed + unsigned, all widths)
    if re.fullmatch(r"u?int(8|16|32|64|128|256)?", base):
        return "int"
    # Float / decimal family
    if base in ("float32", "float64", "float") or base.startswith("decimal"):
        return "float"
    # String family
    if base in ("string", "fixedstring", "uuid", "ipv4", "ipv6", "json"):
        return "string"
    # Boolean
    if base in ("bool", "boolean"):
        return "bool"
    # Temporal
    if base.startswith("datetime"):
        return "datetime"
    if base.startswith("date"):
        return "date"

    return "unknown"
