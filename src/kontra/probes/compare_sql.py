# src/kontra/probes/compare_sql.py
"""Same-server set-based compare (Mode A) with a graceful Polars fallback.

When both sides of ``compare`` resolve to the SAME database engine, the counts
can be computed with set-based SQL (one join, per-column ``SUM(CASE ...)``) with
zero rows moved to the client. This must return the SAME counts as the Polars
reference path — and faithfully replicating Polars value-semantics in arbitrary
SQL is not always possible (type coercion, collation, NULL-key policy, dialect
join quirks). So Mode A fires only inside a provably-safe envelope and otherwise
raises ``_FallbackToPolars`` — the caller then runs the proven Polars path.

Safe envelope (v1, PostgreSQL):
  - both sides same PostgreSQL engine (same connection, or same host/port/db)
  - symmetric key (same column names on both sides)
  - aggregate-only (sample_limit == 0)
  - no NULL key values (checked at runtime)
  - common non-key columns have identical types on both sides (checked at runtime)
Anything else -> fallback. Any SQL error -> fallback.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from kontra.api.compare import CompareResult
from kontra.engine.sql_ir import esc_ident


class _FallbackToPolars(Exception):
    """Mode A cannot guarantee Polars-equivalent counts; use the Polars path."""


# --------------------------------------------------------------------------- #
# Planning: decide whether both sides are one engine we can push down to.
# --------------------------------------------------------------------------- #

def _as_db_handle(source: Any, table: Optional[str]):
    """Resolve a compare source to a DatasetHandle if it is a database source.

    Returns None for DataFrames / files / anything not backed by a DB engine,
    which makes the pair ineligible for set-based compare.
    """
    import polars as pl

    from kontra.connectors.handle import DatasetHandle
    from kontra.connectors.query import Query

    if isinstance(source, pl.DataFrame):
        return None
    if isinstance(source, Query):
        return DatasetHandle.from_query(source)
    if isinstance(source, str):
        from kontra.probes.utils import _resolve_named_datasource

        uri = _resolve_named_datasource(source)
        lower = uri.lower()
        if lower.startswith(("postgres://", "postgresql://", "mssql://", "clickhouse://", "clickhouses://")):
            return DatasetHandle.from_uri(uri)
        return None  # file / cloud path
    # In-memory (pandas/list/dict) is not a DB source; a live connection is.
    from kontra.connectors.detection import is_database_connection

    if is_database_connection(source):
        if not table:
            return None
        return DatasetHandle.from_connection(source, table)
    return None


_DIALECT = {
    "postgres": "postgres",
    "postgresql": "postgres",
    "query": None,  # resolved via handle.dialect
}


def _dialect_of(handle) -> Optional[str]:
    scheme = handle.scheme
    if scheme in ("postgres", "postgresql"):
        return "postgres"
    # query / byoc handles carry the flavor in `dialect`
    d = getattr(handle, "dialect", None)
    if d in ("postgresql", "postgres"):
        return "postgres"
    return None  # sqlserver / clickhouse: not implemented in v1 -> no Mode A


def _engine_key(handle) -> Optional[Tuple]:
    """A hashable identity for 'same engine'. None if it cannot be determined."""
    if handle.external_conn is not None:
        return ("conn", id(handle.external_conn))
    dp = handle.db_params
    if dp is not None:
        return (
            "params",
            getattr(dp, "host", None),
            getattr(dp, "port", None),
            getattr(dp, "database", getattr(dp, "dbname", None)),
        )
    return None


def plan_compare(
    before: Any,
    after: Any,
    before_key: List[str],
    after_key: List[str],
    before_table: Optional[str],
    after_table: Optional[str],
    sample_limit: int,
) -> Optional[Dict[str, Any]]:
    """Return a Mode-A plan if the pair is eligible, else None (use Polars)."""
    if sample_limit and sample_limit > 0:
        return None  # v1: samples come from the Polars path
    if before_key != after_key:
        return None  # v1: symmetric keys only

    hb = _as_db_handle(before, before_table)
    ha = _as_db_handle(after, after_table)
    if hb is None or ha is None:
        return None

    db = _dialect_of(hb)
    if db is None or db != _dialect_of(ha):
        return None

    kb, ka = _engine_key(hb), _engine_key(ha)
    if kb is None or kb != ka:
        return None

    return {"dialect": db, "before": hb, "after": ha, "key": list(before_key)}


# --------------------------------------------------------------------------- #
# PostgreSQL set-based execution
# --------------------------------------------------------------------------- #

def _relation(handle, dialect: str) -> str:
    """A FROM-able relation for a handle: a parenthesized query or a table."""
    sql = getattr(handle, "sql", None)
    if sql:
        return f"({sql})"
    # table source
    if handle.external_conn is not None and handle.table_ref:
        from kontra.connectors.detection import parse_table_reference, get_default_schema, POSTGRESQL

        _db, schema, table = parse_table_reference(handle.table_ref)
        schema = schema or get_default_schema(POSTGRESQL)
        return f"{esc_ident(schema, dialect)}.{esc_ident(table, dialect)}"
    dp = handle.db_params
    if dp is not None:
        return f"{esc_ident(dp.schema, dialect)}.{esc_ident(dp.table, dialect)}"
    raise _FallbackToPolars("cannot resolve relation for handle")


def _distinct_pred(bc: str, ac: str) -> str:
    """NULL-safe 'values differ' predicate matching Polars ne_missing.

    TRUE when exactly one is NULL, or both are non-NULL and unequal. Written
    explicitly (no IS DISTINCT FROM) so it ports across dialects.
    """
    return (
        f"(({bc} IS NULL AND {ac} IS NOT NULL) "
        f"OR ({ac} IS NULL AND {bc} IS NOT NULL) "
        f"OR ({bc} IS NOT NULL AND {ac} IS NOT NULL AND {bc} <> {ac}))"
    )


def _describe(cur, relation: str) -> List[Tuple[str, Any]]:
    """(name, type_oid) for each column of a relation, without loading data."""
    cur.execute(f"SELECT * FROM {relation} AS _kontra_desc LIMIT 0")
    return [(d[0], d[1]) for d in cur.description] if cur.description else []


def compare_sql(plan: Dict[str, Any], sample_limit: int) -> CompareResult:
    """Compute a CompareResult with set-based SQL. Raises _FallbackToPolars when
    it cannot guarantee equivalence."""
    dialect = plan["dialect"]
    if dialect != "postgres":
        raise _FallbackToPolars(f"dialect {dialect} not implemented")

    hb, ha = plan["before"], plan["after"]
    key: List[str] = plan["key"]

    from kontra.connectors.db_utils import get_connection_ctx

    q = lambda name: esc_ident(name, dialect)  # noqa: E731

    with get_connection_ctx(hb, "postgres") as conn:
        b_rel = _relation(hb, dialect)
        a_rel = _relation(ha, dialect)

        with conn.cursor() as cur:
            b_cols = _describe(cur, b_rel)
            a_cols = _describe(cur, a_rel)
        b_names = [n for n, _ in b_cols]
        a_names = [n for n, _ in a_cols]
        b_types = dict(b_cols)
        a_types = dict(a_cols)

        for k in key:
            if k not in b_names:
                raise ValueError(f"Key column '{k}' not found in before dataset")
            if k not in a_names:
                raise ValueError(f"Key column '{k}' not found in after dataset")

        keyset = set(key)
        b_nonkey = [c for c in b_names if c not in keyset]
        a_nonkey = [c for c in a_names if c not in keyset]
        common = sorted(set(b_nonkey) & set(a_nonkey))
        # Cross-type common columns: Polars would string-cast and may see changes
        # SQL cannot faithfully mirror; defer to Polars.
        for c in common:
            if b_types.get(c) != a_types.get(c):
                raise _FallbackToPolars(f"cross-type common column '{c}'")

        columns_added = sorted(set(a_nonkey) - set(b_nonkey))
        columns_removed = sorted(set(b_nonkey) - set(a_nonkey))

        key_sql = ", ".join(q(k) for k in key)
        key_null = " OR ".join(f"{q(k)} IS NULL" for k in key)
        cte = f"WITH b AS MATERIALIZED (SELECT * FROM {b_rel} AS _b), " \
              f"a AS MATERIALIZED (SELECT * FROM {a_rel} AS _a) "

        with conn.cursor() as cur:
            cur.execute(
                cte + f"""
                SELECT
                  (SELECT count(*) FROM b) AS before_rows,
                  (SELECT count(*) FROM a) AS after_rows,
                  (SELECT count(*) FROM b WHERE {key_null}) AS b_null_keys,
                  (SELECT count(*) FROM a WHERE {key_null}) AS a_null_keys,
                  (SELECT count(*) FROM (SELECT DISTINCT {key_sql} FROM b) t) AS unique_before,
                  (SELECT count(*) FROM (SELECT DISTINCT {key_sql} FROM a) t) AS unique_after,
                  (SELECT count(*) FROM (
                     SELECT DISTINCT {key_sql} FROM b
                     INTERSECT SELECT DISTINCT {key_sql} FROM a) t) AS preserved,
                  (SELECT count(*) FROM (
                     SELECT {key_sql} FROM a GROUP BY {key_sql} HAVING count(*) > 1) t
                  ) AS duplicated_after
                """
            )
            row = cur.fetchone()
        (before_rows, after_rows, b_null_keys, a_null_keys,
         unique_before, unique_after, preserved, duplicated_after) = row

        # NULL keys: distinct/intersect vs join disagree; defer to Polars.
        if b_null_keys or a_null_keys:
            raise _FallbackToPolars("NULL key values present")

        dropped = unique_before - preserved
        added = unique_after - preserved

        # Change stats over the row-level join (duplicate keys cross-multiply).
        m = 0
        changed_rows = 0
        per_col_changed: Dict[str, int] = {}
        if preserved > 0 and common:
            on_clause = " AND ".join(f"b.{q(k)} = a.{q(k)}" for k in key)
            row_pred = " OR ".join(_distinct_pred(f"b.{q(c)}", f"a.{q(c)}") for c in common)
            col_sums = ", ".join(
                f"COALESCE(SUM(CASE WHEN {_distinct_pred(f'b.{q(c)}', f'a.{q(c)}')} "
                f"THEN 1 ELSE 0 END), 0) AS {q('chg__' + c)}"
                for c in common
            )
            with conn.cursor() as cur:
                cur.execute(
                    cte + f"""
                    SELECT
                      count(*) AS m,
                      COALESCE(SUM(CASE WHEN {row_pred} THEN 1 ELSE 0 END), 0) AS changed_rows,
                      {col_sums}
                    FROM b JOIN a ON {on_clause}
                    """
                )
                crow = cur.fetchone()
                cnames = [d[0] for d in cur.description]
            result_map = dict(zip(cnames, crow))
            m = int(result_map["m"])
            changed_rows = int(result_map["changed_rows"])
            for c in common:
                per_col_changed[c] = int(result_map["chg__" + c])
        elif preserved > 0:
            # preserved rows but no common non-key columns -> nothing can change
            on_clause = " AND ".join(f"b.{q(k)} = a.{q(k)}" for k in key)
            with conn.cursor() as cur:
                cur.execute(cte + f"SELECT count(*) FROM b JOIN a ON {on_clause}")
                m = int(cur.fetchone()[0])

        unchanged_rows = m - changed_rows

        columns_modified = [c for c in common if per_col_changed.get(c, 0) > 0]
        modified_fraction = {
            c: per_col_changed[c] / m for c in columns_modified if m > 0
        }

        # Nullability delta (over full tables) for modified + added columns.
        nullability_delta: Dict[str, Dict[str, Optional[float]]] = {}
        need_before = list(columns_modified)
        need_after = list(columns_modified) + list(columns_added)
        b_nulls = _null_counts(conn, cte, "b", need_before, q) if need_before else {}
        a_nulls = _null_counts(conn, cte, "a", need_after, q) if need_after else {}
        for c in columns_modified:
            nullability_delta[c] = {
                "before": (b_nulls.get(c, 0) / before_rows) if before_rows else 0.0,
                "after": (a_nulls.get(c, 0) / after_rows) if after_rows else 0.0,
            }
        for c in columns_added:
            nullability_delta[c] = {
                "before": None,
                "after": (a_nulls.get(c, 0) / after_rows) if after_rows else 0.0,
            }

    row_delta = after_rows - before_rows
    row_ratio = after_rows / before_rows if before_rows > 0 else float("inf")

    return CompareResult(
        before_rows=before_rows,
        after_rows=after_rows,
        key=key,
        execution_tier="sql",
        row_delta=row_delta,
        row_ratio=row_ratio,
        unique_before=unique_before,
        unique_after=unique_after,
        preserved=preserved,
        dropped=dropped,
        added=added,
        duplicated_after=duplicated_after,
        unchanged_rows=unchanged_rows,
        changed_rows=changed_rows,
        columns_added=columns_added,
        columns_removed=columns_removed,
        columns_modified=columns_modified,
        modified_fraction=modified_fraction,
        nullability_delta=nullability_delta,
        samples_duplicated_keys=[],
        samples_dropped_keys=[],
        samples_changed_rows=[],
        sample_limit=sample_limit,
    )


def _null_counts(conn, cte: str, alias: str, cols: List[str], q) -> Dict[str, int]:
    """NULL count per column over CTE `alias` (b or a)."""
    if not cols:
        return {}
    sums = ", ".join(
        f"SUM(CASE WHEN {q(c)} IS NULL THEN 1 ELSE 0 END) AS {q('n__' + c)}" for c in cols
    )
    with conn.cursor() as cur:
        cur.execute(cte + f"SELECT {sums} FROM {alias}")
        row = cur.fetchone()
        names = [d[0] for d in cur.description]
    m = dict(zip(names, row))
    return {c: int(m["n__" + c] or 0) for c in cols}
