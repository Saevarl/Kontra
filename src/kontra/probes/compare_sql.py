# src/kontra/probes/compare_sql.py
"""Same-server set-based compare (Mode A) with a graceful Polars fallback.

When both sides of ``compare`` resolve to the SAME database engine, the counts
can be computed with set-based SQL (one join, per-column ``SUM(CASE ...)``) with
zero rows moved to the client. This MUST return the same counts as the Polars
reference path — and faithfully replicating Polars value-semantics in arbitrary
SQL is not always possible (type coercion, collation, NULL-key policy, dialect
join quirks). So Mode A fires only inside a provably-safe envelope and otherwise
raises ``_FallbackToPolars`` — the caller then runs the proven Polars path.

Design (PostgreSQL + SQL Server):
  - Query sources are materialized ONCE into a session temp table (so a
    non-deterministic SELECT can't make different metrics disagree, and to avoid
    relying on CTE-materialization guarantees SQL Server does not provide).
    Table sources are referenced directly (two-part on PG, three-part on MSSQL
    so a cross-database same-server compare works).
  - String comparisons and string key grouping use a binary collation on SQL
    Server so case/space/accent match Polars' exact semantics.

Safe envelope: same engine + identity (host/port/user), symmetric non-duplicate
key, aggregate-only. Runtime guards fall back on NULL keys, cross-type key or
common columns, PostgreSQL bpchar/CHAR(n), SQL Server char keys, or any SQL
error. Caller-owned connections run inside a savepoint so a fallback never
disturbs the caller's uncommitted work.

Known limitations (rare; would need extra native-type/isolation work):
  - SQL Server VARCHAR-vs-NVARCHAR for the *same* column across sides shares one
    DB-API type code, so the VARBINARY compare can over-report a change.
  - Reads run at the connection's isolation (READ COMMITTED); a table mutated by
    another session *between* the metric statements could mix states. The shadow-
    eval workflow compares stable snapshots, so this does not arise there.
"""

from __future__ import annotations

import uuid
from typing import Any, Dict, List, Optional, Tuple

from kontra.api.compare import CompareResult
from kontra.engine.sql_ir import esc_ident

# PostgreSQL text-like type OIDs (psycopg type_code).
_PG_TEXT_OIDS = {18, 19, 25, 1042, 1043, 1015, 1014}
# bpchar / CHAR(n) and its array: SQL comparison ignores trailing padding, which
# Polars does not — defer these to Polars.
_PG_BPCHAR_OIDS = {1042, 1014}


class _FallbackToPolars(Exception):
    """Mode A cannot guarantee Polars-equivalent counts; use the Polars path."""


# --------------------------------------------------------------------------- #
# Planning
# --------------------------------------------------------------------------- #

def _as_db_handle(source: Any, table: Optional[str]):
    """Resolve a compare source to a DatasetHandle if it is a database source."""
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
        if lower.startswith(("postgres://", "postgresql://", "mssql://", "sqlserver://")):
            return DatasetHandle.from_uri(uri)
        return None
    from kontra.connectors.detection import is_database_connection

    if is_database_connection(source):
        if not table:
            return None
        return DatasetHandle.from_connection(source, table)
    return None


def _dialect_of(handle) -> Optional[str]:
    scheme = handle.scheme
    if scheme in ("postgres", "postgresql"):
        return "postgres"
    if scheme in ("mssql", "sqlserver"):
        return "sqlserver"
    d = getattr(handle, "dialect", None)  # query / byoc carry flavor here
    if d in ("postgresql", "postgres"):
        return "postgres"
    if d in ("sqlserver", "mssql"):
        return "sqlserver"
    return None


def _engine_key(handle, dialect: str) -> Optional[Tuple]:
    """Hashable 'same engine' identity: same host/port AND same identity (user).
    SQL Server drops the database ONLY for table handles (three-part names
    qualify them); a query handle runs in the connection's database, so its
    database stays in the key to keep cross-database query pairs ineligible."""
    if handle.external_conn is not None:
        return ("conn", id(handle.external_conn))
    dp = handle.db_params
    if dp is not None:
        host = getattr(dp, "host", None)
        port = getattr(dp, "port", None)
        user = getattr(dp, "user", getattr(dp, "username", None))
        db = getattr(dp, "database", getattr(dp, "dbname", None))
        if dialect == "sqlserver":
            is_query = getattr(handle, "sql", None) is not None
            return ("params", host, port, user, db if is_query else None)
        return ("params", host, port, user, db)
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
        return None
    if before_key != after_key:
        return None
    # Duplicate or empty keys: the Polars reference rejects them; don't let SQL
    # silently succeed where Polars raises.
    if not before_key or len(set(before_key)) != len(before_key):
        return None

    hb = _as_db_handle(before, before_table)
    ha = _as_db_handle(after, after_table)
    if hb is None or ha is None:
        return None

    db = _dialect_of(hb)
    if db is None or db != _dialect_of(ha):
        return None
    if db not in ("postgres", "sqlserver"):
        return None

    kb = _engine_key(hb, db)
    ka = _engine_key(ha, db)
    if kb is None or kb != ka:
        return None

    return {"dialect": db, "before": hb, "after": ha, "key": list(before_key)}


# --------------------------------------------------------------------------- #
# Dialect helpers
# --------------------------------------------------------------------------- #

def _q(name: str, dialect: str) -> str:
    return esc_ident(name, dialect)


def _is_char(type_code: Any, dialect: str) -> bool:
    if dialect == "postgres":
        return type_code in _PG_TEXT_OIDS
    import pymssql

    return type_code == pymssql.STRING


def _char_cmp(expr: str, is_char: bool, dialect: str) -> str:
    """Byte-exact comparison operand. SQL Server string ``=``/``<>`` ignore
    trailing spaces and honor a (often case-insensitive) collation; casting to
    VARBINARY makes the comparison byte-exact, matching Polars. PostgreSQL text
    comparison is already byte-exact under a deterministic collation."""
    if dialect == "sqlserver" and is_char:
        return f"CAST({expr} AS VARBINARY(MAX))"
    return expr


def _table_ref(handle, dialect: str) -> str:
    """A direct table reference (two-part PG, three-part MSSQL)."""
    if handle.external_conn is not None and handle.table_ref:
        from kontra.connectors.detection import (
            POSTGRESQL, SQLSERVER, get_default_schema, parse_table_reference,
        )

        db, schema, table = parse_table_reference(handle.table_ref)
        default = SQLSERVER if dialect == "sqlserver" else POSTGRESQL
        schema = schema or get_default_schema(default)
        parts = [schema, table]
        if dialect == "sqlserver" and db:
            parts = [db, schema, table]
        return ".".join(_q(p, dialect) for p in parts)
    dp = handle.db_params
    if dp is not None:
        if dialect == "sqlserver":
            return ".".join(_q(p, dialect) for p in (dp.database, dp.schema, dp.table))
        return f"{_q(dp.schema, dialect)}.{_q(dp.table, dialect)}"
    raise _FallbackToPolars("cannot resolve table reference")


def _cursor_exec(conn, sql: str, fetch: str = "none"):
    cur = conn.cursor()
    try:
        cur.execute(sql)
        if fetch == "one":
            return cur.fetchone(), [d[0] for d in (cur.description or [])]
        if fetch == "desc":
            return [(d[0], d[1]) for d in (cur.description or [])]
        return None
    finally:
        cur.close()


def _prepare(conn, handle, dialect: str, side: str, run: str, temps: List[str]) -> str:
    """Return a referable relation for a handle; materialize query sources to a
    uniquely-named session temp table (recorded in `temps` for cleanup)."""
    sql = getattr(handle, "sql", None)
    if not sql:
        return _table_ref(handle, dialect)
    if dialect == "postgres":
        name = f"_kontra_cmp_{side}_{run}"
        _cursor_exec(conn, f"CREATE TEMP TABLE {name} AS {sql}")
        temps.append(name)
        return name
    # sqlserver
    name = f"#kontra_cmp_{side}_{run}"
    _cursor_exec(conn, f"SELECT * INTO {name} FROM ({sql}) _kontra_src")
    temps.append(name)
    return name


def _describe(conn, ref: str, dialect: str) -> List[Tuple[str, Any]]:
    if dialect == "postgres":
        sql = f"SELECT * FROM {ref} AS _kontra_d LIMIT 0"
    else:
        sql = f"SELECT TOP 0 * FROM {ref} AS _kontra_d"
    return _cursor_exec(conn, sql, fetch="desc")


def _distinct_pred(bexpr: str, aexpr: str, cmp=None) -> str:
    """NULL-safe 'values differ' predicate matching Polars ne_missing. Written
    explicitly (no IS DISTINCT FROM) so it ports across dialects. IS NULL uses the
    raw column; the ``<>`` operands go through ``cmp`` (byte-exact for strings)."""
    c = cmp or (lambda e: e)
    return (
        f"(({bexpr} IS NULL AND {aexpr} IS NOT NULL) "
        f"OR ({aexpr} IS NULL AND {bexpr} IS NOT NULL) "
        f"OR ({bexpr} IS NOT NULL AND {aexpr} IS NOT NULL AND {c(bexpr)} <> {c(aexpr)}))"
    )


def _drop_temps(conn, temps: List[str], dialect: str) -> None:
    for name in temps:
        try:
            if dialect == "postgres":
                _cursor_exec(conn, f"DROP TABLE IF EXISTS {name}")
            else:
                _cursor_exec(conn, f"IF OBJECT_ID('tempdb..{name}') IS NOT NULL DROP TABLE {name}")
        except Exception:
            pass


def _savepoint_begin(conn, dialect: str) -> Optional[str]:
    """Create a savepoint so Mode A can undo ONLY its own work on a caller-owned
    connection (never the caller's prior uncommitted work). None if unavailable."""
    name = "kontra_cmp_" + uuid.uuid4().hex[:10]
    try:
        cur = conn.cursor()
        try:
            cur.execute(
                f"SAVEPOINT {name}" if dialect == "postgres"
                else f"SAVE TRANSACTION {name}"
            )
        finally:
            cur.close()
        return name
    except Exception:
        return None


def _savepoint_rollback(conn, name: str, dialect: str) -> None:
    try:
        cur = conn.cursor()
        try:
            cur.execute(
                f"ROLLBACK TO SAVEPOINT {name}" if dialect == "postgres"
                else f"ROLLBACK TRANSACTION {name}"
            )
        finally:
            cur.close()
    except Exception:
        pass


# --------------------------------------------------------------------------- #
# Execution
# --------------------------------------------------------------------------- #

def compare_sql(plan: Dict[str, Any], sample_limit: int) -> CompareResult:
    """Compute a CompareResult with set-based SQL. Raises _FallbackToPolars when
    it cannot guarantee equivalence."""
    dialect = plan["dialect"]
    hb, ha = plan["before"], plan["after"]
    key: List[str] = plan["key"]
    conn_label = "postgres" if dialect == "postgres" else "sqlserver"

    from kontra.connectors.db_utils import get_connection_ctx

    external = hb.external_conn is not None  # caller-owned connection
    run = uuid.uuid4().hex[:10]
    with get_connection_ctx(hb, conn_label) as conn:
        # On a caller-owned connection, isolate all Mode A work behind a savepoint
        # so a fallback or error undoes only our temp tables — never the caller's
        # uncommitted work. If we can't isolate, don't touch the connection.
        savepoint = _savepoint_begin(conn, dialect) if external else None
        if external and savepoint is None:
            raise _FallbackToPolars("cannot isolate Mode A on the caller's connection")
        temps: List[str] = []
        try:
            b_ref = _prepare(conn, hb, dialect, "b", run, temps)
            a_ref = _prepare(conn, ha, dialect, "a", run, temps)

            b_cols = _describe(conn, b_ref, dialect)
            a_cols = _describe(conn, a_ref, dialect)
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

            # Key columns must have identical types on both sides — Polars joins on
            # matching dtypes; SQL would coerce (e.g. int vs '01'). Common non-key
            # columns too (Polars string-casts differing types; SQL cannot mirror).
            for k in key:
                if b_types.get(k) != a_types.get(k):
                    raise _FallbackToPolars(f"cross-type key column '{k}'")
            for c in common:
                if b_types.get(c) != a_types.get(c):
                    raise _FallbackToPolars(f"cross-type common column '{c}'")

            # PostgreSQL bpchar/CHAR(n) ignores trailing padding; defer keys+values.
            if dialect == "postgres":
                for name in list(key) + common:
                    if b_types.get(name) in _PG_BPCHAR_OIDS:
                        raise _FallbackToPolars(f"bpchar/CHAR column '{name}' on PostgreSQL")
            # SQL Server char keys: collation + trailing-space equality in
            # GROUP BY/DISTINCT/JOIN is not reliably byte-equal; defer (either side).
            if dialect == "sqlserver":
                for k in key:
                    if _is_char(b_types[k], dialect) or _is_char(a_types[k], dialect):
                        raise _FallbackToPolars(f"char key column '{k}' on SQL Server")

            columns_added = sorted(set(a_nonkey) - set(b_nonkey))
            columns_removed = sorted(set(b_nonkey) - set(a_nonkey))

            key_out = ", ".join(_q(k, dialect) for k in key)
            key_null = " OR ".join(f"{_q(k, dialect)} IS NULL" for k in key)

            stats_sql = f"""
                SELECT
                  (SELECT count(*) FROM {b_ref}) AS before_rows,
                  (SELECT count(*) FROM {a_ref}) AS after_rows,
                  (SELECT count(*) FROM {b_ref} WHERE {key_null}) AS b_null_keys,
                  (SELECT count(*) FROM {a_ref} WHERE {key_null}) AS a_null_keys,
                  (SELECT count(*) FROM (SELECT DISTINCT {key_out} FROM {b_ref}) t) AS unique_before,
                  (SELECT count(*) FROM (SELECT DISTINCT {key_out} FROM {a_ref}) t) AS unique_after,
                  (SELECT count(*) FROM (
                     SELECT DISTINCT {key_out} FROM {b_ref}
                     INTERSECT SELECT DISTINCT {key_out} FROM {a_ref}) t) AS preserved,
                  (SELECT count(*) FROM (
                     SELECT {key_out} FROM {a_ref} GROUP BY {key_out} HAVING count(*) > 1) t
                  ) AS dup_after
            """
            row, _ = _cursor_exec(conn, stats_sql, fetch="one")
            (before_rows, after_rows, b_null_keys, a_null_keys,
             unique_before, unique_after, preserved, duplicated_after) = row

            if b_null_keys or a_null_keys:
                raise _FallbackToPolars("NULL key values present")

            dropped = unique_before - preserved
            added = unique_after - preserved

            on_clause = " AND ".join(
                f"b.{_q(k, dialect)} = a.{_q(k, dialect)}" for k in key
            )

            def cmp_pred(c: str) -> str:
                is_char = _is_char(b_types[c], dialect)
                cmp = (lambda e: _char_cmp(e, is_char, dialect)) if is_char else None
                return _distinct_pred("b." + _q(c, dialect), "a." + _q(c, dialect), cmp)

            m = 0
            changed_rows = 0
            unchanged_rows = 0
            per_col_changed: Dict[str, int] = {}
            if preserved > 0 and common:
                row_pred = " OR ".join(cmp_pred(c) for c in common)
                col_sums = ", ".join(
                    f"COALESCE(SUM(CASE WHEN {cmp_pred(c)} THEN 1 ELSE 0 END), 0) AS {_q('chg__' + c, dialect)}"
                    for c in common
                )
                change_sql = f"""
                    SELECT count(*) AS m,
                      COALESCE(SUM(CASE WHEN {row_pred} THEN 1 ELSE 0 END), 0) AS changed_rows,
                      {col_sums}
                    FROM {b_ref} b JOIN {a_ref} a ON {on_clause}
                """
                crow, cnames = _cursor_exec(conn, change_sql, fetch="one")
                cmap = dict(zip(cnames, crow))
                m = int(cmap["m"])
                changed_rows = int(cmap["changed_rows"])
                unchanged_rows = m - changed_rows
                for c in common:
                    per_col_changed[c] = int(cmap["chg__" + c])
            elif preserved > 0:
                # No common non-key columns: nothing can change. The Polars
                # reference sets unchanged_rows = preserved (distinct keys), NOT
                # the row-level join count.
                unchanged_rows = preserved

            columns_modified = [c for c in common if per_col_changed.get(c, 0) > 0]
            modified_fraction = {c: per_col_changed[c] / m for c in columns_modified if m > 0}

            nullability_delta: Dict[str, Dict[str, Optional[float]]] = {}
            need_before = list(columns_modified)
            need_after = list(columns_modified) + list(columns_added)
            b_nulls = _null_counts(conn, b_ref, need_before, dialect) if need_before else {}
            a_nulls = _null_counts(conn, a_ref, need_after, dialect) if need_after else {}
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
        finally:
            if savepoint is not None:
                # Undo ONLY Mode A's work (temp tables + our tx state); the
                # caller's prior uncommitted work on this connection is preserved.
                _savepoint_rollback(conn, savepoint, dialect)
            else:
                _drop_temps(conn, temps, dialect)

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


def _null_counts(conn, ref: str, cols: List[str], dialect: str) -> Dict[str, int]:
    if not cols:
        return {}
    sums = ", ".join(
        f"SUM(CASE WHEN {_q(c, dialect)} IS NULL THEN 1 ELSE 0 END) AS {_q('n__' + c, dialect)}"
        for c in cols
    )
    row, names = _cursor_exec(conn, f"SELECT {sums} FROM {ref}", fetch="one")
    m = dict(zip(names, row))
    return {c: int(m["n__" + c] or 0) for c in cols}
