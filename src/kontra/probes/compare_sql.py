# src/kontra/probes/compare_sql.py
"""Same-server set-based compare (Mode A) with a graceful Polars fallback.

When both sides of ``compare`` resolve to the SAME database engine, the counts
can be computed with set-based SQL (one join, per-column ``SUM(CASE ...)``) with
zero rows moved to the client. This MUST return the same counts as the Polars
reference path — and faithfully replicating Polars value-semantics in arbitrary
SQL is not always possible (type coercion, collation, NULL-key policy, dialect
join quirks). So Mode A fires only inside a provably-safe envelope and otherwise
raises ``_FallbackToPolars`` — the caller then runs the proven Polars path.

Per-dialect behavior lives in a registered ``_Backend`` (quoting, table-ref arity,
temp-table DDL, describe, type classification, value operand, savepoint syntax,
engine identity, and type guards). Orchestration (``plan_compare`` / ``compare_sql``)
is dialect-agnostic; a new dialect is a new backend class, not new branches.

Design:
  - Query sources are materialized ONCE into a uniquely-named session temp table
    (single evaluation; avoids CTE-materialization guarantees SQL Server lacks).
    Table sources are referenced directly (two-part on PG, three-part on MSSQL so
    a cross-database same-server compare works).
  - Only PRIMITIVE facts are measured here; result derivation is shared with the
    Polars path via ``finalize_compare_result``.

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

from kontra.engine.sql_ir import esc_ident
from kontra.probes.compare_facts import (
    CompareFacts,
    CompareResult,
    CompareSchema,
    finalize_compare_result,
)

# PostgreSQL text-like type OIDs (psycopg type_code).
_PG_TEXT_OIDS = {18, 19, 25, 1042, 1043, 1015, 1014}
# bpchar / CHAR(n) and its array: SQL comparison ignores trailing padding, which
# Polars does not — defer these to Polars (keys and values).
_PG_BPCHAR_OIDS = {1042, 1014}


class _FallbackToPolars(Exception):
    """Mode A cannot guarantee Polars-equivalent counts; use the Polars path."""


# --------------------------------------------------------------------------- #
# Dialect backends
# --------------------------------------------------------------------------- #

class _Backend:
    """Per-dialect behavior for set-based compare. Subclass + register per engine."""

    dialect: str = ""
    conn_label: str = ""
    schemes: frozenset = frozenset()
    aliases: frozenset = frozenset()  # values seen in handle.dialect
    temp_prefix: str = "_kontra_cmp"

    # -- identity ----------------------------------------------------------- #
    def engine_key(self, handle) -> Optional[Tuple]:
        if handle.external_conn is not None:
            return ("conn", id(handle.external_conn))
        dp = handle.db_params
        if dp is None:
            return None
        return (
            "params",
            getattr(dp, "host", None),
            getattr(dp, "port", None),
            getattr(dp, "user", getattr(dp, "username", None)),
            self._key_database(handle, dp),
        )

    def _key_database(self, handle, dp) -> Any:
        return getattr(dp, "database", getattr(dp, "dbname", None))

    # -- SQL syntax --------------------------------------------------------- #
    def quote(self, name: str) -> str:
        return esc_ident(name, self.dialect)

    def table_ref(self, handle) -> str:
        if handle.external_conn is not None and handle.table_ref:
            from kontra.connectors.detection import get_default_schema, parse_table_reference

            db, schema, table = parse_table_reference(handle.table_ref)
            schema = schema or get_default_schema(self._default_schema_dialect)
            return self._qualify(db, schema, table)
        dp = handle.db_params
        if dp is not None:
            return self._qualify(getattr(dp, "database", None), dp.schema, dp.table)
        raise _FallbackToPolars("cannot resolve table reference")

    def _qualify(self, db: Optional[str], schema: str, table: str) -> str:
        return f"{self.quote(schema)}.{self.quote(table)}"  # two-part default

    def temp_name(self, side: str, run: str) -> str:
        return f"{self.temp_prefix}_{side}_{run}"

    def create_temp_sql(self, name: str, source_sql: str) -> str:
        raise NotImplementedError

    def describe_sql(self, ref: str) -> str:
        raise NotImplementedError

    def drop_temp_sql(self, name: str) -> str:
        raise NotImplementedError

    def savepoint_begin_sql(self, name: str) -> str:
        raise NotImplementedError

    def savepoint_rollback_sql(self, name: str) -> str:
        raise NotImplementedError

    # -- value semantics ---------------------------------------------------- #
    def is_char(self, type_code: Any) -> bool:
        return False

    def value_operand(self, expr: str, is_char: bool) -> str:
        """Comparison operand for the `<>` test (byte-exact for strings)."""
        return expr

    def key_type_unsafe(self, type_code: Any) -> bool:
        """Key columns of this type can't be matched byte-for-byte -> defer."""
        return False

    def value_type_unsafe(self, type_code: Any) -> bool:
        """Value columns of this type can't be compared byte-for-byte -> defer."""
        return False


_BACKENDS: Dict[str, _Backend] = {}


def _register(cls):
    _BACKENDS[cls.dialect] = cls()
    return cls


@_register
class _Postgres(_Backend):
    dialect = "postgres"
    conn_label = "postgres"
    schemes = frozenset({"postgres", "postgresql"})
    aliases = frozenset({"postgres", "postgresql"})
    temp_prefix = "_kontra_cmp"
    _default_schema_dialect = "postgresql"

    def create_temp_sql(self, name, source_sql):
        return f"CREATE TEMP TABLE {name} AS {source_sql}"

    def describe_sql(self, ref):
        return f"SELECT * FROM {ref} AS _kontra_d LIMIT 0"

    def drop_temp_sql(self, name):
        return f"DROP TABLE IF EXISTS {name}"

    def savepoint_begin_sql(self, name):
        return f"SAVEPOINT {name}"

    def savepoint_rollback_sql(self, name):
        return f"ROLLBACK TO SAVEPOINT {name}"

    def is_char(self, type_code):
        return type_code in _PG_TEXT_OIDS

    def key_type_unsafe(self, type_code):
        return type_code in _PG_BPCHAR_OIDS  # CHAR(n) padding

    def value_type_unsafe(self, type_code):
        return type_code in _PG_BPCHAR_OIDS


@_register
class _SqlServer(_Backend):
    dialect = "sqlserver"
    conn_label = "sqlserver"
    schemes = frozenset({"mssql", "sqlserver"})
    aliases = frozenset({"mssql", "sqlserver"})
    temp_prefix = "#kontra_cmp"
    _default_schema_dialect = "sqlserver"

    def _key_database(self, handle, dp):
        # Table handles are three-part qualified so the DB can differ on one
        # server; a query handle runs in the connection's DB, so keep the DB in
        # the key to make cross-database query pairs ineligible.
        return getattr(dp, "database", None) if getattr(handle, "sql", None) else None

    def _qualify(self, db, schema, table):
        parts = [db, schema, table] if db else [schema, table]
        return ".".join(self.quote(p) for p in parts)

    def create_temp_sql(self, name, source_sql):
        return f"SELECT * INTO {name} FROM ({source_sql}) _kontra_src"

    def describe_sql(self, ref):
        return f"SELECT TOP 0 * FROM {ref} AS _kontra_d"

    def drop_temp_sql(self, name):
        return f"IF OBJECT_ID('tempdb..{name}') IS NOT NULL DROP TABLE {name}"

    def savepoint_begin_sql(self, name):
        return f"SAVE TRANSACTION {name}"

    def savepoint_rollback_sql(self, name):
        return f"ROLLBACK TRANSACTION {name}"

    def is_char(self, type_code):
        # pyodbc (the Entra/ODBC driver) reports Python types in cursor.description;
        # pymssql reports its own DBAPIType objects. Detect char columns for both,
        # and never hard-import pymssql — an Entra-only install has no pymssql.
        if isinstance(type_code, type):
            return type_code is str
        try:
            import pymssql
        except ImportError:
            return False
        return type_code == pymssql.STRING

    def value_operand(self, expr, is_char):
        # SQL Server string =/<> ignore trailing spaces and honor a (often
        # case-insensitive) collation; VARBINARY makes the compare byte-exact.
        return f"CAST({expr} AS VARBINARY(MAX))" if is_char else expr

    def key_type_unsafe(self, type_code):
        return self.is_char(type_code)  # collation/padding key equality


def _backend_for(handle) -> Optional[_Backend]:
    scheme = handle.scheme
    for b in _BACKENDS.values():
        if scheme in b.schemes:
            return b
    d = getattr(handle, "dialect", None)
    for b in _BACKENDS.values():
        if d in b.aliases:
            return b
    return None


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

    backend = _backend_for(hb)
    if backend is None or backend is not _backend_for(ha):
        return None

    kb = backend.engine_key(hb)
    ka = backend.engine_key(ha)
    if kb is None or kb != ka:
        return None

    return {"backend": backend, "before": hb, "after": ha, "key": list(before_key)}


# --------------------------------------------------------------------------- #
# Execution
# --------------------------------------------------------------------------- #

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


def _prepare(conn, backend: _Backend, handle, side: str, run: str, temps: List[str]) -> str:
    """A referable relation for a handle; materialize query sources to a unique
    session temp table (recorded in `temps` for cleanup)."""
    sql = getattr(handle, "sql", None)
    if not sql:
        return backend.table_ref(handle)
    name = backend.temp_name(side, run)
    _cursor_exec(conn, backend.create_temp_sql(name, sql))
    temps.append(name)
    return name


def _null_counts(conn, backend: _Backend, ref: str, cols: List[str]) -> Dict[str, int]:
    if not cols:
        return {}
    sums = ", ".join(
        f"SUM(CASE WHEN {backend.quote(c)} IS NULL THEN 1 ELSE 0 END) AS {backend.quote('n__' + c)}"
        for c in cols
    )
    row, names = _cursor_exec(conn, f"SELECT {sums} FROM {ref}", fetch="one")
    m = dict(zip(names, row))
    return {c: int(m["n__" + c] or 0) for c in cols}


def compare_sql(plan: Dict[str, Any], sample_limit: int) -> CompareResult:
    """Measure the compare facts with set-based SQL. Raises _FallbackToPolars when
    it cannot guarantee Polars-equivalent counts."""
    backend: _Backend = plan["backend"]
    hb, ha = plan["before"], plan["after"]
    key: List[str] = plan["key"]

    from kontra.connectors.db_utils import get_connection_ctx

    q = backend.quote
    external = hb.external_conn is not None  # caller-owned connection
    run = uuid.uuid4().hex[:10]

    with get_connection_ctx(hb, backend.conn_label) as conn:
        # On a caller-owned connection, isolate all Mode A work behind a savepoint
        # so a fallback or error undoes only our temp tables — never the caller's
        # uncommitted work. If we can't isolate, don't touch the connection.
        savepoint = _begin_savepoint(conn, backend) if external else None
        if external and savepoint is None:
            raise _FallbackToPolars("cannot isolate Mode A on the caller's connection")
        temps: List[str] = []
        try:
            b_ref = _prepare(conn, backend, hb, "b", run, temps)
            a_ref = _prepare(conn, backend, ha, "a", run, temps)

            b_cols = _cursor_exec(conn, backend.describe_sql(b_ref), fetch="desc")
            a_cols = _cursor_exec(conn, backend.describe_sql(a_ref), fetch="desc")
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
            columns_added = sorted(set(a_nonkey) - set(b_nonkey))
            columns_removed = sorted(set(b_nonkey) - set(a_nonkey))

            _guard_types(backend, key, common, b_types, a_types)

            key_out = ", ".join(q(k) for k in key)
            key_null = " OR ".join(f"{q(k)} IS NULL" for k in key)

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

            on_clause = " AND ".join(f"b.{q(k)} = a.{q(k)}" for k in key)

            def cmp_pred(c: str) -> str:
                is_char = backend.is_char(b_types[c])
                cmp = (lambda e: backend.value_operand(e, is_char)) if is_char else None
                return _distinct_pred("b." + q(c), "a." + q(c), cmp)

            matched_rows = 0
            changed_rows = 0
            per_col_changed: Dict[str, int] = {}
            if preserved > 0 and common:
                row_pred = " OR ".join(cmp_pred(c) for c in common)
                col_sums = ", ".join(
                    f"COALESCE(SUM(CASE WHEN {cmp_pred(c)} THEN 1 ELSE 0 END), 0) AS {q('chg__' + c)}"
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
                matched_rows = int(cmap["m"])
                changed_rows = int(cmap["changed_rows"])
                for c in common:
                    per_col_changed[c] = int(cmap["chg__" + c])

            # Null counts only for changed columns (+ added on the after side).
            changed_cols = [c for c in common if per_col_changed.get(c, 0) > 0]
            b_nulls = _null_counts(conn, backend, b_ref, changed_cols) if changed_cols else {}
            a_nulls = (
                _null_counts(conn, backend, a_ref, changed_cols + columns_added)
                if (changed_cols or columns_added) else {}
            )
        finally:
            if savepoint is not None:
                # Undo ONLY Mode A's work (temp tables + our tx state); the
                # caller's prior uncommitted work on this connection is preserved.
                _rollback_savepoint(conn, backend, savepoint)
            else:
                _drop_temps(conn, backend, temps)

    schema = CompareSchema(
        key=list(key), common=common, added=columns_added, removed=columns_removed
    )
    facts = CompareFacts(
        before_rows=before_rows,
        after_rows=after_rows,
        unique_before=unique_before,
        unique_after=unique_after,
        preserved=preserved,
        duplicated_after=duplicated_after,
        matched_rows=matched_rows,
        changed_rows=changed_rows,
        changed_by_column=per_col_changed,
        nulls_before=b_nulls,
        nulls_after=a_nulls,
    )
    return finalize_compare_result(
        facts, schema, execution_tier="sql", sample_limit=sample_limit
    )


def _guard_types(backend: _Backend, key, common, b_types, a_types) -> None:
    """Raise _FallbackToPolars when SQL can't match Polars value/key semantics."""
    for k in key:
        if b_types.get(k) != a_types.get(k):
            raise _FallbackToPolars(f"cross-type key column '{k}'")
        if backend.key_type_unsafe(b_types[k]) or backend.key_type_unsafe(a_types[k]):
            raise _FallbackToPolars(f"unsafe key column '{k}' for {backend.dialect}")
    for c in common:
        if b_types.get(c) != a_types.get(c):
            raise _FallbackToPolars(f"cross-type common column '{c}'")
        if backend.value_type_unsafe(b_types[c]):
            raise _FallbackToPolars(f"unsafe column '{c}' for {backend.dialect}")


def _begin_savepoint(conn, backend: _Backend) -> Optional[str]:
    name = "kontra_cmp_" + uuid.uuid4().hex[:10]
    try:
        _cursor_exec(conn, backend.savepoint_begin_sql(name))
        return name
    except Exception:
        return None


def _rollback_savepoint(conn, backend: _Backend, name: str) -> None:
    try:
        _cursor_exec(conn, backend.savepoint_rollback_sql(name))
    except Exception:
        pass


def _drop_temps(conn, backend: _Backend, temps: List[str]) -> None:
    for name in temps:
        try:
            _cursor_exec(conn, backend.drop_temp_sql(name))
        except Exception:
            pass
