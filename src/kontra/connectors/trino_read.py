# src/kontra/connectors/trino_read.py
"""
One table state per Trino validation.

Every query of a validation (column types, pushed-down rules, custom SQL and
the Polars-tier fetch) has to describe the same table, or a writer committing
in between can make two rules disagree about the same rows. How Kontra holds
the state depends on the connection and the catalog:

* **Kontra's own connection, Iceberg catalog:** one explicit Trino transaction
  for the whole validation. Inside a transaction the Iceberg connector loads a
  table once and serves every later query from that state, under that state's
  (current) schema. Trino doesn't document this as a guarantee, so a guard reads
  the table's newest metadata file when the validation starts and again after
  its last query. Every Iceberg commit, data or schema, writes a new metadata
  file. If the guard sees one, the validation runs once more in a new
  transaction, and raises if that changes too. It never returns a result no
  single table state explains.
* **A caller's connection with an isolation level** (already transactional):
  the same, inside the caller's transaction. Kontra never commits or rolls it
  back. A guard that fires raises at once: a rerun would read the same
  transaction.
* **A caller's autocommit connection:** Kontra can't open a transaction on it
  without changing it. When the newest metadata file is the current snapshot's
  own data commit and that snapshot reads with the current columns and types,
  every data query is pinned to it with ``FOR VERSION AS OF``. Otherwise the
  queries run unpinned between two guard reads, rerun once on a change, and
  raise on a second one. User SQL (``custom_sql_check``, a custom rule's SQL)
  can name the table directly, past the pin, and Trino can't pin ``$files``,
  so a pinned run that pushes user SQL or reads ``$files`` keeps the guard too.
* **Other catalogs, views and materialized views** have no table state Kontra
  can hold (a view reads its base tables; a materialized view may read its
  definition when stale), so they run as before.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from kontra.connectors.handle import DatasetHandle
from kontra.connectors.trino_types import normalize_type as _normalize_type
from kontra.logging import get_logger

_logger = get_logger(__name__)

# Modes. "transaction" owns its connection; the others use the caller's.
TRANSACTION = "transaction"
CALLER_TRANSACTION = "caller_transaction"
PINNED = "pinned"
BRACKETED = "bracketed"


class TableChanged(Exception):
    """The table changed while a validation read it (internal; the engine raises DataError)."""

    def __init__(self, message: str, retryable: bool):
        super().__init__(message)
        self.retryable = retryable


@dataclass
class TrinoReadState:
    """How one validation reads its table, and what it read first."""

    mode: str
    # (column_name, data_type, nullable) in ordinal order, read once per validation.
    columns: list[tuple[str, str, bool]]
    snapshot_id: int | None = None
    guard: str | None = None
    # User SQL ran in Trino; it may read the table past the pin.
    user_sql: bool = False
    # $files was read; Trino can't pin a metadata table to a snapshot.
    files_read: bool = False


def table_parts(handle: DatasetHandle) -> tuple[str | None, str, str]:
    """(catalog, schema, table). A caller's reference may omit the catalog."""
    from kontra.connectors.detection import parse_table_reference

    if handle.scheme == "byoc" and handle.table_ref:
        catalog, schema, table = parse_table_reference(handle.table_ref)
        if not schema:
            raise ValueError(
                f"Trino table reference needs a schema: 'schema.table' or "
                f"'catalog.schema.table' (got: {handle.table_ref!r})"
            )
        return catalog, schema, table
    if handle.db_params:
        params = handle.db_params
        return params.catalog, params.schema, params.table
    raise ValueError("Handle has neither table_ref nor db_params")


def is_trino_table(handle: DatasetHandle | None) -> bool:
    """A Trino table handle (not a query source)."""
    if handle is None or handle.sql is not None:
        return False
    if handle.scheme in ("trino", "trinos"):
        return handle.db_params is not None
    return handle.scheme == "byoc" and handle.dialect == "trino" and bool(handle.table_ref)


def state_of(handle: DatasetHandle | None) -> TrinoReadState | None:
    return getattr(handle, "trino_read", None) if handle is not None else None


def pin_suffix(handle: DatasetHandle) -> str:
    """`` FOR VERSION AS OF <id>`` when the validation is pinned, else empty."""
    state = state_of(handle)
    if state is not None and state.mode == PINNED:
        return f" FOR VERSION AS OF {state.snapshot_id}"
    return ""


def mark_user_sql(handle: DatasetHandle) -> None:
    """User SQL is about to run in Trino: a pinned run must still check the guard."""
    state = state_of(handle)
    if state is not None:
        state.user_sql = True


def in_transaction(handle: DatasetHandle | None) -> bool:
    """The validation's queries run in one Trino transaction (Kontra's or the caller's)."""
    state = state_of(handle)
    return state is not None and state.mode in (TRANSACTION, CALLER_TRANSACTION)


def mark_files_read(handle: DatasetHandle) -> None:
    """``$files`` is about to be read: a pinned run must still check the guard."""
    state = state_of(handle)
    if state is not None:
        state.files_read = True


def held_parts(handle: DatasetHandle) -> tuple[str | None, str, str]:
    """(catalog, schema, table), with a caller's session catalog filled in."""
    catalog, schema, table = table_parts(handle)
    if catalog is None and handle.scheme == "byoc":
        catalog = getattr(handle.external_conn, "catalog", None)
    return catalog, schema, table


def metadata_relation(handle: DatasetHandle, suffix: str) -> str:
    """The quoted name of one of the table's metadata tables, e.g. ``$files``."""
    return _relation(*held_parts(handle), suffix)


def declared_columns(handle: DatasetHandle) -> list[tuple[str, str, bool]] | None:
    """The columns read at the start of the validation, or None outside one."""
    state = state_of(handle)
    return list(state.columns) if state is not None else None


# --------------------------------------------------------------------------- #
# SQL
# --------------------------------------------------------------------------- #


def _esc(name: str) -> str:
    from kontra.engine.sql_ir import esc_ident

    return esc_ident(name, "trino")


def _lit(value: str) -> str:
    from kontra.engine.sql_ir import lit_str

    return lit_str(value, "trino")


def _relation(catalog: str | None, schema: str, table: str, suffix: str = "") -> str:
    parts = [catalog, schema, table + suffix] if catalog else [schema, table + suffix]
    return ".".join(_esc(p) for p in parts)


# The metadata-log version is the number that starts every metadata file name.
_VERSION = r"CAST(regexp_extract(file, '/(\d+)-[^/]*$', 1) AS bigint)"


def _newest_entry_sql(catalog: str | None, schema: str, table: str) -> str:
    """The newest metadata file, its snapshot and sequence number, and the previous file's."""
    log = _relation(catalog, schema, table, "$metadata_log_entries")
    return (
        "SELECT file, latest_snapshot_id, latest_sequence_number, "
        "lag(latest_snapshot_id) OVER w, lag(latest_sequence_number) OVER w, "
        f"{_VERSION} AS v FROM {log} "
        f"WINDOW w AS (ORDER BY timestamp, {_VERSION}) "
        "ORDER BY timestamp DESC, v DESC LIMIT 1"
    )


def _columns_sql(catalog: str | None, schema: str, table: str) -> str:
    source = (
        f"{_esc(catalog)}.information_schema.columns" if catalog else "information_schema.columns"
    )
    return (
        f"SELECT column_name, data_type, is_nullable FROM {source} "
        f"WHERE table_schema = {_lit(schema)} AND table_name = {_lit(table)} "
        "ORDER BY ordinal_position"
    )


def _fetch(conn: Any, sql: str) -> list:
    cur = conn.cursor()
    try:
        cur.execute(sql)
        return cur.fetchall()
    finally:
        cur.close()


def _read_columns(conn: Any, parts: tuple[str | None, str, str]) -> list[tuple[str, str, bool]]:
    return [(r[0], r[1], r[2] != "NO") for r in _fetch(conn, _columns_sql(*parts))]


def _newest_entry(conn: Any, parts: tuple[str | None, str, str]) -> tuple | None:
    rows = _fetch(conn, _newest_entry_sql(*parts))
    return tuple(rows[0]) if rows else None


def _pinned_columns(conn: Any, parts: tuple[str | None, str, str], snapshot_id: int) -> list:
    cur = conn.cursor()
    try:
        cur.execute(f"SELECT * FROM {_relation(*parts)} FOR VERSION AS OF {snapshot_id} LIMIT 0")
        cur.fetchall()
        return [(d[0], _normalize_type(d[1])) for d in (cur.description or [])]
    finally:
        cur.close()


def _iceberg_table(conn: Any, parts: tuple[str | None, str, str]) -> bool:
    """An Iceberg catalog's physical table: not a view, not a materialized view."""
    catalog, schema, table = parts
    if not catalog:
        return False
    rows = _fetch(
        conn,
        f"SELECT connector_name FROM system.metadata.catalogs WHERE catalog_name = {_lit(catalog)}",
    )
    if not rows or rows[0][0] != "iceberg":
        return False
    rows = _fetch(
        conn,
        f"SELECT table_type FROM {_esc(catalog)}.information_schema.tables "
        f"WHERE table_schema = {_lit(schema)} AND table_name = {_lit(table)}",
    )
    if not rows or rows[0][0] != "BASE TABLE":
        return False
    # information_schema lists a materialized view as a BASE TABLE.
    rows = _fetch(
        conn,
        "SELECT 1 FROM system.metadata.materialized_views "
        f"WHERE catalog_name = {_lit(catalog)} AND schema_name = {_lit(schema)} "
        f"AND name = {_lit(table)}",
    )
    return not rows


def _autocommit(conn: Any) -> bool:
    try:
        from trino.transaction import IsolationLevel
    except ImportError:  # pragma: no cover - trino is required for a Trino handle
        return True
    return getattr(conn, "isolation_level", IsolationLevel.AUTOCOMMIT) == IsolationLevel.AUTOCOMMIT


# --------------------------------------------------------------------------- #
# Lifecycle: begin -> (validation) -> finish -> end
# --------------------------------------------------------------------------- #


def begin(handle: DatasetHandle) -> None:
    """Choose how this validation reads its table, and read the starting state."""
    if not is_trino_table(handle) or state_of(handle) is not None:
        return

    if handle.scheme == "byoc":
        conn = handle.external_conn
        catalog, schema, table = table_parts(handle)
        catalog = catalog or getattr(conn, "catalog", None)
        parts = (catalog, schema, table)
        if not _iceberg_table(conn, parts):
            return
        if not _autocommit(conn):
            guard = _newest_entry(conn, parts)
            _set_state(
                handle,
                TrinoReadState(
                    CALLER_TRANSACTION, _read_columns(conn, parts), guard=guard and guard[0]
                ),
            )
            return
        _set_state(handle, _autocommit_state(conn, parts))
        return

    from trino.transaction import IsolationLevel

    from kontra.connectors.trino import get_connection

    parts = table_parts(handle)
    # Any level other than AUTOCOMMIT makes the client run every query in one transaction.
    conn = get_connection(handle.db_params, isolation_level=IsolationLevel.READ_UNCOMMITTED)
    try:
        if not _iceberg_table(conn, parts):
            conn.commit()
            conn.close()
            return
        guard = _newest_entry(conn, parts)
        columns = _read_columns(conn, parts)
    except BaseException:
        _discard(conn)
        raise
    object.__setattr__(handle, "owned_conn", conn)
    _set_state(handle, TrinoReadState(TRANSACTION, columns, guard=guard and guard[0]))


def _autocommit_state(conn: Any, parts: tuple[str | None, str, str]) -> TrinoReadState:
    entry = _newest_entry(conn, parts)
    columns = _read_columns(conn, parts)
    if entry is None:
        return TrinoReadState(BRACKETED, columns)
    file, snapshot, seq, prev_snapshot, prev_seq, version = entry
    # The newest file is a data commit when it moved main to a newer snapshot
    # (or is the table's first file). A schema-only change, a rollback or a
    # property change writes a file that isn't one, and pinning would then read
    # an older schema.
    data_commit = snapshot is not None and (
        (prev_snapshot is None and version == 0)
        or (prev_snapshot is not None and snapshot != prev_snapshot and seq > prev_seq)
    )
    if data_commit:
        current = [(name, _normalize_type(data_type)) for name, data_type, _ in columns]
        if _pinned_columns(conn, parts, snapshot) == current:
            return TrinoReadState(PINNED, columns, snapshot_id=snapshot, guard=file)
    return TrinoReadState(BRACKETED, columns, guard=file)


def finish(handle: DatasetHandle | None) -> None:
    """Read the guard again after the validation's last query; raise TableChanged if it moved."""
    state = state_of(handle)
    if state is None or (state.mode == PINNED and not (state.user_sql or state.files_read)):
        return
    conn = handle.owned_conn if state.mode == TRANSACTION else handle.external_conn
    entry = _newest_entry(conn, held_parts(handle))
    now = entry and entry[0]
    if now == state.guard:
        return

    _logger.info(
        "Trino table changed during validation (%s): %s -> %s", state.mode, state.guard, now
    )
    if state.mode == CALLER_TRANSACTION:
        raise TableChanged(
            "The Trino table changed while it was being validated, and the caller's "
            "transaction did not hold one table state. Validate again.",
            retryable=False,
        )
    advice = (
        " Pass a connection opened with an isolation level, so Kontra reads one "
        "table state in a transaction."
        if state.mode in (BRACKETED, PINNED)
        else ""
    )
    raise TableChanged(
        "The Trino table changed during validation, twice in a row, so no single "
        f"table state explains the results.{advice}",
        retryable=True,
    )


def end(handle: DatasetHandle | None) -> None:
    """Close the validation's own transaction and connection. A caller's is left untouched."""
    state = state_of(handle)
    if state is None:
        return
    _set_state(handle, None)
    if state.mode != TRANSACTION or handle.owned_conn is None:
        return
    conn = handle.owned_conn
    object.__setattr__(handle, "owned_conn", None)
    try:
        conn.commit()  # nothing was written; this ends the read transaction
    except Exception as e:  # noqa: BLE001 - the results are already read
        _logger.info("Trino read transaction did not commit cleanly: %s", e)
        _discard(conn)
        return
    conn.close()


def _discard(conn: Any) -> None:
    try:
        if conn.transaction is not None:
            conn.rollback()
    except Exception as e:  # noqa: BLE001 - closing anyway
        _logger.info("Trino read transaction did not roll back: %s", e)
    finally:
        conn.close()


def _set_state(handle: DatasetHandle, state: TrinoReadState | None) -> None:
    object.__setattr__(handle, "trino_read", state)
