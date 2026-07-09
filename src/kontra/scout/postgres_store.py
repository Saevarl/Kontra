# src/kontra/scout/postgres_store.py
"""
PostgreSQL-backed profile storage for Kontra Scout.

Persists ``ProfileState`` rows to a single ``kontra_profiles`` table, keyed by
``source_fingerprint`` with one row per ``profiled_at`` (history). Re-saving the
same profiled_at upserts in place, matching the local store where the timestamp
is the filename.

Mirrors ``kontra.state.backends.postgres.PostgresStore``: same connection-param
resolution (URI > DATABASE_URL > PGXXX env vars > defaults) and lazy psycopg
import so ``import kontra`` never loads psycopg.
"""

from __future__ import annotations

import json
import logging
from typing import List, Optional

from .types import ProfileState

_logger = logging.getLogger(__name__)

# Lazy-loaded psycopg base error class (psycopg may not be installed and must
# never be imported at package load time).
_PsycopgError = None


def _get_db_error():
    """Get the psycopg base error class, lazy-loaded."""
    global _PsycopgError
    if _PsycopgError is None:
        try:
            import psycopg

            _PsycopgError = psycopg.Error
        except ImportError:
            _PsycopgError = Exception  # Fallback if psycopg unavailable
    return _PsycopgError


class PostgresProfileStore:
    """
    PostgreSQL database profile storage backend.

    Uses psycopg3 (psycopg) for database access. Automatically creates the
    required table if it doesn't exist.

    URI format: postgres://user:pass@host:port/database
                postgresql://user:pass@host:port/database

    Also supports standard PostgreSQL environment variables:
        PGHOST, PGPORT, PGUSER, PGPASSWORD, PGDATABASE, DATABASE_URL

    Public interface mirrors LocalProfileStore:
        save(state)                         -> None
        get_latest(source_fingerprint)      -> Optional[ProfileState]
        get_history(source_fingerprint, limit=10) -> List[ProfileState]
        list_sources()                      -> List[str]
        clear(source_fingerprint=None)      -> int
    """

    PROFILES_TABLE = "kontra_profiles"

    CREATE_TABLES_SQL = """
    CREATE TABLE IF NOT EXISTS kontra_profiles (
        id SERIAL PRIMARY KEY,

        -- Identity
        source_fingerprint TEXT NOT NULL,
        source_uri TEXT NOT NULL,

        -- Timing (ISO-8601 string; lexically sortable, mirrors local filenames)
        profiled_at TEXT NOT NULL,

        -- Metadata
        schema_version TEXT NOT NULL DEFAULT '1.0',
        engine_version TEXT,

        -- Full ProfileState.to_dict() payload for lossless round-trip
        state JSONB NOT NULL,

        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),

        -- One row per (source, timestamp): re-saving upserts in place
        UNIQUE (source_fingerprint, profiled_at)
    );

    CREATE INDEX IF NOT EXISTS idx_kontra_profiles_fp_time
        ON kontra_profiles (source_fingerprint, profiled_at DESC);
    """

    def __init__(self, uri: str = "postgres://"):
        """
        Initialize the PostgreSQL profile store.

        Args:
            uri: PostgreSQL connection URI. May be just the scheme, with
                connection details supplied via environment variables.
        """
        # Reuse the state backend's battle-tested connection-param resolution.
        # Imported lazily so this module stays import-light.
        from kontra.state.backends.postgres import PostgresStore

        self.uri = uri
        self._conn_params = PostgresStore._parse_connection_params(uri)
        self._conn = None
        self._tables_created = False

    def _get_conn(self):
        """Get or create the database connection."""
        if self._conn is not None:
            return self._conn

        try:
            import psycopg
        except ImportError as e:
            raise RuntimeError(
                "PostgreSQL profile backend requires 'psycopg'. "
                "Install with: pip install psycopg[binary]"
            ) from e

        conn_str = f"host={self._conn_params['host']} port={self._conn_params['port']}"
        if self._conn_params.get("user"):
            conn_str += f" user={self._conn_params['user']}"
        if self._conn_params.get("password"):
            conn_str += f" password={self._conn_params['password']}"
        if self._conn_params.get("dbname"):
            conn_str += f" dbname={self._conn_params['dbname']}"

        try:
            self._conn = psycopg.connect(conn_str)
            self._ensure_tables()
        except psycopg.Error as e:
            raise ConnectionError(
                f"Failed to connect to PostgreSQL: {e}\n\n"
                "Set environment variables:\n"
                "  export PGHOST=localhost\n"
                "  export PGPORT=5432\n"
                "  export PGUSER=your_user\n"
                "  export PGPASSWORD=your_password\n"
                "  export PGDATABASE=your_database\n\n"
                "Or use full URI:\n"
                "  postgres://user:pass@host:5432/database"
            ) from e

        return self._conn

    def _ensure_tables(self) -> None:
        """Create the profiles table if it doesn't exist."""
        if self._tables_created:
            return

        conn = self._conn
        with conn.cursor() as cur:
            cur.execute(self.CREATE_TABLES_SQL)
        conn.commit()
        self._tables_created = True

    def save(self, state: ProfileState) -> None:
        """
        Save a profile state to the database.

        Appends a history row keyed by (source_fingerprint, profiled_at).
        Re-saving the same profiled_at upserts the existing row in place.
        """
        conn = self._get_conn()

        sql = f"""
        INSERT INTO {self.PROFILES_TABLE} (
            source_fingerprint,
            source_uri,
            profiled_at,
            schema_version,
            engine_version,
            state
        ) VALUES (
            %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (source_fingerprint, profiled_at) DO UPDATE SET
            source_uri = EXCLUDED.source_uri,
            schema_version = EXCLUDED.schema_version,
            engine_version = EXCLUDED.engine_version,
            state = EXCLUDED.state,
            created_at = now()
        """

        try:
            with conn.cursor() as cur:
                cur.execute(
                    sql,
                    (
                        state.source_fingerprint,
                        state.source_uri,
                        state.profiled_at,
                        state.schema_version,
                        state.engine_version,
                        json.dumps(state.to_dict()),
                    ),
                )
            conn.commit()
        except _get_db_error() as e:
            conn.rollback()
            raise IOError(f"Failed to save profile to PostgreSQL: {e}") from e

    def get_latest(self, source_fingerprint: str) -> Optional[ProfileState]:
        """Get the most recent profile for a source."""
        history = self.get_history(source_fingerprint, limit=1)
        return history[0] if history else None

    def get_history(
        self,
        source_fingerprint: str,
        limit: int = 10,
    ) -> List[ProfileState]:
        """Get recent profile history for a source, newest first."""
        conn = self._get_conn()

        sql = f"""
        SELECT state
        FROM {self.PROFILES_TABLE}
        WHERE source_fingerprint = %s
        ORDER BY profiled_at DESC, id DESC
        LIMIT %s
        """

        try:
            with conn.cursor() as cur:
                cur.execute(sql, (source_fingerprint, limit))
                rows = cur.fetchall()
        except _get_db_error() as e:
            _logger.debug(
                f"Database error getting profile history for {source_fingerprint}: {e}"
            )
            return []

        states: List[ProfileState] = []
        for row in rows:
            payload = row[0]
            # psycopg returns JSONB as a parsed dict; tolerate a text column too.
            if isinstance(payload, (str, bytes, bytearray)):
                try:
                    payload = json.loads(payload)
                except (json.JSONDecodeError, ValueError) as e:
                    _logger.debug(f"Skipping unparseable profile row: {e}")
                    continue
            try:
                states.append(ProfileState.from_dict(payload))
            except (KeyError, ValueError, TypeError) as e:
                _logger.debug(f"Skipping malformed profile row: {e}")
                continue

        return states

    def list_sources(self) -> List[str]:
        """List all source fingerprints with stored profiles."""
        conn = self._get_conn()

        sql = f"""
        SELECT DISTINCT source_fingerprint
        FROM {self.PROFILES_TABLE}
        ORDER BY source_fingerprint
        """

        try:
            with conn.cursor() as cur:
                cur.execute(sql)
                return [row[0] for row in cur.fetchall()]
        except _get_db_error() as e:
            _logger.debug(f"Database error listing profile sources: {e}")
            return []

    def clear(self, source_fingerprint: Optional[str] = None) -> int:
        """
        Clear stored profiles.

        Args:
            source_fingerprint: If provided, only clear this source's profiles.
                                If None, clear all profiles.

        Returns:
            Number of rows deleted.
        """
        conn = self._get_conn()

        try:
            with conn.cursor() as cur:
                if source_fingerprint:
                    cur.execute(
                        f"DELETE FROM {self.PROFILES_TABLE} WHERE source_fingerprint = %s",
                        (source_fingerprint,),
                    )
                else:
                    cur.execute(f"DELETE FROM {self.PROFILES_TABLE}")
                deleted = cur.rowcount
            conn.commit()
            return deleted
        except _get_db_error() as e:
            _logger.warning(f"Database error clearing profiles: {e}")
            conn.rollback()
            return 0

    def close(self) -> None:
        """Close the database connection."""
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def __repr__(self) -> str:
        host = self._conn_params.get("host", "?")
        dbname = self._conn_params.get("dbname", "?")
        return f"PostgresProfileStore(host={host}, dbname={dbname})"

    def __del__(self):
        self.close()
