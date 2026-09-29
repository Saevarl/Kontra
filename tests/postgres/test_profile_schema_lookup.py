"""Direct catalog lookup must preserve information_schema's public contract."""

from kontra.connectors.handle import DatasetHandle
from kontra.scout.backends.postgres_backend import PostgreSQLBackend


def test_schema_names_domains_arrays_dropped_columns_and_visibility(postgres_connection):
    conn = postgres_connection
    table = "sqlprof_schema_types"
    uri = f"postgresql://kontra:kontra_test@127.0.0.1:5433/kontra_test/public.{table}"
    conn.execute("CREATE TYPE sqlprof_enum AS ENUM ('a', 'b')")
    conn.execute("CREATE DOMAIN sqlprof_domain AS NUMERIC(12,2)")
    conn.execute("CREATE DOMAIN sqlprof_array_domain AS INTEGER[]")
    conn.execute(f"""CREATE TABLE {table} (
        id BIGINT, "odd'""column" VARCHAR(23), amount sqlprof_domain,
        xs INTEGER[], ys sqlprof_array_domain, state sqlprof_enum,
        ts TIMESTAMPTZ, gone INTEGER, flag BOOLEAN, payload JSONB
    )""")
    conn.execute(f"ALTER TABLE {table} DROP COLUMN gone")
    conn.execute("CREATE ROLE sqlprof_reader LOGIN PASSWORD 'sqlprof_test'")
    conn.execute(f"GRANT SELECT(id) ON {table} TO sqlprof_reader")
    conn.commit()
    try:
        for user, password in [("kontra", "kontra_test"), ("sqlprof_reader", "sqlprof_test")]:
            backend = PostgreSQLBackend(
                DatasetHandle.from_uri(uri.replace("kontra:kontra_test", f"{user}:{password}"))
            )
            backend.connect()
            try:
                expected = backend._conn.execute(
                    "SELECT column_name, data_type FROM information_schema.columns WHERE table_schema=%s AND table_name=%s ORDER BY ordinal_position",
                    ("public", table),
                ).fetchall()
                assert backend.get_schema() == expected
                assert len(expected) == (1 if user == "sqlprof_reader" else 9)
            finally:
                backend.close()
    finally:
        conn.execute(f"DROP TABLE {table}")
        conn.execute("DROP DOMAIN sqlprof_array_domain")
        conn.execute("DROP DOMAIN sqlprof_domain")
        conn.execute("DROP TYPE sqlprof_enum")
        conn.execute("DROP ROLE sqlprof_reader")
        conn.commit()
