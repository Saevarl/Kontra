"""Database backends for the differential fuzzer.

Each backend loads a generated frame into a fresh table and exposes routes that
validate that table: SQL pushdown, local Polars after materializing, and each
of those with the metadata preplan in front. The local file routes run on the
same frame, so every route sees the same data.

A backend may not be able to store every generated value (SQL Server has no
NaN or infinity; ClickHouse dates stop at 2299). ``representable`` blanks those
values to NULL before any route runs, so the comparison stays fair.

Connection settings come from environment variables so the fuzzer can point at
throwaway containers:

    KONTRA_FUZZ_POSTGRES   postgres://user:pass@host:port/db
    KONTRA_FUZZ_SQLSERVER  mssql://user:pass@host:port/db
    KONTRA_FUZZ_CLICKHOUSE clickhouse://user:pass@host:port/db
    KONTRA_FUZZ_TRINO      trino://user@host:port/catalog/schema   (memory catalog)
"""

from __future__ import annotations

import itertools
import math
import os
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import date, datetime
from typing import Any, ClassVar
from urllib.parse import urlparse

import polars as pl

import kontra

_names = itertools.count()


def _table() -> str:
    return f"fuzz_{os.getpid()}_{next(_names)}"


def _blank(df: pl.DataFrame, column: str, bad: Callable[[Any], bool]) -> pl.DataFrame:
    values = [None if v is not None and bad(v) else v for v in df[column].to_list()]
    return df.with_columns(pl.Series(column, values, dtype=df.schema[column]))


def kind(dtype: pl.DataType) -> str:
    """The fuzzer's type family for a column, for picking a database type."""
    if dtype.is_integer():
        return "int"
    if dtype == pl.Float32:
        return "float32"
    if dtype.is_float():
        return "float"
    if dtype == pl.Boolean:
        return "bool"
    if dtype == pl.Date:
        return "date"
    if isinstance(dtype, pl.Datetime):
        return "datetime"
    return "str"


def _columns(df: pl.DataFrame, types: dict[str, str], quote: Callable[[str], str]) -> str:
    return ", ".join(f"{quote(name)} {types[kind(dtype)]}" for name, dtype in df.schema.items())


def _dq(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _nonfinite(v: float) -> bool:
    return math.isnan(v) or math.isinf(v)


class Backend:
    name: str
    env: str

    def __init__(self, url: str):
        self.url = urlparse(url)

    def representable(self, df: pl.DataFrame) -> pl.DataFrame:
        return df

    def prepare(self) -> None:
        """Create the database or schema the tables go in, if needed."""

    @contextmanager
    def loaded(self, df: pl.DataFrame) -> Iterator[str]:
        """Load ``df`` into a new table and yield its kontra URI."""
        raise NotImplementedError

    def routes(self, uri: str) -> dict[str, Callable]:
        def route(preplan: str, pushdown: str):
            def run(df, path, rules, tally):
                return kontra.validate(uri, rules=rules, save=False, tally=tally,
                                       preplan=preplan, pushdown=pushdown)
            return run

        return {
            f"{self.name}:sql": route("off", "on"),
            f"{self.name}:polars": route("off", "off"),
            f"{self.name}:metadata+sql": route("on", "on"),
            f"{self.name}:metadata+polars": route("on", "off"),
        }


# ---------------------------------------------------------------------------


class Postgres(Backend):
    name, env = "postgres", "KONTRA_FUZZ_POSTGRES"
    TYPES: ClassVar[dict[str, str]] = {"int": "bigint", "float": "double precision", "float32": "real",
                                       "str": "text", "bool": "boolean",
                                       "datetime": "timestamp(6)", "date": "date"}

    def _connect(self):
        import psycopg

        u = self.url
        return psycopg.connect(host=u.hostname, port=u.port, user=u.username,
                               password=u.password, dbname=u.path.strip("/"), autocommit=True)

    @contextmanager
    def loaded(self, df):
        table = _table()
        cols = _columns(df, self.TYPES, _dq)
        with self._connect() as conn:
            conn.execute(f'CREATE TABLE public."{table}" ({cols})')
            try:
                if df.height:
                    marks = ", ".join(["%s"] * df.width)
                    with conn.cursor() as cur:
                        cur.executemany(f'INSERT INTO public."{table}" VALUES ({marks})',
                                        df.rows())
                u = self.url
                yield (f"postgres://{u.username}:{u.password}@{u.hostname}:{u.port}"
                       f"{u.path}/public.{table}")
            finally:
                conn.execute(f'DROP TABLE public."{table}"')


class SqlServer(Backend):
    name, env = "sqlserver", "KONTRA_FUZZ_SQLSERVER"
    TYPES: ClassVar[dict[str, str]] = {"int": "BIGINT", "float": "FLOAT", "float32": "REAL",
                                       "str": "NVARCHAR(100)",
                                       "bool": "BIT", "datetime": "DATETIME2(6)", "date": "DATE"}

    def representable(self, df):
        # FLOAT can't hold NaN or infinity, and DATETIME2/DATE start at year 1.
        for name, dtype in df.schema.items():
            if dtype.is_float():
                df = _blank(df, name, _nonfinite)
        return df

    def _connect(self, database: str | None = None):
        import pymssql

        u = self.url
        return pymssql.connect(server=u.hostname, port=str(u.port), user=u.username,
                               password=u.password, database=database or u.path.strip("/"),
                               autocommit=True)

    def prepare(self) -> None:
        db = self.url.path.strip("/")
        with self._connect("master") as conn, conn.cursor() as cur:
            cur.execute(f"IF DB_ID('{db}') IS NULL CREATE DATABASE [{db}]")

    @contextmanager
    def loaded(self, df):
        table = _table()
        cols = _columns(df, self.TYPES, lambda n: "[" + n.replace("]", "]]") + "]")
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(f"CREATE TABLE dbo.[{table}] ({cols})")
            try:
                if df.height:
                    marks = ", ".join(["%s"] * df.width)
                    cur.executemany(f"INSERT INTO dbo.[{table}] VALUES ({marks})", df.rows())
                u = self.url
                yield (f"mssql://{u.username}:{u.password}@{u.hostname}:{u.port}"
                       f"{u.path}/dbo.{table}")
            finally:
                cur.execute(f"DROP TABLE dbo.[{table}]")


class ClickHouse(Backend):
    name, env = "clickhouse", "KONTRA_FUZZ_CLICKHOUSE"
    TYPES: ClassVar[dict[str, str]] = {"int": "Int64", "float": "Float64", "float32": "Float32",
                                       "str": "String",
                                       "bool": "Bool", "datetime": "DateTime64(6)",
                                       "date": "Date32"}

    def representable(self, df):
        # DateTime64 and Date32 cover 1900-01-01 .. 2299-12-31.
        def out(v):
            return not (1900 <= v.year <= 2299)
        for name, dtype in df.schema.items():
            if kind(dtype) in ("date", "datetime"):
                df = _blank(df, name, out)
        return df

    def _client(self):
        import clickhouse_connect

        u = self.url
        return clickhouse_connect.get_client(host=u.hostname, port=u.port, username=u.username,
                                             password=u.password, database=u.path.strip("/"))

    @contextmanager
    def loaded(self, df):
        table = _table()
        client = self._client()
        types = {k: f"Nullable({v})" for k, v in self.TYPES.items()}
        cols = _columns(df, types,
                        lambda n: "`" + n.replace("\\", "\\\\").replace("`", "``") + "`")
        client.command(f"CREATE TABLE `{table}` ({cols}) ENGINE = MergeTree ORDER BY tuple()")
        try:
            if df.height:
                client.insert(table, df.rows(), column_names=df.columns)
            u = self.url
            yield f"clickhouse://{u.username}:{u.password}@{u.hostname}:{u.port}{u.path}/{table}"
        finally:
            client.command(f"DROP TABLE `{table}`")
            client.close()


class Trino(Backend):
    name, env = "trino", "KONTRA_FUZZ_TRINO"
    TYPES: ClassVar[dict[str, str]] = {"int": "bigint", "float": "double", "float32": "real",
                                       "str": "varchar",
                                       "bool": "boolean", "datetime": "timestamp(6)",
                                       "date": "date"}

    def _connect(self):
        import trino

        u = self.url
        catalog, _schema = u.path.strip("/").split("/")
        return trino.dbapi.connect(host=u.hostname, port=u.port, user=u.username,
                                   catalog=catalog)

    def _run(self, sql: str) -> None:
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute(sql)
            cur.fetchall()
        finally:
            conn.close()

    def prepare(self) -> None:
        catalog, schema = self.url.path.strip("/").split("/")
        self._run(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

    @staticmethod
    def _literal(v: Any) -> str:
        if v is None:
            return "NULL"
        if isinstance(v, bool):
            return "true" if v else "false"
        if isinstance(v, int):
            return f"CAST('{v}' AS bigint)"
        if isinstance(v, float):
            if math.isnan(v):
                return "nan()"
            if math.isinf(v):
                return "infinity()" if v > 0 else "-infinity()"
            return f"CAST('{v!r}' AS double)"
        if isinstance(v, datetime):
            return f"TIMESTAMP '{v.isoformat(sep=' ', timespec='microseconds')}'"
        if isinstance(v, date):
            return f"DATE '{v.isoformat()}'"
        return "'" + str(v).replace("'", "''") + "'"

    @contextmanager
    def loaded(self, df):
        catalog, schema = self.url.path.strip("/").split("/")
        table = f"{catalog}.{schema}.{_table()}"
        cols = _columns(df, self.TYPES, _dq)
        self._run(f"CREATE TABLE {table} ({cols})")
        try:
            if df.height:
                rows = ", ".join("(" + ", ".join(self._literal(v) for v in row) + ")"
                                 for row in df.rows())
                self._run(f"INSERT INTO {table} VALUES {rows}")
            u = self.url
            yield f"trino://{u.username}@{u.hostname}:{u.port}/{table.replace('.', '/', 1)}"
        finally:
            self._run(f"DROP TABLE {table}")


ALL = [Postgres, SqlServer, ClickHouse, Trino]


def configured() -> list[Backend]:
    """The backends whose connection URL is set and whose client is installed."""
    out = []
    for cls in ALL:
        url = os.environ.get(cls.env)
        if url:
            backend = cls(url)
            backend.prepare()
            out.append(backend)
    return out
