"""Minimal cases the route fuzzer found, pinned so they stay fixed.

Each case runs through every local route (Polars, DuckDB, metadata in front of
each, an in-memory frame) and through every database configured with a
KONTRA_FUZZ_* URL, and checks the expected verdict and that all routes agree.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from kontra import rules as r

from .backends import configured
from .oracle import LOCAL_ROUTES, Route, disagreements, run_route
from .strategies import COLUMNS

pytestmark = [pytest.mark.integration]

NAN, INF = float("nan"), float("inf")


def frame(**columns) -> pl.DataFrame:
    n = len(next(iter(columns.values()))) if columns else 0
    data = {name: columns.get(name, [None] * n) for name in COLUMNS}
    return pl.DataFrame(data, schema=COLUMNS)


def float32(values) -> pl.DataFrame:
    return frame(i=[0] * len(values)).with_columns(pl.Series("h", values, dtype=pl.Float32))


# (id, frame, rule, expected passed; None: the routes need only agree)
CASES = [
    # A whole-table failure on an empty table used to report failed_count 0.
    ("dtype-mismatch-empty-table", frame(), r.dtype("i", "string"), False),
    ("freshness-empty-table", frame(), r.freshness("t", "1d"), False),
    # NaN is out of range. SQL comparisons missed it: DuckDB and Postgres
    # order NaN above every number, ClickHouse makes every comparison false.
    ("range-min-only-nan", frame(f=[1.0, NAN]), r.range("f", min=0), False),
    ("range-max-only-nan", frame(f=[1.0, NAN]), r.range("f", max=5), False),
    ("range-both-nan", frame(f=[NAN]), r.range("f", min=0, max=5), False),
    ("range-inf-above-max", frame(f=[INF]), r.range("f", max=5), False),
    ("range-negative-zero", frame(f=[-0.0]), r.range("f", min=0), True),
    ("conditional-range-nan", frame(f=[NAN], c=["x"]),
     r.conditional_range("f", "c == 'x'", min=0), False),
    # Numeric value lists were compared as text: Postgres renders 100.0 as
    # '100', ClickHouse 1e300 as '1e300', SQL Server keeps ~6 digits.
    ("allowed-integral-float", frame(f=[100.0]), r.allowed_values("f", [100.0]), True),
    ("allowed-exponent-float", frame(f=[1e300]), r.allowed_values("f", [1e300]), True),
    ("allowed-long-float", frame(f=[0.1 + 0.2]), r.allowed_values("f", [0.1 + 0.2]), True),
    ("disallowed-negative-zero", frame(f=[0.0]), r.disallowed_values("f", [-0.0]), False),
    # A REAL/Float32 0.1: every route over one source must agree. (Polars'
    # is_in compares a Float32 column with the list as Float64, so the local
    # routes say "not in"; Postgres sends REAL values as doubles, so there it is.)
    ("allowed-float32", float32([0.1]), r.allowed_values("h", [0.1]), None),
    ("disallowed-float32", float32([0.1]), r.disallowed_values("h", [0.1]), None),
    ("allowed-integral-float-on-int", frame(i=[5]), r.allowed_values("i", [5.0]), True),
]


# Cases still open on one backend, some fixed on their own branches. Drop an entry
# when its fix lands; the case then runs there too.
OPEN = {
    # ClickHouse IN keeps -0.0 apart from 0.0 (SELECT -0. IN (0) is 0). Open.
    ("disallowed-negative-zero", "clickhouse"): "open: ClickHouse IN on -0.0",
    # Polars' is_in compares single-precision values with the list as doubles
    # (a Float32 column locally, REAL/Float32 values from pymssql and ClickHouse),
    # while SQL casts the literal to the column type. Predates this suite;
    # undecided. Postgres hands REAL values over as doubles, so it agrees.
    **{(case, group): "open: Polars is_in on Float32"
       for case in ("allowed-float32", "disallowed-float32")
       for group in ("local", "sqlserver", "clickhouse")},
}




@pytest.mark.parametrize("name, df, rule, expected",
                         [pytest.param(*case, id=case[0]) for case in CASES])
def test_routes_agree_on_minimal_case(name, df, rule, expected, tmp_path: Path):
    rules = [{**rule, "id": "r0"}]
    groups: list[tuple[pl.DataFrame, dict[str, Route], object]] = []
    if (name, "local") not in OPEN:
        groups.append((df, LOCAL_ROUTES, None))
    groups += [(b.representable(df), None, b) for b in configured()
               if (name, b.name) not in OPEN]
    for data, routes, backend in groups:
        path = tmp_path / "data.parquet"
        data.write_parquet(path)
        if backend is None:
            runs = [run_route(n, fn, data, str(path), rules, t)
                    for n, fn in routes.items() for t in (True, False)]
        else:
            with backend.loaded(data) as uri:
                runs = [run_route(n, fn, data, str(path), rules, t)
                        for n, fn in backend.routes(uri).items() for t in (True, False)]
        assert not disagreements(runs), "\n".join(disagreements(runs))
        for run in runs:
            assert run.error is None, f"{run.route}: {run.error}"
            if expected is not None:
                assert run.outcomes["r0"].passed is expected, (run.route, run.tally, run.outcomes)


@pytest.mark.parametrize("csv_mode", ["auto", "duckdb", "parquet"])
@pytest.mark.parametrize("pushdown", ["on", "off"])
def test_header_only_csv_keeps_its_column_names(csv_mode, pushdown, tmp_path: Path):
    """DuckDB guessed whether a lone row is a header; names that read as values
    (``t`` and ``f`` are booleans to it) became column0, column1, ... and the
    validation raised."""
    import kontra

    path = tmp_path / "empty.csv"
    path.write_text("i,t,f\n")
    result = kontra.validate(str(path), rules=[r.not_null("i"), r.not_null("t")], save=False,
                             csv_mode=csv_mode, pushdown=pushdown, tally=True)
    assert [x.passed for x in result.rules] == [True, True]


@pytest.mark.parametrize("csv_mode", ["auto", "duckdb", "parquet"])
@pytest.mark.parametrize("pushdown", ["on", "off"])
def test_quoted_empty_csv_field_is_an_empty_string(csv_mode, pushdown, tmp_path: Path):
    """Polars reads ``""`` as an empty string and only an unquoted empty field
    as NULL; DuckDB read both as NULL, so not_null disagreed by route."""
    import kontra

    path = tmp_path / "strings.csv"
    path.write_text('i,s\n0,""\n1,\n2,a\n')
    result = kontra.validate(str(path), rules=[r.not_null("s")], save=False,
                             csv_mode=csv_mode, pushdown=pushdown, tally=True)
    assert (result.rules[0].passed, result.rules[0].failed_count) == (False, 1)


@pytest.mark.parametrize("csv_mode", ["auto", "duckdb", "parquet"])
@pytest.mark.parametrize("pushdown", ["on", "off"])
def test_lone_apostrophes_do_not_become_quotes(csv_mode, pushdown, tmp_path: Path):
    """Fields holding just ' made DuckDB sniff it as the quote character and
    read two rows as one; Polars always quotes with "."""
    import kontra

    path = tmp_path / "apostrophes.csv"
    path.write_text("i,s,c\n,,x\n,,x\n0,',x\n,',x\n")
    result = kontra.validate(str(path), rules=[r.not_null("i"), r.min_rows(4)], save=False,
                             csv_mode=csv_mode, pushdown=pushdown, tally=True)
    assert [(x.passed, x.failed_count) for x in result.rules] == [(False, 3), (True, 0)]


def test_csv_dates_stay_text_on_every_route(tmp_path: Path):
    """DuckDB read date- and timestamp-looking CSV text as DATE and TIMESTAMP,
    so the two values below were equal there. Polars keeps both as strings, and
    the shorter string sorts first, so ``d >= t`` fails on its one row."""
    import kontra

    path = tmp_path / "dates.csv"
    path.write_text("d,t\n2000-01-01,2000-01-01 00:00:00\n")
    got = {}
    for csv_mode in ("auto", "duckdb", "parquet"):
        for pushdown in ("on", "off"):
            result = kontra.validate(str(path), rules=[r.compare("d", "t", ">=")], save=False,
                                     csv_mode=csv_mode, pushdown=pushdown, tally=True)
            got[csv_mode, pushdown] = (result.rules[0].passed, result.rules[0].failed_count)
    assert got == {key: (False, 1) for key in got}


def test_nan_bounds_are_rejected():
    """A NaN bound orders nothing; routes disagreed on what it meant."""
    from kontra.errors import RuleParameterError
    from kontra.rule_defs.builtin.conditional_range import ConditionalRangeRule
    from kontra.rule_defs.builtin.range import RangeRule

    with pytest.raises(RuleParameterError):
        RangeRule("range", {"column": "f", "max": NAN})
    with pytest.raises(RuleParameterError):
        ConditionalRangeRule("conditional_range", {"column": "f", "when": "c == 'x'", "min": NAN})


def test_postgres_range_on_huge_numeric(tmp_path: Path):
    """The NaN test must not push a NUMERIC column through float8, which
    overflows above ~1e308."""
    import kontra

    from .backends import Postgres

    backend = next((b for b in configured() if isinstance(b, Postgres)), None)
    if backend is None:
        pytest.skip("set KONTRA_FUZZ_POSTGRES")
    with backend._connect() as conn:
        conn.execute("CREATE TABLE public.fuzz_huge_numeric (x numeric)")
        try:
            conn.execute("INSERT INTO public.fuzz_huge_numeric VALUES (1e400), (5)")
            u = backend.url
            uri = (f"postgres://{u.username}:{u.password}@{u.hostname}:{u.port}"
                   f"{u.path}/public.fuzz_huge_numeric")
            for tally in (True, False):
                result = kontra.validate(uri, rules=[r.range("x", min=0)], save=False,
                                         preplan="off", pushdown="on", tally=tally)
                assert (result.rules[0].passed, result.rules[0].source) == (True, "sql")
        finally:
            conn.execute("DROP TABLE public.fuzz_huge_numeric")
