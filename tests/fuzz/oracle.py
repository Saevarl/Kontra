"""Run one dataset and rule set through every execution route and compare.

Every route must give each rule the same verdict. Where a route counts exactly
(``tally=True`` and not resolved from metadata), it must give the same count as
every other exact route. A fail-fast count is a lower bound: zero exactly when
the rule passes, and never above the exact count.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import polars as pl

import kontra


@dataclass(frozen=True)
class Outcome:
    passed: bool
    failed_count: int
    source: str
    message: str


@dataclass
class Run:
    route: str
    tally: bool
    outcomes: dict[str, Outcome] = field(default_factory=dict)
    error: str | None = None


# A route takes (frame, parquet path, rules, tally) and returns a ValidationResult.
Route = Callable[[pl.DataFrame, str, list, bool], Any]


def _file(preplan: str, pushdown: str) -> Route:
    def run(df, path, rules, tally):
        return kontra.validate(path, rules=rules, save=False, tally=tally,
                               preplan=preplan, pushdown=pushdown)
    return run


def _frame(df, path, rules, tally):
    return kontra.validate(df, rules=rules, save=False, tally=tally)


LOCAL_ROUTES: dict[str, Route] = {
    "polars": _file("off", "off"),
    "duckdb": _file("off", "on"),
    "metadata+polars": _file("on", "off"),
    "metadata+duckdb": _file("on", "on"),
    "dataframe": _frame,
}


def run_route(name: str, route: Route, df, path, rules, tally) -> Run:
    out = Run(name, tally)
    try:
        result = route(df, path, rules, tally)
    except Exception as e:  # noqa: BLE001 - every failure is a finding
        out.error = f"{type(e).__name__}: {e}"
        return out
    for r in result.rules:
        out.outcomes[r.rule_id] = Outcome(
            bool(r.passed), int(r.failed_count), r.source, r.message)
    return out


def exact(run: Run, outcome: Outcome) -> bool:
    return run.tally and outcome.source != "metadata"


def disagreements(runs: list[Run]) -> list[str]:
    """Describe every way the runs disagree. Empty means they all agree."""
    problems: list[str] = []
    errors = {(r.route, r.tally): r.error for r in runs if r.error}
    if errors and len(errors) != len(runs):
        problems.append(f"some routes raised: {errors}")
    ok = [r for r in runs if not r.error]
    if not ok:
        return problems

    ids = set().union(*(r.outcomes for r in ok))
    for rule_id in sorted(ids):
        seen = {(r.route, r.tally): r.outcomes.get(rule_id) for r in ok}
        missing = [k for k, v in seen.items() if v is None]
        if missing:
            problems.append(f"{rule_id}: missing from {missing}")
            continue
        verdicts = {k: v.passed for k, v in seen.items()}
        if len(set(verdicts.values())) > 1:
            problems.append(f"{rule_id}: verdicts differ {_show(seen)}")
            continue
        counts = {(r.route, r.tally): r.outcomes[rule_id].failed_count
                  for r in ok if exact(r, r.outcomes[rule_id])}
        if len(set(counts.values())) > 1:
            problems.append(f"{rule_id}: exact counts differ {_show(seen)}")
            continue
        truth = next(iter(counts.values()), None)
        for k, v in seen.items():
            if v.passed != (v.failed_count == 0):
                problems.append(f"{rule_id}: {k} says passed={v.passed} with "
                                f"failed_count={v.failed_count}")
            elif truth is not None and v.failed_count > truth:
                problems.append(f"{rule_id}: {k} counts {v.failed_count}, above the "
                                f"exact {truth}")
    return problems


def _show(seen: dict) -> str:
    return "; ".join(
        f"{route}{'/tally' if tally else ''}: {'PASS' if o.passed else 'FAIL'}"
        f"({o.failed_count}) via {o.source}"
        for (route, tally), o in seen.items())


def check(df: pl.DataFrame, rules: list, workdir: Path, row_group_size: int | None,
          statistics: bool, routes: dict[str, Route] | None = None) -> list[str]:
    path = workdir / "data.parquet"
    df.write_parquet(path, row_group_size=row_group_size, statistics=statistics)
    runs = [run_route(name, route, df, str(path), rules, tally)
            for name, route in (routes or LOCAL_ROUTES).items()
            for tally in (True, False)]
    return disagreements(runs)


def check_backend(backend, df: pl.DataFrame, rules: list, workdir: Path,
                  row_group_size: int | None, statistics: bool) -> list[str]:
    """Compare a database backend's routes with the local routes on the same data."""
    df = backend.representable(df)
    with backend.loaded(df) as uri:
        routes = {"polars": LOCAL_ROUTES["polars"], "duckdb": LOCAL_ROUTES["duckdb"],
                  **backend.routes(uri)}
        return check(df, rules, workdir, row_group_size, statistics, routes)


def _csv(pushdown: str, csv_mode: str) -> Route:
    def run(df, path, rules, tally):
        return kontra.validate(path, rules=rules, save=False, tally=tally, preplan="on",
                               pushdown=pushdown, csv_mode=csv_mode)
    return run


# CSV has no types, so its routes are compared with each other, not with Parquet.
CSV_ROUTES: dict[str, Route] = {
    "csv:polars": _csv("off", "auto"),
    "csv:duckdb": _csv("on", "duckdb"),
    "csv:parquet": _csv("on", "parquet"),
    "csv:auto": _csv("on", "auto"),
}


def check_csv(df: pl.DataFrame, rules: list, workdir: Path) -> list[str]:
    # Polars doesn't unescape a doubled quote in a CSV header ("we""ird" stays
    # we""ird), so a column whose name has a quote can't round-trip.
    if any('"' in name for name in df.columns):
        return []
    # A column with no values has no type in a CSV beyond each reader's guess
    # (Polars: string), and a literal of another type then errors in Polars
    # while DuckDB casts it. That's the open question of mismatched literals,
    # not a route bug, so such frames are left out.
    if any(df[name].null_count() == df.height for name in df.columns):
        return []
    # A CSV column's type is whatever its reader infers: DuckDB parses dates
    # and timestamps, Polars keeps them as strings. Until that's settled, leave
    # out temporal columns and dtype rules, which only measure the inference.
    temporal = {name for name, dtype in df.schema.items() if dtype.is_temporal()}
    df = df.drop(temporal)
    rules = [rule for rule in rules if rule["name"] != "dtype"
             and not temporal & {rule["params"].get(k) for k in ("column", "left", "right")}]
    if not rules:
        return []
    path = workdir / "data.csv"
    df.write_csv(path)
    runs = [run_route(name, route, df, str(path), rules, tally)
            for name, route in CSV_ROUTES.items() for tally in (True, False)]
    return disagreements(runs)
