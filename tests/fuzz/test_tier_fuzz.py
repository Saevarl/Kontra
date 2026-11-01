"""Differential fuzzing: every execution route must agree on every rule.

Kontra can answer a rule from Parquet metadata, in DuckDB SQL, or in Polars.
The route changes how a result is measured, never what it is. These tests
generate random datasets and rules and check that the routes agree.

Run more examples locally with ``KONTRA_FUZZ_EXAMPLES=2000``.
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from .oracle import check, check_backend, check_csv
from .strategies import frames, rename, renames, rule_sets

EXAMPLES = int(os.environ.get("KONTRA_FUZZ_EXAMPLES", "60"))

pytestmark = [pytest.mark.integration]


@settings(max_examples=EXAMPLES, deadline=None,
          suppress_health_check=[HealthCheck.too_slow, HealthCheck.data_too_large])
@given(
    df=frames(),
    rules=rule_sets(),
    mapping=renames(),
    row_group_size=st.one_of(st.none(), st.integers(1, 8)),
    statistics=st.booleans(),
)
def test_local_routes_agree(df, rules, mapping, row_group_size, statistics):
    df, rules = rename(df, rules, mapping)
    with tempfile.TemporaryDirectory() as tmp:
        problems = check(df, rules, Path(tmp), row_group_size, statistics)
    assert not problems, "\n".join(problems)


BACKEND_EXAMPLES = int(os.environ.get("KONTRA_FUZZ_BACKEND_EXAMPLES", "25"))


def _backends():
    from .backends import ALL

    return [pytest.param(cls, id=cls.name) for cls in ALL]


@pytest.mark.parametrize("backend_cls", _backends())
def test_database_routes_agree(backend_cls):
    """Each database's pushdown and materialized routes agree with the local ones.

    Opt-in: set the backend's KONTRA_FUZZ_* URL (see tests/fuzz/backends.py).
    """
    url = os.environ.get(backend_cls.env)
    if not url:
        pytest.skip(f"set {backend_cls.env} to fuzz {backend_cls.name}")
    backend = backend_cls(url)
    backend.prepare()

    @settings(max_examples=BACKEND_EXAMPLES, deadline=None,
              suppress_health_check=[HealthCheck.too_slow, HealthCheck.data_too_large])
    @given(df=frames(), rules=rule_sets(), mapping=renames())
    def routes_agree(df, rules, mapping):
        df, rules = rename(df, rules, mapping)
        with tempfile.TemporaryDirectory() as tmp:
            problems = check_backend(backend, df, rules, Path(tmp), None, True)
        assert not problems, "\n".join(problems)

    routes_agree()


@settings(max_examples=EXAMPLES, deadline=None,
          suppress_health_check=[HealthCheck.too_slow, HealthCheck.data_too_large])
@given(df=frames(), rules=rule_sets(), mapping=renames())
def test_csv_routes_agree(df, rules, mapping):
    df, rules = rename(df, rules, mapping)
    with tempfile.TemporaryDirectory() as tmp:
        problems = check_csv(df, rules, Path(tmp))
    assert not problems, "\n".join(problems)
