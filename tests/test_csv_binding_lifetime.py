"""A CSV binding belongs to one validation, never to a pathname cache."""

import pytest

import kontra
from kontra import rules


@pytest.mark.parametrize("mode", ["auto", "duckdb", "parquet"])
def test_csv_rewrite_is_reinferred_between_calls(tmp_path, mode):
    path = tmp_path / "reader's source.csv"
    path.write_text('value,note\n1,"hello, world"\n2,"two\nlines"\n')
    first = kontra.validate(
        str(path),
        rules=[rules.not_null("value", tally=True)],
        csv_mode=mode,
        preplan="off",
        save=False,
    )
    assert first.passed
    # Change the schema and data at the same URI. The second call must bind
    # again, and must count the new nulls rather than reuse the earlier scan.
    path.write_text("value,other\na,one\n,two\n,three\n")
    second = kontra.validate(
        str(path),
        rules=[rules.not_null("value", tally=True)],
        csv_mode=mode,
        preplan="off",
        save=False,
    )
    assert not second.passed
    assert second.rules[0].failed_count == 2
