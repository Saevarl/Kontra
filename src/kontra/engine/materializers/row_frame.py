# src/kontra/engine/materializers/row_frame.py
"""
Build a Polars frame from DBAPI rows, falling back to the declared column types.

Polars infers each dtype from the values. That holds while a column has
values, but an empty result has none (it loaded as all Utf8) and an all-NULL
column has only NULLs (Polars' Null dtype). A dtype rule then sees the wrong
type, and comparisons on a Null column raise. Those columns take the type the
database declares instead, mapped to the dtype Polars infers for a populated
column of that type, so a table loads with the same dtypes empty, all-NULL or
full.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import polars as pl

# name -> Polars dtype, or None where the declared type has no fixed mapping
DeclaredTypes = Mapping[str, Any]


def frame_from_rows(
    rows: Sequence[Sequence[Any]],
    col_names: list[str],
    declared: Callable[[], DeclaredTypes],
) -> pl.DataFrame:
    """Row-oriented frame; ``declared()`` is called only when a column needs it.

    Columns with values keep the inferred dtype, exactly as before. Unknown
    declared types keep today's fallback: Utf8 when empty, Null when all-NULL.
    """
    import polars as pl

    if not rows:
        types = declared()
        return pl.DataFrame(schema={n: types.get(n) or pl.Utf8 for n in col_names})

    # Scan every row: a column NULL for its first 100 rows would otherwise be
    # inferred as Null and fail on its first value.
    df = pl.DataFrame(rows, schema=col_names, orient="row", infer_schema_length=None)
    null_cols = [n for n, dtype in df.schema.items() if dtype == pl.Null]
    if not null_cols:
        return df
    types = declared()
    return df.with_columns(pl.col(n).cast(types[n]) for n in null_cols if types.get(n) is not None)
