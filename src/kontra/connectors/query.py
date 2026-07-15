# src/kontra/connectors/query.py
"""Query source: a dataset defined by a read-only SQL SELECT against an engine.

A ``Query`` is the read-only twin of the ``custom_sql_check`` escape hatch,
promoted to a first-class source. It is accepted anywhere a Kontra source is
accepted (``compare``, ``profile``, ``profile_relationship``), so a candidate
transformation can be measured directly against a table without materializing a
physical staging table first.

The ``sql`` body runs verbatim on the engine identified by ``source``; Kontra
never rewrites it. Only single, read-only ``SELECT`` / ``WITH`` statements are
accepted.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

# Strip comments before analysing the statement (they are kept for execution).
_COMMENT = re.compile(r"/\*.*?\*/|--[^\n]*", re.DOTALL)


def validate_read_only_sql(sql: str) -> None:
    """Reject anything that is not a single read-only SELECT/WITH statement.

    Conservative by design: the query is often agent-authored, so we fail fast
    on multiple statements or leading DDL/DML rather than run them.
    """
    if not isinstance(sql, str) or not sql.strip():
        raise ValueError("Query.sql must be a non-empty SQL string.")

    bare = _COMMENT.sub(" ", sql).strip()
    body = bare.rstrip(";").strip()
    if ";" in body:
        raise ValueError(
            "Query.sql must be a single statement (remove ';' separators)."
        )

    first = body.split(None, 1)[0].lower() if body else ""
    if first not in ("select", "with"):
        raise ValueError(
            "Query.sql must be a read-only SELECT (or WITH ... SELECT); "
            f"got '{first or '?'}'."
        )


@dataclass(frozen=True)
class Query:
    """A dataset defined by a read-only SELECT against a named engine.

    Args:
        sql: A single read-only ``SELECT`` / ``WITH`` statement, in the target
            engine's dialect. Kontra runs it verbatim and never rewrites it.
        source: The engine that runs the query. A live database connection
            (psycopg / pyodbc / SQLAlchemy) today; DB URI and named-datasource
            binding are additive.
        params: Optional bound parameters. Reserved for parameterized queries;
            Kontra never string-concatenates them into the SQL.
        name: Optional label carried into result provenance.

    Example:
        >>> compare(Query("SELECT id, total FROM stg_orders", source=conn),
        ...         "warehouse.orders_current", key="id")
    """

    sql: str
    source: Any = None
    params: Optional[Dict[str, Any]] = None
    name: Optional[str] = field(default=None)

    def __post_init__(self) -> None:
        validate_read_only_sql(self.sql)
        if self.source is None:
            raise ValueError("Query(source=...) is required.")
