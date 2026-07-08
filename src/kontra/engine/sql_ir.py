# src/kontra/engine/sql_ir.py
"""
A tiny SQL condition/expression IR for the pushdown executors.

The goal is maintainability: adding a rule should mean writing one IR-building
function (in ``sql_utils``), and adding a SQL dialect should mean writing one
``Renderer`` subclass here. All dialect-divergent rendering (identifier
quoting, string/literal escaping, CAST-to-text, string length, regex support,
the EXISTS wrapper, and the dataset aggregates) lives in exactly one place: the
``Renderer`` hierarchy.

This is deliberately NOT a general SQL AST or parser. It models only the
handful of constructs the 18 built-in rules actually emit. Builders construct
IR nodes; each node knows how to render itself given a ``Renderer`` that
supplies the dialect primitives.

Stdlib only — no heavy dependencies are imported here (import-time cost matters
for cold start; see the lazy-loading invariant in CLAUDE.md).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, List, Literal, Optional

Dialect = Literal["duckdb", "postgres", "sqlserver", "clickhouse"]

# SQL comparison operators (Python op -> SQL op)
SQL_OP_MAP = {
    ">": ">",
    ">=": ">=",
    "<": "<",
    "<=": "<=",
    "==": "=",
    "!=": "<>",
}


# =============================================================================
# Identifier and Literal Escaping (dialect primitives)
# =============================================================================

def esc_ident(name: str, dialect: Dialect = "duckdb") -> str:
    """
    Escape a SQL identifier (column name, table name) for the given dialect.

    - DuckDB/PostgreSQL: "name" with " doubled
    - SQL Server: [name] with ] doubled
    - ClickHouse: `name` with ` doubled AND \\ doubled — ClickHouse honors
      C-style backslash escapes inside backtick identifiers, so an un-doubled
      backslash would be consumed (e.g. `a\\b` -> a + backspace).
    """
    if dialect == "sqlserver":
        return "[" + name.replace("]", "]]") + "]"
    if dialect == "clickhouse":
        return "`" + name.replace("\\", "\\\\").replace("`", "``") + "`"
    return '"' + name.replace('"', '""') + '"'


def lit_str(value: str, dialect: Dialect = "duckdb") -> str:
    """Escape a string literal for SQL (single quotes).

    ClickHouse's literal parser also consumes C-style backslash escapes before
    the value reaches LIKE/regex, so a backslash must be doubled there or the
    pattern is silently corrupted (a wildcard could be eaten -> false pass).
    """
    if dialect == "clickhouse":
        return "'" + value.replace("\\", "\\\\").replace("'", "''") + "'"
    return "'" + value.replace("'", "''") + "'"


def lit_value(value: Any, dialect: Dialect = "duckdb") -> str:
    """Convert a Python value to a SQL literal."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, str):
        return lit_str(value, dialect)
    if isinstance(value, (int, float)):
        return str(value)
    return lit_str(str(value), dialect)


def escape_like_pattern(value: str, escape_char: str = "\\") -> str:
    """
    Escape special characters in a LIKE pattern value.

    LIKE special characters: %, _, and the escape character itself.
    """
    # Order matters: escape the escape char first
    for c in (escape_char, "%", "_"):
        value = value.replace(c, escape_char + c)
    return value


def val_list(values: List[Any], dialect: Dialect) -> str:
    """Comma-separated SQL literals for an IN (...) list. Skips None."""
    return ", ".join(
        lit_str(str(v), dialect) if isinstance(v, str) else str(v)
        for v in values
        if v is not None
    )


# =============================================================================
# IR Nodes
# =============================================================================
#
# Two flavours share a base: "expression" nodes render to a SQL scalar, and
# "condition" nodes render to a SQL boolean fragment. They aren't separated in
# the type system because the renderer treats them uniformly (both produce a
# string given a Renderer). Boolean combinators (And/Or) do NOT add parentheses
# automatically — callers wrap with Group() exactly where the original
# hand-written SQL did, which keeps output byte-identical.


class Node:
    """Base IR node. Subclasses render themselves via ``sql(renderer)``."""

    def sql(self, r: "Renderer") -> str:  # pragma: no cover - overridden
        raise NotImplementedError


@dataclass(frozen=True)
class Col(Node):
    """A column reference (identifier-quoted per dialect)."""

    name: str

    def sql(self, r: "Renderer") -> str:
        return r.ident(self.name)


@dataclass(frozen=True)
class RawExpr(Node):
    """A pre-formatted scalar fragment inserted verbatim (e.g. a numeric bound)."""

    text: str

    def sql(self, r: "Renderer") -> str:
        return self.text


@dataclass(frozen=True)
class Lit(Node):
    """A Python value rendered as a SQL literal via ``lit_value``."""

    value: Any

    def sql(self, r: "Renderer") -> str:
        return lit_value(self.value, r.dialect)


@dataclass(frozen=True)
class CastText(Node):
    """Cast an inner expression to text for value-list comparison."""

    inner: Node

    def sql(self, r: "Renderer") -> str:
        return r.cast_text(self.inner.sql(r))


@dataclass(frozen=True)
class LenOf(Node):
    """String-length of an inner expression (LEN/LENGTH per dialect)."""

    inner: Node

    def sql(self, r: "Renderer") -> str:
        return r.len_of(self.inner.sql(r))


@dataclass(frozen=True)
class IsNull(Node):
    inner: Node

    def sql(self, r: "Renderer") -> str:
        return f"{self.inner.sql(r)} IS NULL"


@dataclass(frozen=True)
class IsNotNull(Node):
    inner: Node

    def sql(self, r: "Renderer") -> str:
        return f"{self.inner.sql(r)} IS NOT NULL"


@dataclass(frozen=True)
class Cmp(Node):
    """A binary comparison; ``op`` is a Python op mapped through SQL_OP_MAP."""

    left: Node
    op: str
    right: Node

    def sql(self, r: "Renderer") -> str:
        sql_op = SQL_OP_MAP.get(self.op, self.op)
        return f"{self.left.sql(r)} {sql_op} {self.right.sql(r)}"


@dataclass(frozen=True)
class Not(Node):
    inner: Node

    def sql(self, r: "Renderer") -> str:
        return f"NOT ({self.inner.sql(r)})"


@dataclass(frozen=True)
class In(Node):
    """``expr IN (...)`` / ``expr NOT IN (...)``. None values are skipped."""

    expr: Node
    values: List[Any]
    negate: bool = False

    def sql(self, r: "Renderer") -> str:
        keyword = "NOT IN" if self.negate else "IN"
        return f"{self.expr.sql(r)} {keyword} ({val_list(self.values, r.dialect)})"


@dataclass(frozen=True)
class Like(Node):
    """``expr LIKE 'pattern'`` / ``expr NOT LIKE 'pattern'`` with ESCAPE '\\'."""

    expr: Node
    pattern: str
    negate: bool = False

    def sql(self, r: "Renderer") -> str:
        keyword = "NOT LIKE" if self.negate else "LIKE"
        # lit_str quote-escapes the pattern: LIKE metachars (%, _, \) were
        # already escaped by escape_like_pattern, but a single quote in the
        # user substring would otherwise break out of the literal (SQL
        # injection → silent false PASS). lit_str only doubles ', leaving the
        # wildcards and \-escapes intact.
        return (
            f"{self.expr.sql(r)} {keyword} {lit_str(self.pattern, r.dialect)}"
            f"{r.like_escape()}"
        )


@dataclass(frozen=True)
class RegexNoMatch(Node):
    """NULL-or-no-regex-match condition; regex support differs per dialect."""

    col: Node
    pattern: str

    def sql(self, r: "Renderer") -> str:
        return r.regex_no_match(self.col.sql(r), self.pattern)


@dataclass(frozen=True)
class Group(Node):
    """Parenthesize a condition: ``(inner)``."""

    inner: Node

    def sql(self, r: "Renderer") -> str:
        return f"({self.inner.sql(r)})"


@dataclass(frozen=True)
class Raw(Node):
    """A verbatim boolean fragment (custom WHERE, or a literal like ``0=1``)."""

    text: str

    def sql(self, r: "Renderer") -> str:
        return self.text


class And(Node):
    """Conjunction joined by ' AND ' with no automatic parentheses."""

    def __init__(self, *parts: Node) -> None:
        self.parts = parts

    def sql(self, r: "Renderer") -> str:
        return " AND ".join(p.sql(r) for p in self.parts)


class Or(Node):
    """Disjunction joined by ' OR ' with no automatic parentheses."""

    def __init__(self, *parts: Node) -> None:
        self.parts = parts

    def sql(self, r: "Renderer") -> str:
        return " OR ".join(p.sql(r) for p in self.parts)


# =============================================================================
# Renderers (one class per dialect — the only place dialects diverge)
# =============================================================================

class Renderer:
    """
    Base renderer. Defaults match DuckDB; PostgreSQL and SQL Server override the
    handful of primitives that differ. To add a new dialect, subclass this and
    override whichever primitives diverge — nothing else in the codebase needs
    to change.
    """

    dialect: Dialect = "duckdb"

    # --- identifier & literal primitives ---
    def ident(self, name: str) -> str:
        return esc_ident(name, self.dialect)

    # --- expression primitives (overridable) ---
    def cast_text(self, inner_sql: str) -> str:
        """Cast to text for value-list comparison (no-op on DuckDB)."""
        return inner_sql

    def len_of(self, inner_sql: str) -> str:
        return f"LENGTH({inner_sql})"

    def like_escape(self) -> str:
        """Trailing `` ESCAPE '\\' `` clause for LIKE. escape_like_pattern
        emits backslash-escaped metachars; most dialects need the explicit
        ESCAPE char declared. ClickHouse rejects the clause (backslash is its
        implicit escape) and overrides this to return empty."""
        return " ESCAPE '\\'"

    def regex_no_match(self, col_sql: str, pattern: str) -> str:
        escaped_pattern = pattern.replace("'", "''")
        return (
            f"{col_sql} IS NULL "
            f"OR NOT regexp_matches(CAST({col_sql} AS VARCHAR), '{escaped_pattern}')"
        )

    # --- statement-shaped builders (overridable) ---
    def sum_case(self, conditions: List[str], rule_id: str) -> str:
        """
        SUM(CASE WHEN c1 THEN 1 [WHEN c2 THEN 1 ...] ELSE 0 END) AS rule_id.

        A single condition yields the common tally aggregate; multiple
        conditions map several disjoint predicates to the same 1 (used by
        allowed_values when NULL is not allowed).
        """
        whens = " ".join(f"WHEN {c} THEN 1" for c in conditions)
        return f"SUM(CASE {whens} ELSE 0 END) AS {self.ident(rule_id)}"

    def exists_wrap(self, inner: str, rule_id: str) -> str:
        """Wrap an inner SELECT so it yields 1 when a violation exists."""
        return f"EXISTS ({inner} LIMIT 1) AS {self.ident(rule_id)}"

    def min_rows(self, n: int, rule_id: str) -> str:
        return f"GREATEST(0, {n} - COUNT(*)) AS {self.ident(rule_id)}"

    def max_rows(self, n: int, rule_id: str) -> str:
        return f"GREATEST(0, COUNT(*) - {n}) AS {self.ident(rule_id)}"

    def freshness(self, col_sql: str, secs: int, rule_id: str) -> str:
        threshold = f"(NOW() - INTERVAL '{secs} seconds')"
        return (
            f"CASE WHEN MAX({col_sql}) >= {threshold} THEN 0 ELSE 1 END "
            f"AS {self.ident(rule_id)}"
        )


class DuckDBRenderer(Renderer):
    dialect: Dialect = "duckdb"


class PostgresRenderer(Renderer):
    dialect: Dialect = "postgres"

    def cast_text(self, inner_sql: str) -> str:
        return f"{inner_sql}::text"

    def regex_no_match(self, col_sql: str, pattern: str) -> str:
        escaped_pattern = pattern.replace("'", "''")
        return f"{col_sql} IS NULL OR NOT ({col_sql}::text ~ '{escaped_pattern}')"


class SqlServerRenderer(Renderer):
    dialect: Dialect = "sqlserver"

    def cast_text(self, inner_sql: str) -> str:
        return f"CAST({inner_sql} AS NVARCHAR(MAX))"

    def len_of(self, inner_sql: str) -> str:
        # Plain LEN() ignores trailing spaces, so "abc   " reports length 3 —
        # diverging from Polars str.len_chars() and DuckDB/Postgres LENGTH(),
        # which count them. LEN(x + N'.') - 1 counts trailing spaces while
        # staying a character count (not DATALENGTH's byte count). NULL
        # propagates (NULL + ... = NULL), and the length rule's IS NULL guard
        # handles nulls separately.
        return f"(LEN({inner_sql} + N'.') - 1)"

    def regex_no_match(self, col_sql: str, pattern: str) -> str:
        # SQL Server has no regex; PATINDEX with LIKE-style patterns (limited).
        escaped_pattern = pattern.replace("'", "''")
        return (
            f"{col_sql} IS NULL "
            f"OR PATINDEX('%{escaped_pattern}%', CAST({col_sql} AS NVARCHAR(MAX))) = 0"
        )

    def exists_wrap(self, inner: str, rule_id: str) -> str:
        # SQL Server has no boolean EXISTS in a select list; needs CASE wrapper.
        return f"(SELECT CASE WHEN EXISTS ({inner}) THEN 1 ELSE 0 END) AS {self.ident(rule_id)}"

    def min_rows(self, n: int, rule_id: str) -> str:
        # SQL Server doesn't have GREATEST.
        return f"CASE WHEN COUNT(*) >= {n} THEN 0 ELSE {n} - COUNT(*) END AS {self.ident(rule_id)}"

    def max_rows(self, n: int, rule_id: str) -> str:
        return f"CASE WHEN COUNT(*) <= {n} THEN 0 ELSE COUNT(*) - {n} END AS {self.ident(rule_id)}"

    def freshness(self, col_sql: str, secs: int, rule_id: str) -> str:
        threshold = f"DATEADD(SECOND, -{secs}, GETUTCDATE())"
        return (
            f"CASE WHEN MAX({col_sql}) >= {threshold} THEN 0 ELSE 1 END "
            f"AS {self.ident(rule_id)}"
        )


class ClickHouseRenderer(Renderer):
    dialect: Dialect = "clickhouse"

    def cast_text(self, inner_sql: str) -> str:
        # toString handles any type -> String; needed so numeric columns
        # compare against a string value-list. NULL propagates through
        # toString, and the value-list conditions guard NULL separately.
        return f"toString({inner_sql})"

    def len_of(self, inner_sql: str) -> str:
        # length() counts BYTES on ClickHouse; lengthUTF8() counts characters,
        # matching Polars str.len_chars() and the other dialects' LENGTH().
        return f"lengthUTF8({inner_sql})"

    def like_escape(self) -> str:
        # ClickHouse LIKE has no ESCAPE clause (it errors); backslash is the
        # implicit escape, so the \-escaped pattern already works as-is.
        return ""

    def regex_no_match(self, col_sql: str, pattern: str) -> str:
        # ClickHouse has native regex via match() (RE2). Double backslashes so
        # the pattern survives ClickHouse's literal parser intact (otherwise
        # r"a\\b" would collapse and RE2 would see \b). Patterns using the
        # ASCII/Unicode-ambiguous shorthand classes (\w \d \s \b) are NOT
        # pushed here — the executor defers them to Polars (see
        # ClickHouseSqlExecutor); this handles literal/anchor/escaped-meta
        # patterns, which RE2 and Polars agree on.
        escaped_pattern = pattern.replace("\\", "\\\\").replace("'", "''")
        return f"{col_sql} IS NULL OR NOT match(toString({col_sql}), '{escaped_pattern}')"


_RENDERERS = {
    "duckdb": DuckDBRenderer(),
    "postgres": PostgresRenderer(),
    "sqlserver": SqlServerRenderer(),
    "clickhouse": ClickHouseRenderer(),
}


def renderer_for(dialect: Dialect) -> Renderer:
    """Return the (shared, stateless) renderer for a dialect."""
    return _RENDERERS[dialect]


# =============================================================================
# Small IR-building helpers shared by rule builders
# =============================================================================

def bounds(expr: Node, min_val: Optional[Any], max_val: Optional[Any]) -> List[Node]:
    """Out-of-bounds comparison nodes for an expression (either bound optional).

    Rendered via ``lit_value``: numeric bounds inline bare (``... < 5``) exactly
    as before, while string/temporal bounds (e.g. a date "2020-01-01") are
    quoted (``... < '2020-01-01'``) so the database casts them instead of
    parsing the literal as integer arithmetic and erroring out — which would
    poison the entire pushdown batch.
    """
    conds: List[Node] = []
    if min_val is not None:
        conds.append(Cmp(expr, "<", Lit(min_val)))
    if max_val is not None:
        conds.append(Cmp(expr, ">", Lit(max_val)))
    return conds


def when_condition(when_column: str, when_op: str, when_value: Any) -> Node:
    """
    IR for a conditional rule's WHEN clause.

    NULL condition values only make sense with ==/!= (IS NULL / IS NOT NULL);
    any other operator against NULL is always false.
    """
    when_col = Col(when_column)
    if when_value is None:
        if when_op == "==":
            return IsNull(when_col)
        if when_op == "!=":
            return IsNotNull(when_col)
        return Raw("1=0")
    return Cmp(when_col, when_op, Lit(when_value))


def allowed_values_violation(column: str, values: List[Any]) -> Node:
    """
    Violation condition for allowed_values (used by the EXISTS path).

    NULLs are violations unless None is explicitly in values, matching
    Polars behavior for tier equivalence.
    """
    col = Col(column)
    null_allowed = None in values
    non_null_values = [v for v in values if v is not None]

    if not non_null_values:
        # Only None in values — everything non-null is a violation
        return IsNotNull(col)
    in_check = In(CastText(col), non_null_values, negate=True)
    if null_allowed:
        return And(IsNotNull(col), in_check)
    return Or(IsNull(col), in_check)
