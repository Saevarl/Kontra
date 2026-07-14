"""Version-matched reference metadata for Kontra's built-in rules.

This module is deliberately data-only. Agent and service integrations can import
it without registering rules or loading execution dependencies.
"""

from __future__ import annotations

from copy import deepcopy
from typing import Any


_COMMON_CONTRACT_FIELDS: dict[str, Any] = {
    "shape": "Each rules entry has name and params, with optional id, severity, tally, and context.",
    "fields": {
        "id": "Optional stable rule ID. Use it when the same rule is applied more than once to the same column.",
        "severity": "blocking (default), warning, or info. Only blocking failures make result.passed false.",
        "tally": "Optional boolean override. True requests an exact violation count; false permits fail-fast execution. Unsupported by scalar/schema rules.",
        "context": "Optional consumer-owned mapping. Kontra carries it but does not interpret it.",
    },
}


def _param(
    name: str,
    type_: str,
    description: str,
    *,
    required: bool = True,
    default: Any = None,
    constraints: str | None = None,
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "name": name,
        "type": type_,
        "required": required,
        "description": description,
    }
    if not required:
        result["default"] = default
    if constraints:
        result["constraints"] = constraints
    return result


_RULE_SPECS: tuple[dict[str, Any], ...] = (
    {
        "name": "allowed_values",
        "scope": "column",
        "summary": "Require every value to belong to an explicit allowed set.",
        "fails_when": "A value is absent from values. NULL fails unless null is explicitly included in values.",
        "nulls": "Allowed only when YAML null is present in values.",
        "counting": "One violation per failing row.",
        "supports_tally": True,
        "parameters": [
            _param("column", "string", "Column to inspect.", constraints="non-empty"),
            _param("values", "list[scalar|null]", "Complete set of accepted values.", constraints="non-empty list"),
        ],
        "example": "- name: allowed_values\n  params:\n    column: status\n    values: [active, paused, null]",
        "notes": ["Membership is exact; Kontra does not normalize case or whitespace."],
    },
    {
        "name": "compare",
        "scope": "cross-column",
        "summary": "Require a row-wise comparison between two columns.",
        "fails_when": "The comparison is false, or either operand is NULL.",
        "nulls": "NULL in either column is a failure.",
        "counting": "One violation per failing row.",
        "supports_tally": True,
        "parameters": [
            _param("left", "string", "Left-hand column.", constraints="non-empty"),
            _param("right", "string", "Right-hand column.", constraints="non-empty"),
            _param("op", "string", "Comparison operator.", constraints="one of >, >=, <, <=, ==, !="),
        ],
        "example": "- name: compare\n  params:\n    left: ended_at\n    right: started_at\n    op: \">=\"",
        "notes": ["Kontra compares existing values; it does not cast columns to make them comparable."],
    },
    {
        "name": "conditional_not_null",
        "scope": "cross-column",
        "summary": "Require a column to be non-NULL on rows matching a condition.",
        "fails_when": "when is true and column is NULL. Rows where when is false do not participate.",
        "nulls": "The checked column fails only on matching rows. In when, `x == null` means IS NULL and `x != null` means IS NOT NULL.",
        "counting": "One violation per matching row whose checked value is NULL.",
        "supports_tally": True,
        "parameters": [
            _param("column", "string", "Column that becomes required.", constraints="non-empty"),
            _param("when", "condition", "Single condition in the form `column operator literal`.", constraints="operators: ==, !=, >, >=, <, <=; literals: quoted strings, numbers, true, false, null; no AND/OR"),
        ],
        "example": "- name: conditional_not_null\n  params:\n    column: shipped_at\n    when: \"status == 'shipped'\"",
        "notes": ["Comparisons other than == or != against null never activate the rule."],
    },
    {
        "name": "conditional_range",
        "scope": "cross-column",
        "summary": "Require an inclusive range only on rows matching a condition.",
        "fails_when": "when is true and the checked value is NULL, below min, or above max.",
        "nulls": "The checked column is a failure on matching rows. NULL handling inside when matches conditional_not_null.",
        "counting": "One violation per matching row outside the bounds or NULL.",
        "supports_tally": True,
        "parameters": [
            _param("column", "string", "Column whose value is bounded.", constraints="non-empty"),
            _param("when", "condition", "Single condition in the form `column operator literal`.", constraints="operators: ==, !=, >, >=, <, <=; literals: quoted strings, numbers, true, false, null; no AND/OR"),
            _param("min", "number", "Inclusive lower bound.", required=False, constraints="at least one of min or max; min <= max"),
            _param("max", "number", "Inclusive upper bound.", required=False, constraints="at least one of min or max; min <= max"),
        ],
        "example": "- name: conditional_range\n  params:\n    column: discount_percent\n    when: \"customer_type == 'premium'\"\n    min: 10\n    max: 50",
    },
    {
        "name": "contains",
        "scope": "column",
        "summary": "Require a literal substring in every value.",
        "fails_when": "The string representation does not contain substring, or the value is NULL.",
        "nulls": "NULL is a failure.",
        "counting": "One violation per failing row.",
        "supports_tally": True,
        "parameters": [
            _param("column", "string", "Column to inspect.", constraints="non-empty"),
            _param("substring", "string", "Literal substring to find.", constraints="non-empty"),
        ],
        "example": "- name: contains\n  params: { column: email, substring: '@' }",
        "notes": ["This is literal matching, not regular expression matching."],
    },
    {
        "name": "custom_sql_check",
        "scope": "dataset",
        "summary": "Count rows returned by a read-only SQL query as violations.",
        "fails_when": "The query returns more rows than threshold.",
        "nulls": "Defined entirely by the supplied SQL.",
        "counting": "The query row count is the violation count; pass when count <= threshold.",
        "supports_tally": False,
        "parameters": [
            _param("sql", "string", "SELECT query whose result rows represent violations.", constraints="read-only; use {table} for the validated dataset"),
            _param("threshold", "integer", "Maximum permitted returned rows.", required=False, default=0, constraints=">= 0"),
        ],
        "example": "- name: custom_sql_check\n  params:\n    sql: SELECT * FROM {table} WHERE balance < 0\n    threshold: 0",
        "notes": ["Use only when a built-in rule cannot express the measurement.", "The {table} placeholder is required for remote database execution."],
    },
    {
        "name": "disallowed_values",
        "scope": "column",
        "summary": "Reject values belonging to an explicit disallowed set.",
        "fails_when": "A non-NULL value belongs to values.",
        "nulls": "NULL passes, including when null appears in values.",
        "counting": "One violation per failing row.",
        "supports_tally": True,
        "parameters": [
            _param("column", "string", "Column to inspect.", constraints="non-empty"),
            _param("values", "list[scalar|null]", "Values to reject."),
        ],
        "example": "- name: disallowed_values\n  params:\n    column: status\n    values: [deleted, banned]",
    },
    {
        "name": "dtype",
        "scope": "column",
        "summary": "Require a column's physical type or logical type family.",
        "fails_when": "The actual column dtype is not a member of the requested exact type or family.",
        "nulls": "Values are not inspected; this is a schema-level rule.",
        "counting": "Binary schema result: zero on pass, dataset row count on failure.",
        "supports_tally": False,
        "parameters": [
            _param("column", "string", "Column whose dtype is inspected.", constraints="non-empty"),
            _param("type", "string", "Expected exact type or family.", constraints="exact: int8/16/32/64, uint8/16/32/64, float32/64, double, bool/boolean, date, datetime, time; families: int/integer, float, numeric, string/str/text/utf8"),
            _param("mode", "string", "Type comparison mode.", required=False, default="strict", constraints="only strict is implemented"),
        ],
        "example": "- name: dtype\n  params: { column: user_id, type: int64 }",
        "notes": ["Kontra validates without casting.", "Contract YAML uses `type`; `dtype` is only a Python-helper alias."],
    },
    {
        "name": "ends_with",
        "scope": "column",
        "summary": "Require every value to end with a literal suffix.",
        "fails_when": "The string representation does not end with suffix, or the value is NULL.",
        "nulls": "NULL is a failure.",
        "counting": "One violation per failing row.",
        "supports_tally": True,
        "parameters": [_param("column", "string", "Column to inspect.", constraints="non-empty"), _param("suffix", "string", "Literal required suffix.", constraints="non-empty")],
        "example": "- name: ends_with\n  params: { column: filename, suffix: .csv }",
    },
    {
        "name": "freshness",
        "scope": "column",
        "summary": "Require the latest timestamp to be no older than a duration.",
        "fails_when": "MAX(column) is earlier than current UTC time minus max_age, or no usable timestamp exists.",
        "nulls": "NULL rows are ignored by MAX; an empty or all-NULL column fails.",
        "counting": "Binary aggregate result; tally is unsupported.",
        "supports_tally": False,
        "parameters": [
            _param("column", "string", "Timestamp column.", constraints="non-empty"),
            _param("max_age", "duration string", "Maximum age of the latest timestamp.", constraints="positive or zero integer components using s, m, h, d, w or full unit names; components may be combined, e.g. 1h30m"),
        ],
        "example": "- name: freshness\n  params: { column: updated_at, max_age: 24h }",
        "notes": ["Naive timestamps are interpreted as UTC.", "A future latest timestamp passes the age comparison."],
    },
    {
        "name": "length",
        "scope": "column",
        "summary": "Require inclusive character-length bounds.",
        "fails_when": "The string representation is shorter than min, longer than max, or NULL.",
        "nulls": "NULL is a failure.",
        "counting": "One violation per failing row.",
        "supports_tally": True,
        "parameters": [
            _param("column", "string", "Column to inspect.", constraints="non-empty"),
            _param("min", "integer", "Inclusive minimum character count.", required=False, constraints=">= 0; at least one of min or max; min <= max"),
            _param("max", "integer", "Inclusive maximum character count.", required=False, constraints=">= 0; at least one of min or max; min <= max"),
        ],
        "example": "- name: length\n  params: { column: username, min: 3, max: 50 }",
        "notes": ["Length counts Unicode characters after conversion to string."],
    },
    {
        "name": "max_rows",
        "scope": "dataset",
        "summary": "Require at most a specified number of rows.",
        "fails_when": "Dataset row count is greater than threshold.",
        "nulls": "Not applicable.",
        "counting": "Violation count is actual rows minus threshold.",
        "supports_tally": False,
        "parameters": [_param("threshold", "integer", "Inclusive maximum row count.", constraints=">= 0")],
        "example": "- name: max_rows\n  params: { threshold: 1000000 }",
    },
    {
        "name": "min_rows",
        "scope": "dataset",
        "summary": "Require at least a specified number of rows.",
        "fails_when": "Dataset row count is less than threshold.",
        "nulls": "Not applicable.",
        "counting": "Violation count is threshold minus actual rows.",
        "supports_tally": False,
        "parameters": [_param("threshold", "integer", "Inclusive minimum row count.", constraints=">= 0")],
        "example": "- name: min_rows\n  params: { threshold: 1 }",
    },
    {
        "name": "not_null",
        "scope": "column",
        "summary": "Reject NULL values, optionally including floating-point NaN.",
        "fails_when": "The value is NULL, or is NaN when include_nan is true.",
        "nulls": "NULL always fails. NaN passes by default and fails only with include_nan: true on float columns.",
        "counting": "One violation per failing row.",
        "supports_tally": True,
        "parameters": [
            _param("column", "string", "Column to inspect.", constraints="non-empty"),
            _param("include_nan", "boolean", "Treat float NaN as missing.", required=False, default=False),
        ],
        "example": "- name: not_null\n  params: { column: user_id, include_nan: false }",
    },
    {
        "name": "range",
        "scope": "column",
        "summary": "Require values within inclusive numeric or temporal bounds.",
        "fails_when": "The value is below min, above max, NULL, or floating-point NaN.",
        "nulls": "NULL and floating-point NaN are failures.",
        "counting": "One violation per failing row.",
        "supports_tally": True,
        "parameters": [
            _param("column", "string", "Column to inspect.", constraints="non-empty"),
            _param("min", "number|ISO date|ISO datetime", "Inclusive lower bound.", required=False, constraints="at least one of min or max; min <= max"),
            _param("max", "number|ISO date|ISO datetime", "Inclusive upper bound.", required=False, constraints="at least one of min or max; min <= max"),
        ],
        "example": "- name: range\n  params: { column: age, min: 0, max: 120 }",
        "notes": ["ISO date/datetime strings are coerced only for temporal columns."],
    },
    {
        "name": "regex",
        "scope": "column",
        "summary": "Require a Rust-compatible regular expression to match each value.",
        "fails_when": "The string representation has no regex match, or the value is NULL.",
        "nulls": "NULL is a failure.",
        "counting": "One violation per failing row.",
        "supports_tally": True,
        "parameters": [_param("column", "string", "Column to inspect.", constraints="non-empty"), _param("pattern", "string", "Regular expression searched within the value.", constraints="valid Rust regex; look-around and backreferences are unsupported")],
        "example": "- name: regex\n  params: { column: email, pattern: '^[^@]+@[^@]+$' }",
        "notes": ["Matching searches the value; use ^ and $ when a full-string match is required."],
    },
    {
        "name": "starts_with",
        "scope": "column",
        "summary": "Require every value to start with a literal prefix.",
        "fails_when": "The string representation does not start with prefix, or the value is NULL.",
        "nulls": "NULL is a failure.",
        "counting": "One violation per failing row.",
        "supports_tally": True,
        "parameters": [_param("column", "string", "Column to inspect.", constraints="non-empty"), _param("prefix", "string", "Literal required prefix.", constraints="non-empty")],
        "example": "- name: starts_with\n  params: { column: url, prefix: 'https://' }",
    },
    {
        "name": "unique",
        "scope": "column",
        "summary": "Require non-NULL values to occur at most once.",
        "fails_when": "A non-NULL value occurs more than once.",
        "nulls": "NULL values are ignored and may repeat.",
        "counting": "Counts extra non-NULL occurrences: a value appearing N times contributes N - 1 violations. Samples may show every row participating in a duplicate group.",
        "supports_tally": True,
        "parameters": [_param("column", "string", "Column to inspect.", constraints="non-empty")],
        "example": "- name: unique\n  params: { column: email }",
    },
)


_BY_NAME = {spec["name"]: spec for spec in _RULE_SPECS}


def list_rule_summaries() -> list[dict[str, Any]]:
    """Return the compact, backwards-compatible built-in rule index."""
    result = []
    for spec in sorted(_RULE_SPECS, key=lambda item: item["name"]):
        params = {
            parameter["name"]: (
                "required"
                if parameter["required"]
                else f"optional (default: {parameter.get('default')!r})"
            )
            for parameter in spec["parameters"]
        }
        result.append({
            "name": spec["name"],
            "description": spec["summary"],
            "params": params,
            "scope": spec["scope"],
        })
    return result


def describe_rule_spec(name: str) -> dict[str, Any]:
    """Return the exact reference entry for one built-in rule."""
    normalized = name.strip().lower()
    try:
        spec = deepcopy(_BY_NAME[normalized])
    except KeyError:
        available = ", ".join(sorted(_BY_NAME))
        raise ValueError(f"Unknown Kontra rule {name!r}. Available rules: {available}") from None
    spec["contract"] = deepcopy(_COMMON_CONTRACT_FIELDS)
    return spec
