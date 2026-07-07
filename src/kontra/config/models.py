# src/kontra/config/models.py
"""
Contract data models (Contract, RuleSpec).

These are the models on the validate() hot path. They are deliberately plain
stdlib dataclasses with explicit validation rather than pydantic: even a
rules-only validation constructs a synthetic Contract, so keeping pydantic off
this path preserves cold-start / `import kontra` performance.

The validation surface mirrors what the previous pydantic models did so existing
callers and saved state keep working byte-for-byte:
  - `name` on RuleSpec is required (missing -> ValueError, matching pydantic's
    ValidationError which is itself a ValueError subclass).
  - String fields reject non-strings (no int -> str coercion, like pydantic).
  - `severity` is restricted to its literal set.
  - `tally` uses pydantic's lax bool coercion (1 -> True, "yes" -> True, ...).
  - `params` / `context` must be mappings; unknown keys are ignored.
  - Contract accepts `dataset` as a deprecated alias for `datasource`.
Anything invalid raises ValueError (or TypeError only where pydantic itself did),
so existing `except ValueError` handlers in the loader/API keep catching it.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field, fields
from typing import Any, Dict, List, Optional

_REQUIRED = object()  # sentinel for required fields with no default

_SEVERITIES = ("blocking", "warning", "info")

# Pydantic-core lax bool string tables (case-insensitive).
_BOOL_TRUE = frozenset({"1", "true", "t", "yes", "y", "on"})
_BOOL_FALSE = frozenset({"0", "false", "f", "no", "n", "off"})


def _require_string(value: Any, field_name: str, *, allow_none: bool) -> Any:
    """Validate a string field the way pydantic does (no int/float coercion)."""
    if value is None:
        if allow_none:
            return None
        raise ValueError(f"{field_name}: input should be a valid string, got None")
    if isinstance(value, str):
        return value
    raise ValueError(
        f"{field_name}: input should be a valid string, got {type(value).__name__}"
    )


def _require_mapping(value: Any, field_name: str) -> Dict[str, Any]:
    """Validate/copy a mapping field the way pydantic does (dict required)."""
    if isinstance(value, Mapping):
        return dict(value)
    raise ValueError(
        f"{field_name}: input should be a valid dictionary, got {type(value).__name__}"
    )


def _coerce_optional_bool(value: Any, field_name: str) -> Optional[bool]:
    """Coerce to Optional[bool] using pydantic's lax bool rules."""
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, int):  # bool already handled above
        if value == 0:
            return False
        if value == 1:
            return True
        raise ValueError(
            f"{field_name}: input should be a valid boolean, got {value!r}"
        )
    if isinstance(value, float):
        if value == 0.0:
            return False
        if value == 1.0:
            return True
        raise ValueError(
            f"{field_name}: input should be a valid boolean, got {value!r}"
        )
    if isinstance(value, str):
        token = value.strip().lower()
        if token in _BOOL_TRUE:
            return True
        if token in _BOOL_FALSE:
            return False
        raise ValueError(
            f"{field_name}: input should be a valid boolean, unable to parse {value!r}"
        )
    raise ValueError(
        f"{field_name}: input should be a valid boolean, got {type(value).__name__}"
    )


def _pick_known(cls: type, data: Mapping) -> Dict[str, Any]:
    """Select only known dataclass field names from data (ignore extras)."""
    names = {f.name for f in fields(cls)}
    return {k: v for k, v in data.items() if k in names}


@dataclass(init=False)
class RuleSpec:
    """
    Declarative specification for a rule from contract.yml

    The `context` field is for consumer-defined metadata that Kontra stores
    but does not use for validation. Consumers/agents can read context for
    routing, explanations, fix hints, etc.
    """

    name: str = _REQUIRED  # type: ignore[assignment]
    id: Optional[str] = None
    params: Dict[str, Any] = field(default_factory=dict)
    severity: str = "blocking"
    tally: Optional[bool] = None
    context: Dict[str, Any] = field(default_factory=dict)

    def __init__(
        self,
        name: Any = _REQUIRED,
        id: Any = None,
        params: Any = _REQUIRED,
        severity: Any = "blocking",
        tally: Any = None,
        context: Any = _REQUIRED,
        **_extra: Any,  # unknown keys ignored (pydantic extra='ignore')
    ) -> None:
        if name is _REQUIRED:
            raise ValueError("name: field required")
        self.name = _require_string(name, "name", allow_none=False)
        self.id = _require_string(id, "id", allow_none=True)
        self.params = _require_mapping({} if params is _REQUIRED else params, "params")
        if severity not in _SEVERITIES:
            opts = ", ".join(repr(s) for s in _SEVERITIES)
            raise ValueError(
                f"severity: input should be one of {opts}, got {severity!r}"
            )
        self.severity = severity
        self.tally = _coerce_optional_bool(tally, "tally")
        self.context = _require_mapping({} if context is _REQUIRED else context, "context")

    @classmethod
    def model_validate(cls, data: Any) -> "RuleSpec":
        if not isinstance(data, Mapping):
            raise ValueError(
                f"RuleSpec: expected a mapping, got {type(data).__name__}"
            )
        return cls(**_pick_known(cls, data))


def _coerce_rule(value: Any) -> RuleSpec:
    """Coerce a rules-list entry to a RuleSpec (accepts RuleSpec or mapping)."""
    if isinstance(value, RuleSpec):
        return value
    if isinstance(value, Mapping):
        return RuleSpec.model_validate(value)
    raise ValueError(
        f"rules: each entry should be a RuleSpec or mapping, got {type(value).__name__}"
    )


@dataclass(init=False)
class Contract:
    """
    Data contract specification.

    The `datasource` field can be:
    - A named datasource from config: "prod_db.users"
    - A file path: "./data/users.parquet"
    - A URI: "s3://bucket/users.parquet", "postgres:///public.users"
    - Omitted when data is passed directly to validate()
    """

    name: Optional[str] = None
    datasource: str = "inline"
    rules: List[RuleSpec] = field(default_factory=list)

    def __init__(
        self,
        name: Optional[str] = None,
        datasource: Any = _REQUIRED,
        rules: Optional[List[Any]] = None,
        dataset: Any = _REQUIRED,
        **_extra: Any,  # unknown keys ignored (pydantic extra='ignore')
    ) -> None:
        # Backwards compatibility: accept 'dataset' as deprecated alias for
        # 'datasource'. datasource wins when both are supplied.
        if datasource is _REQUIRED:
            datasource = "inline" if dataset is _REQUIRED else dataset

        self.name = _require_string(name, "name", allow_none=True)
        self.datasource = _require_string(datasource, "datasource", allow_none=False)

        if rules is None:
            self.rules = []
        elif isinstance(rules, str) or not isinstance(rules, (list, tuple)):
            raise ValueError(
                f"rules: input should be a valid list, got {type(rules).__name__}"
            )
        else:
            self.rules = [_coerce_rule(r) for r in rules]

    @classmethod
    def model_validate(cls, data: Any) -> "Contract":
        if not isinstance(data, Mapping):
            raise ValueError(
                f"Contract: expected a mapping, got {type(data).__name__}"
            )
        data = dict(data)
        # 'dataset' is a deprecated alias for 'datasource'.
        if "dataset" in data and "datasource" not in data:
            data["datasource"] = data.pop("dataset")
        else:
            data.pop("dataset", None)
        known = _pick_known(cls, data)
        return cls(**known)
