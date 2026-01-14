# src/contra/rules/base.py
from abc import ABC, abstractmethod
from typing import Any, Dict, Set
import polars as pl

class BaseRule(ABC):
    """
    Abstract base class for all validation rules.
    """

    name: str
    params: Dict[str, Any]

    def __init__(self, name: str, params: Dict[str, Any]):
        self.name = name
        self.params = params
        # rule_id is set by the factory (based on id/name/column)
        self.rule_id: str = name
        # severity is set by the factory (from contract spec)
        self.severity: str = "blocking"
    
    def __str__(self) -> str:
        return f"{self.name}({self.params})"

    def __repr__(self) -> str:
        return str(self)

    @abstractmethod
    def validate(self, df: pl.DataFrame) -> Dict[str, Any]:
        """Executes validation on a Polars DataFrame and returns a result dict."""
        ...

    # NEW: rules can declare columns they need even if not vectorizable
    def required_columns(self) -> Set[str]:
        """
        Columns this rule requires to run `validate()`.
        Default: none. Override in dataset/column rules that read specific columns.
        """
        return set()

    def _get_required_param(self, key: str, param_type: type = str) -> Any:
        """
        Get a required parameter, raising a clear error if missing or wrong type.

        Args:
            key: Parameter name
            param_type: Expected type (default: str)

        Returns:
            The parameter value

        Raises:
            ValueError: If parameter is missing or has wrong type
        """
        if key not in self.params:
            raise ValueError(
                f"Rule '{self.name}' requires parameter '{key}' but it was not provided"
            )
        value = self.params[key]
        if not isinstance(value, param_type):
            raise ValueError(
                f"Rule '{self.name}' parameter '{key}' must be {param_type.__name__}, "
                f"got {type(value).__name__}"
            )
        return value

    def _get_optional_param(self, key: str, default: Any = None) -> Any:
        """
        Get an optional parameter with a default value.

        Args:
            key: Parameter name
            default: Default value if not provided

        Returns:
            The parameter value or default
        """
        return self.params.get(key, default)

    def _failures(self, df: pl.DataFrame, mask: pl.Series, message: str) -> Dict[str, Any]:
        """Utility to summarize failing rows."""
        failed_count = mask.sum()
        return {
            "rule_id": getattr(self, "rule_id", self.name),
            "passed": failed_count == 0,
            "failed_count": int(failed_count),
            "message": message if failed_count > 0 else "Passed",
        }
