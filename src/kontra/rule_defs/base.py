# src/kontra/rule_defs/base.py
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Dict, Literal, Optional, Set

if TYPE_CHECKING:
    import polars as pl


# Rule scope types
RuleScope = Literal["column", "cross_column", "dataset", "schema", "custom"]


class BaseRule(ABC):
    """
    Abstract base class for all validation rules.

    Class attributes (override in subclasses):
        rule_scope: Category of rule ("column", "cross_column", "dataset", "schema", "custom")
        supports_tally: Whether this rule can count all violations vs early-stop
    """

    name: str
    params: Dict[str, Any]

    # Rule classification - override in subclasses
    rule_scope: RuleScope = "column"
    supports_tally: bool = True

    def __init__(self, name: str, params: Dict[str, Any]):
        self.name = name
        self.params = params
        # rule_id is set by the factory (based on id/name/column)
        self.rule_id: str = name
        # severity is set by the factory (from contract spec)
        self.severity: str = "blocking"
        # tally is set by the factory (from contract spec or global default)
        # None = use global default, True = count all, False = early stop
        self.tally: Optional[bool] = None
        # context is set by the factory (from contract spec)
        # Consumer-defined metadata, ignored by validation
        self.context: Dict[str, Any] = {}
    
    def __str__(self) -> str:
        return f"{self.name}({self.params})"

    def __repr__(self) -> str:
        return str(self)

    @abstractmethod
    def validate(self, df: "pl.DataFrame") -> Dict[str, Any]:
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

    def _failures(self, df: "pl.DataFrame", mask: "pl.Series", message: str) -> Dict[str, Any]:
        """Utility to summarize failing rows."""
        failed_count = mask.sum()
        return {
            "rule_id": getattr(self, "rule_id", self.name),
            "passed": failed_count == 0,
            "failed_count": int(failed_count),
            "message": message if failed_count > 0 else "Passed",
        }

    def _check_columns(self, df: "pl.DataFrame", columns: Set[str]) -> Dict[str, Any] | None:
        """
        Check if required columns exist in the DataFrame.

        Returns a failure result dict if any columns are missing, None if all exist.
        This allows rules to fail gracefully instead of raising exceptions.

        Args:
            df: The DataFrame to check
            columns: Set of required column names

        Returns:
            Failure result dict if columns missing, None if all present
        """
        if not columns:
            return None

        available = set(df.columns)
        missing = columns - available

        if not missing:
            return None

        # Build helpful error message
        missing_list = sorted(missing)
        available_list = sorted(available)

        if len(missing_list) == 1:
            msg = f"Column '{missing_list[0]}' not found"
        else:
            msg = f"Columns not found: {', '.join(missing_list)}"

        # Check if data might be nested (single column that looks like a wrapper)
        nested_hint = ""
        if len(available) == 1 and len(missing) > 0:
            nested_hint = ". Data may be nested - Kontra requires flat tabular data"

        from kontra.state.types import FailureMode

        return {
            "rule_id": getattr(self, "rule_id", self.name),
            "passed": False,
            "failed_count": df.height,  # All rows fail if column missing
            "message": f"{msg}{nested_hint}",
            "failure_mode": str(FailureMode.CONFIG_ERROR),  # Mark as config issue, not data issue
            "details": {
                "missing_columns": missing_list,
                "available_columns": available_list[:20],  # Limit for readability
            },
        }

    def to_sql_filter(self, dialect: str = "postgres") -> str | None:
        """
        Return a SQL WHERE clause that matches failing rows.

        Used by sample_failures() to push filtering to the database instead of
        loading the entire table. Returns None if the rule doesn't support SQL filters.

        Args:
            dialect: SQL dialect ("postgres", "mssql", "duckdb")

        Returns:
            SQL WHERE clause string (without "WHERE"), or None if not supported.

        Example:
            not_null rule returns: "email IS NULL"
            range rule returns: "amount < 0 OR amount > 100 OR amount IS NULL"
        """
        return None

    def to_sql_agg(self, dialect: str = "duckdb") -> str | None:
        """
        Return a SQL aggregate expression for counting violations.

        This enables SQL pushdown for custom rules without modifying executors.
        The executor wraps this as: {expr} AS "{rule_id}"

        Args:
            dialect: SQL dialect ("duckdb", "postgres", "mssql")

        Returns:
            SQL aggregate expression string, or None if not supported.

        Example:
            A "positive" rule checking col > 0:
            return 'SUM(CASE WHEN "amount" IS NULL OR "amount" <= 0 THEN 1 ELSE 0 END)'

        Note:
            - Use double quotes for column names: "column"
            - Return the full aggregate expression (SUM, COUNT, etc.)
            - Handle NULL appropriately (usually NULL = violation)
            - For dialect differences, check the dialect parameter
        """
        return None
