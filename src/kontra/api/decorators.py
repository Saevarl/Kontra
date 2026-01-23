# src/kontra/api/decorators.py
"""
Pipeline validation decorators for Kontra.

Decorators for validating data returned from functions.
"""

from __future__ import annotations

import functools
import warnings
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Literal,
    Optional,
    TypeVar,
    Union,
)

from kontra.errors import ValidationError

F = TypeVar("F", bound=Callable[..., Any])

# Built-in mode shortcuts
OnFailMode = Literal["raise", "warn", "return_result"]

# Callback signature: (result, data) -> data (or raise)
OnFailCallback = Callable[["ValidationResult", Any], Any]  # type: ignore

# Accept either a mode string or a callback
OnFailHandler = Union[OnFailMode, OnFailCallback]


def validate(
    contract: Optional[str] = None,
    rules: Optional[List[Dict[str, Any]]] = None,
    on_fail: OnFailHandler = "raise",
    save: bool = False,
    sample: int = 0,
    sample_columns: Optional[Union[List[str], str]] = None,
) -> Callable[[F], F]:
    """
    Decorator to validate data returned from a function.

    The decorated function must return a DataFrame (Polars or pandas)
    or other data type supported by `kontra.validate()`.

    Args:
        contract: Path to a YAML contract file
        rules: List of rule definitions (alternative to contract)
        on_fail: Action when validation fails. Either a mode string or a callback:
            - "raise": Raise ValidationError on blocking failures (default)
            - "warn": Log warning, return data anyway
            - "return_result": Return (data, ValidationResult) tuple
            - Callable[[ValidationResult, data], data]: Custom handler
        save: Whether to save the validation result to state
        sample: Number of sample rows to collect for failures
        sample_columns: Columns to include in samples (None=all, list, or "relevant")

    Returns:
        Decorated function

    Raises:
        ValueError: If neither contract nor rules is provided
        ValidationError: If on_fail="raise" and validation has blocking failures

    Example:
        ```python
        import kontra
        from kontra import rules

        # Built-in modes
        @kontra.validate_decorator(
            rules=[rules.not_null("id"), rules.unique("email")],
            on_fail="raise"
        )
        def load_users() -> pl.DataFrame:
            return pl.read_parquet("users.parquet")

        # Custom callback - Kontra measures, you decide
        def notify_slack(result, data):
            if not result.passed:
                slack.post(f"Validation failed: {result.failed_count} violations")
            return data  # or raise, or transform, etc.

        @kontra.validate_decorator(
            rules=[rules.not_null("id")],
            on_fail=notify_slack
        )
        def fetch_orders():
            return db.query("SELECT * FROM orders")
        ```
    """
    if contract is None and rules is None:
        raise ValueError("Either 'contract' or 'rules' must be provided")

    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            # Import here to avoid circular imports
            import kontra

            # Call the original function
            data = func(*args, **kwargs)

            # Validate the returned data
            result = kontra.validate(
                data,
                contract=contract,
                rules=rules,
                save=save,
                sample=sample,
                sample_columns=sample_columns,
            )

            # Handle based on on_fail mode or callback
            if callable(on_fail) and not isinstance(on_fail, str):
                # User-provided callback: Kontra measured, user decides
                return on_fail(result, data)

            if on_fail == "return_result":
                return (data, result)

            if not result.passed:
                # Check for blocking failures
                blocking_failures = [
                    r for r in result.rules if not r.passed and r.severity == "blocking"
                ]
                if blocking_failures:
                    if on_fail == "raise":
                        raise ValidationError(result)
                    elif on_fail == "warn":
                        # Log warning
                        warnings.warn(
                            f"Validation failed in {func.__name__}: "
                            f"{len(blocking_failures)} blocking rule(s) failed "
                            f"({result.failed_count} total violations)",
                            UserWarning,
                            stacklevel=2,
                        )

            return data

        return wrapper  # type: ignore

    return decorator


# Alias for import convenience
validate_decorator = validate
