"""
Input helpers shared by the top-level API functions.

DataFrame detection and normalization (without importing pandas/polars
eagerly) and the contract-path check used by the state functions.
"""

from typing import Any


def _is_pandas_dataframe(obj: Any) -> bool:
    """Check if object is a pandas DataFrame without importing pandas."""
    return (
        hasattr(obj, "__dataframe__")
        and type(obj).__module__.startswith("pandas")
        and type(obj).__name__ == "DataFrame"
    )


def _is_polars_dataframe(obj: Any) -> bool:
    """Check if object is a polars DataFrame without importing polars.

    If polars was never imported, obj cannot be a polars DataFrame — this
    keeps string-path validations from paying the ~115ms polars import.
    """
    import sys

    pl_mod = sys.modules.get("polars")
    return pl_mod is not None and isinstance(obj, pl_mod.DataFrame)


def _normalize_to_dataframe(data: Any) -> Any:
    """
    Convert list[dict], dict, or pandas DataFrame to Polars DataFrame.

    Returns the original data unchanged if it's not one of these types.
    """
    import polars as pl

    if isinstance(data, list):
        if not data:
            return pl.DataFrame()
        # Only list-of-dicts is valid per docs (BUG-058)
        if not isinstance(data[0], dict):
            from kontra.errors import InvalidDataError
            raise InvalidDataError(
                f"List data must be a list of dicts, got list of {type(data[0]).__name__}. "
                f"Example: [{{'col': 1}}, {{'col': 2}}]"
            )
        return pl.DataFrame(data)
    if isinstance(data, dict) and not isinstance(data, pl.DataFrame):
        if not data:
            return pl.DataFrame()
        first_val = next(iter(data.values()))
        is_columnar = isinstance(first_val, (list, tuple))
        return pl.DataFrame(data) if is_columnar else pl.DataFrame([data])
    if _is_pandas_dataframe(data):
        return pl.from_pandas(data)
    return data


# Data file extensions that should not be passed to state functions
_DATA_FILE_EXTENSIONS = {".parquet", ".csv", ".json", ".ndjson", ".jsonl", ".arrow", ".feather"}


def _validate_contract_path(path: str, function_name: str) -> None:
    """
    Validate that a path looks like a contract file, not a data file.

    Raises ValueError with a helpful message if the file appears to be a data file.
    """
    lower = path.lower()
    for ext in _DATA_FILE_EXTENSIONS:
        if lower.endswith(ext):
            raise ValueError(
                f"{function_name}() requires a contract YAML file path, not a data file. "
                f"Received: '{path}' (appears to be a {ext[1:].upper()} file). "
                f"Example: kontra.{function_name}('contract.yml')"
            )
