# src/kontra/probes/utils.py
"""
Shared utilities for probe implementations.
"""

from __future__ import annotations

import os
from typing import Any, Dict, Optional, Union

import polars as pl


def _get_azure_storage_options() -> Dict[str, str]:
    """
    Build Azure storage_options from environment variables.

    Checks for common Azure credential env vars and returns Polars-compatible
    storage_options dict. This ensures probes use the same auth as DuckDB.

    Environment variables checked:
    - AZURE_STORAGE_ACCOUNT_NAME → account_name
    - AZURE_STORAGE_ACCESS_KEY → access_key
    - AZURE_STORAGE_ACCOUNT_KEY → access_key (alternative)
    - AZURE_STORAGE_SAS_TOKEN → sas_key

    Returns:
        Dict with Azure credentials, or empty dict if none found
    """
    opts: Dict[str, str] = {}

    # Account name
    account_name = os.environ.get("AZURE_STORAGE_ACCOUNT_NAME")
    if account_name:
        opts["account_name"] = account_name

    # Access key (try both common env var names)
    access_key = os.environ.get("AZURE_STORAGE_ACCESS_KEY") or os.environ.get(
        "AZURE_STORAGE_ACCOUNT_KEY"
    )
    if access_key:
        opts["access_key"] = access_key

    # SAS token
    sas_token = os.environ.get("AZURE_STORAGE_SAS_TOKEN")
    if sas_token:
        opts["sas_key"] = sas_token

    return opts


def _is_azure_uri(uri: str) -> bool:
    """Check if URI is an Azure storage URI."""
    lower = uri.lower()
    return lower.startswith("abfss://") or lower.startswith("abfs://") or lower.startswith("az://")


def load_data(
    data: Union[pl.DataFrame, str],
    storage_options: Optional[Dict[str, Any]] = None,
) -> pl.DataFrame:
    """
    Load data from DataFrame or path/URI.

    Args:
        data: Either a Polars DataFrame or a path/URI string
        storage_options: Cloud storage credentials (S3, Azure, GCS)

    Returns:
        Polars DataFrame

    Raises:
        ValueError: If data type is not supported

    Notes:
        For MVP, only Polars DataFrames are fully supported.
        File paths are loaded via Polars read functions.

        For Azure URIs, if storage_options is not provided, credentials
        are auto-populated from environment variables (AZURE_STORAGE_ACCOUNT_NAME,
        AZURE_STORAGE_ACCESS_KEY, AZURE_STORAGE_SAS_TOKEN) to match DuckDB behavior.
    """
    if isinstance(data, pl.DataFrame):
        return data

    if isinstance(data, str):
        # Reject database URIs early with a clear message
        _lower = data.lower()
        if _lower.startswith(("postgres://", "postgresql://", "mssql://")):
            raise ValueError(
                "Database URIs are not supported for probes. "
                "Load the data as a DataFrame first."
            )

        so = storage_options or {}

        # Auto-populate Azure credentials from env vars if not provided
        if not so and _is_azure_uri(data):
            so = _get_azure_storage_options()

        # Simple file loading for MVP
        if data.lower().endswith(".parquet"):
            return pl.read_parquet(data, storage_options=so) if so else pl.read_parquet(data)
        elif data.lower().endswith(".csv"):
            return pl.read_csv(data, storage_options=so) if so else pl.read_csv(data)
        elif data.startswith("s3://") or _is_azure_uri(data):
            return pl.read_parquet(data, storage_options=so) if so else pl.read_parquet(data)
        else:
            # Try parquet first, then CSV
            try:
                return pl.read_parquet(data, storage_options=so) if so else pl.read_parquet(data)
            except (OSError, ValueError):
                return pl.read_csv(data, storage_options=so) if so else pl.read_csv(data)

    raise ValueError(f"Unsupported data type: {type(data)}")
