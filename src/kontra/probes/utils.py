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
    data: Any,
    storage_options: Optional[Dict[str, Any]] = None,
    table: Optional[str] = None,
) -> pl.DataFrame:
    """
    Materialize any Kontra-supported data source into a Polars DataFrame.

    Supported sources (any combination can be compared against any other):
      - Polars DataFrame — returned as-is
      - pandas DataFrame / list-of-dicts / dict-of-columns — normalized
      - File / cloud path or URI — .parquet/.csv, s3://, abfss://
      - Database URI — postgres://.../schema.table, mssql://.../schema.table
      - Named datasource — "prod_db.users" (resolved from .kontra/config.yml)
      - BYOC connection object — pass ``table="schema.table"`` alongside it

    Args:
        data: Any of the sources above.
        storage_options: Cloud storage credentials (S3, Azure, GCS).
        table: Table reference, required only when ``data`` is a live database
            connection object (BYOC).

    Returns:
        Polars DataFrame.

    Raises:
        ValueError: If the source cannot be resolved.

    Notes:
        Database tables and named datasources are materialized through the same
        connectors/materializers the validation engine uses, so probes accept
        exactly the sources ``kontra.validate()`` accepts. For Azure URIs,
        credentials auto-populate from environment variables when
        storage_options is not provided.
    """
    if isinstance(data, pl.DataFrame):
        return data

    # Query source: a read-only SELECT run on its bound engine.
    from kontra.connectors.query import Query

    if isinstance(data, Query):
        from kontra.connectors.handle import DatasetHandle

        return _materialize_handle(DatasetHandle.from_query(data))

    # In-memory: pandas DataFrame, list-of-dicts, dict-of-columns.
    if not isinstance(data, str):
        from kontra.connectors.detection import is_database_connection

        if is_database_connection(data):
            if not table:
                raise ValueError(
                    "A database connection requires table=... "
                    "e.g. compare(conn, df, key='id', before_table='public.users')"
                )
            from kontra.connectors.handle import DatasetHandle

            return _materialize_handle(DatasetHandle.from_connection(data, table))

        # list / dict / pandas → Polars (reuses the engine's normalizer)
        from kontra import _normalize_to_dataframe

        normalized = _normalize_to_dataframe(data)
        if isinstance(normalized, pl.DataFrame):
            return normalized
        raise ValueError(f"Unsupported data type: {type(data)}")

    # String: named datasource → URI, then dispatch by scheme.
    uri = _resolve_named_datasource(data)
    lower = uri.lower()

    if lower.startswith(("postgres://", "postgresql://", "mssql://", "clickhouse://", "clickhouses://")):
        from kontra.connectors.handle import DatasetHandle

        return _materialize_handle(DatasetHandle.from_uri(uri))

    # File / cloud path — fast Polars read.
    so = storage_options or {}
    if not so and _is_azure_uri(uri):
        so = _get_azure_storage_options()

    if lower.endswith(".csv"):
        return pl.read_csv(uri, storage_options=so) if so else pl.read_csv(uri)
    if lower.endswith(".parquet") or uri.startswith("s3://") or _is_azure_uri(uri):
        return pl.read_parquet(uri, storage_options=so) if so else pl.read_parquet(uri)
    # Unknown extension: try parquet then CSV.
    try:
        return pl.read_parquet(uri, storage_options=so) if so else pl.read_parquet(uri)
    except (OSError, ValueError):
        return pl.read_csv(uri, storage_options=so) if so else pl.read_csv(uri)


def _resolve_named_datasource(data: str) -> str:
    """Resolve a named datasource (``prod_db.users``) to a URI; pass through
    anything that is already a path/URI or has no matching config."""
    if "://" in data or "/" in data or "\\" in data or data.endswith((".parquet", ".csv")):
        return data
    try:
        from kontra.config.settings import resolve_datasource

        return resolve_datasource(data)
    except (ValueError, KeyError):
        # Not a named datasource — treat as a literal path/URI.
        return data


def _materialize_handle(handle: Any) -> pl.DataFrame:
    """Load a full table for a DatasetHandle via the engine's materializers.

    Registers the materializer needed for this handle's flavor first (the same
    lazy path-aware registration the validation engine performs), then loads
    every column.
    """
    from kontra.engine.materializers.registry import (
        pick_materializer,
        register_materializers_for_path,
    )

    # BYOC handles carry scheme="byoc" and identify the flavor via dialect.
    db_type = None
    for key in (handle.scheme, getattr(handle, "dialect", None)):
        if key in ("postgres", "postgresql"):
            db_type = "postgres"
            break
        if key in ("mssql", "sqlserver"):
            db_type = "sqlserver"
            break
        if key in ("clickhouse", "clickhouses"):
            db_type = "clickhouse"
            break

    execution_path = "database" if db_type else "file"
    register_materializers_for_path(execution_path, db_type)

    materializer = pick_materializer(handle)
    return materializer.to_polars(None)
