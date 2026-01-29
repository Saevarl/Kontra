# src/kontra/probes/utils.py
"""
Shared utilities for probe implementations.
"""

from __future__ import annotations

from typing import Union

import polars as pl


def load_data(data: Union[pl.DataFrame, str]) -> pl.DataFrame:
    """
    Load data from DataFrame or path/URI.

    Args:
        data: Either a Polars DataFrame or a path/URI string

    Returns:
        Polars DataFrame

    Raises:
        ValueError: If data type is not supported

    Notes:
        For MVP, only Polars DataFrames are fully supported.
        File paths are loaded via Polars read functions.
    """
    if isinstance(data, pl.DataFrame):
        return data

    if isinstance(data, str):
        # Simple file loading for MVP
        if data.lower().endswith(".parquet"):
            return pl.read_parquet(data)
        elif data.lower().endswith(".csv"):
            return pl.read_csv(data)
        elif data.startswith("s3://"):
            return pl.read_parquet(data)
        else:
            # Try parquet first, then CSV
            try:
                return pl.read_parquet(data)
            except (OSError, IOError, ValueError):
                return pl.read_csv(data)

    raise ValueError(f"Unsupported data type: {type(data)}")
