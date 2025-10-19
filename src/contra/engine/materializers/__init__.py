# src/contra/engine/materializers/__init__.py
# Importing these modules triggers the @register_materializer decorators.
from .duckdb import DuckDBMaterializer          # noqa: F401
from .polars_connector import PolarsConnectorMaterializer  # noqa: F401
