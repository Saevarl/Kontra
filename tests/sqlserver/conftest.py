# tests/sqlserver/conftest.py
"""
Pytest fixtures for SQL Server integration tests.

Requires SQL Server container to be running:
    cd tests/sqlserver && docker compose up -d
"""

import pytest


@pytest.fixture(scope="session")
def sqlserver_uri():
    """Return SQL Server connection URI for users table."""
    return "mssql://sa:Kontra_Test123!@localhost:1434/kontra_test/dbo.users"


@pytest.fixture(scope="session")
def sqlserver_products_uri():
    """Return SQL Server connection URI for products table."""
    return "mssql://sa:Kontra_Test123!@localhost:1434/kontra_test/dbo.products"


@pytest.fixture(scope="session")
def sqlserver_orders_uri():
    """Return SQL Server connection URI for orders table."""
    return "mssql://sa:Kontra_Test123!@localhost:1434/kontra_test/dbo.orders"
