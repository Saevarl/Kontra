# tests/postgres/conftest.py
"""
Pytest fixtures for PostgreSQL integration tests.

Usage:
    pytest tests/postgres/ -v

Requires PostgreSQL container to be running:
    cd tests/postgres && docker compose up -d
"""

from __future__ import annotations

import os
import subprocess
import time
from typing import Generator

import pytest


def _is_postgres_ready(host: str = "localhost", port: int = 5433) -> bool:
    """Check if PostgreSQL is accepting connections."""
    try:
        import psycopg

        with psycopg.connect(
            host=host,
            port=port,
            user="kontra",
            password="kontra_test",
            dbname="kontra_test",
            connect_timeout=5,
        ) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                return True
    except Exception:
        return False


@pytest.fixture(scope="session")
def postgres_container() -> Generator[dict, None, None]:
    """
    Ensure PostgreSQL container is running.

    This fixture checks if the container is already running and starts it if not.
    The container is NOT automatically stopped after tests to allow for faster
    subsequent test runs. Use `docker compose down -v` to clean up manually.
    """
    compose_dir = os.path.dirname(__file__)

    # Check if already running
    if _is_postgres_ready():
        yield {
            "host": "localhost",
            "port": 5433,
            "user": "kontra",
            "password": "kontra_test",
            "database": "kontra_test",
        }
        return

    # Start container
    subprocess.run(
        ["docker", "compose", "up", "-d"],
        cwd=compose_dir,
        check=True,
        capture_output=True,
    )

    # Wait for container to be healthy
    for _ in range(30):
        if _is_postgres_ready():
            break
        time.sleep(1)
    else:
        pytest.fail("PostgreSQL container failed to start within 30 seconds")

    yield {
        "host": "localhost",
        "port": 5433,
        "user": "kontra",
        "password": "kontra_test",
        "database": "kontra_test",
    }

    # Note: We don't stop the container here for faster subsequent runs
    # Run `cd tests/postgres && docker compose down -v` to clean up


@pytest.fixture
def postgres_uri(postgres_container: dict) -> str:
    """Return full PostgreSQL URI for the users table."""
    return "postgres://kontra:kontra_test@localhost:5433/kontra_test/public.users"


@pytest.fixture
def postgres_products_uri(postgres_container: dict) -> str:
    """Return PostgreSQL URI for the products table."""
    return "postgres://kontra:kontra_test@localhost:5433/kontra_test/public.products"


@pytest.fixture
def postgres_orders_uri(postgres_container: dict) -> str:
    """Return PostgreSQL URI for the orders table."""
    return "postgres://kontra:kontra_test@localhost:5433/kontra_test/public.orders"


@pytest.fixture
def postgres_connection(postgres_container: dict):
    """Return a psycopg connection for direct queries."""
    import psycopg

    conn = psycopg.connect(
        host=postgres_container["host"],
        port=postgres_container["port"],
        user=postgres_container["user"],
        password=postgres_container["password"],
        dbname=postgres_container["database"],
    )
    yield conn
    conn.close()


@pytest.fixture
def postgres_env_vars(postgres_container: dict, monkeypatch):
    """Set standard PGXXX environment variables for testing env-based auth."""
    monkeypatch.setenv("PGHOST", postgres_container["host"])
    monkeypatch.setenv("PGPORT", str(postgres_container["port"]))
    monkeypatch.setenv("PGUSER", postgres_container["user"])
    monkeypatch.setenv("PGPASSWORD", postgres_container["password"])
    monkeypatch.setenv("PGDATABASE", postgres_container["database"])
    return postgres_container
