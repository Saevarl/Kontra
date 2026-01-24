# tests/test_sql_security.py
"""
Security tests for SQL validation.

These tests verify that the SQL validator blocks access to system catalogs
and other potential information disclosure vectors.
"""

import pytest
from kontra.engine.sql_validator import validate_sql


class TestSystemCatalogBlocking:
    """Test blocking of system catalog access."""

    @pytest.mark.parametrize("sql,description", [
        # PostgreSQL system catalogs
        ("SELECT * FROM pg_user", "pg_user"),
        ("SELECT * FROM pg_shadow", "pg_shadow (password hashes)"),
        ("SELECT * FROM pg_authid", "pg_authid"),
        ("SELECT * FROM pg_roles", "pg_roles"),
        ("SELECT * FROM pg_database", "pg_database"),
        ("SELECT * FROM pg_tablespace", "pg_tablespace"),
        ("SELECT * FROM pg_settings", "pg_settings"),
        ("SELECT * FROM pg_stat_activity", "pg_stat_activity"),
        # PostgreSQL information schema
        ("SELECT * FROM information_schema.tables", "information_schema.tables"),
        ("SELECT * FROM information_schema.columns", "information_schema.columns"),
        ("SELECT * FROM information_schema.schemata", "information_schema.schemata"),
    ])
    def test_blocks_postgres_system_catalogs(self, sql, description):
        """Verify PostgreSQL system catalog access is blocked."""
        result = validate_sql(sql, dialect="postgres")
        assert not result.is_safe, f"Should block access to {description}"
        assert "system catalog" in result.reason.lower()

    @pytest.mark.parametrize("sql,description", [
        # SQL Server system views
        ("SELECT * FROM sys.databases", "sys.databases"),
        ("SELECT * FROM sys.tables", "sys.tables"),
        ("SELECT * FROM sys.columns", "sys.columns"),
        ("SELECT * FROM sys.sql_logins", "sys.sql_logins"),
        ("SELECT * FROM sys.server_principals", "sys.server_principals"),
        # SQL Server legacy system tables
        ("SELECT * FROM sysobjects", "sysobjects"),
        ("SELECT * FROM syscolumns", "syscolumns"),
        ("SELECT * FROM sysusers", "sysusers"),
        # SQL Server information schema
        ("SELECT * FROM INFORMATION_SCHEMA.TABLES", "INFORMATION_SCHEMA.TABLES"),
    ])
    def test_blocks_sqlserver_system_catalogs(self, sql, description):
        """Verify SQL Server system catalog access is blocked."""
        result = validate_sql(sql, dialect="sqlserver")
        assert not result.is_safe, f"Should block access to {description}"
        assert "system catalog" in result.reason.lower()


class TestSubqueryBlocking:
    """Test that subqueries to system catalogs are also blocked."""

    @pytest.mark.parametrize("sql", [
        # EXISTS subqueries
        "SELECT * FROM users WHERE EXISTS(SELECT 1 FROM pg_user WHERE usename='admin')",
        "SELECT * FROM users WHERE EXISTS(SELECT 1 FROM pg_shadow)",
        "SELECT * FROM users WHERE EXISTS(SELECT 1 FROM information_schema.tables)",
        # Scalar subqueries
        "SELECT * FROM users WHERE (SELECT COUNT(*) FROM pg_database) > 0",
        "SELECT * FROM users WHERE id = (SELECT usesysid FROM pg_user LIMIT 1)",
        # IN subqueries
        "SELECT * FROM users WHERE name IN (SELECT usename FROM pg_user)",
    ])
    def test_blocks_subqueries_to_system_catalogs(self, sql):
        """Verify subqueries to system catalogs are blocked."""
        result = validate_sql(sql, dialect="postgres")
        assert not result.is_safe, f"Should block: {sql}"
        assert "system catalog" in result.reason.lower()


class TestValidQueriesAllowed:
    """Test that valid queries are still allowed."""

    @pytest.mark.parametrize("sql", [
        "SELECT * FROM users",
        "SELECT * FROM users WHERE status = 'active'",
        "SELECT id, name FROM users WHERE created_at > '2024-01-01'",
        "SELECT a.id, b.name FROM users a JOIN orders b ON a.id = b.user_id",
        "SELECT * FROM users WHERE EXISTS(SELECT 1 FROM orders WHERE orders.user_id = users.id)",
        "SELECT COUNT(*) FROM users GROUP BY status",
    ])
    def test_allows_normal_queries(self, sql):
        """Verify normal queries are allowed."""
        result = validate_sql(sql, dialect="postgres")
        assert result.is_safe, f"Should allow: {sql}. Reason: {result.reason}"


class TestExistingSecurityChecks:
    """Verify existing security checks still work."""

    @pytest.mark.parametrize("sql,blocked_reason", [
        # Forbidden functions
        ("SELECT pg_sleep(10)", "Forbidden function"),
        ("SELECT dblink('connstr', 'SELECT 1')", "Forbidden function"),
        ("SELECT lo_import('/etc/passwd')", "Forbidden function"),
        # Write operations
        ("INSERT INTO users VALUES (1, 'test')", "SELECT"),
        ("UPDATE users SET name = 'hacked'", "SELECT"),
        ("DELETE FROM users", "SELECT"),
        ("DROP TABLE users", "SELECT"),
        # Multiple statements
        ("SELECT 1; DROP TABLE users", "Multiple statements"),
    ])
    def test_existing_blocks_still_work(self, sql, blocked_reason):
        """Verify existing security checks are not broken."""
        result = validate_sql(sql, dialect="postgres")
        assert not result.is_safe, f"Should block: {sql}"
        assert blocked_reason.lower() in result.reason.lower(), f"Expected '{blocked_reason}' in '{result.reason}'"


class TestFileAccessBlocking:
    """Test blocking of file access functions (SEC-001)."""

    @pytest.mark.parametrize("sql,description", [
        # DuckDB file reading functions
        ("SELECT * FROM read_csv('/etc/passwd')", "read_csv"),
        ("SELECT * FROM read_parquet('/tmp/secrets.parquet')", "read_parquet"),
        ("SELECT * FROM read_json('/tmp/config.json')", "read_json"),
        ("SELECT * FROM read_csv_auto('/etc/passwd')", "read_csv_auto"),
        ("SELECT * FROM read_json_auto('/tmp/config.json')", "read_json_auto"),
    ])
    def test_blocks_file_read_functions(self, sql, description):
        """Verify file reading functions are blocked."""
        result = validate_sql(sql, dialect="duckdb")
        assert not result.is_safe, f"Should block {description}"
        assert "forbidden function" in result.reason.lower()


class TestExternalAccessBlocking:
    """Test blocking of external database access (SEC-002)."""

    @pytest.mark.parametrize("sql,description", [
        # ATTACH command
        ("ATTACH '/path/to/db' AS prod", "ATTACH database"),
        # COPY command
        ("COPY users TO '/tmp/data.csv'", "COPY TO"),
        ("COPY users FROM '/tmp/data.csv'", "COPY FROM"),
    ])
    def test_blocks_external_access(self, sql, description):
        """Verify external access commands are blocked."""
        result = validate_sql(sql, dialect="duckdb")
        assert not result.is_safe, f"Should block {description}"
