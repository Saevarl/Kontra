# src/kontra/errors.py
"""
Kontra error types with actionable error messages.

All errors inherit from KontraError and provide:
- Clear description of what went wrong
- Suggestions for how to fix it
- Context about what was being attempted
"""

from __future__ import annotations

from typing import Optional, List


class KontraError(Exception):
    """Base class for all Kontra errors."""

    def __init__(
        self,
        message: str,
        *,
        suggestions: Optional[List[str]] = None,
        context: Optional[str] = None,
    ):
        self.message = message
        self.suggestions = suggestions or []
        self.context = context
        super().__init__(self._format())

    def _format(self) -> str:
        parts = [self.message]
        if self.context:
            parts.append(f"\n  Context: {self.context}")
        if self.suggestions:
            parts.append("\n\nTry:")
            for s in self.suggestions:
                parts.append(f"  - {s}")
        return "".join(parts)


# =============================================================================
# Contract Errors
# =============================================================================


class ContractError(KontraError):
    """Base class for contract-related errors."""

    pass


class ContractNotFoundError(ContractError):
    """Contract file not found."""

    def __init__(self, path: str):
        super().__init__(
            f"Contract file not found: {path}",
            suggestions=[
                "Check the file path is correct",
                "Ensure the file exists and is readable",
                f"Create a contract at: {path}",
            ],
        )


class ContractParseError(ContractError):
    """Contract YAML is invalid or malformed."""

    def __init__(self, path: str, error: str):
        super().__init__(
            f"Failed to parse contract YAML: {error}",
            context=path,
            suggestions=[
                "Check YAML syntax (indentation, colons, quotes)",
                "Validate YAML online: https://www.yamllint.com/",
                "See contract examples in docs/",
            ],
        )


class ContractValidationError(ContractError):
    """Contract structure is invalid."""

    def __init__(self, issue: str, path: str):
        super().__init__(
            f"Invalid contract: {issue}",
            context=path,
            suggestions=[
                "Contract must have 'dataset' and 'rules' keys",
                "Each rule must have a 'name' key",
                "Check rule parameters match the rule type",
            ],
        )


# =============================================================================
# Rule Errors
# =============================================================================


class RuleError(KontraError):
    """Base class for rule-related errors."""

    pass


class UnknownRuleError(RuleError):
    """Rule type is not recognized."""

    def __init__(self, rule_name: str, available_rules: Optional[List[str]] = None):
        suggestions = []
        if available_rules:
            suggestions.append(f"Available rules: {', '.join(sorted(available_rules))}")
        suggestions.extend([
            "Check rule name spelling",
            "See docs/rules.md for all supported rules",
        ])
        super().__init__(
            f"Unknown rule type: '{rule_name}'",
            suggestions=suggestions,
        )


class RuleParameterError(RuleError):
    """Rule parameters are invalid."""

    def __init__(self, rule_name: str, param: str, issue: str):
        super().__init__(
            f"Invalid parameter '{param}' for rule '{rule_name}': {issue}",
            suggestions=[
                f"Check {rule_name} documentation for valid parameters",
                "Ensure parameter types are correct (string, int, list, etc.)",
            ],
        )


class DuplicateRuleIdError(RuleError):
    """Duplicate rule ID detected in contract.

    This error occurs when multiple rules resolve to the same ID.
    The automatic ID format is:
      - COL:{column}:{rule_name} for column-based rules
      - DATASET:{rule_name} for dataset-level rules
    """

    def __init__(
        self,
        rule_id: str,
        rule_name: str,
        rule_index: int,
        conflict_index: int,
        column: Optional[str] = None,
    ):
        self.rule_id = rule_id
        self.rule_name = rule_name
        self.rule_index = rule_index
        self.conflict_index = conflict_index
        self.column = column

        # Build suggestion with example
        if column:
            example = (
                f"    - name: {rule_name}\n"
                f"      id: {column}_{rule_name}_v2  # Choose a unique ID\n"
                f"      params:\n"
                f"        column: {column}"
            )
        else:
            example = (
                f"    - name: {rule_name}\n"
                f"      id: {rule_name}_v2  # Choose a unique ID"
            )

        super().__init__(
            f"Duplicate rule ID '{rule_id}' in contract",
            context=f"Rule at index {rule_index} conflicts with rule at index {conflict_index}",
            suggestions=[
                "Multiple rules resolved to the same ID",
                f"Add an explicit 'id' field to distinguish rules:\n\n{example}",
            ],
        )


# =============================================================================
# Connection Errors
# =============================================================================


class ConnectionError(KontraError):
    """Base class for connection-related errors."""

    pass


class PostgresConnectionError(ConnectionError):
    """PostgreSQL connection failed."""

    def __init__(self, host: str, port: int, database: str, error: str):
        super().__init__(
            f"PostgreSQL connection failed: {error}",
            context=f"{host}:{port}/{database}",
            suggestions=[
                "Verify the database server is running",
                "Check host, port, and database name",
                "Verify username and password",
                "Set environment variables:\n"
                "    export PGHOST=localhost\n"
                "    export PGPORT=5432\n"
                "    export PGUSER=your_user\n"
                "    export PGPASSWORD=your_password\n"
                "    export PGDATABASE=your_database",
                "Or use full URI: postgres://user:pass@host:5432/database/schema.table",
            ],
        )


class SqlServerConnectionError(ConnectionError):
    """SQL Server connection failed."""

    def __init__(self, host: str, database: str, error: str):
        super().__init__(
            f"SQL Server connection failed: {error}",
            context=f"{host}/{database}",
            suggestions=[
                "Verify the database server is running",
                "Check host and database name",
                "Verify username and password",
                "Check if SQL Server allows TCP/IP connections",
                "Use full URI: mssql://user:pass@host/database/schema.table",
            ],
        )


class S3ConnectionError(ConnectionError):
    """S3/MinIO connection failed."""

    def __init__(self, uri: str, error: str):
        super().__init__(
            f"S3 access failed: {error}",
            context=uri,
            suggestions=[
                "Set AWS credentials:\n"
                "    export AWS_ACCESS_KEY_ID=your_key\n"
                "    export AWS_SECRET_ACCESS_KEY=your_secret",
                "For MinIO/custom S3:\n"
                "    export AWS_ENDPOINT_URL=http://localhost:9000",
                "Check bucket and key names are correct",
                "Verify bucket permissions",
            ],
        )


# =============================================================================
# Data Errors
# =============================================================================


class DataError(KontraError):
    """Base class for data-related errors."""

    pass


class InvalidDataError(DataError):
    """Data type is invalid for validation."""

    def __init__(self, data_type: str, *, detail: Optional[str] = None):
        message = f"Invalid data type: {data_type}"
        if detail:
            message = f"{message}. {detail}"

        super().__init__(
            message,
            suggestions=[
                "Supported data types:",
                "  - Polars DataFrame",
                "  - pandas DataFrame",
                "  - dict (single record)",
                "  - list[dict] (multiple records)",
                "  - str (file path, URI, or datasource name)",
                "  - Database connection (requires table= parameter)",
            ],
        )


class InvalidPathError(DataError):
    """Path is invalid (e.g., directory instead of file)."""

    def __init__(self, path: str, issue: str):
        super().__init__(
            f"Invalid path: {issue}",
            context=path,
            suggestions=[
                "Provide a path to a file, not a directory",
                "Supported formats: .parquet, .csv",
                "Or use a URI: s3://bucket/key, postgres://..., mssql://...",
            ],
        )


class DataNotFoundError(DataError):
    """Data file or table not found."""

    def __init__(self, source: str):
        suggestions = ["Check the path or URI is correct"]
        if source.lower().startswith("s3://"):
            suggestions.extend([
                "Verify bucket and key exist",
                "Check S3 credentials are set",
            ])
        elif source.lower().startswith("postgres://"):
            suggestions.extend([
                "Verify schema.table exists",
                "Check database permissions",
            ])
        else:
            suggestions.append("Ensure the file exists and is readable")

        super().__init__(
            f"Data source not found: {source}",
            suggestions=suggestions,
        )


class DataFormatError(DataError):
    """Data format is invalid or unsupported."""

    def __init__(self, source: str, issue: str):
        super().__init__(
            f"Data format error: {issue}",
            context=source,
            suggestions=[
                "Supported formats: Parquet, CSV",
                "Check file extension matches actual format",
                "For CSV: check encoding (UTF-8 recommended)",
            ],
        )


# =============================================================================
# Config Errors
# =============================================================================


class ConfigError(KontraError):
    """Base class for configuration errors."""

    pass


class ConfigNotFoundError(ConfigError):
    """Config file not found (only raised if explicitly required)."""

    def __init__(self, path: str):
        super().__init__(
            f"Config file not found: {path}",
            suggestions=[
                "Run 'kontra init' to create a default config",
                "Or continue without a config file (all defaults apply)",
            ],
        )


class ConfigParseError(ConfigError):
    """Config YAML is invalid."""

    def __init__(self, path: str, error: str):
        super().__init__(
            f"Failed to parse config: {error}",
            context=path,
            suggestions=[
                "Check YAML syntax (indentation, colons, quotes)",
                "Validate at https://www.yamllint.com/",
                "Run 'kontra init --force' to regenerate",
            ],
        )


class ConfigValidationError(ConfigError):
    """Config structure is invalid."""

    def __init__(self, errors: list, path: str):
        formatted = "\n".join(f"  - {e}" for e in errors)
        super().__init__(
            f"Invalid config:\n{formatted}",
            context=path,
            suggestions=[
                "Check field names and types",
                "Valid preplan/pushdown values: on, off, auto",
                "Valid projection values: on, off",
                "Run 'kontra init --force' to see valid structure",
            ],
        )


class UnknownEnvironmentError(ConfigError):
    """Requested environment doesn't exist in config."""

    def __init__(self, env_name: str, available: list):
        available_str = ", ".join(available) if available else "(none defined)"
        super().__init__(
            f"Unknown environment: '{env_name}'",
            suggestions=[
                f"Available environments: {available_str}",
                "Add the environment to .kontra/config.yml",
                "Or remove the --env flag to use defaults",
            ],
        )


# =============================================================================
# State Errors
# =============================================================================


class StateError(KontraError):
    """Base class for state-related errors."""

    pass


class StateCorruptedError(StateError):
    """State files are corrupted or unreadable."""

    def __init__(self, contract: str, error: str):
        super().__init__(
            f"State data is corrupted for contract '{contract}': {error}",
            suggestions=[
                "Delete the corrupted state files in .kontra/state/",
                "Run 'kontra validate' again to regenerate state",
                "Check if state files were modified externally",
            ],
        )


class StateNotFoundError(StateError):
    """No state history exists for the contract."""

    def __init__(self, contract: str):
        super().__init__(
            f"No validation history found for contract '{contract}'",
            suggestions=[
                "Run 'kontra validate' at least twice to generate history for diff",
                "Check the contract name is correct",
            ],
        )


# =============================================================================
# Validation Errors
# =============================================================================


class ValidationError(KontraError):
    """
    Raised when validation fails in a decorated function.

    This error is raised by the @kontra.validate() decorator when
    on_fail="raise" and the decorated function returns data that
    fails blocking validation rules.

    Attributes:
        result: The ValidationResult with details about failures
    """

    def __init__(self, result: "ValidationResult", message: Optional[str] = None):
        from kontra.api.results import ValidationResult  # noqa: F811

        self.result = result
        if message is None:
            blocking = [r for r in result.rules if not r.passed and r.severity == "blocking"]
            message = (
                f"Validation failed: {len(blocking)} blocking rule(s) failed "
                f"({result.failed_count} total violations)"
            )
        # Don't use suggestions for this - the message is clear
        super().__init__(message)

    def _format(self) -> str:
        # Override to not add "Try:" section
        return self.message


# Type hint for ValidationResult (avoid circular import)
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from kontra.api.results import ValidationResult


# =============================================================================
# Helper Functions
# =============================================================================


def format_error_for_cli(error: Exception) -> str:
    """Format any exception for CLI display."""
    if isinstance(error, KontraError):
        return str(error)

    # Handle Pydantic validation errors with friendly messages
    error_type = type(error).__name__
    if error_type == "ValidationError" and hasattr(error, "errors"):
        # Pydantic ValidationError - extract field-level issues
        try:
            issues = []
            for err in error.errors():  # type: ignore
                loc = ".".join(str(x) for x in err.get("loc", []))
                msg = err.get("msg", "invalid value")
                if loc:
                    issues.append(f"  - {loc}: {msg}")
                else:
                    issues.append(f"  - {msg}")
            if issues:
                return f"Configuration error:\n" + "\n".join(issues)
        except (AttributeError, TypeError, KeyError):
            pass  # Fall through to default handling

    # Handle common exception types with better messages
    error_str = str(error).lower()

    if isinstance(error, FileNotFoundError):
        return f"File not found: {error}\n\nCheck the file path is correct."

    if "connection refused" in error_str:
        return (
            f"Connection refused: {error}\n\n"
            "The database server may not be running, or the host/port is incorrect."
        )

    if "timeout" in error_str or "timed out" in error_str:
        return (
            f"Connection timed out: {error}\n\n"
            "The server took too long to respond. Check network connectivity."
        )

    if "permission denied" in error_str or "access denied" in error_str:
        return (
            f"Permission denied: {error}\n\n"
            "Check credentials and access permissions."
        )

    if "authentication" in error_str or "password" in error_str:
        return (
            f"Authentication failed: {error}\n\n"
            "Check username and password are correct."
        )

    # S3-specific errors
    if "nosuchbucket" in error_str or "bucket" in error_str and "not found" in error_str:
        return (
            f"S3 bucket not found: {error}\n\n"
            "Check the bucket name is correct and you have access."
        )

    if "nosuchkey" in error_str or "key" in error_str and "not found" in error_str:
        return (
            f"S3 object not found: {error}\n\n"
            "Check the object key (path) is correct."
        )

    if "nocredentials" in error_str or "credentials" in error_str:
        return (
            f"AWS credentials not found: {error}\n\n"
            "Set credentials via environment variables:\n"
            "  export AWS_ACCESS_KEY_ID=your_key\n"
            "  export AWS_SECRET_ACCESS_KEY=your_secret\n"
            "  export AWS_REGION=us-east-1"
        )

    if "invalidaccesskeyid" in error_str or "signaturemismatch" in error_str:
        return (
            f"AWS credentials invalid: {error}\n\n"
            "Check your AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are correct."
        )

    # Default: just return the error message
    return str(error)
