"""FastMCP server factory. Importing this module does not import the MCP SDK."""

from contextlib import asynccontextmanager
import ipaddress
import logging
import re
from typing import Any

from kontra.mcp.service import KontraMCPService, MCPSettings


_logger = logging.getLogger(__name__)
_URI_CREDENTIAL_RE = re.compile(r"([a-z][a-z0-9+.-]*://[^:/\s'\"]+:)[^\s'\"]+@", re.I)
_SECRET_QUERY_RE = re.compile(
    r"([?&](?:sig|signature|token|sas|password|pass|secret|key|credential)=)[^&\s'\"]+",
    re.I,
)


def _sanitize_error_message(message: str) -> str:
    """Remove URI userinfo and common query-string secrets from errors."""
    from kontra.connectors.db_utils import mask_credentials

    sanitized = mask_credentials(message)
    sanitized = _URI_CREDENTIAL_RE.sub(r"\1***@", sanitized)
    return _SECRET_QUERY_RE.sub(r"\1***", sanitized)


class _SafeServiceProxy:
    """Sanitize every exception before the MCP SDK serializes it."""

    def __init__(self, target: KontraMCPService):
        self._target = target

    def __getattr__(self, name: str) -> Any:
        attribute = getattr(self._target, name)
        if not callable(attribute):
            return attribute

        def safe_call(*args: Any, **kwargs: Any) -> Any:
            try:
                return attribute(*args, **kwargs)
            except Exception as exc:
                sanitized = _sanitize_error_message(str(exc))
                _logger.warning("Kontra MCP request failed: %s", sanitized)
                raise ValueError(sanitized) from None

        return safe_call


def _is_loopback_host(host: str) -> bool:
    if host.lower() == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def create_server(
    settings: MCPSettings | None = None,
    *,
    host: str = "127.0.0.1",
    port: int = 8000,
    allow_remote_unauthenticated: bool = False,
) -> Any:
    """Create the official Kontra MCP server."""
    if not _is_loopback_host(host) and not allow_remote_unauthenticated:
        raise ValueError(
            "Refusing an unauthenticated non-loopback bind. Use an authenticating "
            "proxy or explicitly set allow_remote_unauthenticated=True."
        )
    try:
        from mcp.server.fastmcp import Context, FastMCP
    except ImportError as exc:
        raise RuntimeError(
            "The MCP dependencies are not installed. Install 'kontra[mcp-postgres]'."
        ) from exc

    configured = settings or MCPSettings.from_env()

    @asynccontextmanager
    async def lifespan(_server: Any):
        service = KontraMCPService(configured)
        try:
            yield service
        finally:
            service.close()

    server = FastMCP(
        "Kontra",
        instructions=(
            "Measure configured datasets against trusted Kontra contracts. "
            "Counts are measurements; interpret policy outside this server."
        ),
        lifespan=lifespan,
        stateless_http=True,
        json_response=True,
        host=host,
        port=port,
    )

    def service(ctx: Any) -> _SafeServiceProxy:
        return _SafeServiceProxy(ctx.request_context.lifespan_context)

    def request_service() -> _SafeServiceProxy:
        return service(server.get_context())

    @server.resource("kontra://health")
    def health() -> dict[str, Any]:
        """Return sanitized server and backend readiness metadata."""
        return request_service().health()

    @server.resource("kontra://rules")
    def rules() -> list[dict[str, Any]]:
        """List built-in measurement rules and their parameters."""
        return request_service().list_rules()

    @server.resource("kontra://datasources")
    def datasources() -> dict[str, list[str]]:
        """List configured datasource names and tables without credentials."""
        return request_service().list_datasources()

    @server.tool()
    def validate(
        datasource: str,
        contract: str,
        ctx: Context,
        env: str | None = None,
        tally: bool | None = None,
        sample: int = 0,
    ) -> dict[str, Any]:
        """Measure a named datasource against a trusted server-side contract."""
        return service(ctx).validate(
            datasource, contract, env=env, tally=tally, sample=sample
        )

    @server.tool()
    def profile(
        datasource: str,
        ctx: Context,
        preset: str = "scan",
        columns: list[str] | None = None,
        sample: int | None = None,
        save: bool = True,
    ) -> dict[str, Any]:
        """Measure bounded structural and statistical properties of a named datasource."""
        return service(ctx).profile(
            datasource, preset=preset, columns=columns, sample=sample, save=save
        )

    @server.tool()
    def validation_history(
        contract: str,
        ctx: Context,
        limit: int = 20,
        since: str | None = None,
        failed_only: bool = False,
    ) -> list[dict[str, Any]]:
        """Read bounded PostgreSQL validation history for a trusted contract."""
        return service(ctx).validation_history(
            contract, limit=limit, since=since, failed_only=failed_only
        )

    @server.tool()
    def validation_diff(contract: str, ctx: Context) -> dict[str, Any]:
        """Compare the two latest persisted validation measurements."""
        return service(ctx).validation_diff(contract)

    @server.tool()
    def get_validation_run(
        contract: str, ctx: Context, run_id: str | None = None
    ) -> dict[str, Any] | None:
        """Get one persisted validation run, scoped to a trusted contract."""
        return service(ctx).get_validation_run(contract, run_id=run_id)

    @server.tool()
    def measure_failure_samples(
        datasource: str,
        contract: str,
        rule_id: str,
        ctx: Context,
        n: int = 5,
        env: str | None = None,
    ) -> dict[str, Any]:
        """Measure bounded current-data failure samples; this is not a historical sample."""
        return service(ctx).measure_failure_samples(
            datasource, contract, rule_id, n=n, env=env
        )

    @server.tool()
    def profile_history(
        datasource: str, ctx: Context, limit: int = 20
    ) -> list[dict[str, Any]]:
        """Read bounded PostgreSQL profile history for a named datasource."""
        return service(ctx).profile_history(datasource, limit=limit)

    @server.tool()
    def profile_diff(datasource: str, ctx: Context) -> dict[str, Any] | None:
        """Compare the two latest persisted profiles for a named datasource."""
        return service(ctx).profile_diff(datasource)

    @server.tool()
    def compare_datasets(
        before: str,
        after: str,
        ctx: Context,
        key: str | list[str] | None = None,
        before_key: str | list[str] | None = None,
        after_key: str | list[str] | None = None,
    ) -> dict[str, Any]:
        """Measure structural differences between two bounded configured datasets."""
        return service(ctx).compare_datasets(
            before,
            after,
            key=key,
            before_key=before_key,
            after_key=after_key,
        )

    @server.tool()
    def profile_relationship(
        left: str,
        right: str,
        ctx: Context,
        on: str | list[str] | None = None,
        left_on: str | list[str] | None = None,
        right_on: str | list[str] | None = None,
    ) -> dict[str, Any]:
        """Measure relational shape without recommending or executing a join."""
        return service(ctx).profile_relationship(
            left,
            right,
            on=on,
            left_on=left_on,
            right_on=right_on,
        )

    return server
