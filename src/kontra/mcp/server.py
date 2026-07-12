"""FastMCP server factory. Importing this module does not import the MCP SDK."""

from contextlib import asynccontextmanager
import ipaddress
from typing import Any

from kontra.mcp.service import KontraMCPService, MCPSettings


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

    def service(ctx: Any) -> KontraMCPService:
        return ctx.request_context.lifespan_context

    def request_service() -> KontraMCPService:
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
    def profile_history(
        datasource: str, ctx: Context, limit: int = 20
    ) -> list[dict[str, Any]]:
        """Read bounded PostgreSQL profile history for a named datasource."""
        return service(ctx).profile_history(datasource, limit=limit)

    return server
