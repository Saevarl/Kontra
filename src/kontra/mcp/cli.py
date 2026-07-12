"""Command-line entry point for the official Kontra MCP."""

from __future__ import annotations

import argparse
from collections.abc import Sequence


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="kontra-mcp", description="Official Kontra MCP server")
    parser.add_argument(
        "--transport",
        choices=("stdio", "streamable-http"),
        default="stdio",
        help="MCP transport (default: stdio)",
    )
    parser.add_argument("--host", default="127.0.0.1", help="HTTP bind host")
    parser.add_argument("--port", type=int, default=8000, help="HTTP bind port")
    parser.add_argument(
        "--allow-remote-unauthenticated",
        action="store_true",
        help="Allow unauthenticated HTTP on a non-loopback host (unsafe without an auth proxy)",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> None:
    args = build_parser().parse_args(argv)
    if not 1 <= args.port <= 65535:
        raise SystemExit("--port must be between 1 and 65535")
    from kontra.mcp.server import create_server

    try:
        server = create_server(
            host=args.host,
            port=args.port,
            allow_remote_unauthenticated=(
                args.transport == "stdio" or args.allow_remote_unauthenticated
            ),
        )
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc
    server.run(transport=args.transport)


if __name__ == "__main__":
    main()
