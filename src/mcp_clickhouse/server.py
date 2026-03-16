"""FastMCP server for ClickHouse."""

from mcp.server.fastmcp import FastMCP

from mcp_clickhouse.client import ClickHouseClient
from mcp_clickhouse.config import ClickHouseConfig

mcp = FastMCP("mcp-clickhouse")

# Lazy-initialised singleton — created on first tool call.
_client: ClickHouseClient | None = None


def get_client() -> ClickHouseClient:
    """Return the shared ClickHouseClient, creating it on first use."""
    global _client  # noqa: PLW0603
    if _client is None:
        config = ClickHouseConfig.from_env()
        _client = ClickHouseClient(config)
    return _client


# Import tool modules so their @mcp.tool() decorators register with this server.
from mcp_clickhouse.tools import monitoring, queries  # noqa: E402, F401


def main() -> None:
    """Entry-point for the ``mcp-clickhouse`` console script."""
    mcp.run()


if __name__ == "__main__":
    main()
