"""Monitoring-oriented ClickHouse tools."""

from __future__ import annotations

from mcp_clickhouse.server import get_client, mcp


@mcp.tool()
async def check_table_freshness(
    table: str,
    timestamp_col: str | None = None,
    database: str | None = None,
) -> str:
    """Check how fresh a table's data is by finding the MAX timestamp.

    Args:
        table: Table name.
        timestamp_col: Column holding the timestamp. Defaults to
                       ``_timestamp`` if not provided.
        database: Database containing the table. Uses the configured default
                  if omitted.

    Returns:
        JSON with the most recent timestamp value.
    """
    client = get_client()
    db = database or client._config.database
    col = timestamp_col or "_timestamp"
    sql = f"SELECT max({col}) AS latest FROM {db}.{table}"
    return await client.query(sql)


@mcp.tool()
async def get_row_counts(tables: list[str], database: str | None = None) -> str:
    """Get the row count for one or more tables.

    Args:
        tables: List of table names.
        database: Database containing the tables. Uses the configured default
                  if omitted.

    Returns:
        JSON with table names and their row counts.
    """
    client = get_client()
    db = database or client._config.database

    unions = " UNION ALL ".join(
        f"SELECT '{t}' AS table_name, count(*) AS row_count FROM {db}.{t}"
        for t in tables
    )
    return await client.query(unions)
