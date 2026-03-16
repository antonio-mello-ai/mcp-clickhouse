"""Query-oriented ClickHouse tools."""

from __future__ import annotations

from mcp_clickhouse.server import get_client, mcp

# Allowed first-token prefixes (upper-cased).
_ALLOWED_PREFIXES = frozenset({"SELECT", "WITH", "SHOW", "DESCRIBE", "EXPLAIN"})


def _validate_read_only(sql: str) -> None:
    """Raise ValueError if *sql* is not a read-only statement.

    Defence-in-depth: the ClickHouse user should also have read-only grants,
    but we reject obviously mutating statements at the application layer.
    """
    first_token = sql.strip().split(maxsplit=1)[0].upper()
    if first_token not in _ALLOWED_PREFIXES:
        raise ValueError(
            f"Only read-only queries are allowed. "
            f"Got '{first_token}' — permitted prefixes: {sorted(_ALLOWED_PREFIXES)}"
        )


@mcp.tool()
async def execute_query(sql: str) -> str:
    """Execute a READ-ONLY SQL query against ClickHouse.

    Rejects any DML/DDL — only SELECT, WITH, SHOW, DESCRIBE, and EXPLAIN
    statements are accepted.

    Args:
        sql: The SQL query to execute.

    Returns:
        Query results as JSON.
    """
    _validate_read_only(sql)
    client = get_client()
    return await client.query(sql)


@mcp.tool()
async def list_databases() -> str:
    """List all databases available in ClickHouse.

    Returns:
        JSON with the list of databases.
    """
    client = get_client()
    return await client.query("SHOW DATABASES")


@mcp.tool()
async def list_tables(database: str | None = None) -> str:
    """List tables in a ClickHouse database.

    Args:
        database: Database to inspect. Uses the configured default if omitted.

    Returns:
        JSON with the list of tables.
    """
    client = get_client()
    db = database or client._config.database
    return await client.query(f"SHOW TABLES FROM {db}")


@mcp.tool()
async def describe_table(table: str, database: str | None = None) -> str:
    """Describe the schema of a ClickHouse table.

    Args:
        table: Table name.
        database: Database containing the table. Uses the configured default
                  if omitted.

    Returns:
        JSON with column names, types, and defaults.
    """
    client = get_client()
    db = database or client._config.database
    return await client.query(f"DESCRIBE TABLE {db}.{table}")
