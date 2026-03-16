"""Monitoring-oriented ClickHouse tools."""

from __future__ import annotations

from mcp_clickhouse.server import get_client, mcp

# Common timestamp column names, checked in order.
_TIMESTAMP_CANDIDATES = (
    "_timestamp",
    "timestamp",
    "created_at",
    "updated_at",
    "event_time",
    "date",
)


def _resolve_qualified_name(
    table: str, default_db: str
) -> str:
    """Return a fully-qualified ``database.table`` name.

    If *table* already contains a dot (e.g. ``bronze.my_table``), use it
    as-is.  Otherwise prepend *default_db*.
    """
    if "." in table:
        return table
    return f"{default_db}.{table}"


@mcp.tool()
async def check_table_freshness(
    table: str,
    timestamp_col: str | None = None,
    database: str | None = None,
) -> str:
    """Check how fresh a table's data is by finding the MAX timestamp.

    Args:
        table: Table name (plain or qualified like ``bronze.my_table``).
        timestamp_col: Column holding the timestamp. If omitted the tool
                       auto-detects by trying common names (_timestamp,
                       timestamp, created_at, updated_at, event_time, date).
        database: Database containing the table. Uses the configured default
                  if omitted.

    Returns:
        JSON with the most recent timestamp value.
    """
    client = get_client()
    db = database or client._config.database
    fqn = _resolve_qualified_name(table, db)

    if timestamp_col:
        sql = f"SELECT max({timestamp_col}) AS latest FROM {fqn}"
        return await client.query(sql)

    # Auto-detect: try each candidate column.
    for col in _TIMESTAMP_CANDIDATES:
        try:
            sql = f"SELECT max({col}) AS latest FROM {fqn}"
            return await client.query(sql)
        except Exception:
            continue

    return (
        f'{{"error": "No timestamp column found in {fqn}. '
        f'Tried: {", ".join(_TIMESTAMP_CANDIDATES)}. '
        f'Pass timestamp_col explicitly."}}'
    )


@mcp.tool()
async def get_row_counts(tables: list[str], database: str | None = None) -> str:
    """Get the row count for one or more tables.

    Args:
        tables: List of table names (plain or qualified).
        database: Database containing the tables. Uses the configured default
                  if omitted.

    Returns:
        JSON with table names and their row counts.
    """
    client = get_client()
    db = database or client._config.database

    unions = " UNION ALL ".join(
        f"SELECT '{t}' AS table_name, count(*) AS row_count "
        f"FROM {_resolve_qualified_name(t, db)}"
        for t in tables
    )
    return await client.query(unions)
