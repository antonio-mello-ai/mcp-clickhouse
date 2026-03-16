# mcp-clickhouse

MCP server for interacting with ClickHouse databases via [FastMCP](https://github.com/jlowin/fastmcp).

## Install

```bash
pip install -e ".[dev]"
```

## Environment variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `CLICKHOUSE_HOST` | yes | — | HTTP interface URL (e.g. `http://100.x.x.x:8123`) |
| `CLICKHOUSE_USER` | yes | — | ClickHouse user |
| `CLICKHOUSE_PASSWORD` | yes | — | ClickHouse password |
| `CLICKHOUSE_DATABASE` | no | `default` | Default database |

Copy `.env.example` and fill in your values.

## Tools

| Tool | Description |
|------|-------------|
| `execute_query(sql)` | Run a read-only query (SELECT, WITH, SHOW, DESCRIBE, EXPLAIN) |
| `list_databases()` | List all databases |
| `list_tables(database?)` | List tables in a database |
| `describe_table(table, database?)` | Describe table schema |
| `check_table_freshness(table, timestamp_col?, database?)` | Get MAX timestamp from a table |
| `get_row_counts(tables, database?)` | Get row counts for multiple tables |

## Run

```bash
mcp-clickhouse
```

## Tests

```bash
pytest
```
