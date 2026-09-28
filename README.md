---
title: MCP ClickHouse
kind: source_doc
area: engineering
project: mcp-clickhouse
collection: mcp-clickhouse
owner: maintainers
status: current
canonical: README.md
globalRef: qmd://mcp-clickhouse/README.md
reviewCadenceDays: 90
lastReviewedAt: 2026-09-28
sourceRefs:
  - pyproject.toml
related:
  - AGENTS.md
  - docs/fluxos-negocio.md
  - docs/arquitetura.md
  - docs/operacao.md
  - docs/index.md
supersedes: []
supersededBy: []
sensitivity: public
---
# mcp-clickhouse

MCP server for inspecting and querying ClickHouse through the FastMCP API from
the [MCP Python SDK](https://github.com/modelcontextprotocol/python-sdk).

This is an open-source package. The maintainers do not operate a public hosted
ClickHouse endpoint through this repository.

## Install

```bash
uvx mcp-clickhouse
# or
pip install mcp-clickhouse
```

For development, use `uv sync --extra dev`.

## Environment variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `CLICKHOUSE_HOST` | yes | — | HTTP interface URL (for example, `https://clickhouse.example.com:8443`) |
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

## Security boundary

The SQL-prefix validation is an application guardrail, not the authoritative
readonly control. Use a least-privilege ClickHouse account with server-side
readonly settings, resource limits, and HTTPS or a trusted private network.
See [`docs/operacao.md`](docs/operacao.md).

## Documentation

Start at [`docs/index.md`](docs/index.md). Roadmap and delivery status live in
GitHub Issues and pull requests.

## License

MIT
