---
title: Architecture
kind: architecture
area: engineering
project: mcp-clickhouse
collection: mcp-clickhouse
owner: maintainers
status: current
canonical: docs/arquitetura.md
globalRef: qmd://mcp-clickhouse/docs/arquitetura.md
reviewCadenceDays: 90
lastReviewedAt: 2026-09-28
sourceRefs:
  - pyproject.toml
  - src/mcp_clickhouse/server.py
  - src/mcp_clickhouse/client.py
  - src/mcp_clickhouse/config.py
  - src/mcp_clickhouse/identifiers.py
related:
  - docs/fluxos-negocio.md
  - docs/operacao.md
supersedes: []
supersededBy: []
sensitivity: public
---
# Architecture

## Components

- `server.py` creates the FastMCP server and lazy ClickHouse client.
- `config.py` reads the HTTP endpoint and credentials from the environment.
- `client.py` owns the reusable asynchronous HTTP client and response contract.
- `identifiers.py` validates and quotes SQL identifiers.
- `tools/queries.py` exposes general inspection and read-query tools.
- `tools/monitoring.py` exposes freshness and row-count tools.

The package uses the official MCP Python SDK and `httpx`. It does not persist
query results or credentials.

## Request path

1. An MCP host starts `mcp-clickhouse` and invokes a registered tool.
2. The tool validates its structured arguments and any SQL/identifier boundary.
3. The shared HTTP client sends the query to the configured ClickHouse endpoint.
4. ClickHouse executes the query under the configured user's server-side grants
   and settings.
5. The tool returns JSON text or a concise validation/error result.

## Security boundaries

Application prefix and identifier checks reduce accidental misuse but cannot
replace ClickHouse authorization. A deployment should use a dedicated readonly
user with explicit database/table grants and server-side query limits.

The current client places credentials in URL parameters. GitHub issue #9 tracks
moving them to supported authentication headers so URLs, proxy logs, and traces
do not carry passwords.

## Error boundary

The client currently converts HTTP errors into strings. Callers that need to
distinguish a missing column from authentication, network, or server failure
cannot reliably do so. GitHub issue #8 owns the typed/error-contract correction.
