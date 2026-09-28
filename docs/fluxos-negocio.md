---
title: Supported operator flows
kind: source_doc
area: product
project: mcp-clickhouse
collection: mcp-clickhouse
owner: maintainers
status: current
canonical: docs/fluxos-negocio.md
globalRef: qmd://mcp-clickhouse/docs/fluxos-negocio.md
reviewCadenceDays: 90
lastReviewedAt: 2026-09-28
sourceRefs:
  - src/mcp_clickhouse/tools/queries.py
  - src/mcp_clickhouse/tools/monitoring.py
related:
  - README.md
  - docs/arquitetura.md
  - docs/operacao.md
supersedes: []
supersededBy: []
sensitivity: public
---
# Supported operator flows

## Execute a read query

`execute_query` accepts statements beginning with `SELECT`, `WITH`, `SHOW`,
`DESCRIBE`, or `EXPLAIN` and returns ClickHouse JSON output. The prefix check is
only defense in depth. The configured ClickHouse account must enforce readonly
grants and limits on the server side; GitHub issues #3 and #6 track the remaining
resource-limit and documentation work.

## Discover databases and tables

- `list_databases` returns visible databases.
- `list_tables` returns tables in the selected or default database.
- `describe_table` returns a table schema.

Database, table, and column identifiers are validated against a conservative
character set and quoted before interpolation.

## Check freshness

`check_table_freshness` reads the maximum value from an explicit timestamp
column or tries a documented set of common column names. GitHub issue #8 tracks
a mismatch between the fallback logic and the current client error contract.

## Count rows

`get_row_counts` combines counts for one or more tables. Qualified names are
supported and validated. Empty input is not yet rejected clearly; GitHub issue
#11 tracks that correction.

## Future inspection tools

Partition/parts introspection and a dedicated explain tool remain in GitHub
issues #4 and #5. Roadmap items live in GitHub rather than this document.
