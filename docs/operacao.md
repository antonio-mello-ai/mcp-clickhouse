---
title: Operations
kind: runbook
area: operations
project: mcp-clickhouse
collection: mcp-clickhouse
owner: maintainers
status: current
canonical: docs/operacao.md
globalRef: qmd://mcp-clickhouse/docs/operacao.md
reviewCadenceDays: 90
lastReviewedAt: 2026-09-28
sourceRefs:
  - README.md
  - pyproject.toml
  - .github/workflows/ci.yml
related:
  - docs/arquitetura.md
  - docs/fluxos-negocio.md
supersedes: []
supersededBy: []
sensitivity: public
---
# Operations

## Runtime configuration

| Variable | Required | Purpose |
|---|---|---|
| `CLICKHOUSE_HOST` | yes | ClickHouse HTTP(S) endpoint |
| `CLICKHOUSE_USER` | yes | Dedicated ClickHouse account |
| `CLICKHOUSE_PASSWORD` | yes | Account credential |
| `CLICKHOUSE_DATABASE` | no | Default database, falling back to `default` |

Provide credentials through the MCP host or an authorized secret adapter. Never
put real values in repository files, shell history, issues, or tool output.

## Deployment boundary

- Prefer HTTPS with certificate verification or a trusted private network.
- Use a dedicated readonly user with the minimum database/table grants.
- Enforce execution-time, result-row, and memory limits in ClickHouse.
- Restrict the MCP server to authorized callers.
- Review proxy and query logs for credential and data-retention behavior.
- Treat arbitrary read queries as potentially expensive even when non-mutating.

## Local validation

```bash
uv sync --extra dev
uv run --all-extras pytest -q
uvx ruff check src/ tests/
uvx ruff format --check src/ tests/
```

Integration tests should use a disposable or non-production database and a
least-privilege account. Do not print environment variables or authenticated
request URLs while diagnosing.

## Release process

`pyproject.toml` is the package-version source of truth. GitHub releases trigger
trusted publishing to PyPI. GitHub issue #12 tracks building and validating the
wheel and sdist earlier in pull-request CI. GitHub issue #10 tracks removal of
the stale hook that still expects a deleted `VERSION` file.

Before publishing:

- ensure CI passes on supported Python versions;
- confirm the version and release tag match;
- verify an isolated wheel installation and MCP entry point;
- review dependencies and public documentation;
- verify the PyPI artifact after one trusted-publishing run.

## Failure interpretation

- A successful HTTP response proves query acceptance, not data correctness.
- A ClickHouse error string must not be interpreted as valid query JSON.
- Retrying a failed expensive query requires understanding the original failure.
- Readonly SQL validation does not prove server-side least privilege.
