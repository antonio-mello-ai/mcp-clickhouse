---
title: AGENTS.md — MCP ClickHouse
kind: policy
area: engineering
project: mcp-clickhouse
collection: mcp-clickhouse
owner: maintainers
status: current
canonical: AGENTS.md
globalRef: qmd://mcp-clickhouse/AGENTS.md
reviewCadenceDays: 90
lastReviewedAt: 2026-09-28
sourceRefs: []
related:
  - README.md
  - docs/fluxos-negocio.md
  - docs/arquitetura.md
  - docs/operacao.md
  - docs/index.md
supersedes:
  - CLAUDE.md
  - GEMINI.md
supersededBy: []
sensitivity: public
---
# AGENTS.md — MCP ClickHouse

## Purpose

Public MCP server that exposes bounded ClickHouse inspection and read-query
operations as tools. This repository is an open-source package and must not
contain private deployment context.

## Public-repository boundary

- Never commit real hostnames, IP addresses, usernames, credentials, customer
  names, local filesystem paths, private repository references, or deployment
  topology.
- Use reserved example domains and explicit placeholders in documentation.
- Every committed document must be suitable for anonymous public access and use
  `sensitivity: public` when it has structural frontmatter.
- Before every pull request, scan the complete documentation diff for internal
  identifiers and review deleted content for possible exposure in Git history.

## Repository rules

- Preserve the read-only product boundary. Application validation is defense in
  depth; production safety also requires a server-side readonly ClickHouse user.
- Never print passwords, authentication headers, or authenticated URLs.
- Keep identifier validation on every tool that interpolates database, table,
  or column names.
- Do not silently broaden the tool surface or ClickHouse grants.
- Roadmap and priority live in GitHub Issues. Delivery evidence lives in closed
  Issues, pull requests, releases, and PyPI records.
- Do not create `roadmap.md`, `docs/backlog.md`, or `CHANGELOG.md`.

## Development

```bash
uv sync --extra dev
uv run --all-extras pytest
uvx ruff check src/ tests/
uvx ruff format --check src/ tests/
```

`pyproject.toml` is the single source of truth for the package version. The
legacy `VERSION` file no longer exists. Until GitHub issue #10 is implemented,
do not enable the stale `.githooks/pre-push` hook described in older clones.

## Verification

- Add tests for every behavior change and failure path.
- Prefer request-level tests for authentication, limits, and error contracts.
- Keep the server import and registered-tool smoke tests passing.
- Run `git diff --check` and the public-document sanitization scan before push.

## Active documentation

- `README.md`: public overview and installation
- `docs/fluxos-negocio.md`: supported query and monitoring flows
- `docs/arquitetura.md`: components and security boundaries
- `docs/operacao.md`: configuration, validation, and releases
- `docs/index.md`: documentation index
