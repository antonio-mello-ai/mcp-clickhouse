"""Async HTTP client wrapper for ClickHouse."""

from __future__ import annotations

import httpx

from mcp_clickhouse.config import ClickHouseConfig


class ClickHouseClient:
    """Thin wrapper around httpx.AsyncClient for the ClickHouse HTTP interface."""

    def __init__(self, config: ClickHouseConfig) -> None:
        self._config = config
        self._http: httpx.AsyncClient | None = None

    async def _get_http(self) -> httpx.AsyncClient:
        if self._http is None or self._http.is_closed:
            self._http = httpx.AsyncClient(timeout=30.0)
        return self._http

    async def query(self, sql: str, *, database: str | None = None) -> str:
        """Execute a query against ClickHouse and return the raw JSON response.

        Args:
            sql: The SQL statement to execute.
            database: Override the default database for this query.

        Returns:
            Raw response text (JSON).
        """
        http = await self._get_http()
        db = database or self._config.database

        # Append FORMAT JSON unless the query already specifies a format
        query_text = sql.strip().rstrip(";")
        upper = query_text.upper()
        if "FORMAT " not in upper:
            query_text = f"{query_text} FORMAT JSON"

        response = await http.post(
            self._config.host,
            content=query_text,
            params={
                "database": db,
                "user": self._config.user,
                "password": self._config.password,
            },
            headers={"Content-Type": "text/plain"},
        )
        # ClickHouse returns non-200 for query errors (e.g., 404 for unknown table)
        # but includes the error message in the body — return it for the LLM
        if response.status_code >= 400:
            return f"[ClickHouse Error {response.status_code}] {response.text[:500]}"
        return response.text

    async def close(self) -> None:
        """Close the underlying HTTP client."""
        if self._http is not None and not self._http.is_closed:
            await self._http.aclose()
