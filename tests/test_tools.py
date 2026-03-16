"""Tests for mcp-clickhouse tools."""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, patch

import pytest

from mcp_clickhouse.tools.queries import _validate_read_only

# ---------------------------------------------------------------------------
# SQL injection / read-only validation
# ---------------------------------------------------------------------------


class TestValidateReadOnly:
    """Tests for the read-only prefix check."""

    @pytest.mark.parametrize(
        "sql",
        [
            "SELECT 1",
            "  select * from t",
            "WITH cte AS (SELECT 1) SELECT * FROM cte",
            "SHOW DATABASES",
            "DESCRIBE TABLE t",
            "EXPLAIN SELECT 1",
        ],
    )
    def test_allowed_statements(self, sql: str) -> None:
        _validate_read_only(sql)  # should not raise

    @pytest.mark.parametrize(
        "sql",
        [
            "INSERT INTO t VALUES (1)",
            "DROP TABLE t",
            "ALTER TABLE t ADD COLUMN c Int32",
            "CREATE TABLE t (id Int32) ENGINE = Memory",
            "DELETE FROM t WHERE 1=1",
            "TRUNCATE TABLE t",
            "UPDATE t SET x=1",
            "GRANT SELECT ON t TO user",
            "  insert into t values (1)",
            "SYSTEM RELOAD DICTIONARY",
        ],
    )
    def test_rejected_statements(self, sql: str) -> None:
        with pytest.raises(ValueError, match="Only read-only queries"):
            _validate_read_only(sql)

    def test_empty_query(self) -> None:
        with pytest.raises((ValueError, IndexError)):
            _validate_read_only("")

    def test_whitespace_only(self) -> None:
        with pytest.raises((ValueError, IndexError)):
            _validate_read_only("   ")


# ---------------------------------------------------------------------------
# Tool integration tests (mocked HTTP)
# ---------------------------------------------------------------------------

MOCK_JSON_RESPONSE = json.dumps(
    {
        "meta": [{"name": "count()", "type": "UInt64"}],
        "data": [{"count()": "42"}],
        "rows": 1,
    }
)


@pytest.fixture()
def mock_client():
    """Patch get_client to return an AsyncMock ClickHouseClient."""
    client = AsyncMock()
    client.query = AsyncMock(return_value=MOCK_JSON_RESPONSE)
    client._config = type("C", (), {"database": "default"})()
    with patch("mcp_clickhouse.tools.queries.get_client", return_value=client):
        with patch("mcp_clickhouse.tools.monitoring.get_client", return_value=client):
            yield client


@pytest.mark.asyncio
async def test_execute_query_select(mock_client: AsyncMock) -> None:
    from mcp_clickhouse.tools.queries import execute_query

    result = await execute_query("SELECT count() FROM events")
    assert result == MOCK_JSON_RESPONSE
    mock_client.query.assert_called_once_with("SELECT count() FROM events")


@pytest.mark.asyncio
async def test_execute_query_rejects_insert(mock_client: AsyncMock) -> None:
    from mcp_clickhouse.tools.queries import execute_query

    with pytest.raises(ValueError, match="Only read-only"):
        await execute_query("INSERT INTO events VALUES (1)")


@pytest.mark.asyncio
async def test_list_databases(mock_client: AsyncMock) -> None:
    from mcp_clickhouse.tools.queries import list_databases

    result = await list_databases()
    assert result == MOCK_JSON_RESPONSE
    mock_client.query.assert_called_once_with("SHOW DATABASES")


@pytest.mark.asyncio
async def test_list_tables_default_db(mock_client: AsyncMock) -> None:
    from mcp_clickhouse.tools.queries import list_tables

    await list_tables()
    mock_client.query.assert_called_once_with("SHOW TABLES FROM default")


@pytest.mark.asyncio
async def test_list_tables_custom_db(mock_client: AsyncMock) -> None:
    from mcp_clickhouse.tools.queries import list_tables

    await list_tables(database="analytics")
    mock_client.query.assert_called_once_with("SHOW TABLES FROM analytics")


@pytest.mark.asyncio
async def test_describe_table(mock_client: AsyncMock) -> None:
    from mcp_clickhouse.tools.queries import describe_table

    await describe_table("events", database="analytics")
    mock_client.query.assert_called_once_with("DESCRIBE TABLE analytics.events")


@pytest.mark.asyncio
async def test_check_table_freshness_defaults(mock_client: AsyncMock) -> None:
    from mcp_clickhouse.tools.monitoring import check_table_freshness

    await check_table_freshness("events")
    mock_client.query.assert_called_once_with(
        "SELECT max(_timestamp) AS latest FROM default.events"
    )


@pytest.mark.asyncio
async def test_check_table_freshness_custom_col(mock_client: AsyncMock) -> None:
    from mcp_clickhouse.tools.monitoring import check_table_freshness

    await check_table_freshness("events", timestamp_col="created_at", database="prod")
    mock_client.query.assert_called_once_with(
        "SELECT max(created_at) AS latest FROM prod.events"
    )


@pytest.mark.asyncio
async def test_get_row_counts(mock_client: AsyncMock) -> None:
    from mcp_clickhouse.tools.monitoring import get_row_counts

    await get_row_counts(["events", "users"])
    call_sql = mock_client.query.call_args[0][0]
    assert "events" in call_sql
    assert "users" in call_sql
    assert "UNION ALL" in call_sql
