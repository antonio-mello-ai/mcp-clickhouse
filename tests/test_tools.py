"""Tests for mcp-clickhouse tools."""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, patch

import pytest

from mcp_clickhouse.identifiers import (
    quote_identifier,
    quote_qualified_name,
    validate_identifier,
)
from mcp_clickhouse.tools.queries import _validate_read_only

# ---------------------------------------------------------------------------
# Identifier quoting / validation (SQL injection defence)
# ---------------------------------------------------------------------------


class TestIdentifierQuoting:
    """Tests for backtick-quoting and validating SQL identifiers."""

    @pytest.mark.parametrize(
        "ident",
        ["events", "my_table", "_private", "Col123", "DATABASE"],
    )
    def test_valid_identifiers_pass(self, ident: str) -> None:
        assert validate_identifier(ident) == ident
        assert quote_identifier(ident) == f"`{ident}`"

    @pytest.mark.parametrize(
        "ident",
        [
            "events`; DROP TABLE users; --",  # backtick break-out + injection
            "tbl; DROP TABLE x",  # semicolon
            "tbl name",  # space
            "tbl--comment",  # SQL comment / dash
            "db.table",  # dot (qualified, not a single identifier)
            "tbl'",  # single quote
            'tbl"',  # double quote
            "tbl)",  # paren
            "123tbl",  # leading digit
            "",  # empty
            "select * from secrets",  # whole injected clause
        ],
    )
    def test_malicious_identifiers_rejected(self, ident: str) -> None:
        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            validate_identifier(ident)
        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            quote_identifier(ident)

    def test_qualified_name_quoted(self) -> None:
        assert quote_qualified_name("db.table") == "`db`.`table`"
        assert quote_qualified_name("events") == "`events`"

    @pytest.mark.parametrize(
        "name",
        [
            "db.tbl`; DROP TABLE x; --",  # injection in table part
            "bad`db.tbl",  # injection in db part
            "db.",  # empty table part
            ".tbl",  # empty db part
            "a.b.c.d`",  # injection in a deep part
        ],
    )
    def test_qualified_name_rejects_injection(self, name: str) -> None:
        with pytest.raises(ValueError):
            quote_qualified_name(name)


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
    mock_client.query.assert_called_once_with("SHOW TABLES FROM `default`")


@pytest.mark.asyncio
async def test_list_tables_custom_db(mock_client: AsyncMock) -> None:
    from mcp_clickhouse.tools.queries import list_tables

    await list_tables(database="analytics")
    mock_client.query.assert_called_once_with("SHOW TABLES FROM `analytics`")


@pytest.mark.asyncio
async def test_describe_table(mock_client: AsyncMock) -> None:
    from mcp_clickhouse.tools.queries import describe_table

    await describe_table("events", database="analytics")
    mock_client.query.assert_called_once_with("DESCRIBE TABLE `analytics`.`events`")


@pytest.mark.asyncio
async def test_check_table_freshness_auto_detect(mock_client: AsyncMock) -> None:
    from mcp_clickhouse.tools.monitoring import check_table_freshness

    # First candidate (_timestamp) succeeds
    await check_table_freshness("events")
    mock_client.query.assert_called_once_with(
        "SELECT max(`_timestamp`) AS latest FROM `default`.`events`"
    )


@pytest.mark.asyncio
async def test_check_table_freshness_custom_col(mock_client: AsyncMock) -> None:
    from mcp_clickhouse.tools.monitoring import check_table_freshness

    await check_table_freshness("events", timestamp_col="created_at", database="prod")
    mock_client.query.assert_called_once_with(
        "SELECT max(`created_at`) AS latest FROM `prod`.`events`"
    )


@pytest.mark.asyncio
async def test_check_table_freshness_qualified_name(mock_client: AsyncMock) -> None:
    """Qualified table name should NOT get double-prefixed."""
    from mcp_clickhouse.tools.monitoring import check_table_freshness

    await check_table_freshness("bronze.src_marketplace_vendas_mysql")
    sql = mock_client.query.call_args[0][0]
    assert "`bronze`.`src_marketplace_vendas_mysql`" in sql
    assert "`default`.`bronze`" not in sql


@pytest.mark.asyncio
async def test_check_table_freshness_auto_detect_fallback(
    mock_client: AsyncMock,
) -> None:
    """When first candidates fail, try the next ones."""
    from mcp_clickhouse.tools.monitoring import check_table_freshness

    call_count = 0

    async def fail_then_succeed(sql: str) -> str:
        nonlocal call_count
        call_count += 1
        if call_count <= 2:
            raise Exception("column not found")
        return MOCK_JSON_RESPONSE

    mock_client.query = AsyncMock(side_effect=fail_then_succeed)

    result = await check_table_freshness("events")
    assert result == MOCK_JSON_RESPONSE
    assert call_count == 3  # _timestamp failed, timestamp failed, created_at worked


@pytest.mark.asyncio
async def test_check_table_freshness_no_timestamp_col(mock_client: AsyncMock) -> None:
    """When all candidates fail, return error message."""
    from mcp_clickhouse.tools.monitoring import check_table_freshness

    mock_client.query = AsyncMock(side_effect=Exception("column not found"))

    result = await check_table_freshness("events")
    assert "error" in result
    assert "No timestamp column found" in result


@pytest.mark.asyncio
async def test_get_row_counts(mock_client: AsyncMock) -> None:
    from mcp_clickhouse.tools.monitoring import get_row_counts

    await get_row_counts(["events", "users"])
    call_sql = mock_client.query.call_args[0][0]
    assert "`events`" in call_sql
    assert "`users`" in call_sql
    assert "UNION ALL" in call_sql


@pytest.mark.asyncio
async def test_get_row_counts_qualified_names(mock_client: AsyncMock) -> None:
    """Qualified table names should NOT get double-prefixed."""
    from mcp_clickhouse.tools.monitoring import get_row_counts

    await get_row_counts(["bronze.events", "silver.users"])
    call_sql = mock_client.query.call_args[0][0]
    assert "`bronze`.`events`" in call_sql
    assert "`silver`.`users`" in call_sql
    assert "`default`.`bronze`" not in call_sql


# ---------------------------------------------------------------------------
# Tool-level injection: malicious identifiers must be rejected end-to-end
# ---------------------------------------------------------------------------

_MALICIOUS = "x`; DROP TABLE users; --"


@pytest.mark.asyncio
async def test_list_tables_rejects_injection(mock_client: AsyncMock) -> None:
    from mcp_clickhouse.tools.queries import list_tables

    with pytest.raises(ValueError, match="Invalid SQL identifier"):
        await list_tables(database=_MALICIOUS)
    mock_client.query.assert_not_called()


@pytest.mark.asyncio
async def test_describe_table_rejects_injection(mock_client: AsyncMock) -> None:
    from mcp_clickhouse.tools.queries import describe_table

    with pytest.raises(ValueError, match="Invalid SQL identifier"):
        await describe_table(_MALICIOUS, database="analytics")
    mock_client.query.assert_not_called()


@pytest.mark.asyncio
async def test_check_table_freshness_rejects_injection_in_table(
    mock_client: AsyncMock,
) -> None:
    from mcp_clickhouse.tools.monitoring import check_table_freshness

    with pytest.raises(ValueError, match="Invalid SQL identifier"):
        await check_table_freshness(_MALICIOUS)
    mock_client.query.assert_not_called()


@pytest.mark.asyncio
async def test_check_table_freshness_rejects_injection_in_column(
    mock_client: AsyncMock,
) -> None:
    from mcp_clickhouse.tools.monitoring import check_table_freshness

    with pytest.raises(ValueError, match="Invalid SQL identifier"):
        await check_table_freshness("events", timestamp_col=_MALICIOUS)
    mock_client.query.assert_not_called()


@pytest.mark.asyncio
async def test_get_row_counts_rejects_injection(mock_client: AsyncMock) -> None:
    from mcp_clickhouse.tools.monitoring import get_row_counts

    with pytest.raises(ValueError, match="Invalid SQL identifier"):
        await get_row_counts(["events", _MALICIOUS])
    mock_client.query.assert_not_called()
