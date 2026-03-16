"""ClickHouse connection configuration."""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class ClickHouseConfig:
    """Configuration for connecting to ClickHouse HTTP interface."""

    host: str
    user: str
    password: str
    database: str

    @classmethod
    def from_env(cls) -> ClickHouseConfig:
        """Build config from environment variables.

        Required:
            CLICKHOUSE_HOST — e.g. http://100.x.x.x:8123
            CLICKHOUSE_USER
            CLICKHOUSE_PASSWORD

        Optional:
            CLICKHOUSE_DATABASE — defaults to "default"
        """
        host = os.environ.get("CLICKHOUSE_HOST", "")
        if not host:
            raise ValueError("CLICKHOUSE_HOST environment variable is required")

        user = os.environ.get("CLICKHOUSE_USER", "")
        if not user:
            raise ValueError("CLICKHOUSE_USER environment variable is required")

        password = os.environ.get("CLICKHOUSE_PASSWORD", "")
        if not password:
            raise ValueError("CLICKHOUSE_PASSWORD environment variable is required")

        database = os.environ.get("CLICKHOUSE_DATABASE", "default")

        return cls(
            host=host.rstrip("/"),
            user=user,
            password=password,
            database=database,
        )
