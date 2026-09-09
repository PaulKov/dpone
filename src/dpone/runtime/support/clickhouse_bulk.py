"""Shared ClickHouse bulk runner facade for runtime sinks."""

from __future__ import annotations

from dpone.runtime.connectors.clickhouse_bulk import (
    ClickHouseClientCredentials,
    ClickHouseClientOptions,
    ClickHouseClientResult,
    ClickHouseClientRunner,
)
from dpone.runtime.connectors.clickhouse_http_bulk import (
    ClickHouseHttpBulkRunner,
    ClickHouseHttpCredentials,
    ClickHouseHttpOptions,
    ClickHouseHttpResult,
)

__all__ = [
    "ClickHouseClientCredentials",
    "ClickHouseClientOptions",
    "ClickHouseClientResult",
    "ClickHouseClientRunner",
    "ClickHouseHttpBulkRunner",
    "ClickHouseHttpCredentials",
    "ClickHouseHttpOptions",
    "ClickHouseHttpResult",
]
