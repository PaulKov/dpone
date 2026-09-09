"""Deprecated compatibility shim for ClickHouse sink imports.

Use ``dpone.runtime.sinks.clickhouse`` for public imports or
``dpone.runtime.sinks.clickhouse_sink`` for internal implementation imports.
"""

from __future__ import annotations

from dpone.runtime.sinks.clickhouse_sink import ClickHouseSink

__all__ = ["ClickHouseSink"]
