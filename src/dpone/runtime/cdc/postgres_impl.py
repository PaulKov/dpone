"""Deprecated compatibility shim for Postgres logical CDC reader imports.

Use ``dpone.runtime.cdc.postgres`` for public imports or
``dpone.runtime.cdc.postgres_logical_reader`` for focused reader imports.
"""

from __future__ import annotations

from dpone.runtime.cdc.postgres_logical_reader import (
    PostgresLogicalCDCReader,
    PostgresLogicalCDCReaderConfig,
    PostgresLogicalPlugin,
)

__all__ = [
    "PostgresLogicalCDCReaderConfig",
    "PostgresLogicalPlugin",
    "PostgresLogicalCDCReader",
]
