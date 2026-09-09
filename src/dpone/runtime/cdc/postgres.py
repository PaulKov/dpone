"""Public facade for Postgres logical CDC reader contracts."""

from __future__ import annotations

from dpone.runtime.cdc.postgres_logical_reader import (
    PostgresLogicalCDCReader,
    PostgresLogicalCDCReaderConfig,
    PostgresLogicalPlugin,
)
from dpone.runtime.cdc.postgres_pgoutput import (
    PgOutputColumn,
    PgOutputMessageParser,
    PgOutputRelation,
    _BinaryReader,
)
from dpone.runtime.cdc.postgres_test_decoding import (
    TestDecodingMessageParser,
)

__all__ = [
    "PostgresLogicalCDCReaderConfig",
    "PgOutputColumn",
    "PgOutputRelation",
    "PgOutputMessageParser",
    "_BinaryReader",
    "TestDecodingMessageParser",
    "PostgresLogicalPlugin",
    "PostgresLogicalCDCReader",
]
