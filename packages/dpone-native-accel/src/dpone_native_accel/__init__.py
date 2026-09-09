"""Optional dpone native acceleration provider."""

from __future__ import annotations

from dpone_native_accel._provider import (
    DIRECT_INGEST_SCHEMA_VERSION,
    SCHEMA_VERSION,
    __version__,
    capabilities,
    direct_ingest_capabilities,
    insert_clickhouse_native,
    transcode,
    transcode_batches,
)

__all__ = [
    "DIRECT_INGEST_SCHEMA_VERSION",
    "SCHEMA_VERSION",
    "__version__",
    "capabilities",
    "direct_ingest_capabilities",
    "insert_clickhouse_native",
    "transcode",
    "transcode_batches",
]
