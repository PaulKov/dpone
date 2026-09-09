"""Retention probe adapters for CDC ops services."""

from __future__ import annotations

from typing import Any

from dpone.ops.cdc.runtime_adapters import CDCBackend, required_text
from dpone.runtime.cdc.retention_models import CdcRetentionBounds
from dpone.runtime.cdc.retention_probes import (
    MssqlCdcRetentionProbe,
    MssqlChangeTrackingRetentionProbe,
    StaticCdcRetentionProbe,
)


def static_retention_probe(
    *,
    backend: CDCBackend,
    min_available_offset: str | None,
    high_watermark: str | None,
    current_offset: str | None,
    retention_seconds: int | None,
) -> StaticCdcRetentionProbe:
    return StaticCdcRetentionProbe(
        CdcRetentionBounds(
            backend=backend,
            min_available_offset=required_text(min_available_offset, "--min-available-offset"),
            high_watermark=required_text(high_watermark, "--high-watermark"),
            current_offset=current_offset or required_text(high_watermark, "--high-watermark"),
            retention_seconds=retention_seconds,
        )
    )


def mssql_retention_probe(
    *,
    connector: Any,
    backend: CDCBackend,
    source_schema: str,
    source_table: str,
    capture_instance: str | None,
) -> MssqlCdcRetentionProbe | MssqlChangeTrackingRetentionProbe:
    if backend == CDCBackend.MSSQL_CHANGE_TRACKING:
        return MssqlChangeTrackingRetentionProbe(
            connector=connector,
            source_schema=source_schema,
            source_table=source_table,
        )
    if backend == CDCBackend.MSSQL_CDC:
        return MssqlCdcRetentionProbe(
            connector=connector,
            capture_instance=capture_instance or f"{source_schema}_{source_table}",
        )
    raise ValueError(f"Unsupported MSSQL retention backend: {backend.value}")


__all__ = ["mssql_retention_probe", "static_retention_probe"]
