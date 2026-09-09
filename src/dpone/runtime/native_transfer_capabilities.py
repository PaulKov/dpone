from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from dpone.runtime.native_transfer_transport import (
    NativeTransferTransportPolicy,
    SliceTransportPlan,
    StreamCapability,
    TransferTransportResolver,
)


class NativeTransferCapabilityPlanner:
    """Resolve stream/file native-transfer capabilities for plan and runtime UX."""

    def __init__(self, resolver: TransferTransportResolver | None = None) -> None:
        self._resolver = resolver or TransferTransportResolver()

    def plan(
        self,
        *,
        source_type: str,
        sink_type: str,
        source_options: Mapping[str, Any],
        sink_options: Mapping[str, Any],
        transport: NativeTransferTransportPolicy,
    ) -> SliceTransportPlan:
        return self._resolver.resolve(
            transport,
            source=_source_capability(source_type, source_options),
            sink=_sink_capability(sink_type, sink_options),
            codec=_codec_capability(source_type, sink_type, source_options),
        )


def _source_capability(source_type: str, options: Mapping[str, Any]) -> StreamCapability:
    normalized = source_type.lower()
    extract_mode = str(options.get("extract_mode") or "").lower()
    if normalized == "postgres" and extract_mode in {"copy_to_stdout", "copy_to_stream", "copy"}:
        return StreamCapability.supported("postgres_copy_stdout")
    if normalized == "mssql" and extract_mode in {"", "bcp", "bcp_queryout"}:
        return StreamCapability.unsupported("bcp_queryout_is_file_transport")
    return StreamCapability.unsupported(f"{normalized}_stream_export_not_declared")


def _sink_capability(sink_type: str, options: Mapping[str, Any]) -> StreamCapability:
    normalized = sink_type.lower()
    clickhouse_bulk = options.get("clickhouse_bulk") if isinstance(options.get("clickhouse_bulk"), Mapping) else {}
    bulk_mode = str((clickhouse_bulk or {}).get("mode") or "").lower()
    if normalized == "clickhouse" and bulk_mode == "http":
        return StreamCapability.supported("clickhouse_http_body")
    if normalized == "clickhouse":
        return StreamCapability.unsupported("clickhouse_http_bulk_not_configured")
    return StreamCapability.unsupported(f"{normalized}_stream_staging_not_declared")


def _codec_capability(source_type: str, sink_type: str, options: Mapping[str, Any]) -> StreamCapability:
    del options
    if source_type.lower() == "postgres" and sink_type.lower() == "clickhouse":
        return StreamCapability.supported("tabseparated_codec")
    if source_type.lower() == "mssql" and sink_type.lower() == "clickhouse":
        return StreamCapability.supported("tabseparated_codec")
    return StreamCapability.unsupported("native_transfer_codec_not_stream_certified")


__all__ = ["NativeTransferCapabilityPlanner"]
