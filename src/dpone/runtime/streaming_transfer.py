"""Connector-neutral policy and evidence for byte-stream transfer routes."""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import asdict, dataclass, field
from typing import Any

from dpone.runtime.storage_policy import parse_byte_size

STREAMING_ROUTE_SCHEMA_VERSION = "dpone.native_transfer.streaming_route.v1"
DEFAULT_STREAM_READ_BUFFER_BYTES = 4 * 1024 * 1024
MIN_STREAM_READ_BUFFER_BYTES = 64 * 1024
MAX_STREAM_READ_BUFFER_BYTES = 16 * 1024 * 1024
STREAM_READ_BUFFER_LITERAL_PATTERN = (
    r"^\s*(?:(?:6[4-9]|[7-9][0-9]|[1-9][0-9]{2,3}|1[0-5][0-9]{3}|16[0-2][0-9]{2}|"
    r"163[0-7][0-9]|1638[0-4])\s*[Kk][Ii][Bb]|(?:[1-9]|1[0-6])\s*[Mm][Ii][Bb])\s*$"
)
LEGACY_TARGET_CHUNK_LITERAL_PATTERN = (
    r"^\s*(?:[1-9][0-9]*(?:\.[0-9]+)?|0\.[0-9]*[1-9][0-9]*)"
    r"\s*(?:[KkMmGgTt]i?[Bb]|[Bb])?\s*$"
)
DEFAULT_LEGACY_TARGET_CHUNK_BYTES = 512 * 1024 * 1024
STREAMING_TARGET_CHUNK_BYTES_ALIAS = "source.options.native_transfer.snapshot.streaming.target_chunk_bytes"
STREAMING_TARGET_CHUNK_BYTES_DEPRECATION = "streaming_target_chunk_bytes_deprecated_use_read_buffer_bytes"


@dataclass(frozen=True, slots=True)
class StreamingTransferPolicy:
    """User-facing streaming transfer policy."""

    mode: str = "auto"
    provider: str = "bcp_pipe"
    pipe_mode: str = "fifo"
    target_chunk_bytes: int = DEFAULT_LEGACY_TARGET_CHUNK_BYTES
    cleanup_policy: str = "eager"
    delimiter_safety: str = "certified_only"
    read_buffer_bytes: int = DEFAULT_STREAM_READ_BUFFER_BYTES
    configured_read_buffer_bytes: int | None = field(default=None, init=False)
    deprecated_aliases: tuple[str, ...] = field(default=(), init=False)

    def __post_init__(self) -> None:
        read_buffer_bytes = self.read_buffer_bytes
        if (
            isinstance(read_buffer_bytes, bool)
            or not isinstance(read_buffer_bytes, int)
            or not MIN_STREAM_READ_BUFFER_BYTES <= read_buffer_bytes <= MAX_STREAM_READ_BUFFER_BYTES
        ):
            raise ValueError("streaming_read_buffer_bytes_out_of_range")
        if self.configured_read_buffer_bytes is None and read_buffer_bytes != DEFAULT_STREAM_READ_BUFFER_BYTES:
            object.__setattr__(self, "configured_read_buffer_bytes", read_buffer_bytes)

    @classmethod
    def from_options(cls, options: Mapping[str, Any] | None) -> StreamingTransferPolicy:
        native = _mapping((options or {}).get("native_transfer"))
        snapshot = _mapping(native.get("snapshot"))
        streaming = _mapping(snapshot.get("streaming"))
        has_read_buffer = "read_buffer_bytes" in streaming
        has_legacy_target = "target_chunk_bytes" in streaming
        configured_read_buffer = _parse_read_buffer_bytes(streaming["read_buffer_bytes"]) if has_read_buffer else None
        legacy_target = DEFAULT_LEGACY_TARGET_CHUNK_BYTES
        if has_legacy_target and not has_read_buffer:
            legacy_target = _parse_legacy_target_chunk_bytes(streaming["target_chunk_bytes"])
        policy = cls(
            mode=_normalized(streaming.get("mode"), "off"),
            provider=_normalized(streaming.get("provider"), "bcp_pipe"),
            pipe_mode=_normalized(streaming.get("pipe_mode"), "fifo"),
            target_chunk_bytes=legacy_target,
            cleanup_policy=_normalized(streaming.get("cleanup_policy"), "eager"),
            delimiter_safety=_normalized(streaming.get("delimiter_safety"), "certified_only"),
            read_buffer_bytes=(
                configured_read_buffer if configured_read_buffer is not None else DEFAULT_STREAM_READ_BUFFER_BYTES
            ),
        )
        if has_read_buffer:
            object.__setattr__(policy, "configured_read_buffer_bytes", configured_read_buffer)
        if has_legacy_target:
            object.__setattr__(policy, "deprecated_aliases", (STREAMING_TARGET_CHUNK_BYTES_ALIAS,))
        return policy

    @property
    def enabled(self) -> bool:
        return self.mode not in {"off", "false", "0", "none"}

    @property
    def required(self) -> bool:
        return self.mode == "required"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class StreamingRouteDecision:
    """Resolved streaming route selection."""

    selected_route: str
    requested_mode: str
    provider: str
    pipe_mode: str
    blockers: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()
    configured_read_buffer_bytes: int | None = None
    effective_read_buffer_bytes: int = DEFAULT_STREAM_READ_BUFFER_BYTES
    deprecated_aliases: tuple[str, ...] = ()

    def to_evidence(self) -> dict[str, Any]:
        return {
            "schema_version": STREAMING_ROUTE_SCHEMA_VERSION,
            "selected_route": self.selected_route,
            "requested": self.requested_mode,
            "selected": self.selected_route,
            "provider": self.provider,
            "pipe_mode": self.pipe_mode,
            "blockers": list(self.blockers),
            "warnings": list(self.warnings),
            "configured_read_buffer_bytes": self.configured_read_buffer_bytes,
            "effective_read_buffer_bytes": self.effective_read_buffer_bytes,
            "deprecated_aliases": list(self.deprecated_aliases),
        }


def decide_streaming_route(
    *,
    policy: StreamingTransferPolicy,
    artifact_format: str,
    bulk_wire_contract: Any | None,
    sink_type: str,
) -> StreamingRouteDecision | None:
    """Return a streaming decision or ``None`` when streaming is disabled."""

    if not policy.enabled:
        return None
    blockers: list[str] = []
    warnings: list[str] = []
    if STREAMING_TARGET_CHUNK_BYTES_ALIAS in policy.deprecated_aliases:
        warnings.append(STREAMING_TARGET_CHUNK_BYTES_DEPRECATION)
    if policy.provider != "bcp_pipe":
        blockers.append("streaming_provider_unsupported")
    if policy.pipe_mode != "fifo":
        blockers.append("streaming_pipe_mode_unsupported")
    if sink_type != "ClickHouseConnector":
        blockers.append("streaming_sink_unsupported")
    if artifact_format != "mssql-delimited":
        blockers.append("streaming_requires_mssql_delimited_artifact")
    if getattr(bulk_wire_contract, "selected_route", None) != "typed_raw_direct":
        blockers.append("streaming_requires_typed_raw_direct")
    if policy.delimiter_safety == "certified_only":
        blockers.append("streaming_delimiter_safety_uncertified")
    elif policy.delimiter_safety != "advisory":
        warnings.append("streaming_delimiter_safety_unknown_policy")
    selected = "mssql_bcp_typed_raw_pipe_to_clickhouse_streaming" if not blockers else "blocked"
    return StreamingRouteDecision(
        selected_route=selected,
        requested_mode=policy.mode,
        provider=policy.provider,
        pipe_mode=policy.pipe_mode,
        blockers=tuple(blockers),
        warnings=tuple(warnings),
        configured_read_buffer_bytes=policy.configured_read_buffer_bytes,
        effective_read_buffer_bytes=policy.read_buffer_bytes,
        deprecated_aliases=policy.deprecated_aliases,
    )


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _normalized(value: Any, default: str) -> str:
    return str(value if value is not None else default).strip().lower()


def _parse_read_buffer_bytes(value: Any) -> int:
    try:
        if isinstance(value, bool) or not isinstance(value, (int, str)):
            raise ValueError
        if isinstance(value, str) and re.fullmatch(STREAM_READ_BUFFER_LITERAL_PATTERN, value) is None:
            raise ValueError
        return parse_byte_size(value)
    except (TypeError, ValueError, OverflowError):
        raise ValueError("streaming_read_buffer_bytes_out_of_range") from None


def _parse_legacy_target_chunk_bytes(value: Any) -> int:
    try:
        if isinstance(value, bool) or not isinstance(value, (int, str)):
            raise ValueError
        if isinstance(value, str) and re.fullmatch(LEGACY_TARGET_CHUNK_LITERAL_PATTERN, value) is None:
            raise ValueError
        parsed = parse_byte_size(value)
        if parsed <= 0:
            raise ValueError
        return parsed
    except (TypeError, ValueError, OverflowError):
        raise ValueError("streaming_target_chunk_bytes_invalid") from None


__all__ = [
    "STREAMING_ROUTE_SCHEMA_VERSION",
    "DEFAULT_LEGACY_TARGET_CHUNK_BYTES",
    "DEFAULT_STREAM_READ_BUFFER_BYTES",
    "LEGACY_TARGET_CHUNK_LITERAL_PATTERN",
    "MAX_STREAM_READ_BUFFER_BYTES",
    "MIN_STREAM_READ_BUFFER_BYTES",
    "STREAM_READ_BUFFER_LITERAL_PATTERN",
    "STREAMING_TARGET_CHUNK_BYTES_ALIAS",
    "STREAMING_TARGET_CHUNK_BYTES_DEPRECATION",
    "StreamingRouteDecision",
    "StreamingTransferPolicy",
    "decide_streaming_route",
]
