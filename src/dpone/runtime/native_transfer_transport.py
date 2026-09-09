from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

from dpone.runtime.storage_policy import parse_byte_size


@dataclass(frozen=True, slots=True)
class NativeTransferTransportPolicy:
    mode: str = "auto"
    prefer_streaming: bool = True
    fallback_to_file: bool = True
    stream_buffer_bytes: int = 16 * 1024 * 1024
    checksum: str = "rolling"
    max_stream_seconds: int = 3600

    @classmethod
    def from_mapping(cls, value: dict[str, Any] | None = None) -> NativeTransferTransportPolicy:
        raw = dict(value or {})
        return cls(
            mode=str(raw.get("mode") or "auto"),
            prefer_streaming=_bool(raw.get("prefer_streaming"), default=True),
            fallback_to_file=_bool(raw.get("fallback_to_file"), default=True),
            stream_buffer_bytes=parse_byte_size(raw.get("stream_buffer_bytes", "16MiB")),
            checksum=str(raw.get("checksum") or "rolling"),
            max_stream_seconds=max(1, int(raw.get("max_stream_seconds", 3600))),
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class StreamCapability:
    is_supported: bool
    reason: str

    @classmethod
    def supported(cls, reason: str) -> StreamCapability:
        return cls(True, reason)

    @classmethod
    def unsupported(cls, reason: str) -> StreamCapability:
        return cls(False, reason)

    @property
    def supported_flag(self) -> bool:
        return self.is_supported


@dataclass(frozen=True, slots=True)
class StreamEligibility:
    source: bool
    sink: bool
    codec: bool
    reasons: tuple[str, ...] = ()

    @property
    def supported(self) -> bool:
        return self.source and self.sink and self.codec

    def to_dict(self) -> dict[str, Any]:
        return {
            "source": self.source,
            "sink": self.sink,
            "codec": self.codec,
            "reasons": list(self.reasons),
        }


@dataclass(frozen=True, slots=True)
class SliceTransportPlan:
    transport: str
    eligibility: StreamEligibility
    fallback_allowed: bool
    fallback_reason: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "transport": self.transport,
            "fallback_allowed": self.fallback_allowed,
            "fallback_reason": self.fallback_reason,
            "eligibility": self.eligibility.to_dict(),
        }


class TransferTransportResolver:
    """Resolve zero-file stream eligibility without connector-specific branching."""

    def resolve(
        self,
        policy: NativeTransferTransportPolicy,
        *,
        source: StreamCapability,
        sink: StreamCapability,
        codec: StreamCapability,
    ) -> SliceTransportPlan:
        eligibility = StreamEligibility(
            source=source.supported_flag,
            sink=sink.supported_flag,
            codec=codec.supported_flag,
            reasons=tuple(reason.reason for reason in (source, sink, codec) if not reason.supported_flag),
        )
        if policy.mode in {"file", "object"}:
            return SliceTransportPlan(policy.mode, eligibility, fallback_allowed=True)
        if not policy.prefer_streaming:
            return SliceTransportPlan("file", eligibility, fallback_allowed=True)
        if eligibility.supported:
            return SliceTransportPlan("stream", eligibility, fallback_allowed=policy.fallback_to_file)

        fallback_reason = _fallback_reason(eligibility)
        if policy.mode == "stream" or not policy.fallback_to_file:
            raise RuntimeError(f"native_transfer_stream_unsupported: {fallback_reason}")
        return SliceTransportPlan(
            "file",
            eligibility,
            fallback_allowed=True,
            fallback_reason=fallback_reason,
        )


def _fallback_reason(eligibility: StreamEligibility) -> str:
    if not eligibility.source:
        return "native_transfer_stream_fallback_file_only_source"
    if not eligibility.sink:
        return "native_transfer_stream_fallback_file_only_sink"
    return "native_transfer_codec_not_stream_safe"


def _bool(value: Any, *, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


__all__ = [
    "NativeTransferTransportPolicy",
    "SliceTransportPlan",
    "StreamCapability",
    "StreamEligibility",
    "TransferTransportResolver",
]
