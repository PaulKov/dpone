from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

from dpone.runtime.native_transfer_route_models import RouteCertificationPolicy
from dpone.runtime.native_transfer_transport import NativeTransferTransportPolicy
from dpone.runtime.storage_policy import parse_byte_size


@dataclass(frozen=True, slots=True)
class NativeTransferResourcePolicy:
    max_active_files: int = 2
    max_active_bytes: int = 512 * 1024 * 1024
    target_file_bytes: int = 128 * 1024 * 1024
    max_file_bytes: int = 256 * 1024 * 1024
    adaptive_sizing: bool = True
    oversize_policy: str = "split_and_retry"
    min_slice_rows: int = 1000
    max_slice_rows: int = 250000
    disk_headroom_pct: int = 20

    @classmethod
    def from_mapping(cls, value: dict[str, Any] | None, *, profile: str = "balanced") -> NativeTransferResourcePolicy:
        defaults = _profile_defaults(profile)
        raw = {**defaults, **dict(value or {})}
        return cls(
            max_active_files=max(1, int(raw.get("max_active_files", 2))),
            max_active_bytes=parse_byte_size(raw.get("max_active_bytes", "512MiB")),
            target_file_bytes=parse_byte_size(raw.get("target_file_bytes", "128MiB")),
            max_file_bytes=parse_byte_size(raw.get("max_file_bytes", "256MiB")),
            adaptive_sizing=_bool(raw.get("adaptive_sizing"), default=True),
            oversize_policy=str(raw.get("oversize_policy") or "split_and_retry"),
            min_slice_rows=max(1, int(raw.get("min_slice_rows", 1000))),
            max_slice_rows=max(1, int(raw.get("max_slice_rows", 250000))),
            disk_headroom_pct=max(0, min(95, int(raw.get("disk_headroom_pct", 20)))),
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class NativeTransferExecutionPolicy:
    mode: str = "auto"
    profile: str = "balanced"
    cleanup_policy: str = "eager"
    resume_policy: str = "staging_if_verified"
    transport: NativeTransferTransportPolicy = field(default_factory=NativeTransferTransportPolicy)
    certification: RouteCertificationPolicy = field(default_factory=RouteCertificationPolicy)
    resource: NativeTransferResourcePolicy = field(default_factory=NativeTransferResourcePolicy)
    warnings: tuple[str, ...] = ()

    @classmethod
    def from_mapping(cls, value: dict[str, Any] | None = None) -> NativeTransferExecutionPolicy:
        raw = dict(value or {})
        profile = str(raw.get("profile") or "balanced")
        return cls(
            mode=str(raw.get("mode") or "auto"),
            profile=profile,
            cleanup_policy=str(raw.get("cleanup_policy") or "eager"),
            resume_policy=str(raw.get("resume_policy") or "staging_if_verified"),
            transport=NativeTransferTransportPolicy.from_mapping(
                raw.get("transport") if isinstance(raw.get("transport"), dict) else None
            ),
            certification=RouteCertificationPolicy.from_mapping(
                raw.get("certification") if isinstance(raw.get("certification"), dict) else None
            ),
            resource=NativeTransferResourcePolicy.from_mapping(
                raw.get("resource_policy") if isinstance(raw.get("resource_policy"), dict) else None,
                profile=profile,
            ),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "mode": self.mode,
            "profile": self.profile,
            "cleanup_policy": self.cleanup_policy,
            "resume_policy": self.resume_policy,
            "transport": self.transport.to_dict(),
            "certification": self.certification.to_dict(),
            "resource_policy": self.resource.to_dict(),
            "warnings": list(self.warnings),
        }


def _profile_defaults(profile: str) -> dict[str, Any]:
    if profile == "safe_worker":
        return {
            "max_active_files": 1,
            "max_active_bytes": "256MiB",
            "target_file_bytes": "64MiB",
            "max_file_bytes": "128MiB",
            "max_slice_rows": 100000,
        }
    if profile == "throughput":
        return {
            "max_active_files": 4,
            "max_active_bytes": "2GiB",
            "target_file_bytes": "512MiB",
            "max_file_bytes": "768MiB",
            "max_slice_rows": 1000000,
        }
    if profile == "debug":
        return {"max_active_files": 1, "target_file_bytes": "32MiB", "max_file_bytes": "64MiB"}
    return {}


def _bool(value: Any, *, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


__all__ = ["NativeTransferExecutionPolicy", "NativeTransferResourcePolicy"]
