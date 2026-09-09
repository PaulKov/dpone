"""Source materialization cleanup policy and evidence models."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict, dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class SourceMaterializationCleanupPolicy:
    """Bounded cleanup policy for source-side work artifacts."""

    lock_timeout_ms: int = 5000
    defer_on_lock_timeout: bool = True
    reset_lock_timeout: bool = True
    retry_attempts: int = 2
    retry_backoff_ms: int = 250

    @classmethod
    def from_mapping(cls, raw: Mapping[str, Any] | None) -> SourceMaterializationCleanupPolicy:
        config = raw if isinstance(raw, Mapping) else {}
        return cls(
            lock_timeout_ms=_int(config.get("lock_timeout_ms"), 5000),
            defer_on_lock_timeout=_bool(config.get("defer_on_lock_timeout"), True),
            reset_lock_timeout=_bool(config.get("reset_lock_timeout"), True),
            retry_attempts=max(0, _int(config.get("retry_attempts"), 2)),
            retry_backoff_ms=max(0, _int(config.get("retry_backoff_ms"), 250)),
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class SourceMaterializationCleanupResult:
    """Stable cleanup result persisted into source materialization evidence."""

    status: str
    reason: str | None = None
    details: Mapping[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {"status": self.status, "reason": self.reason}
        payload["details"] = dict(self.details or {})
        return payload


@dataclass(frozen=True, slots=True)
class SourceMaterializationSweepResult:
    """Stable evidence for TTL cleanup of deferred source work artifacts."""

    status: str
    scanned: int = 0
    deleted: int = 0
    deferred: int = 0
    blockers: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()
    details: Mapping[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "scanned": self.scanned,
            "deleted": self.deleted,
            "deferred": self.deferred,
            "blockers": list(self.blockers),
            "warnings": list(self.warnings),
            "details": dict(self.details or {}),
        }


def cleanup_result_to_dict(value: Any) -> dict[str, Any] | None:
    if value is None:
        return None
    if isinstance(value, SourceMaterializationCleanupResult):
        return value.to_dict()
    if isinstance(value, Mapping):
        return dict(value)
    return {"status": "unknown", "reason": None, "details": {"value": str(value)}}


def _bool(value: Any, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _int(value: Any, default: int) -> int:
    return int(value if value is not None else default)


__all__ = [
    "SourceMaterializationCleanupPolicy",
    "SourceMaterializationCleanupResult",
    "SourceMaterializationSweepResult",
    "cleanup_result_to_dict",
]
