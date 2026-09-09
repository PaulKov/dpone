"""Connector-neutral load profile advisor contracts."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

PROFILE_ADVICE_SCHEMA = "dpone.profile.advice.v1"


@dataclass(frozen=True, slots=True)
class SourceShape:
    row_count: int | None = None
    column_count: int | None = None
    estimated_bytes_per_row: int | None = None
    source_kind: str = "unknown"

    @property
    def bytes_per_row(self) -> int:
        if self.estimated_bytes_per_row and self.estimated_bytes_per_row > 0:
            return self.estimated_bytes_per_row
        if self.column_count and self.column_count > 0:
            return max(self.column_count * 32, 64)
        return 512

    @property
    def width_class(self) -> str:
        if self.bytes_per_row <= 128:
            return "narrow"
        if self.bytes_per_row >= 768:
            return "wide"
        return "balanced"

    def to_dict(self) -> dict[str, Any]:
        return {
            "row_count": self.row_count,
            "column_count": self.column_count,
            "estimated_bytes_per_row": self.estimated_bytes_per_row,
            "source_kind": self.source_kind,
            "width_class": self.width_class,
        }


@dataclass(frozen=True, slots=True)
class WorkerProfile:
    name: str = "balanced"
    memory_bytes: int = 4 * 1024 * 1024 * 1024
    max_inflight_chunks: int = 1

    @classmethod
    def from_name(cls, name: str | None) -> WorkerProfile:
        normalized = (name or "balanced").strip().lower()
        if normalized in {"weak", "weak_worker", "safe_worker"}:
            return cls(name="weak_worker", memory_bytes=2 * 1024 * 1024 * 1024, max_inflight_chunks=1)
        if normalized in {"throughput", "strong", "throughput_worker"}:
            return cls(name="throughput", memory_bytes=8 * 1024 * 1024 * 1024, max_inflight_chunks=2)
        return cls(name="balanced", memory_bytes=4 * 1024 * 1024 * 1024, max_inflight_chunks=1)

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "memory_bytes": self.memory_bytes,
            "max_inflight_chunks": self.max_inflight_chunks,
        }


@dataclass(frozen=True, slots=True)
class ProfileAdviceRequest:
    source_type: str
    sink_type: str
    route_id: str = "auto"
    execution_mode: str = "chunked"
    optimization_goal: str = "balanced"
    target_chunk_bytes: int = 512 * 1024 * 1024
    current_max_chunk_rows: int = 1_000_000
    source_shape: SourceShape = field(default_factory=SourceShape)
    worker_profile: WorkerProfile = field(default_factory=WorkerProfile)


@dataclass(frozen=True, slots=True)
class LoadProfileDecision:
    selected_profile: str
    selected_route: str
    execution_mode: str
    expected_rps_range: tuple[int, int]
    confidence: str
    warnings: tuple[str, ...]
    blockers: tuple[str, ...]
    recommendations: tuple[dict[str, Any], ...]
    rejected_routes: tuple[dict[str, Any], ...]
    recommended_patch: dict[str, Any]
    details: dict[str, Any]

    @property
    def warning_codes(self) -> tuple[str, ...]:
        return self.warnings

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": PROFILE_ADVICE_SCHEMA,
            "selected_profile": self.selected_profile,
            "selected_route": self.selected_route,
            "execution_mode": self.execution_mode,
            "expected_rps_range": {
                "min": self.expected_rps_range[0],
                "max": self.expected_rps_range[1],
            },
            "confidence": self.confidence,
            "warnings": list(self.warnings),
            "blockers": list(self.blockers),
            "recommendations": list(self.recommendations),
            "rejected_routes": list(self.rejected_routes),
            "recommended_patch": self.recommended_patch,
            "details": self.details,
        }


__all__ = [
    "LoadProfileDecision",
    "PROFILE_ADVICE_SCHEMA",
    "ProfileAdviceRequest",
    "SourceShape",
    "WorkerProfile",
]
