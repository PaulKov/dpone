"""Models and policies for adaptive native snapshot optimization."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from typing import Any

SNAPSHOT_OPTIMIZATION_SCHEMA_VERSION = "dpone.native_transfer.snapshot_optimization.v1"
SNAPSHOT_FALLBACK_CHAIN: tuple[str, ...] = ("native_tcp", "client", "http", "python")


@dataclass(frozen=True, slots=True)
class SnapshotExecutionPolicy:
    mode: str = "auto"
    profile: str = "balanced"
    max_parallel_exports: int = 4
    max_parallel_loads: int = 2
    adaptive_parallelism: bool = True

    @property
    def effective_max_parallel_exports(self) -> int:
        return _profile_cap(self.profile, self.max_parallel_exports)

    @property
    def effective_max_parallel_loads(self) -> int:
        return _profile_cap(self.profile, self.max_parallel_loads)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["effective_max_parallel_exports"] = self.effective_max_parallel_exports
        payload["effective_max_parallel_loads"] = self.effective_max_parallel_loads
        return payload


@dataclass(frozen=True, slots=True)
class SourceGovernorPolicy:
    enabled: bool = True
    max_source_cpu_pct: int = 60
    max_query_seconds: int = 3600
    backoff_policy: str = "adaptive"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class TargetGovernorPolicy:
    enabled: bool = True
    max_inflight_blocks: int = 4
    max_parts_per_partition: int = 50
    merge_pressure_policy: str = "throttle"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class SnapshotTuningPolicy:
    packet_size: str | int = "auto"
    block_rows: str | int = "auto"
    block_bytes: str | int = "auto"
    compression: str = "auto"
    presort: str = "auto"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class SnapshotOptimizationPolicy:
    execution: SnapshotExecutionPolicy
    source_governor: SourceGovernorPolicy
    target_governor: TargetGovernorPolicy
    tuning: SnapshotTuningPolicy

    @classmethod
    def from_source_options(cls, source_options: Mapping[str, Any] | None) -> SnapshotOptimizationPolicy:
        snapshot = _snapshot_mapping(source_options)
        execution = _mapping(snapshot.get("execution"))
        source_governor = _mapping(snapshot.get("source_governor"))
        target_governor = _mapping(snapshot.get("target_governor"))
        tuning = _mapping(snapshot.get("tuning"))
        return cls(
            execution=SnapshotExecutionPolicy(
                mode=_text(execution.get("mode"), "auto"),
                profile=_text(execution.get("profile"), "balanced"),
                max_parallel_exports=_int(execution.get("max_parallel_exports"), 4),
                max_parallel_loads=_int(execution.get("max_parallel_loads"), 2),
                adaptive_parallelism=_bool(execution.get("adaptive_parallelism"), True),
            ),
            source_governor=SourceGovernorPolicy(
                enabled=_bool(source_governor.get("enabled"), True),
                max_source_cpu_pct=_int(source_governor.get("max_source_cpu_pct"), 60),
                max_query_seconds=_int(source_governor.get("max_query_seconds"), 3600),
                backoff_policy=_text(source_governor.get("backoff_policy"), "adaptive"),
            ),
            target_governor=TargetGovernorPolicy(
                enabled=_bool(target_governor.get("enabled"), True),
                max_inflight_blocks=_int(target_governor.get("max_inflight_blocks"), 4),
                max_parts_per_partition=_int(target_governor.get("max_parts_per_partition"), 50),
                merge_pressure_policy=_text(target_governor.get("merge_pressure_policy"), "throttle"),
            ),
            tuning=SnapshotTuningPolicy(
                packet_size=tuning.get("packet_size") or "auto",
                block_rows=tuning.get("block_rows") or "auto",
                block_bytes=tuning.get("block_bytes") or "auto",
                compression=_text(tuning.get("compression"), "auto"),
                presort=_text(tuning.get("presort"), "auto"),
            ),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "execution": self.execution.to_dict(),
            "source_governor": self.source_governor.to_dict(),
            "target_governor": self.target_governor.to_dict(),
            "tuning": self.tuning.to_dict(),
        }


@dataclass(frozen=True, slots=True)
class GovernorTelemetry:
    cpu_pct: int | None = None
    duration_seconds: int | None = None
    parts_per_partition: int | None = None
    active_merges: int | None = None


@dataclass(frozen=True, slots=True)
class SnapshotRouteRequest:
    source_type: str
    sink_type: str
    source_options: Mapping[str, Any]
    sink_options: Mapping[str, Any]
    runner_policy: str | None = None
    route_certified: bool = False
    available_ingest_backends: Sequence[str] = SNAPSHOT_FALLBACK_CHAIN
    available_native_tcp_backends: Sequence[str] = ("client",)
    direct_ingest_certified: bool = False
    partition_planner: str = "statistics"
    stats_confidence: str = "unknown"
    source_telemetry: GovernorTelemetry | None = None
    target_telemetry: GovernorTelemetry | None = None


@dataclass(frozen=True, slots=True)
class SnapshotRouteDecision:
    requested_backend: str
    selected_backend: str | None
    native_tcp_backend: str | None
    compression: str
    packet_size: str | int
    block_rows: str | int
    block_bytes: str | int
    max_parallel_exports: int
    max_parallel_loads: int
    release_gate: str
    route_certified: bool
    fallback_chain: tuple[str, ...]
    partition_planner: str
    stats_confidence: str
    fallback_reason: str | None = None
    source_scan: dict[str, Any] | None = None
    source_export_optimizer: dict[str, Any] | None = None
    source_materialization: dict[str, Any] | None = None
    blockers: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()
    reasons: tuple[str, ...] = ()

    def to_evidence(self) -> dict[str, Any]:
        return {
            "schema_version": SNAPSHOT_OPTIMIZATION_SCHEMA_VERSION,
            "requested_backend": self.requested_backend,
            "selected_backend": self.selected_backend,
            "native_tcp_backend": self.native_tcp_backend,
            "input_format": "Native" if self.selected_backend in {"native_tcp", "client", "http"} else None,
            "compression": self.compression,
            "packet_size": self.packet_size,
            "block_rows": self.block_rows,
            "block_bytes": self.block_bytes,
            "max_parallel_exports": self.max_parallel_exports,
            "max_parallel_loads": self.max_parallel_loads,
            "release_gate": self.release_gate,
            "route_certified": self.route_certified,
            "fallback_chain": list(self.fallback_chain),
            "partition_planner": self.partition_planner,
            "stats_confidence": self.stats_confidence,
            "fallback_reason": self.fallback_reason,
            "source_scan": dict(self.source_scan) if self.source_scan else None,
            "source_export_optimizer": (dict(self.source_export_optimizer) if self.source_export_optimizer else None),
            "source_materialization": (dict(self.source_materialization) if self.source_materialization else None),
            "blockers": list(self.blockers),
            "warnings": list(self.warnings),
            "reasons": list(self.reasons),
        }


def _snapshot_mapping(source_options: Mapping[str, Any] | None) -> Mapping[str, Any]:
    native = _mapping((source_options or {}).get("native_transfer"))
    return _mapping(native.get("snapshot"))


def _profile_cap(profile: str, value: int) -> int:
    if profile == "safe_worker":
        return 1
    return max(1, value)


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _text(value: Any, default: str) -> str:
    return str(value if value is not None else default).strip().lower()


def _int(value: Any, default: int) -> int:
    return int(value if value is not None else default)


def _bool(value: Any, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


__all__ = [
    "GovernorTelemetry",
    "SNAPSHOT_FALLBACK_CHAIN",
    "SNAPSHOT_OPTIMIZATION_SCHEMA_VERSION",
    "SnapshotOptimizationPolicy",
    "SnapshotRouteDecision",
    "SnapshotRouteRequest",
]
