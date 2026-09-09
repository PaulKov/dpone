"""Models for source export provider benchmarking and selection."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from typing import Any

EXPORT_OPTIMIZER_SCHEMA_VERSION = "dpone.native_transfer.export_optimizer.v1"
EXPORT_PROVIDER_AUTO_CANDIDATES: tuple[str, ...] = (
    "mssql_bcp_native",
    "mssql_bcp_character_raw",
    "mssql_odbc_array",
    "mssql_driver_rowset",
    "range_partitioned",
    "single_scan_chunks",
)


@dataclass(frozen=True, slots=True)
class ExportOptimizerPolicy:
    """Manifest-level policy for adaptive source export selection."""

    mode: str = "auto"
    candidates: tuple[str, ...] = EXPORT_PROVIDER_AUTO_CANDIDATES
    probe_rows: int = 100_000
    max_probe_seconds: int = 30
    min_speedup_pct: float = 15.0
    cache_policy: str = "route_schema_hash"
    rebenchmark_policy: str = "schema_or_source_shape_change"
    source_impact_policy: str = "conservative"
    bcp_probe_packets: tuple[int, ...] = (16_384, 32_768, 65_535)
    odbc_fetch_size: int = 50_000

    @classmethod
    def from_source_options(cls, source_options: Mapping[str, Any] | None) -> ExportOptimizerPolicy:
        snapshot = _snapshot_mapping(source_options)
        raw = _mapping(snapshot.get("export_optimizer"))
        candidates = _candidates(raw.get("candidates"))
        return cls(
            mode=_mode_text(raw.get("mode"), "auto"),
            candidates=candidates,
            probe_rows=_int(raw.get("probe_rows"), 100_000),
            max_probe_seconds=_int(raw.get("max_probe_seconds"), 30),
            min_speedup_pct=float(raw.get("min_speedup_pct", 15)),
            cache_policy=_text(raw.get("cache_policy"), "route_schema_hash"),
            rebenchmark_policy=_text(raw.get("rebenchmark_policy"), "schema_or_source_shape_change"),
            source_impact_policy=_text(raw.get("source_impact_policy"), "conservative"),
            bcp_probe_packets=_ints(raw.get("bcp_probe_packets"), (16_384, 32_768, 65_535)),
            odbc_fetch_size=_int(raw.get("odbc_fetch_size"), 50_000),
        )

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["candidates"] = list(self.candidates)
        return payload


@dataclass(frozen=True, slots=True)
class ExportProviderProbe:
    """Bounded probe result for one source export provider."""

    provider_id: str
    rows_per_second: float | None = None
    bytes_per_second: float | None = None
    first_row_latency_ms: float | None = None
    temp_bytes: int = 0
    safe: bool = True
    certified: bool = True
    row_fidelity_hash: str | None = None
    blockers: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()
    reasons: tuple[str, ...] = ()
    provider_version: str | None = None

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any]) -> ExportProviderProbe:
        return cls(
            provider_id=str(payload.get("provider_id") or payload.get("id") or ""),
            rows_per_second=_optional_float(payload.get("rows_per_second")),
            bytes_per_second=_optional_float(payload.get("bytes_per_second")),
            first_row_latency_ms=_optional_float(payload.get("first_row_latency_ms")),
            temp_bytes=_int(payload.get("temp_bytes"), 0),
            safe=_bool(payload.get("safe"), True),
            certified=_bool(payload.get("certified"), True),
            row_fidelity_hash=_optional_text(payload.get("row_fidelity_hash")),
            blockers=_strings(payload.get("blockers")),
            warnings=_strings(payload.get("warnings")),
            reasons=_strings(payload.get("reasons")),
            provider_version=_optional_text(payload.get("provider_version")),
        )

    @property
    def primary_speed(self) -> float | None:
        return self.rows_per_second if self.rows_per_second is not None else self.bytes_per_second

    @property
    def rejection_reason(self) -> str | None:
        if self.blockers:
            return self.blockers[0]
        if not self.safe:
            return "export_provider_unsafe"
        if not self.certified:
            return "export_provider_uncertified"
        if self.primary_speed is None:
            return "export_provider_probe_metric_missing"
        return None

    def to_evidence(self) -> dict[str, Any]:
        return {
            "provider_id": self.provider_id,
            "rows_per_second": self.rows_per_second,
            "bytes_per_second": self.bytes_per_second,
            "first_row_latency_ms": self.first_row_latency_ms,
            "temp_bytes": self.temp_bytes,
            "safe": self.safe,
            "certified": self.certified,
            "row_fidelity_hash": self.row_fidelity_hash,
            "blockers": list(self.blockers),
            "warnings": list(self.warnings),
            "reasons": list(self.reasons),
            "provider_version": self.provider_version,
        }


@dataclass(frozen=True, slots=True)
class ExportProviderDecision:
    """Selected source export provider and benchmark rationale."""

    requested_mode: str
    current_default: str
    selected_provider: str | None
    recommended_provider: str | None
    measured_speedup_pct: float | None
    source_bottleneck: str
    cache_key: str | None
    release_gate: str
    probes: tuple[ExportProviderProbe, ...] = ()
    rejected: Mapping[str, str] | None = None
    blockers: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()
    reasons: tuple[str, ...] = ()

    def to_evidence(self) -> dict[str, Any]:
        return {
            "schema_version": EXPORT_OPTIMIZER_SCHEMA_VERSION,
            "requested_mode": self.requested_mode,
            "current_default": self.current_default,
            "selected_provider": self.selected_provider,
            "recommended_provider": self.recommended_provider,
            "measured_speedup_pct": self.measured_speedup_pct,
            "source_bottleneck": self.source_bottleneck,
            "cache_key": self.cache_key,
            "release_gate": self.release_gate,
            "probes": [probe.to_evidence() for probe in self.probes],
            "rejected": dict(self.rejected or {}),
            "blockers": list(self.blockers),
            "warnings": list(self.warnings),
            "reasons": list(self.reasons),
        }


def parse_export_probes(snapshot: Mapping[str, Any]) -> tuple[ExportProviderProbe, ...]:
    raw = snapshot.get("export_optimizer_probes")
    if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes)):
        return ()
    return tuple(ExportProviderProbe.from_mapping(item) for item in raw if isinstance(item, Mapping))


def _snapshot_mapping(source_options: Mapping[str, Any] | None) -> Mapping[str, Any]:
    native = _mapping((source_options or {}).get("native_transfer"))
    return _mapping(native.get("snapshot"))


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _candidates(value: Any) -> tuple[str, ...]:
    if value == "auto" or value is None:
        return EXPORT_PROVIDER_AUTO_CANDIDATES
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        return tuple(str(item).strip().lower() for item in value if str(item).strip())
    return tuple(item.strip().lower() for item in str(value).split(",") if item.strip())


def _strings(value: Any) -> tuple[str, ...]:
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        return tuple(str(item) for item in value if str(item))
    return (str(value),) if value else ()


def _text(value: Any, default: str) -> str:
    return str(value if value is not None else default).strip().lower()


def _mode_text(value: Any, default: str) -> str:
    if isinstance(value, bool):
        return "auto" if value else "off"
    return _text(value, default)


def _optional_text(value: Any) -> str | None:
    return None if value is None else str(value)


def _int(value: Any, default: int) -> int:
    return int(value if value is not None else default)


def _ints(value: Any, default: tuple[int, ...]) -> tuple[int, ...]:
    if value is None:
        return default
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        return tuple(int(item) for item in value)
    return tuple(int(item.strip()) for item in str(value).split(",") if item.strip())


def _optional_float(value: Any) -> float | None:
    return None if value is None else float(value)


def _bool(value: Any, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


__all__ = [
    "EXPORT_OPTIMIZER_SCHEMA_VERSION",
    "EXPORT_PROVIDER_AUTO_CANDIDATES",
    "ExportOptimizerPolicy",
    "ExportProviderDecision",
    "ExportProviderProbe",
    "parse_export_probes",
]
