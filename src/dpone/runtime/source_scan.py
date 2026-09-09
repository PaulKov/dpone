"""Source-shape-aware scan planning for native transfer snapshots."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from dpone.runtime.physical_chunk_policy import PhysicalChunkPolicy

SOURCE_SCAN_SCHEMA_VERSION = "dpone.native_transfer.source_scan_decision.v1"


@dataclass(frozen=True, slots=True)
class SourceShape:
    """Minimal source metadata required to choose a safe snapshot scan mode."""

    table_kind: str = "unknown"
    has_seekable_boundary: bool = False
    stats_confidence: str = "unknown"

    @property
    def heap_like(self) -> bool:
        return self.table_kind.lower() in {"heap", "view", "unknown"}

    @property
    def low_confidence(self) -> bool:
        return self.stats_confidence.lower() in {"low", "unknown", "none"}


@dataclass(frozen=True, slots=True)
class SourceScanDecision:
    """Connector-neutral decision for range partitioning vs one source scan."""

    selected_scan: str
    table_kind: str
    stats_confidence: str
    physical_chunking: dict[str, Any]
    blockers: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()
    reasons: tuple[str, ...] = ()

    def to_evidence(self) -> dict[str, Any]:
        return {
            "schema_version": SOURCE_SCAN_SCHEMA_VERSION,
            "selected_scan": self.selected_scan,
            "table_kind": self.table_kind,
            "stats_confidence": self.stats_confidence,
            "physical_chunking": dict(self.physical_chunking),
            "blockers": list(self.blockers),
            "warnings": list(self.warnings),
            "reasons": list(self.reasons),
        }


class SourceScanPlanner:
    """Pure decision service for source scan shape selection."""

    def plan(self, *, source_options: Mapping[str, Any], source_shape: SourceShape) -> SourceScanDecision:
        scan = _scan_policy(source_options)
        physical = _physical_chunking_policy(source_options)
        mode = _text(scan.get("mode"), "auto")
        require_index = _bool(scan.get("require_index_for_range"), True)
        blockers: list[str] = []
        warnings: list[str] = []
        reasons: list[str] = []

        if mode == "range_partitioned":
            if require_index and not _range_is_safe(source_shape):
                blockers.append(_range_blocker(source_shape))
            return self._decision("range_partitioned", source_shape, physical, blockers, warnings, reasons)

        if mode == "single_scan":
            selected = _single_scan_mode(physical)
            if selected == "single_scan_chunks":
                reasons.append("physical_chunking_enabled")
            return self._decision(selected, source_shape, physical, blockers, warnings, reasons)

        if _range_is_safe(source_shape):
            return self._decision("range_partitioned", source_shape, physical, blockers, warnings, reasons)

        warning = _range_blocker(source_shape)
        if warning:
            warnings.append(warning)
        if source_shape.low_confidence and warning != "source_stats_low_confidence_single_scan":
            warnings.append("source_stats_low_confidence_single_scan")

        heap_policy = _text(scan.get("heap_policy"), "single_scan_chunks")
        if heap_policy == "fail_fast":
            blockers.append(warning or "source_range_parallelism_unsafe")
            return self._decision("range_partitioned", source_shape, physical, blockers, warnings, reasons)

        selected = _single_scan_mode(physical)
        if selected == "single_scan_chunks":
            reasons.append("physical_chunking_enabled")
        return self._decision(selected, source_shape, physical, blockers, warnings, reasons)

    @staticmethod
    def _decision(
        selected: str,
        source_shape: SourceShape,
        physical: Mapping[str, Any],
        blockers: list[str],
        warnings: list[str],
        reasons: list[str],
    ) -> SourceScanDecision:
        return SourceScanDecision(
            selected_scan=selected,
            table_kind=source_shape.table_kind,
            stats_confidence=source_shape.stats_confidence,
            physical_chunking={
                "enabled": selected == "single_scan_chunks",
                "mode": physical.get("mode", "auto"),
                "target_chunk_bytes": physical.get("target_chunk_bytes"),
                "max_chunk_bytes": physical.get("max_chunk_bytes"),
                "spool_mode": physical.get("spool_mode", "file"),
                "cleanup_policy": physical.get("cleanup_policy", "eager"),
            },
            blockers=tuple(dict.fromkeys(item for item in blockers if item)),
            warnings=tuple(dict.fromkeys(item for item in warnings if item)),
            reasons=tuple(dict.fromkeys(item for item in reasons if item)),
        )


def _range_is_safe(shape: SourceShape) -> bool:
    return shape.has_seekable_boundary and not shape.low_confidence and not shape.heap_like


def _range_blocker(shape: SourceShape) -> str:
    if shape.heap_like:
        return "source_heap_range_parallelism_blocked"
    if shape.low_confidence:
        return "source_stats_low_confidence_single_scan"
    return "source_range_parallelism_unsafe"


def _single_scan_mode(physical: Mapping[str, Any]) -> str:
    mode = _text(physical.get("mode"), "auto")
    if mode == "off":
        return "single_file"
    return "single_scan_chunks"


def _scan_policy(source_options: Mapping[str, Any]) -> Mapping[str, Any]:
    snapshot = _snapshot(source_options)
    scan = snapshot.get("scan")
    return scan if isinstance(scan, Mapping) else {}


def _physical_chunking_policy(source_options: Mapping[str, Any]) -> dict[str, Any]:
    return PhysicalChunkPolicy.from_source_options(source_options).to_evidence()


def _snapshot(source_options: Mapping[str, Any]) -> Mapping[str, Any]:
    native = source_options.get("native_transfer")
    if not isinstance(native, Mapping):
        return {}
    snapshot = native.get("snapshot")
    return snapshot if isinstance(snapshot, Mapping) else {}


def _text(value: Any, default: str) -> str:
    return str(value if value is not None else default).strip().lower()


def _bool(value: Any, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


__all__ = [
    "SOURCE_SCAN_SCHEMA_VERSION",
    "SourceScanDecision",
    "SourceScanPlanner",
    "SourceShape",
]
