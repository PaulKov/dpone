"""CDC observability evidence service."""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

from dpone.ops.cdc.models import CdcStreamKey
from dpone.ops.cdc.observability_models import (
    OBSERVABILITY_EVIDENCE_DOMAINS,
    CdcObservabilityDecision,
    CdcObservabilityEvidenceItem,
    CdcObservabilityReport,
    CdcSloProfile,
    CdcTelemetrySnapshot,
)
from dpone.ops.certification_artifacts import artifact_payload_passed

EvidenceFactory = Callable[[CdcTelemetrySnapshot, CdcSloProfile], CdcObservabilityEvidenceItem]


class CdcObservabilityEvidenceService:
    """Build CDC observability evidence from handoff, apply, and telemetry artifacts."""

    def __init__(self, *, evidence_factories: Mapping[str, EvidenceFactory] | None = None) -> None:
        self._evidence_factories = dict(evidence_factories or _default_evidence_factories())

    def evaluate(
        self,
        *,
        output_dir: str | Path,
        handoff_json: str | Path,
        apply_certification_json: str | Path,
        metrics_json: str | Path,
        slo_json: str | Path | None = None,
    ) -> CdcObservabilityReport:
        directory = Path(output_dir)
        handoff = _read_json(handoff_json)
        apply = _read_json(apply_certification_json)
        stream = _stream_from_payloads(handoff=handoff, apply=apply)
        telemetry = CdcTelemetrySnapshot.from_path(metrics_json)
        slo = CdcSloProfile.from_path(slo_json) if slo_json else CdcSloProfile.default()
        evidence = tuple(factory(telemetry, slo) for factory in self._ordered_factories())
        upstream_blockers = _upstream_blockers(handoff=handoff, apply=apply)
        decision = _decide(evidence=evidence, upstream_blockers=upstream_blockers, handoff=handoff, apply=apply)
        artifacts = self._write_evidence(directory=directory / "evidence", stream=stream, evidence=evidence)
        report = CdcObservabilityReport(
            stream=stream,
            passed=decision.passed,
            blockers=decision.blockers,
            warnings=decision.warnings,
            metrics=telemetry.to_dict(),
            slo_profile=slo,
            evidence=evidence,
            evidence_artifacts={name: str(path) for name, path in artifacts.items()},
            upstream_artifacts={
                "handoff_json": str(handoff_json),
                "apply_certification_json": str(apply_certification_json),
                "metrics_json": str(metrics_json),
                **({"slo_json": str(slo_json)} if slo_json else {}),
            },
            output_dir=str(directory),
            json_path=str(directory / "cdc_observability.json"),
            markdown_path=str(directory / "cdc_observability.md"),
        )
        report.write()
        return report

    def _ordered_factories(self) -> tuple[EvidenceFactory, ...]:
        return tuple(self._evidence_factories[name] for name in OBSERVABILITY_EVIDENCE_DOMAINS)

    @staticmethod
    def _write_evidence(
        *,
        directory: Path,
        stream: CdcStreamKey,
        evidence: tuple[CdcObservabilityEvidenceItem, ...],
    ) -> dict[str, Path]:
        directory.mkdir(parents=True, exist_ok=True)
        paths: dict[str, Path] = {}
        for item in evidence:
            path = directory / f"{item.name}.json"
            payload = item.to_dict()
            payload["stream"] = stream.to_dict()
            payload["route"] = stream.route.to_dict()
            path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            paths[item.name] = path
        return paths


def _default_evidence_factories() -> dict[str, EvidenceFactory]:
    return {
        "cdc_lag_slo": _lag_evidence,
        "cdc_freshness_slo": _freshness_evidence,
        "cdc_retention_risk": _retention_evidence,
        "cdc_offset_commit_health": _offset_evidence,
        "cdc_duplicate_replay_rate": _duplicate_replay_evidence,
        "cdc_throughput_slo": _throughput_evidence,
    }


def _lag_evidence(snapshot: CdcTelemetrySnapshot, slo: CdcSloProfile) -> CdcObservabilityEvidenceItem:
    passed = snapshot.lag_seconds <= slo.max_lag_seconds
    return _item(
        name="cdc_lag_slo",
        passed=passed,
        summary="CDC source-to-sink lag is within the configured SLO",
        blockers=() if passed else ("cdc_lag_slo.exceeded",),
        metrics={"lag_seconds": snapshot.lag_seconds},
        slo={"max_lag_seconds": slo.max_lag_seconds},
    )


def _freshness_evidence(snapshot: CdcTelemetrySnapshot, slo: CdcSloProfile) -> CdcObservabilityEvidenceItem:
    passed = snapshot.freshness_seconds <= slo.max_freshness_seconds
    return _item(
        name="cdc_freshness_slo",
        passed=passed,
        summary="Latest applied CDC event is fresh enough for the route SLO",
        blockers=() if passed else ("cdc_freshness_slo.exceeded",),
        metrics={"freshness_seconds": snapshot.freshness_seconds},
        slo={"max_freshness_seconds": slo.max_freshness_seconds},
    )


def _retention_evidence(snapshot: CdcTelemetrySnapshot, slo: CdcSloProfile) -> CdcObservabilityEvidenceItem:
    passed = snapshot.retention_remaining_seconds >= slo.min_retention_remaining_seconds
    return _item(
        name="cdc_retention_risk",
        passed=passed,
        summary="Source CDC retention still covers the recovery window",
        blockers=() if passed else ("cdc_retention_risk.window_not_available",),
        metrics={"retention_remaining_seconds": snapshot.retention_remaining_seconds},
        slo={"min_retention_remaining_seconds": slo.min_retention_remaining_seconds},
    )


def _offset_evidence(snapshot: CdcTelemetrySnapshot, slo: CdcSloProfile) -> CdcObservabilityEvidenceItem:
    passed = snapshot.offset_committed or not slo.require_committed_offset
    return _item(
        name="cdc_offset_commit_health",
        passed=passed,
        summary="CDC consumer offset is durably committed",
        blockers=() if passed else ("cdc_offset_commit_health.not_committed",),
        metrics={
            "offset_commit_status": snapshot.offset_commit_status,
            "last_committed_offset": snapshot.last_committed_offset,
            "offset_committed": snapshot.offset_committed,
        },
        slo={"require_committed_offset": slo.require_committed_offset},
    )


def _duplicate_replay_evidence(snapshot: CdcTelemetrySnapshot, slo: CdcSloProfile) -> CdcObservabilityEvidenceItem:
    duplicate_ok = snapshot.duplicate_events <= slo.max_duplicate_events
    replay_ok = slo.max_replayed_events is None or snapshot.replayed_events <= slo.max_replayed_events
    passed = duplicate_ok and replay_ok
    return _item(
        name="cdc_duplicate_replay_rate",
        passed=passed,
        summary="Duplicate and replayed events stay inside the idempotency budget",
        blockers=() if passed else ("cdc_duplicate_replay_rate.exceeded",),
        metrics={"duplicate_events": snapshot.duplicate_events, "replayed_events": snapshot.replayed_events},
        slo={
            "max_duplicate_events": slo.max_duplicate_events,
            **({"max_replayed_events": slo.max_replayed_events} if slo.max_replayed_events is not None else {}),
        },
    )


def _throughput_evidence(snapshot: CdcTelemetrySnapshot, slo: CdcSloProfile) -> CdcObservabilityEvidenceItem:
    passed = snapshot.events_per_second >= slo.min_events_per_second
    return _item(
        name="cdc_throughput_slo",
        passed=passed,
        summary="CDC apply throughput is above the configured floor",
        blockers=() if passed else ("cdc_throughput_slo.too_low",),
        metrics={"events_per_second": snapshot.events_per_second},
        slo={"min_events_per_second": slo.min_events_per_second},
    )


def _item(
    *,
    name: str,
    passed: bool,
    summary: str,
    blockers: tuple[str, ...],
    metrics: Mapping[str, object],
    slo: Mapping[str, object],
) -> CdcObservabilityEvidenceItem:
    return CdcObservabilityEvidenceItem(
        name=name,
        passed=passed,
        summary=summary,
        blockers=blockers,
        metrics=metrics,
        slo=slo,
    )


def _decide(
    *,
    evidence: tuple[CdcObservabilityEvidenceItem, ...],
    upstream_blockers: tuple[str, ...],
    handoff: Mapping[str, Any],
    apply: Mapping[str, Any],
) -> CdcObservabilityDecision:
    blockers = [*upstream_blockers]
    warnings: list[str] = []
    for item in evidence:
        blockers.extend(item.blockers)
    if _stream_id(handoff) != _stream_id(apply):
        blockers.append("cdc_observability.stream_mismatch")
    blockers = list(dict.fromkeys(blockers))
    return CdcObservabilityDecision(
        passed=not blockers,
        blockers=tuple(blockers),
        warnings=tuple(dict.fromkeys(warnings)),
    )


def _upstream_blockers(*, handoff: Mapping[str, Any], apply: Mapping[str, Any]) -> tuple[str, ...]:
    blockers: list[str] = []
    if not bool(handoff.get("passed", False)):
        blockers.append("cdc_handoff.not_passed")
        blockers.extend(str(item) for item in _sequence(handoff.get("blockers")))
    if not artifact_payload_passed(apply, name="cdc_apply_certification"):
        blockers.append("cdc_apply_certification.not_passed")
        blockers.extend(str(item) for item in _sequence(apply.get("blockers")))
    return tuple(dict.fromkeys(blockers))


def _stream_from_payloads(*, handoff: Mapping[str, Any], apply: Mapping[str, Any]) -> CdcStreamKey:
    stream = _mapping(handoff.get("stream") or apply.get("stream"))
    return CdcStreamKey.of(
        source=str(stream["source"]),
        sink=str(stream["sink"]),
        strategy=str(stream.get("strategy", "cdc")),
        source_dataset=str(stream["source_dataset"]),
        target_dataset=str(stream["target_dataset"]),
    )


def _stream_id(payload: Mapping[str, Any]) -> str:
    stream = payload.get("stream")
    if isinstance(stream, Mapping):
        return str(stream.get("stream_id", ""))
    return ""


def _read_json(path: str | Path) -> Mapping[str, Any]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    return _mapping(payload)


def _mapping(value: object) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    raise ValueError("CDC observability inputs must be JSON objects")


def _sequence(value: object) -> tuple[object, ...]:
    if value is None:
        return tuple()
    if isinstance(value, list | tuple):
        return tuple(value)
    return (value,)
