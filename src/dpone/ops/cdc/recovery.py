"""CDC fault-injection recovery evidence service."""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

from dpone.ops.cdc.models import CdcStreamKey
from dpone.ops.cdc.recovery_models import (
    RECOVERY_EVIDENCE_DOMAINS,
    CdcFailureScenario,
    CdcRecoveryDecision,
    CdcRecoveryEvidenceItem,
    CdcRecoveryPolicy,
    CdcRecoveryReport,
)
from dpone.ops.certification_artifacts import artifact_payload_passed

RecoveryEvidenceFactory = Callable[[CdcFailureScenario, CdcRecoveryPolicy], CdcRecoveryEvidenceItem]


class CdcRecoveryEvidenceService:
    """Build CDC recovery evidence from upstream CDC artifacts and fault scenarios."""

    def __init__(self, *, evidence_factories: Mapping[str, RecoveryEvidenceFactory] | None = None) -> None:
        self._evidence_factories = dict(evidence_factories or _default_evidence_factories())

    def evaluate(
        self,
        *,
        output_dir: str | Path,
        handoff_json: str | Path,
        apply_certification_json: str | Path,
        observability_json: str | Path,
        scenario_json: str | Path,
        policy_json: str | Path | None = None,
    ) -> CdcRecoveryReport:
        directory = Path(output_dir)
        handoff = _read_json(handoff_json)
        apply = _read_json(apply_certification_json)
        observability = _read_json(observability_json)
        stream = _stream_from_payloads(handoff=handoff, apply=apply, observability=observability)
        scenario = CdcFailureScenario.from_path(scenario_json)
        policy = CdcRecoveryPolicy.from_path(policy_json) if policy_json else CdcRecoveryPolicy.default()
        evidence = tuple(factory(scenario, policy) for factory in self._ordered_factories())
        decision = _decide(evidence=evidence, handoff=handoff, apply=apply, observability=observability)
        artifacts = self._write_evidence(
            directory=directory / "evidence",
            stream=stream,
            scenario=scenario,
            evidence=evidence,
        )
        report = CdcRecoveryReport(
            stream=stream,
            scenario=scenario,
            passed=decision.passed,
            blockers=decision.blockers,
            warnings=decision.warnings,
            policy=policy,
            evidence=evidence,
            evidence_artifacts={name: str(path) for name, path in artifacts.items()},
            upstream_artifacts={
                "handoff_json": str(handoff_json),
                "apply_certification_json": str(apply_certification_json),
                "observability_json": str(observability_json),
                "scenario_json": str(scenario_json),
                **({"policy_json": str(policy_json)} if policy_json else {}),
            },
            output_dir=str(directory),
            json_path=str(directory / "cdc_recovery_evidence.json"),
            markdown_path=str(directory / "cdc_recovery_evidence.md"),
        )
        report.write()
        return report

    def _ordered_factories(self) -> tuple[RecoveryEvidenceFactory, ...]:
        return tuple(self._evidence_factories[name] for name in RECOVERY_EVIDENCE_DOMAINS)

    @staticmethod
    def _write_evidence(
        *,
        directory: Path,
        stream: CdcStreamKey,
        scenario: CdcFailureScenario,
        evidence: tuple[CdcRecoveryEvidenceItem, ...],
    ) -> dict[str, Path]:
        directory.mkdir(parents=True, exist_ok=True)
        paths: dict[str, Path] = {}
        for item in evidence:
            path = directory / f"{item.name}.json"
            payload = item.to_dict()
            payload["stream"] = stream.to_dict()
            payload["route"] = stream.route.to_dict()
            payload["scenario"] = scenario.to_dict()
            path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            paths[item.name] = path
        return paths


def _default_evidence_factories() -> dict[str, RecoveryEvidenceFactory]:
    return {
        "cdc_restart_resume": _restart_resume_evidence,
        "cdc_offset_commit_ordering": _offset_commit_ordering_evidence,
        "cdc_idempotent_replay_window": _idempotent_replay_evidence,
        "cdc_partial_commit_repair": _partial_commit_repair_evidence,
        "cdc_poison_event_quarantine": _poison_event_quarantine_evidence,
        "cdc_retention_recovery_margin": _retention_recovery_margin_evidence,
    }


def _restart_resume_evidence(scenario: CdcFailureScenario, policy: CdcRecoveryPolicy) -> CdcRecoveryEvidenceItem:
    del policy
    passed = scenario.resume_completed and bool(scenario.recovered_offset)
    return _item(
        name="cdc_restart_resume",
        passed=passed,
        summary="CDC consumer resumed from a durable offset after restart",
        blockers=() if passed else ("cdc_restart_resume.not_resumed",),
        metrics={
            "failed_after_offset": scenario.failed_after_offset,
            "recovered_offset": scenario.recovered_offset,
            "resume_completed": scenario.resume_completed,
        },
        policy={},
    )


def _offset_commit_ordering_evidence(
    scenario: CdcFailureScenario, policy: CdcRecoveryPolicy
) -> CdcRecoveryEvidenceItem:
    passed = (not policy.require_offset_commit_ordering) or scenario.offset_committed_after_sink_commit
    return _item(
        name="cdc_offset_commit_ordering",
        passed=passed,
        summary="CDC offset is committed only after the sink-side commit is durable",
        blockers=() if passed else ("cdc_offset_commit_ordering.unsafe",),
        metrics={
            "sink_commit_state": scenario.sink_commit_state,
            "offset_commit_state": scenario.offset_commit_state,
            "offset_committed_after_sink_commit": scenario.offset_committed_after_sink_commit,
        },
        policy={"require_offset_commit_ordering": policy.require_offset_commit_ordering},
    )


def _idempotent_replay_evidence(scenario: CdcFailureScenario, policy: CdcRecoveryPolicy) -> CdcRecoveryEvidenceItem:
    duplicate_ok = scenario.duplicate_events <= policy.max_duplicate_events
    replay_ok = policy.max_replayed_events is None or scenario.replayed_events <= policy.max_replayed_events
    passed = scenario.idempotent_replay_passed and duplicate_ok and replay_ok
    return _item(
        name="cdc_idempotent_replay_window",
        passed=passed,
        summary="Bounded replay produced an idempotent final sink state",
        blockers=() if passed else ("cdc_idempotent_replay_window.failed",),
        metrics={
            "duplicate_events": scenario.duplicate_events,
            "replayed_events": scenario.replayed_events,
            "idempotent_replay_passed": scenario.idempotent_replay_passed,
        },
        policy={
            "max_duplicate_events": policy.max_duplicate_events,
            **({"max_replayed_events": policy.max_replayed_events} if policy.max_replayed_events is not None else {}),
        },
    )


def _partial_commit_repair_evidence(scenario: CdcFailureScenario, policy: CdcRecoveryPolicy) -> CdcRecoveryEvidenceItem:
    del policy
    passed = (not scenario.partial_sink_commit_detected) or scenario.partial_sink_commit_repaired
    return _item(
        name="cdc_partial_commit_repair",
        passed=passed,
        summary="Partial sink commits are detected and repaired before offset promotion",
        blockers=() if passed else ("cdc_partial_commit_repair.unrepaired",),
        metrics={
            "partial_sink_commit_detected": scenario.partial_sink_commit_detected,
            "partial_sink_commit_repaired": scenario.partial_sink_commit_repaired,
            "repair_actions": list(scenario.repair_actions),
        },
        policy={},
    )


def _poison_event_quarantine_evidence(
    scenario: CdcFailureScenario, policy: CdcRecoveryPolicy
) -> CdcRecoveryEvidenceItem:
    passed = (not policy.require_poison_quarantine) or scenario.quarantined_events >= scenario.poison_events
    return _item(
        name="cdc_poison_event_quarantine",
        passed=passed,
        summary="Poison CDC events are quarantined instead of blocking replay safety",
        blockers=() if passed else ("cdc_poison_event_quarantine.missing",),
        metrics={
            "poison_events": scenario.poison_events,
            "quarantined_events": scenario.quarantined_events,
        },
        policy={"require_poison_quarantine": policy.require_poison_quarantine},
    )


def _retention_recovery_margin_evidence(
    scenario: CdcFailureScenario, policy: CdcRecoveryPolicy
) -> CdcRecoveryEvidenceItem:
    passed = scenario.recovery_margin_seconds >= policy.min_recovery_margin_seconds
    return _item(
        name="cdc_retention_recovery_margin",
        passed=passed,
        summary="Source CDC retention leaves enough time to recover and replay safely",
        blockers=() if passed else ("cdc_retention_recovery_margin.too_low",),
        metrics={
            "retention_remaining_seconds": scenario.retention_remaining_seconds,
            "recovery_margin_seconds": scenario.recovery_margin_seconds,
        },
        policy={"min_recovery_margin_seconds": policy.min_recovery_margin_seconds},
    )


def _item(
    *,
    name: str,
    passed: bool,
    summary: str,
    blockers: tuple[str, ...],
    metrics: Mapping[str, object],
    policy: Mapping[str, object],
) -> CdcRecoveryEvidenceItem:
    return CdcRecoveryEvidenceItem(
        name=name,
        passed=passed,
        summary=summary,
        blockers=blockers,
        metrics=metrics,
        policy=policy,
    )


def _decide(
    *,
    evidence: tuple[CdcRecoveryEvidenceItem, ...],
    handoff: Mapping[str, Any],
    apply: Mapping[str, Any],
    observability: Mapping[str, Any],
) -> CdcRecoveryDecision:
    blockers = [*_upstream_blockers(handoff=handoff, apply=apply, observability=observability)]
    warnings: list[str] = []
    for item in evidence:
        blockers.extend(item.blockers)
    if len({_stream_id(handoff), _stream_id(apply), _stream_id(observability)} - {""}) > 1:
        blockers.append("cdc_recovery.stream_mismatch")
    return CdcRecoveryDecision(
        passed=not blockers,
        blockers=tuple(dict.fromkeys(blockers)),
        warnings=tuple(dict.fromkeys(warnings)),
    )


def _upstream_blockers(
    *,
    handoff: Mapping[str, Any],
    apply: Mapping[str, Any],
    observability: Mapping[str, Any],
) -> tuple[str, ...]:
    blockers: list[str] = []
    if not bool(handoff.get("passed", False)):
        blockers.append("cdc_handoff.not_passed")
        blockers.extend(str(item) for item in _sequence(handoff.get("blockers")))
    if not artifact_payload_passed(apply, name="cdc_apply_certification"):
        blockers.append("cdc_apply_certification.not_passed")
        blockers.extend(str(item) for item in _sequence(apply.get("blockers")))
    if not bool(observability.get("passed", False)):
        blockers.append("cdc_observability.not_passed")
        blockers.extend(str(item) for item in _sequence(observability.get("blockers")))
    return tuple(dict.fromkeys(blockers))


def _stream_from_payloads(
    *,
    handoff: Mapping[str, Any],
    apply: Mapping[str, Any],
    observability: Mapping[str, Any],
) -> CdcStreamKey:
    stream = _mapping(handoff.get("stream") or apply.get("stream") or observability.get("stream"))
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
    raise ValueError("CDC recovery inputs must be JSON objects")


def _sequence(value: object) -> tuple[object, ...]:
    if value is None:
        return tuple()
    if isinstance(value, list | tuple):
        return tuple(value)
    return (value,)
