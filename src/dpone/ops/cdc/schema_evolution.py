"""CDC schema evolution evidence service."""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

from dpone.ops.cdc.models import CdcStreamKey
from dpone.ops.cdc.schema_evolution_models import (
    CDC_SCHEMA_CHANGE_KINDS,
    SCHEMA_EVOLUTION_EVIDENCE_DOMAINS,
    CdcSchemaChangeEvent,
    CdcSchemaEvolutionDecision,
    CdcSchemaEvolutionEvidenceItem,
    CdcSchemaEvolutionPlan,
    CdcSchemaEvolutionPolicy,
    CdcSchemaEvolutionReport,
    load_schema_evolution_input,
)
from dpone.ops.certification_artifacts import artifact_payload_passed

SchemaEvolutionEvidenceFactory = Callable[
    [CdcSchemaChangeEvent, CdcSchemaEvolutionPlan, CdcSchemaEvolutionPolicy],
    CdcSchemaEvolutionEvidenceItem,
]


class CdcSchemaEvolutionEvidenceService:
    """Build CDC schema evolution evidence from upstream CDC artifacts and schema plans."""

    def __init__(self, *, evidence_factories: Mapping[str, SchemaEvolutionEvidenceFactory] | None = None) -> None:
        self._evidence_factories = dict(evidence_factories or _default_evidence_factories())

    def evaluate(
        self,
        *,
        output_dir: str | Path,
        handoff_json: str | Path,
        apply_certification_json: str | Path,
        observability_json: str | Path,
        recovery_json: str | Path,
        schema_change_json: str | Path,
        policy_json: str | Path | None = None,
    ) -> CdcSchemaEvolutionReport:
        directory = Path(output_dir)
        handoff = _read_json(handoff_json)
        apply = _read_json(apply_certification_json)
        observability = _read_json(observability_json)
        recovery = _read_json(recovery_json)
        stream = _stream_from_payloads(handoff=handoff, apply=apply, observability=observability, recovery=recovery)
        change, plan = load_schema_evolution_input(schema_change_json)
        policy = CdcSchemaEvolutionPolicy.from_path(policy_json) if policy_json else CdcSchemaEvolutionPolicy.default()
        evidence = tuple(factory(change, plan, policy) for factory in self._ordered_factories())
        decision = _decide(
            evidence=evidence, handoff=handoff, apply=apply, observability=observability, recovery=recovery
        )
        artifacts = self._write_evidence(
            directory=directory / "evidence",
            stream=stream,
            change=change,
            plan=plan,
            evidence=evidence,
        )
        report = CdcSchemaEvolutionReport(
            stream=stream,
            change=change,
            plan=plan,
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
                "recovery_json": str(recovery_json),
                "schema_change_json": str(schema_change_json),
                **({"policy_json": str(policy_json)} if policy_json else {}),
            },
            output_dir=str(directory),
            json_path=str(directory / "cdc_schema_evolution_evidence.json"),
            markdown_path=str(directory / "cdc_schema_evolution_evidence.md"),
        )
        report.write()
        return report

    def _ordered_factories(self) -> tuple[SchemaEvolutionEvidenceFactory, ...]:
        return tuple(self._evidence_factories[name] for name in SCHEMA_EVOLUTION_EVIDENCE_DOMAINS)

    @staticmethod
    def _write_evidence(
        *,
        directory: Path,
        stream: CdcStreamKey,
        change: CdcSchemaChangeEvent,
        plan: CdcSchemaEvolutionPlan,
        evidence: tuple[CdcSchemaEvolutionEvidenceItem, ...],
    ) -> dict[str, Path]:
        directory.mkdir(parents=True, exist_ok=True)
        paths: dict[str, Path] = {}
        for item in evidence:
            path = directory / f"{item.name}.json"
            payload = item.to_dict()
            payload["stream"] = stream.to_dict()
            payload["route"] = stream.route.to_dict()
            payload["change"] = change.to_dict()
            payload["plan"] = plan.to_dict()
            path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            paths[item.name] = path
        return paths


def _default_evidence_factories() -> dict[str, SchemaEvolutionEvidenceFactory]:
    return {
        "cdc_schema_change_capture": _schema_change_capture_evidence,
        "cdc_schema_compatibility": _schema_compatibility_evidence,
        "cdc_type_widening_safety": _type_widening_safety_evidence,
        "cdc_target_ddl_dry_run": _target_ddl_dry_run_evidence,
        "cdc_backfill_requirement": _backfill_requirement_evidence,
        "cdc_breaking_change_gate": _breaking_change_gate_evidence,
        "cdc_offset_schema_ordering": _offset_schema_ordering_evidence,
    }


def _schema_change_capture_evidence(
    change: CdcSchemaChangeEvent,
    plan: CdcSchemaEvolutionPlan,
    policy: CdcSchemaEvolutionPolicy,
) -> CdcSchemaEvolutionEvidenceItem:
    del plan, policy
    passed = (
        bool(change.change_id and change.source_table and change.source_offset)
        and change.kind in CDC_SCHEMA_CHANGE_KINDS
    )
    return _item(
        name="cdc_schema_change_capture",
        passed=passed,
        summary="Source CDC schema change is captured with a stable id and offset",
        blockers=() if passed else ("cdc_schema_change_capture.invalid",),
        details={
            "change_id": change.change_id,
            "kind": change.kind,
            "source_table": change.source_table,
            "source_offset": change.source_offset,
        },
        policy={"allowed_kinds": list(CDC_SCHEMA_CHANGE_KINDS)},
    )


def _schema_compatibility_evidence(
    change: CdcSchemaChangeEvent,
    plan: CdcSchemaEvolutionPlan,
    policy: CdcSchemaEvolutionPolicy,
) -> CdcSchemaEvolutionEvidenceItem:
    del change
    passed = plan.compatibility_level in policy.allowed_compatibility_levels
    return _item(
        name="cdc_schema_compatibility",
        passed=passed,
        summary="Schema change is compatible with the configured CDC route policy",
        blockers=() if passed else ("cdc_schema_compatibility.incompatible",),
        details={"compatibility_level": plan.compatibility_level, "sink_impact": plan.sink_impact},
        policy={"allowed_compatibility_levels": list(policy.allowed_compatibility_levels)},
    )


def _type_widening_safety_evidence(
    change: CdcSchemaChangeEvent,
    plan: CdcSchemaEvolutionPlan,
    policy: CdcSchemaEvolutionPolicy,
) -> CdcSchemaEvolutionEvidenceItem:
    del policy
    passed = change.kind != "alter_type" or plan.type_widening_safe
    return _item(
        name="cdc_type_widening_safety",
        passed=passed,
        summary="Type changes are widening-safe for the sink contract",
        blockers=() if passed else ("cdc_type_widening_safety.unsafe",),
        details={
            "kind": change.kind,
            "old_type": change.old_type,
            "new_type": change.new_type,
            "type_widening_safe": plan.type_widening_safe,
        },
        policy={},
    )


def _target_ddl_dry_run_evidence(
    change: CdcSchemaChangeEvent,
    plan: CdcSchemaEvolutionPlan,
    policy: CdcSchemaEvolutionPolicy,
) -> CdcSchemaEvolutionEvidenceItem:
    del change
    passed = (not policy.require_ddl_dry_run) or (bool(plan.target_ddl_preview) and plan.ddl_dry_run_passed)
    return _item(
        name="cdc_target_ddl_dry_run",
        passed=passed,
        summary="Target DDL preview was generated and dry-run validation passed",
        blockers=() if passed else ("cdc_target_ddl_dry_run.failed",),
        details={"target_ddl_preview": plan.target_ddl_preview, "ddl_dry_run_passed": plan.ddl_dry_run_passed},
        policy={"require_ddl_dry_run": policy.require_ddl_dry_run},
    )


def _backfill_requirement_evidence(
    change: CdcSchemaChangeEvent,
    plan: CdcSchemaEvolutionPlan,
    policy: CdcSchemaEvolutionPolicy,
) -> CdcSchemaEvolutionEvidenceItem:
    del change, policy
    passed = (not plan.backfill_required) or bool(plan.backfill_plan)
    return _item(
        name="cdc_backfill_requirement",
        passed=passed,
        summary="Backfill requirement is explicit and planned when needed",
        blockers=() if passed else ("cdc_backfill_requirement.missing_plan",),
        details={"backfill_required": plan.backfill_required, "backfill_plan": plan.backfill_plan},
        policy={},
    )


def _breaking_change_gate_evidence(
    change: CdcSchemaChangeEvent,
    plan: CdcSchemaEvolutionPlan,
    policy: CdcSchemaEvolutionPolicy,
) -> CdcSchemaEvolutionEvidenceItem:
    passed = (not policy.require_breaking_approval) or (not change.breaking) or bool(plan.approved_by)
    return _item(
        name="cdc_breaking_change_gate",
        passed=passed,
        summary="Breaking CDC schema changes require explicit approval before offset promotion",
        blockers=() if passed else ("cdc_breaking_change_gate.unapproved",),
        details={"breaking": change.breaking, "approved_by": list(plan.approved_by)},
        policy={"require_breaking_approval": policy.require_breaking_approval},
    )


def _offset_schema_ordering_evidence(
    change: CdcSchemaChangeEvent,
    plan: CdcSchemaEvolutionPlan,
    policy: CdcSchemaEvolutionPolicy,
) -> CdcSchemaEvolutionEvidenceItem:
    passed = (not policy.require_offset_schema_ordering) or plan.offset_schema_ordering_safe
    return _item(
        name="cdc_offset_schema_ordering",
        passed=passed,
        summary="CDC offsets are not advanced before schema evidence is governed",
        blockers=() if passed else ("cdc_offset_schema_ordering.unsafe",),
        details={
            "source_offset": change.source_offset,
            "offset_schema_ordering_safe": plan.offset_schema_ordering_safe,
        },
        policy={"require_offset_schema_ordering": policy.require_offset_schema_ordering},
    )


def _item(
    *,
    name: str,
    passed: bool,
    summary: str,
    blockers: tuple[str, ...],
    details: Mapping[str, object],
    policy: Mapping[str, object],
) -> CdcSchemaEvolutionEvidenceItem:
    return CdcSchemaEvolutionEvidenceItem(
        name=name,
        passed=passed,
        summary=summary,
        blockers=blockers,
        details=details,
        policy=policy,
    )


def _decide(
    *,
    evidence: tuple[CdcSchemaEvolutionEvidenceItem, ...],
    handoff: Mapping[str, Any],
    apply: Mapping[str, Any],
    observability: Mapping[str, Any],
    recovery: Mapping[str, Any],
) -> CdcSchemaEvolutionDecision:
    blockers = [*_upstream_blockers(handoff=handoff, apply=apply, observability=observability, recovery=recovery)]
    warnings: list[str] = []
    for item in evidence:
        blockers.extend(item.blockers)
    if len({_stream_id(handoff), _stream_id(apply), _stream_id(observability), _stream_id(recovery)} - {""}) > 1:
        blockers.append("cdc_schema_evolution.stream_mismatch")
    return CdcSchemaEvolutionDecision(
        passed=not blockers,
        blockers=tuple(dict.fromkeys(blockers)),
        warnings=tuple(dict.fromkeys(warnings)),
    )


def _upstream_blockers(
    *,
    handoff: Mapping[str, Any],
    apply: Mapping[str, Any],
    observability: Mapping[str, Any],
    recovery: Mapping[str, Any],
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
    if not bool(recovery.get("passed", False)):
        blockers.append("cdc_recovery.not_passed")
        blockers.extend(str(item) for item in _sequence(recovery.get("blockers")))
    return tuple(dict.fromkeys(blockers))


def _stream_from_payloads(
    *,
    handoff: Mapping[str, Any],
    apply: Mapping[str, Any],
    observability: Mapping[str, Any],
    recovery: Mapping[str, Any],
) -> CdcStreamKey:
    stream = _mapping(
        handoff.get("stream") or apply.get("stream") or observability.get("stream") or recovery.get("stream")
    )
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
    raise ValueError("CDC schema evolution inputs must be JSON objects")


def _sequence(value: object) -> tuple[object, ...]:
    if value is None:
        return tuple()
    if isinstance(value, list | tuple):
        return tuple(value)
    return (value,)
