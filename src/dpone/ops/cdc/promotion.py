"""CDC promotion gate service."""

from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from dpone.ops.cdc.models import CdcStreamKey
from dpone.ops.cdc.promotion_models import (
    CdcPromotionDecision,
    CdcPromotionEvidenceItem,
    CdcPromotionReport,
)
from dpone.ops.certification_artifacts import artifact_payload_passed

_GATE_INPUTS: tuple[tuple[str, str, str], ...] = (
    ("cdc_apply_gate", "apply_certification", "CDC apply certification evidence is green"),
    ("cdc_handoff_gate", "handoff", "CDC snapshot handoff evidence is green"),
    ("cdc_observability_gate", "observability", "CDC observability evidence is green"),
    ("cdc_recovery_gate", "recovery", "CDC recovery evidence is green"),
    ("cdc_schema_evolution_gate", "schema_evolution", "CDC schema evolution evidence is green"),
)


class CdcPromotionGateService:
    """Aggregate CDC evidence packs into a production promotion decision."""

    def evaluate(
        self,
        *,
        output_dir: str | Path,
        apply_certification_json: str | Path,
        handoff_json: str | Path,
        observability_json: str | Path,
        recovery_json: str | Path,
        schema_evolution_json: str | Path,
    ) -> CdcPromotionReport:
        directory = Path(output_dir)
        reports = {
            "apply_certification": _read_json(apply_certification_json),
            "handoff": _read_json(handoff_json),
            "observability": _read_json(observability_json),
            "recovery": _read_json(recovery_json),
            "schema_evolution": _read_json(schema_evolution_json),
        }
        stream = _stream_from_reports(reports)
        gate_evidence = tuple(
            _gate_item(name=name, key=key, summary=summary, report=reports[key]) for name, key, summary in _GATE_INPUTS
        )
        identity_evidence = _identity_item(reports)
        preliminary_evidence = (*gate_evidence, identity_evidence)
        blockers = _blockers(preliminary_evidence)
        promotion_evidence = _promotion_item(blockers=blockers)
        evidence = (*preliminary_evidence, promotion_evidence)
        decision = _decision(evidence)
        artifacts = self._write_evidence(directory=directory / "evidence", stream=stream, evidence=evidence)
        report = CdcPromotionReport(
            stream=stream,
            passed=decision.passed,
            production_ready=decision.production_ready,
            promote_offsets=decision.promote_offsets,
            blockers=decision.blockers,
            warnings=decision.warnings,
            next_actions=decision.next_actions,
            evidence=evidence,
            evidence_artifacts={name: str(path) for name, path in artifacts.items()},
            upstream_artifacts={
                "apply_certification_json": str(apply_certification_json),
                "handoff_json": str(handoff_json),
                "observability_json": str(observability_json),
                "recovery_json": str(recovery_json),
                "schema_evolution_json": str(schema_evolution_json),
            },
            output_dir=str(directory),
            json_path=str(directory / "cdc_promotion_gate.json"),
            markdown_path=str(directory / "cdc_promotion_gate.md"),
        )
        report.write()
        return report

    @staticmethod
    def _write_evidence(
        *,
        directory: Path,
        stream: CdcStreamKey,
        evidence: tuple[CdcPromotionEvidenceItem, ...],
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


def _gate_item(*, name: str, key: str, summary: str, report: Mapping[str, Any]) -> CdcPromotionEvidenceItem:
    evidence_name = "cdc_apply_certification" if key == "apply_certification" else key
    passed = artifact_payload_passed(report, name=evidence_name)
    return CdcPromotionEvidenceItem(
        name=name,
        passed=passed,
        summary=summary,
        blockers=()
        if passed
        else (f"{name}.not_passed", *tuple(str(item) for item in _sequence(report.get("blockers")))),
        details={
            "upstream_key": key,
            "schema_version": str(report.get("schema_version", "")),
            "passed": passed,
        },
    )


def _identity_item(reports: Mapping[str, Mapping[str, Any]]) -> CdcPromotionEvidenceItem:
    stream_ids = {key: _stream_id(report) for key, report in reports.items()}
    route_ids = {key: _route_id(report) for key, report in reports.items()}
    passed = all(stream_ids.values()) and len(set(stream_ids.values())) == 1
    passed = passed and all(route_ids.values()) and len(set(route_ids.values())) == 1
    return CdcPromotionEvidenceItem(
        name="cdc_stream_identity_consistency",
        passed=passed,
        summary="All CDC evidence artifacts describe the same route and stream",
        blockers=() if passed else ("cdc_stream_identity_consistency.mismatch",),
        details={"stream_ids": stream_ids, "route_ids": route_ids},
    )


def _promotion_item(*, blockers: tuple[str, ...]) -> CdcPromotionEvidenceItem:
    passed = not blockers
    return CdcPromotionEvidenceItem(
        name="cdc_offset_promotion_decision",
        passed=passed,
        summary="CDC offsets may be promoted only when every upstream gate is green",
        blockers=() if passed else ("cdc_offset_promotion_decision.blocked",),
        details={"promote_offsets": passed, "production_ready": passed},
    )


def _decision(evidence: tuple[CdcPromotionEvidenceItem, ...]) -> CdcPromotionDecision:
    blockers = _blockers(evidence)
    passed = not blockers
    return CdcPromotionDecision(
        passed=passed,
        production_ready=passed,
        promote_offsets=passed,
        blockers=blockers,
        warnings=tuple(),
        next_actions=_next_actions(blockers),
    )


def _next_actions(blockers: tuple[str, ...]) -> tuple[str, ...]:
    if not blockers:
        return ("Promote CDC offsets only after the runtime sink commit is durable and recorded.",)
    return tuple(f"Resolve blocker `{blocker}` before promoting CDC offsets." for blocker in blockers[:5])


def _blockers(evidence: tuple[CdcPromotionEvidenceItem, ...]) -> tuple[str, ...]:
    values: list[str] = []
    for item in evidence:
        values.extend(item.blockers)
    return tuple(dict.fromkeys(values))


def _stream_from_reports(reports: Mapping[str, Mapping[str, Any]]) -> CdcStreamKey:
    for report in reports.values():
        stream = report.get("stream")
        if isinstance(stream, Mapping):
            return CdcStreamKey.of(
                source=str(stream["source"]),
                sink=str(stream["sink"]),
                strategy=str(stream.get("strategy", "cdc")),
                source_dataset=str(stream["source_dataset"]),
                target_dataset=str(stream["target_dataset"]),
            )
    raise ValueError("CDC promotion inputs must include at least one stream object")


def _stream_id(report: Mapping[str, Any]) -> str:
    stream = report.get("stream")
    if isinstance(stream, Mapping):
        return str(stream.get("stream_id", ""))
    return ""


def _route_id(report: Mapping[str, Any]) -> str:
    route = report.get("route")
    if isinstance(route, Mapping):
        return str(route.get("case_id", ""))
    return ""


def _read_json(path: str | Path) -> Mapping[str, Any]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, Mapping):
        raise ValueError("CDC promotion inputs must be JSON objects")
    return payload


def _sequence(value: object) -> tuple[object, ...]:
    if value is None:
        return tuple()
    if isinstance(value, list | tuple):
        return tuple(value)
    return (value,)
