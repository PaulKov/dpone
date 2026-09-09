"""Route run lifecycle supervisor over existing route evidence artifacts."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from dpone.ops.certification_artifacts import artifact_payload_passed, artifact_requires_certification_trust
from dpone.ops.checksums import sha256_file
from dpone.ops.routes.catalog import RouteProfileCatalog
from dpone.ops.routes.models import RouteKey, RouteProfile
from dpone.ops.routes.run_supervisor_contract import (
    DEFAULT_ROUTE_RUN_MODE,
    RouteRunExecutionContractBuilder,
    RouteRunExecutionContractSpec,
    normalize_run_mode,
    required_evidence_for_mode,
)
from dpone.ops.routes.run_supervisor_models import (
    RouteRunContractStage,
    RouteRunEvidence,
    RouteRunEvidenceBundle,
    RouteRunExecutionContract,
    RouteRunIdentity,
    RouteRunPhase,
)
from dpone.ops.routes.run_supervisor_policy import RouteRunSupervisorPolicy

DEFAULT_ROUTE_RUN_EVIDENCE: tuple[str, ...] = (
    "route_readiness",
    "route_execution_ledger",
    "state_promotion",
)

_PHASE_ORDER: tuple[str, ...] = (
    "preflight",
    "schema",
    "execution",
    "state",
    "cdc",
    "reconciliation",
    "repair",
    "observability",
    "release",
    "other",
)

_EVIDENCE_PHASES: dict[str, str] = {
    "route_readiness": "preflight",
    "route_certification_pack": "preflight",
    "route_refresh_plan": "preflight",
    "route_schema_evolution": "schema",
    "route_refresh_execution": "execution",
    "route_execution_ledger": "execution",
    "state_promotion": "state",
    "route_state_promotion": "state",
    "cdc_handoff": "cdc",
    "cdc_apply": "cdc",
    "cdc_observability": "observability",
    "route_refresh_snapshot_capture": "reconciliation",
    "route_refresh_verification": "reconciliation",
    "route_reconciliation_repair": "repair",
    "route_release_gate": "release",
}


class RouteRunSupervisorService:
    """Build one route run lifecycle receipt without executing the route."""

    def __init__(
        self,
        *,
        catalog: RouteProfileCatalog | None = None,
        policy: RouteRunSupervisorPolicy | None = None,
        contract_builder: RouteRunExecutionContractBuilder | None = None,
    ) -> None:
        self._catalog = catalog or RouteProfileCatalog.default()
        self._policy = policy or RouteRunSupervisorPolicy()
        self._contract_builder = contract_builder or RouteRunExecutionContractBuilder()

    def evaluate(
        self,
        *,
        output_dir: str | Path,
        source: str,
        sink: str,
        strategy: str,
        run_id: str,
        dataset: str,
        manifest: str | Path | None = None,
        run_mode: str = DEFAULT_ROUTE_RUN_MODE,
        artifacts: Mapping[str, str | Path] | None = None,
        required_evidence: Sequence[str] = (),
    ) -> RouteRunEvidenceBundle:
        directory = Path(output_dir)
        directory.mkdir(parents=True, exist_ok=True)
        route = RouteKey.of(source, sink, strategy)
        profile = self._catalog.get(route)
        normalized_mode = normalize_run_mode(run_mode)
        required = _required_evidence(
            profile=profile,
            mode=normalized_mode,
            extra=tuple(required_evidence),
        )
        artifact_map = dict(artifacts or {})
        names = tuple(dict.fromkeys((*required, *sorted(artifact_map))))
        evidence = tuple(
            _read_evidence(
                name=name,
                path_value=artifact_map.get(name),
                required=name in required,
                route=route,
            )
            for name in names
        )
        decision = self._policy.evaluate(route=route, profile_exists=profile is not None, evidence=evidence)
        run = RouteRunIdentity(run_id=run_id, dataset=dataset, manifest=str(manifest or ""), mode=normalized_mode)
        execution_contract_plan = self._contract_builder.build(
            mode=normalized_mode,
            route=route,
            run=run,
            output_dir=directory,
            required_evidence=required,
            evidence=evidence,
        )
        execution_contract = _execution_contract_from_plan(execution_contract_plan)
        bundle = RouteRunEvidenceBundle(
            route=route,
            run=run,
            profile=profile,
            passed=decision.passed,
            decision=decision,
            required_evidence=required,
            execution_contract=execution_contract,
            phases=_phases(evidence),
            evidence=evidence,
            output_dir=str(directory),
            json_path=str(directory / "route_run_receipt.json"),
            markdown_path=str(directory / "route_run_receipt.md"),
        )
        bundle.write()
        return bundle


def _required_evidence(*, profile: RouteProfile | None, mode: str, extra: tuple[str, ...]) -> tuple[str, ...]:
    del profile
    return tuple(dict.fromkeys((*required_evidence_for_mode(mode), *extra)))


def _execution_contract_from_plan(plan: RouteRunExecutionContractSpec) -> RouteRunExecutionContract:
    return RouteRunExecutionContract(
        mode=plan.mode,
        ready=plan.ready,
        stages=tuple(
            RouteRunContractStage(
                name=stage.name,
                phase=stage.phase,
                evidence=stage.evidence,
                required=stage.required,
                status=stage.status,
                command=stage.command,
                path=stage.path,
                blockers=stage.blockers,
            )
            for stage in plan.stages
        ),
        next_commands=tuple(plan.next_commands),
    )


def _read_evidence(
    *,
    name: str,
    path_value: str | Path | None,
    required: bool,
    route: RouteKey,
) -> RouteRunEvidence:
    phase = _phase(name)
    kind = _kind(name)
    if path_value is None:
        return _missing(name=name, phase=phase, kind=kind, path="", required=required)
    path = Path(path_value)
    if not path.is_file():
        return _missing(name=name, phase=phase, kind=kind, path=str(path), required=required)
    payload = _payload(path)
    route_case_id = _route_case_id(payload)
    route_matched = not route_case_id or route_case_id == route.case_id
    blockers = _payload_blockers(name, payload)
    if route_case_id and not route_matched:
        blockers = (*blockers, f"{name}.route_mismatch")
    requires_approval = _requires_manual_approval(payload)
    passed = _passed(name, payload) and route_matched and not requires_approval
    return RouteRunEvidence(
        name=name,
        phase=phase,
        kind=kind,
        path=str(path),
        required=required,
        missing=False,
        passed=passed,
        sha256=sha256_file(path),
        summary=_summary(payload),
        blockers=tuple(dict.fromkeys(blockers)),
        route_case_id=route_case_id,
        route_matched=route_matched,
        safe_to_retry=False
        if not passed and artifact_requires_certification_trust(name, payload)
        else _safe_to_retry(payload),
        requires_manual_approval=requires_approval,
    )


def _missing(*, name: str, phase: str, kind: str, path: str, required: bool) -> RouteRunEvidence:
    return RouteRunEvidence(
        name=name,
        phase=phase,
        kind=kind,
        path=path,
        required=required,
        missing=True,
        passed=not required,
        sha256="0" * 64,
        summary="required route run evidence is missing" if required else "optional route run evidence is missing",
        blockers=(f"{name}.missing",) if required else tuple(),
        route_case_id="",
        route_matched=True,
        safe_to_retry=None,
        requires_manual_approval=False,
    )


def _phases(evidence: Sequence[RouteRunEvidence]) -> tuple[RouteRunPhase, ...]:
    result: list[RouteRunPhase] = []
    for phase in _PHASE_ORDER:
        items = tuple(item for item in evidence if item.phase == phase)
        if not items:
            continue
        blockers = tuple(dict.fromkeys(blocker for item in items for blocker in item.blockers))
        required = any(item.required for item in items)
        result.append(
            RouteRunPhase(
                name=phase,
                required=required,
                passed=all(item.passed for item in items),
                evidence=tuple(item.name for item in items),
                blockers=blockers,
            )
        )
    return tuple(result)


def _phase(name: str) -> str:
    return _EVIDENCE_PHASES.get(name, "other")


def _kind(name: str) -> str:
    if "release" in name or "readiness" in name or "certification_pack" in name:
        return "release"
    if "benchmark" in name or "slo" in name or "performance" in name:
        return "performance"
    if "type" in name or "schema" in name:
        return "schema"
    if "docs" in name or "runbook" in name:
        return "docs"
    if "ledger" in name or "state" in name or "run" in name or "cdc" in name:
        return "runtime"
    if "matrix" in name or "strategy" in name:
        return "certification"
    return "evidence"


def _payload(path: Path) -> Mapping[str, Any]:
    if path.suffix.lower() != ".json":
        return {"passed": False, "blockers": ["unsupported_artifact_type"]}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {"passed": False, "blockers": ["invalid_json"]}
    return payload if isinstance(payload, Mapping) else {"passed": False, "blockers": ["invalid_shape"]}


def _passed(name: str, payload: Mapping[str, Any]) -> bool:
    return bool(payload) and _has_success_signal(payload) and artifact_payload_passed(payload, name=name)


def _has_success_signal(payload: Mapping[str, Any]) -> bool:
    return any(key in payload for key in ("passed", "status", "decision", "evidence_status"))


def _summary(payload: Mapping[str, Any]) -> str:
    for key in ("summary", "message"):
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    decision = payload.get("decision")
    if isinstance(decision, Mapping) and decision.get("status"):
        return f"status={decision['status']}"
    for key in ("blockers", "violations", "findings", "items", "evidence", "artifacts"):
        value = payload.get(key)
        if isinstance(value, list | tuple):
            return f"{key}={len(value)}"
    if "status" in payload:
        return f"status={payload['status']}"
    return "passed" if artifact_payload_passed(payload) else "failed"


def _payload_blockers(name: str, payload: Mapping[str, Any]) -> tuple[str, ...]:
    blockers = payload.get("blockers")
    if isinstance(blockers, list | tuple):
        values = tuple(_scoped_blocker(name, str(item)) for item in blockers if str(item))
        return values or (() if _passed(name, payload) else (f"{name}.not_passed",))
    if _passed(name, payload):
        return tuple()
    return (f"{name}.not_passed",)


def _scoped_blocker(name: str, blocker: str) -> str:
    if blocker in {"invalid_json", "invalid_shape"}:
        return f"{name}.{blocker}"
    return blocker


def _route_case_id(payload: Mapping[str, Any]) -> str:
    route = payload.get("route")
    if isinstance(route, Mapping):
        case_id = route.get("case_id")
        if isinstance(case_id, str) and case_id.strip():
            return case_id.strip()
        source = route.get("source")
        sink = route.get("sink")
        strategy = route.get("strategy")
        if source is not None and sink is not None and strategy is not None:
            return RouteKey.of(str(source), str(sink), str(strategy)).case_id
    return ""


def _safe_to_retry(payload: Mapping[str, Any]) -> bool | None:
    value = payload.get("safe_to_retry")
    if isinstance(value, bool):
        return value
    decision = payload.get("decision")
    if isinstance(decision, Mapping) and isinstance(decision.get("retry_safe"), bool):
        return bool(decision["retry_safe"])
    return None


def _requires_manual_approval(payload: Mapping[str, Any]) -> bool:
    apply_decision = payload.get("apply_decision")
    if isinstance(apply_decision, Mapping):
        if bool(apply_decision.get("requires_approval")):
            return True
        if str(apply_decision.get("mode", "")).lower() == "manual_approval":
            return True
    decision = payload.get("decision")
    if isinstance(decision, Mapping):
        return str(decision.get("status", "")).lower() == "manual_approval_required"
    return False


__all__ = ["DEFAULT_ROUTE_RUN_EVIDENCE", "RouteRunSupervisorService"]
