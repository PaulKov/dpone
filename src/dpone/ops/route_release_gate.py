"""Route-scoped release go/no-go gate over existing evidence artifacts."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from dpone.ops.certification_artifacts import artifact_payload_passed
from dpone.ops.checksums import sha256_file
from dpone.ops.routes.catalog import RouteProfileCatalog
from dpone.ops.routes.models import RouteKey, RouteProfile
from dpone.ops.routes.release_gate_models import RouteReleaseGateEvidence, RouteReleaseGateReport
from dpone.ops.routes.release_gate_policy import RouteReleaseGatePolicy

DEFAULT_ROUTE_RELEASE_EVIDENCE: tuple[str, ...] = (
    "route_readiness",
    "route_certification_pack",
    "route_execution_ledger",
    "route_refresh_verification",
    "state_promotion",
)


class RouteReleaseGateService:
    """Aggregate route evidence into one release receipt without running heavy checks."""

    def __init__(
        self,
        *,
        catalog: RouteProfileCatalog | None = None,
        policy: RouteReleaseGatePolicy | None = None,
    ) -> None:
        self._catalog = catalog or RouteProfileCatalog.default()
        self._policy = policy or RouteReleaseGatePolicy()

    def evaluate(
        self,
        *,
        output_dir: str | Path,
        release: str,
        source: str,
        sink: str,
        strategy: str,
        artifacts: Mapping[str, str | Path],
        required_evidence: Sequence[str] = (),
    ) -> RouteReleaseGateReport:
        directory = Path(output_dir)
        directory.mkdir(parents=True, exist_ok=True)
        route = RouteKey.of(source, sink, strategy)
        profile = self._catalog.get(route)
        required = _required_evidence(profile=profile, extra=tuple(required_evidence))
        names = tuple(dict.fromkeys((*required, *sorted(artifacts))))
        evidence = tuple(
            _read_evidence(
                name=name,
                path_value=artifacts.get(name),
                required=name in required,
                route=route,
            )
            for name in names
        )
        base_blockers = tuple() if profile is not None else (f"route.unsupported:{route.colon_id}",)
        decision = self._policy.evaluate(
            required_evidence=required,
            evidence=evidence,
            base_blockers=base_blockers,
        )
        report = RouteReleaseGateReport(
            release=release,
            route=route,
            profile=profile,
            passed=decision.passed,
            level=decision.level,
            score=decision.score,
            blockers=decision.blockers,
            warnings=decision.warnings,
            next_actions=decision.next_actions,
            required_evidence=required,
            evidence=evidence,
            output_dir=str(directory),
            json_path=str(directory / "route_release_gate.json"),
            markdown_path=str(directory / "route_release_gate.md"),
        )
        report.write()
        return report


def _required_evidence(*, profile: RouteProfile | None, extra: tuple[str, ...]) -> tuple[str, ...]:
    profile_evidence = tuple(profile.required_evidence) if profile else tuple()
    return tuple(dict.fromkeys((*DEFAULT_ROUTE_RELEASE_EVIDENCE, *profile_evidence, *extra)))


def _read_evidence(
    *,
    name: str,
    path_value: str | Path | None,
    required: bool,
    route: RouteKey,
) -> RouteReleaseGateEvidence:
    if path_value is None:
        return _missing(name=name, path="", required=required)
    path = Path(path_value)
    if not path.is_file():
        return _missing(name=name, path=str(path), required=required)
    payload = _payload(path)
    route_case_id = _route_case_id(payload)
    route_matched = not route_case_id or route_case_id == route.case_id
    blockers = _payload_blockers(name, payload)
    if route_case_id and not route_matched:
        blockers = (*blockers, f"{name}.route_mismatch")
    return RouteReleaseGateEvidence(
        name=name,
        kind=_kind(name),
        path=str(path),
        required=required,
        missing=False,
        passed=_passed(name, payload) and route_matched,
        sha256=sha256_file(path),
        summary=_summary(payload),
        blockers=tuple(dict.fromkeys(blockers)),
        route_case_id=route_case_id,
        route_matched=route_matched,
    )


def _missing(*, name: str, path: str, required: bool) -> RouteReleaseGateEvidence:
    return RouteReleaseGateEvidence(
        name=name,
        kind=_kind(name),
        path=path,
        required=required,
        missing=True,
        passed=not required,
        sha256="0" * 64,
        summary="required route release evidence is missing" if required else "optional evidence is missing",
        blockers=(f"{name}.missing",) if required else tuple(),
        route_case_id="",
        route_matched=True,
    )


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
        values = tuple(str(item) for item in blockers if str(item))
        return values or (() if _passed(name, payload) else (f"{name}.not_passed",))
    return tuple() if _passed(name, payload) else (f"{name}.not_passed",)


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
