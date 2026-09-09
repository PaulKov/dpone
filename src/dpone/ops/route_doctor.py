"""Aggregate route onboarding diagnostics."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from dpone.ops.checksums import sha256_file
from dpone.ops.routes.bootstrap_models import ArtifactSummary, RouteDoctorReport
from dpone.ops.routes.bootstrap_policy import (
    artifact_blockers,
    artifact_warnings,
    artifacts_score,
    next_actions_for_blockers,
    status_from_blockers,
)
from dpone.ops.routes.catalog import RouteProfileCatalog
from dpone.ops.routes.models import RouteKey

DEFAULT_ROUTE_DOCTOR_ARTIFACTS: tuple[str, ...] = ("connection_doctor", "source_discovery", "route_bootstrap")


class RouteDoctorService:
    """Build one route onboarding go/no-go report from upstream artifacts."""

    def __init__(self, *, catalog: RouteProfileCatalog | None = None) -> None:
        self._catalog = catalog or RouteProfileCatalog.default()

    def diagnose(
        self,
        *,
        output_dir: str | Path,
        source: str,
        sink: str,
        strategy: str,
        artifacts: Mapping[str, str | Path],
        required_artifacts: Sequence[str] = DEFAULT_ROUTE_DOCTOR_ARTIFACTS,
    ) -> RouteDoctorReport:
        directory = Path(output_dir)
        route = RouteKey.of(source, sink, strategy)
        profile = self._catalog.get(route)
        names = tuple(dict.fromkeys((*required_artifacts, *sorted(artifacts))))
        summaries = tuple(
            _artifact_summary(route=route, name=name, path=artifacts.get(name), required=name in required_artifacts)
            for name in names
        )
        blockers = list(artifact_blockers(summaries))
        if profile is None:
            blockers.insert(0, f"route.unsupported:{route.colon_id}")
        warnings = artifact_warnings(summaries)
        status = status_from_blockers(blockers, warnings)
        report = RouteDoctorReport(
            route=route,
            profile=profile,
            passed=status != "blocked",
            status=status,
            score=artifacts_score(summaries),
            artifacts=summaries,
            blockers=tuple(dict.fromkeys(blockers)),
            warnings=warnings,
            next_actions=next_actions_for_blockers(
                blockers,
                ready_action="Run route-readiness and route-certify for release evidence.",
            ),
            output_dir=str(directory),
            json_path=str(directory / "route_doctor.json"),
            markdown_path=str(directory / "route_doctor.md"),
        )
        report.write()
        return report


def _artifact_summary(
    *,
    route: RouteKey,
    name: str,
    path: str | Path | None,
    required: bool,
) -> ArtifactSummary:
    if path is None or not Path(path).is_file():
        return ArtifactSummary(
            name=name,
            path=str(path or ""),
            required=required,
            passed=not required,
            missing=True,
            sha256="0" * 64,
            status="blocked" if required else "warning",
            summary="required artifact is missing" if required else "optional artifact is missing",
            blockers=(f"{name}.missing",) if required else tuple(),
            warnings=tuple(),
        )
    artifact_path = Path(path)
    payload = _read_payload(artifact_path)
    blockers = _blockers(route=route, name=name, payload=payload)
    warnings = _warnings(payload)
    passed = _passed(payload) and not blockers
    status = status_from_blockers(blockers, warnings)
    return ArtifactSummary(
        name=name,
        path=str(artifact_path),
        required=required,
        passed=passed,
        missing=False,
        sha256=sha256_file(artifact_path),
        status=status,
        summary=_summary(payload),
        blockers=blockers,
        warnings=warnings,
    )


def _read_payload(path: Path) -> Mapping[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {"passed": False, "blockers": ["invalid_json"], "summary": "invalid JSON"}
    return value if isinstance(value, Mapping) else {"passed": False, "blockers": ["invalid_shape"]}


def _passed(payload: Mapping[str, Any]) -> bool:
    if "passed" in payload:
        return bool(payload["passed"])
    if str(payload.get("status", "")).lower() == "blocked":
        return False
    return not bool(payload.get("blockers"))


def _summary(payload: Mapping[str, Any]) -> str:
    value = payload.get("summary")
    if isinstance(value, str) and value:
        return value
    status = payload.get("status")
    if isinstance(status, str) and status:
        return f"status={status}"
    return "passed" if _passed(payload) else "failed"


def _blockers(*, route: RouteKey, name: str, payload: Mapping[str, Any]) -> tuple[str, ...]:
    blockers = (
        [str(item) for item in payload.get("blockers", ()) if str(item)]
        if isinstance(payload.get("blockers"), list | tuple)
        else []
    )
    payload_route = payload.get("route")
    if isinstance(payload_route, Mapping):
        source = str(payload_route.get("source", ""))
        sink = str(payload_route.get("sink", ""))
        strategy = str(payload_route.get("strategy", ""))
        if (source or sink or strategy) and RouteKey.of(source, sink, strategy) != route:
            blockers.append(f"{name}.route_mismatch")
    if not _passed(payload) and not blockers:
        blockers.append(f"{name}.not_passed")
    return tuple(dict.fromkeys(blockers))


def _warnings(payload: Mapping[str, Any]) -> tuple[str, ...]:
    if _passed(payload):
        return tuple()
    warnings = payload.get("warnings")
    return tuple(str(item) for item in warnings if str(item)) if isinstance(warnings, list | tuple) else tuple()
