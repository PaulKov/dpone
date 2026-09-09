"""Artifact normalization for route refresh planning."""

from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from dpone.ops.checksums import sha256_file
from dpone.ops.routes.models import RouteKey
from dpone.ops.routes.refresh_plan_models import RouteRefreshEvidence


class RouteRefreshEvidenceReader:
    """Read route refresh evidence without making policy decisions."""

    def read(
        self,
        *,
        name: str,
        path_value: str | Path | None,
        required: bool,
        route: RouteKey,
    ) -> RouteRefreshEvidence:
        kind = _kind(name)
        if path_value is None:
            return _missing(name=name, kind=kind, path="", required=required)
        path = Path(path_value)
        if not path.is_file():
            return _missing(name=name, kind=kind, path=str(path), required=required)
        payload = _payload(path)
        route_case_id = _route_case_id(payload)
        route_matched = not route_case_id or route_case_id == route.case_id
        blockers = _payload_blockers(name, payload)
        if route_case_id and not route_matched:
            blockers = (*blockers, f"{name}.route_mismatch")
        return RouteRefreshEvidence(
            name=name,
            kind=kind,
            path=str(path),
            required=required,
            missing=False,
            passed=_passed(payload) and route_matched,
            sha256=sha256_file(path),
            summary=_summary(payload),
            blockers=tuple(dict.fromkeys(blockers)),
            route_case_id=route_case_id,
            route_matched=route_matched,
        )


def _missing(*, name: str, kind: str, path: str, required: bool) -> RouteRefreshEvidence:
    return RouteRefreshEvidence(
        name=name,
        kind=kind,
        path=path,
        required=required,
        missing=True,
        passed=not required,
        sha256="0" * 64,
        summary="required route refresh evidence is missing" if required else "optional evidence is missing",
        blockers=(f"{name}.missing",) if required else tuple(),
        route_case_id="",
        route_matched=True,
    )


def _payload(path: Path) -> Mapping[str, Any]:
    if path.suffix.lower() != ".json":
        return {"passed": True, "summary": "non-json artifact exists"}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {"passed": False, "blockers": ["invalid_json"]}
    return payload if isinstance(payload, Mapping) else {"passed": False, "blockers": ["invalid_shape"]}


def _passed(payload: Mapping[str, Any]) -> bool:
    if "passed" in payload:
        return bool(payload["passed"])
    status = str(payload.get("status", "")).lower()
    if status in {"failed", "blocked", "regression", "rollback_required", "approval_required"}:
        return False
    return not bool(payload.get("blockers") or payload.get("violations") or payload.get("findings"))


def _payload_blockers(name: str, payload: Mapping[str, Any]) -> tuple[str, ...]:
    blockers = _strings(payload.get("blockers"))
    if blockers:
        return tuple(_scoped_blocker(name, item) for item in blockers)
    if _passed(payload):
        return tuple()
    return (f"{name}.not_passed",)


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
    return "passed" if _passed(payload) else "failed"


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
    if "quality" in name or "reconciliation" in name or "repair" in name:
        return "quality"
    if "retention" in name or "cdc" in name or "state" in name or "run" in name:
        return "runtime"
    if "approval" in name or "change" in name or "release" in name:
        return "governance"
    if "schema" in name or "contract" in name:
        return "contract"
    if "slo" in name or "benchmark" in name or "observability" in name:
        return "operability"
    return "evidence"


def _scoped_blocker(name: str, blocker: str) -> str:
    if blocker in {"invalid_json", "invalid_shape"}:
        return f"{name}.{blocker}"
    return blocker


def _strings(value: object) -> tuple[str, ...]:
    if value is None:
        return tuple()
    if isinstance(value, list | tuple):
        return tuple(str(item) for item in value if str(item))
    return (str(value),) if str(value) else tuple()


__all__ = ["RouteRefreshEvidenceReader"]
