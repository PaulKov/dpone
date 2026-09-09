"""Artifact normalization for route data quality scorecards."""

from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from dpone.ops.checksums import sha256_file
from dpone.ops.routes.data_quality_models import RouteDataQualityDimension, RouteDataQualityEvidence
from dpone.ops.routes.models import RouteKey

DEFAULT_ROUTE_DATA_QUALITY_EVIDENCE: tuple[str, ...] = (
    "data_contract",
    "quarantine",
    "reconciliation",
)


class RouteDataQualityEvidenceReader:
    """Read route data quality artifacts without applying policy decisions."""

    def read(
        self,
        *,
        name: str,
        path_value: str | Path | None,
        required: bool,
        route: RouteKey,
    ) -> RouteDataQualityEvidence:
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
        score = _score(payload=payload, passed=_passed(payload))
        dimensions = _dimensions(name=name, payload=payload, score=score, passed=_passed(payload))
        exceptions = _exceptions(payload)
        waiver_required, waiver_approved = _waiver(payload)
        return RouteDataQualityEvidence(
            name=name,
            kind=kind,
            path=str(path),
            required=required,
            missing=False,
            passed=_passed(payload) and route_matched and not waiver_required,
            sha256=sha256_file(path),
            summary=_summary(payload),
            blockers=tuple(dict.fromkeys(blockers)),
            warnings=_payload_warnings(payload),
            route_case_id=route_case_id,
            route_matched=route_matched,
            score=score,
            dimensions=dimensions,
            exception_count=int(exceptions["count"]),
            exception_ratio=float(exceptions["ratio"]),
            max_exception_age_hours=float(exceptions["max_age_hours"]),
            waiver_required=waiver_required,
            waiver_approved=waiver_approved,
        )


def _missing(*, name: str, kind: str, path: str, required: bool) -> RouteDataQualityEvidence:
    return RouteDataQualityEvidence(
        name=name,
        kind=kind,
        path=path,
        required=required,
        missing=True,
        passed=not required,
        sha256="0" * 64,
        summary="required route data quality evidence is missing" if required else "optional evidence is missing",
        blockers=(f"{name}.missing",) if required else tuple(),
        warnings=tuple(),
        route_case_id="",
        route_matched=True,
        score=0.0 if required else 100.0,
        dimensions=(
            RouteDataQualityDimension(
                name=name,
                score=0.0 if required else 100.0,
                passed=not required,
                weight=1.0,
                summary="missing",
                blockers=(f"{name}.missing",) if required else tuple(),
            ),
        ),
        exception_count=0,
        exception_ratio=0.0,
        max_exception_age_hours=0.0,
        waiver_required=False,
        waiver_approved=False,
    )


def _payload(path: Path) -> Mapping[str, Any]:
    if path.suffix.lower() != ".json":
        return {"passed": True, "summary": "non-json artifact exists", "quality": {"score": 100.0}}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {"passed": False, "blockers": ["invalid_json"], "quality": {"score": 0.0}}
    return payload if isinstance(payload, Mapping) else {"passed": False, "blockers": ["invalid_shape"]}


def _passed(payload: Mapping[str, Any]) -> bool:
    if "passed" in payload:
        return bool(payload["passed"])
    if payload.get("status") in {"failed", "blocked", "regression", "rollback_required"}:
        return False
    return not bool(payload.get("blockers") or payload.get("violations") or payload.get("findings"))


def _score(*, payload: Mapping[str, Any], passed: bool) -> float:
    quality = payload.get("quality")
    if isinstance(quality, Mapping):
        value = quality.get("score")
        if isinstance(value, int | float):
            return _bounded_score(float(value))
    value = payload.get("score")
    if isinstance(value, int | float):
        return _bounded_score(float(value))
    decision = payload.get("decision")
    if isinstance(decision, Mapping) and isinstance(decision.get("score"), int | float):
        return _bounded_score(float(decision["score"]))
    return 100.0 if passed else 0.0


def _dimensions(
    *,
    name: str,
    payload: Mapping[str, Any],
    score: float,
    passed: bool,
) -> tuple[RouteDataQualityDimension, ...]:
    quality = payload.get("quality")
    raw_dimensions = quality.get("dimensions") if isinstance(quality, Mapping) else None
    dimensions: list[RouteDataQualityDimension] = []
    if isinstance(raw_dimensions, Mapping):
        for key, value in raw_dimensions.items():
            dimensions.append(_dimension(name=str(key), value=value, fallback_score=score, fallback_passed=passed))
    elif isinstance(raw_dimensions, list | tuple):
        for index, value in enumerate(raw_dimensions):
            dimensions.append(
                _dimension(name=f"{name}_{index + 1}", value=value, fallback_score=score, fallback_passed=passed)
            )
    if dimensions:
        return tuple(dimensions)
    return (
        RouteDataQualityDimension(
            name=name,
            score=score,
            passed=passed,
            weight=1.0,
            summary=_summary(payload),
            blockers=_payload_blockers(name, payload),
            warnings=_payload_warnings(payload),
        ),
    )


def _dimension(
    *,
    name: str,
    value: object,
    fallback_score: float,
    fallback_passed: bool,
) -> RouteDataQualityDimension:
    if isinstance(value, Mapping):
        score = _bounded_score(float(value["score"])) if isinstance(value.get("score"), int | float) else fallback_score
        passed = bool(value.get("passed", score >= 95.0 and fallback_passed))
        weight = float(value["weight"]) if isinstance(value.get("weight"), int | float) else 1.0
        summary = str(value.get("summary") or f"{name} score")
        blockers = _strings(value.get("blockers"))
        warnings = _strings(value.get("warnings"))
        return RouteDataQualityDimension(
            name=name,
            score=score,
            passed=passed,
            weight=max(weight, 0.0),
            summary=summary,
            blockers=blockers,
            warnings=warnings,
        )
    if isinstance(value, int | float):
        score = _bounded_score(float(value))
        return RouteDataQualityDimension(
            name=name,
            score=score,
            passed=score >= 95.0 and fallback_passed,
            weight=1.0,
            summary=f"{name} score",
        )
    return RouteDataQualityDimension(
        name=name,
        score=fallback_score,
        passed=fallback_passed,
        weight=1.0,
        summary=str(value) if value is not None else f"{name} score",
    )


def _exceptions(payload: Mapping[str, Any]) -> dict[str, float | int]:
    exceptions = payload.get("exceptions")
    if isinstance(exceptions, Mapping):
        return {
            "count": _int(exceptions.get("count") or exceptions.get("total_count")),
            "ratio": _float(exceptions.get("ratio") or exceptions.get("max_ratio")),
            "max_age_hours": _float(exceptions.get("max_age_hours") or exceptions.get("age_hours")),
        }
    summary = payload.get("summary")
    summary_map = summary if isinstance(summary, Mapping) else {}
    return {
        "count": _int(
            payload.get("exception_count")
            or payload.get("quarantined_rows")
            or payload.get("failed_rows")
            or summary_map.get("quarantined_rows")
            or summary_map.get("failed_rows")
        ),
        "ratio": _float(payload.get("exception_ratio") or payload.get("failed_ratio")),
        "max_age_hours": _float(payload.get("max_exception_age_hours")),
    }


def _payload_blockers(name: str, payload: Mapping[str, Any]) -> tuple[str, ...]:
    blockers = _strings(payload.get("blockers"))
    if blockers:
        return tuple(_scoped_blocker(name, item) for item in blockers)
    if _passed(payload):
        return tuple()
    return (f"{name}.not_passed",)


def _payload_warnings(payload: Mapping[str, Any]) -> tuple[str, ...]:
    return _strings(payload.get("warnings"))


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


def _waiver(payload: Mapping[str, Any]) -> tuple[bool, bool]:
    waiver = payload.get("waiver")
    if isinstance(waiver, Mapping):
        required = bool(waiver.get("required"))
        approved = bool(waiver.get("approved"))
        return required and not approved, approved
    decision = payload.get("decision")
    if isinstance(decision, Mapping):
        status = str(decision.get("status", "")).lower()
        if status in {"waiver_required", "manual_approval_required"}:
            return True, False
    return False, False


def _kind(name: str) -> str:
    if "contract" in name:
        return "contract"
    if "quarantine" in name or "exception" in name:
        return "exception"
    if "reconciliation" in name or "repair" in name:
        return "reconciliation"
    if "slo" in name or "observability" in name:
        return "observability"
    if "run" in name or "cdc" in name or "state" in name:
        return "runtime"
    return "quality"


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


def _bounded_score(value: float) -> float:
    return round(max(0.0, min(100.0, value)), 2)


def _int(value: object) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str) and value.strip():
        try:
            return int(float(value))
        except ValueError:
            return 0
    return 0


def _float(value: object) -> float:
    if isinstance(value, int | float) and not isinstance(value, bool):
        return float(value)
    if isinstance(value, str) and value.strip():
        try:
            return float(value)
        except ValueError:
            return 0.0
    return 0.0


__all__ = ["DEFAULT_ROUTE_DATA_QUALITY_EVIDENCE", "RouteDataQualityEvidenceReader"]
