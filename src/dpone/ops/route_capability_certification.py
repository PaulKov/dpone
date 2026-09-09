"""Certification harness for runtime route capability planning."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import yaml

from dpone.ops.route_capability_certification_models import (
    DEFAULT_BENCHMARK_ROUTES,
    PREFLIGHT_ROUTES,
    ROUTE_TARGET_PREFIX,
    SECRET_MARKERS,
    CertificationRouteRequest,
    CertificationRouteRun,
    RouteCapabilityCertificationReport,
    RouteCapabilityCertificationRequest,
    RouteCertificationRunner,
)
from dpone.ops.route_capability_certification_quality import RouteQualityGate


class RouteCapabilityCertificationService:
    """Run route capability certification scenarios and write evidence artifacts."""

    def __init__(
        self,
        *,
        route_runner: RouteCertificationRunner,
        quality_gate: RouteQualityGate | None = None,
    ) -> None:
        self._route_runner = route_runner
        self._quality_gate = quality_gate or RouteQualityGate()

    def certify(self, request: RouteCapabilityCertificationRequest) -> RouteCapabilityCertificationReport:
        output_dir = Path(request.output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        route_ids = _routes_for(request)
        if request.scenario == "preflight_only":
            return self._preflight_only(request, output_dir, route_ids)
        return self._benchmark(request, output_dir, route_ids)

    def _preflight_only(
        self,
        request: RouteCapabilityCertificationRequest,
        output_dir: Path,
        route_ids: Sequence[str],
    ) -> RouteCapabilityCertificationReport:
        preflights = [dict(self._route_runner.preflight(_route_request(request, route_id))) for route_id in route_ids]
        blockers = _unique(str(blocker) for item in preflights for blocker in _sequence(item.get("blockers")))
        routes = [
            _route_payload(item["route_id"], bool(item.get("passed")), 0.0, _sequence(item.get("blockers")))
            for item in preflights
        ]
        return _write_report(
            request=request,
            output_dir=output_dir,
            routes=routes,
            route_decisions=preflights,
            load_steps=[],
            quality_report={"passed": not blockers, "routes": []},
            blockers=blockers,
            warnings=_unique(str(warning) for item in preflights for warning in _sequence(item.get("warnings"))),
            preferred_route_id=None,
            source_io_started=False,
        )

    def _benchmark(
        self,
        request: RouteCapabilityCertificationRequest,
        output_dir: Path,
        route_ids: Sequence[str],
    ) -> RouteCapabilityCertificationReport:
        runs = [self._route_runner.run_route(_route_request(request, route_id)) for route_id in route_ids]
        quality_routes = [
            self._quality_gate.evaluate(
                route_id=run.route_id,
                source_quality=run.source_quality,
                target_quality=run.target_quality,
                cleanup=run.cleanup,
            )
            for run in runs
        ]
        routes = [_run_payload(run, quality) for run, quality in zip(runs, quality_routes, strict=True)]
        return _write_report(
            request=request,
            output_dir=output_dir,
            routes=routes,
            route_decisions=[decision for run in runs for decision in run.route_decisions],
            load_steps=[step for run in runs for step in run.load_steps],
            quality_report={"passed": all(item["passed"] for item in quality_routes), "routes": quality_routes},
            blockers=_benchmark_blockers(runs, quality_routes),
            warnings=_unique(str(warning) for run in runs for warning in run.warnings),
            preferred_route_id=_preferred_route(routes),
            source_io_started=True,
        )


def _write_report(
    *,
    request: RouteCapabilityCertificationRequest,
    output_dir: Path,
    routes: list[dict[str, object]],
    route_decisions: Sequence[Mapping[str, object]],
    load_steps: Sequence[Mapping[str, object]],
    quality_report: Mapping[str, object],
    blockers: list[str],
    warnings: list[str],
    preferred_route_id: str | None,
    source_io_started: bool,
) -> RouteCapabilityCertificationReport:
    artifact_index = _artifact_index(output_dir)
    report = RouteCapabilityCertificationReport(
        scenario=request.scenario,
        manifest_path=str(request.manifest_path),
        output_dir=str(output_dir),
        passed=not blockers,
        preferred_route_id=preferred_route_id,
        blockers=blockers,
        warnings=warnings,
        safe_overrides={"source_io_started": source_io_started, "keep_artifacts": request.keep_artifacts},
        routes=routes,
        quality_report=dict(quality_report),
        artifact_index=artifact_index,
    )
    _write_json(Path(artifact_index["certification_json"]), report.to_dict())
    Path(artifact_index["certification_markdown"]).write_text(report.to_markdown(), encoding="utf-8")
    _write_jsonl(Path(artifact_index["route_decisions_jsonl"]), route_decisions)
    _write_json(Path(artifact_index["load_steps_json"]), {"load_steps": list(load_steps)})
    _write_json(Path(artifact_index["quality_report_json"]), quality_report)
    return report


def _route_request(request: RouteCapabilityCertificationRequest, route_id: str) -> CertificationRouteRequest:
    return CertificationRouteRequest(
        manifest_path=request.manifest_path,
        scenario=request.scenario,
        route_id=route_id,
        run_id=request.run_id,
        target_schema=request.target_schema,
        target_table=f"{ROUTE_TARGET_PREFIX}_{_dataset_slug(request)}_{route_id}",
        object_prefix=_object_prefix(request, route_id),
        keep_artifacts=request.keep_artifacts,
        selector=request.selector,
    )


def _routes_for(request: RouteCapabilityCertificationRequest) -> tuple[str, ...]:
    if request.routes:
        return tuple(request.routes)
    if request.scenario == "preflight_only":
        return PREFLIGHT_ROUTES
    return DEFAULT_BENCHMARK_ROUTES


def _dataset_slug(request: RouteCapabilityCertificationRequest) -> str:
    if request.scenario == "work-item_account_sales_benchmark":
        return "work-item_account_sales"
    return request.manifest_path.stem.replace("-", "_")


def _object_prefix(request: RouteCapabilityCertificationRequest, route_id: str) -> str:
    if request.object_prefix:
        base = request.object_prefix.format(run_id=request.run_id, route_id=route_id)
        return base if base.endswith("/") else f"{base}/"
    if manifest_prefix := _manifest_object_prefix(request.manifest_path):
        base = manifest_prefix.format(run_id=request.run_id, route_id=route_id)
        return _with_route_suffix(base, route_id, append_route="{route_id}" not in manifest_prefix)
    if request.scenario == "work-item_account_sales_benchmark":
        return f"s3://dpone-stage/certification/work-item/{request.run_id}/{route_id}/"
    return f"s3://dpone-stage/certification/{_dataset_slug(request)}/{request.run_id}/{route_id}/"


def _manifest_object_prefix(path: Path) -> str | None:
    try:
        manifest = _mapping(yaml.safe_load(path.read_text(encoding="utf-8")))
    except Exception:
        return None
    source = _mapping(manifest.get("source"))
    options = _mapping(source.get("options"))
    native = _mapping(options.get("native_transfer"))
    snapshot = _mapping(native.get("snapshot"))
    columnar = _mapping(snapshot.get("columnar_fast_path"))
    object_storage = _mapping(columnar.get("object_storage"))
    value = object_storage.get("uri_prefix")
    return str(value).strip() if value else None


def _with_route_suffix(base: str, route_id: str, *, append_route: bool) -> str:
    normalized = base if base.endswith("/") else f"{base}/"
    return f"{normalized}{route_id}/" if append_route else normalized


def _benchmark_blockers(
    runs: Sequence[CertificationRouteRun],
    quality_routes: Sequence[Mapping[str, object]],
) -> list[str]:
    return _unique(
        str(blocker)
        for run, quality in zip(runs, quality_routes, strict=True)
        for blocker in (*run.blockers, *_sequence(quality.get("blockers")))
    )


def _route_payload(
    route_id: object,
    passed: bool,
    duration_seconds: float,
    blockers: Sequence[object],
) -> dict[str, object]:
    return {
        "route_id": str(route_id),
        "passed": passed,
        "duration_seconds": duration_seconds,
        "blockers": [str(item) for item in blockers],
    }


def _run_payload(run: CertificationRouteRun, quality: Mapping[str, object]) -> dict[str, object]:
    payload = _route_payload(
        run.route_id,
        run.passed and bool(quality["passed"]),
        run.duration_seconds,
        _sequence(quality["blockers"]),
    )
    payload["quality"] = quality
    payload["timings"] = _timings(run.load_steps)
    return payload


def _preferred_route(routes: Sequence[Mapping[str, object]]) -> str | None:
    green = [route for route in routes if bool(route.get("passed"))]
    if not green:
        return None
    return str(min(green, key=lambda item: float(item.get("duration_seconds") or 0.0))["route_id"])


def _timings(load_steps: Sequence[Mapping[str, object]]) -> dict[str, float]:
    return {
        str(step.get("step_id") or step.get("phase") or "unknown"): float(step.get("duration_seconds") or 0.0)
        for step in load_steps
    }


def _artifact_index(output_dir: Path) -> dict[str, str]:
    return {
        "certification_json": str(output_dir / "certification.json"),
        "certification_markdown": str(output_dir / "certification.md"),
        "route_decisions_jsonl": str(output_dir / "route_decisions.jsonl"),
        "load_steps_json": str(output_dir / "load_steps.json"),
        "quality_report_json": str(output_dir / "quality_report.json"),
    }


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_redact(payload), ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, records: Sequence[Mapping[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "".join(json.dumps(_redact(record), ensure_ascii=False, sort_keys=True) + "\n" for record in records),
        encoding="utf-8",
    )


def _redact(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): "<redacted>" if _secret_key(str(key)) else _redact(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [_redact(item) for item in value]
    return value


def _secret_key(key: str) -> bool:
    lowered = key.lower()
    return any(marker in lowered for marker in SECRET_MARKERS)


def _mapping(value: object | None) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _unique(values: Any) -> list[str]:
    return list(dict.fromkeys(values))


def _sequence(value: object) -> Sequence[Any]:
    return value if isinstance(value, (list, tuple)) else ()


__all__ = [
    "DEFAULT_BENCHMARK_ROUTES",
    "CertificationRouteRequest",
    "CertificationRouteRun",
    "RouteCapabilityCertificationReport",
    "RouteCapabilityCertificationRequest",
    "RouteCapabilityCertificationService",
    "RouteCertificationRunner",
]
