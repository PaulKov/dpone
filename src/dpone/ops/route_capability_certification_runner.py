"""Production runner adapter for route capability certification."""

from __future__ import annotations

import json
import tempfile
from collections.abc import Mapping
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import yaml

from dpone.ops.route_capability_certification_models import CertificationRouteRequest, CertificationRouteRun
from dpone.ops.route_capability_target_quality import collect_target_quality
from dpone.runtime.route_runtime import RouteCapabilityBlocked
from dpone.runtime.route_runtime_factory import RouteCapabilityRuntimeFactory
from dpone.services.manifest import resolve_single_process
from dpone.services.run_manifest import RunManifestService


class RunManifestRouteCertificationRunner:
    """Execute certification routes through the canonical manifest runner."""

    def __init__(self, *, run_service: RunManifestService | None = None) -> None:
        self._run_service = run_service or RunManifestService()

    def preflight(self, request: CertificationRouteRequest) -> Mapping[str, object]:
        if not request.manifest_path.exists():
            return _missing_manifest(request.route_id)
        manifest = _load_manifest(request.manifest_path)
        config = _apply_route_overrides(manifest, request, preflight_only=True)
        with _temporary_manifest(config) as path:
            try:
                payload = _run_preflight(path, request)
            except Exception as exc:
                payload = _preflight_payload(
                    request.route_id,
                    blockers=[f"preflight_failed:{type(exc).__name__}"],
                    summary={"exception": type(exc).__name__, "message": str(exc)},
                )
        blockers = _errors(payload)
        return {
            "route_id": request.route_id,
            "passed": not blockers,
            "blockers": blockers,
            "warnings": list(payload.get("warnings", [])),
            "run_summary": payload,
        }

    def run_route(self, request: CertificationRouteRequest) -> CertificationRouteRun:
        if not request.manifest_path.exists():
            return CertificationRouteRun(
                route_id=request.route_id,
                passed=False,
                run_summary={},
                route_decisions=[],
                load_steps=[],
                source_quality={},
                target_quality={},
                cleanup={},
                duration_seconds=0.0,
                blockers=["manifest_missing"],
            )
        manifest = _load_manifest(request.manifest_path)
        config = _apply_route_overrides(manifest, request, preflight_only=False)
        with _temporary_manifest(config) as path:
            report = self._run_service.run(
                path=path,
                manifest_ctx=_manifest_context(),
                selector=request.selector,
                run_id=request.run_id,
            )
            target_quality = _merge_quality(
                _extract_quality(report.to_dict(), "target_quality"),
                _collect_target_quality(path, request),
            )
        payload = report.to_dict()
        return CertificationRouteRun(
            route_id=request.route_id,
            passed=report.passed,
            run_summary=payload,
            route_decisions=_extract_route_decisions(payload),
            load_steps=_extract_load_steps(payload),
            source_quality=_extract_quality(payload, "source_quality"),
            target_quality=target_quality,
            cleanup=_extract_quality(payload, "cleanup"),
            duration_seconds=float(report.result.duration_seconds),
            blockers=_errors(payload),
        )


def _missing_manifest(route_id: str) -> dict[str, object]:
    return {"route_id": route_id, "passed": False, "blockers": ["manifest_missing"], "warnings": []}


def _load_manifest(path: Path) -> dict[str, Any]:
    return dict(yaml.safe_load(path.read_text(encoding="utf-8")) or {})


def _temporary_manifest(config: Mapping[str, object]):
    class _ManifestContext:
        def __enter__(self) -> Path:
            self._tmp = tempfile.TemporaryDirectory(prefix="dpone-route-capability-cert-")
            self.path = Path(self._tmp.name) / "manifest.yaml"
            self.path.write_text(
                yaml.safe_dump(dict(config), sort_keys=False, allow_unicode=True),
                encoding="utf-8",
            )
            return self.path

        def __exit__(self, exc_type, exc, tb) -> None:
            del exc_type, exc, tb
            self._tmp.cleanup()

    return _ManifestContext()


def _run_preflight(path: Path, request: CertificationRouteRequest) -> dict[str, object]:
    manifest_ctx = _manifest_context()
    loaded = manifest_ctx.loader.load(path, metadata_only=False)
    spec = resolve_single_process(loaded, selector=request.selector)
    spec.config.ensure_runtime_bindings()
    orchestrator = RouteCapabilityRuntimeFactory().build(
        load_config=spec.config.load_config,
        source=spec.config.source_obj,
        sink=spec.config.sink_obj,
        logger=spec.config.etl_logger,
    )
    if orchestrator is None:
        return _preflight_payload(request.route_id, blockers=["route_capability_orchestrator_disabled"])
    load_record = SimpleNamespace(run_id=request.run_id, load_id=f"{request.run_id}:{request.route_id}:preflight")
    try:
        orchestrator.prepare(
            load_config=spec.config.load_config,
            source=spec.config.source_obj,
            sink=spec.config.sink_obj,
            load_record=load_record,
        )
    except RouteCapabilityBlocked as exc:
        return _preflight_payload(
            request.route_id,
            blockers=list(exc.decision.blockers),
            warnings=list(exc.decision.warnings),
            summary=exc.decision.to_evidence(),
        )
    summary = orchestrator.summary() or {}
    return _preflight_payload(request.route_id, warnings=list(summary.get("warnings", [])), summary=summary)


def _preflight_payload(
    route_id: str,
    *,
    blockers: list[str] | None = None,
    warnings: list[str] | None = None,
    summary: Mapping[str, object] | None = None,
) -> dict[str, object]:
    return {
        "route_id": route_id,
        "result": {"errors": blockers or [], "route_capabilities": dict(summary or {})},
        "warnings": warnings or [],
    }


def _apply_route_overrides(
    manifest: Mapping[str, Any],
    request: CertificationRouteRequest,
    *,
    preflight_only: bool,
) -> dict[str, Any]:
    config = _deepcopy_mapping(manifest)
    runtime = _mapping(config.get("runtime"))
    capabilities = _mapping(runtime.get("capabilities"))
    capabilities["requested_route_id"] = request.route_id
    capabilities["mode"] = "required" if preflight_only else "auto"
    runtime["capabilities"] = capabilities
    config["runtime"] = runtime
    sink = _mapping(config.setdefault("sink", {}))
    sink["table"] = {**_mapping(sink.get("table")), "schema": request.target_schema, "name": request.target_table}
    sink_options = _mapping(sink.get("options"))
    _set_capability_overrides(sink_options, request=request, preflight_only=preflight_only)
    sink["options"] = _sink_options_with_columnar_overrides(
        sink_options,
        request=request,
        preflight_only=preflight_only,
    )
    config["sink"] = sink
    source = _mapping(config.setdefault("source", {}))
    source["options"] = _source_options_with_columnar_overrides(
        _mapping(source.get("options")),
        request=request,
        preflight_only=preflight_only,
    )
    config["source"] = source
    return config


def _set_capability_overrides(
    options: dict[str, Any],
    *,
    request: CertificationRouteRequest,
    preflight_only: bool,
) -> None:
    capabilities = options.setdefault("runtime", {}).setdefault("capabilities", {})
    capabilities["requested_route_id"] = request.route_id
    capabilities["mode"] = "required" if preflight_only else "auto"


def _source_options_with_columnar_overrides(
    source_options: dict[str, Any],
    *,
    request: CertificationRouteRequest,
    preflight_only: bool,
) -> dict[str, Any]:
    native_transfer = source_options.setdefault("native_transfer", {})
    snapshot = native_transfer.setdefault("snapshot", {})
    snapshot["columnar_fast_path"] = _columnar_options(
        existing=_mapping(snapshot.get("columnar_fast_path")),
        route_id=request.route_id,
        mode="required" if preflight_only else "auto",
        object_prefix=request.object_prefix,
        keep_artifacts=request.keep_artifacts,
    )
    return source_options


def _sink_options_with_columnar_overrides(
    sink_options: dict[str, Any],
    *,
    request: CertificationRouteRequest,
    preflight_only: bool,
) -> dict[str, Any]:
    native_transfer = sink_options.setdefault("native_transfer", {})
    snapshot = native_transfer.setdefault("snapshot", {})
    snapshot["columnar_fast_path"] = _columnar_options(
        existing=_mapping(snapshot.get("columnar_fast_path")),
        route_id=request.route_id,
        mode="required" if preflight_only else "auto",
        object_prefix=request.object_prefix,
        keep_artifacts=request.keep_artifacts,
    )
    return sink_options


def _columnar_options(
    *,
    existing: Mapping[str, Any] | None = None,
    route_id: str,
    mode: str,
    object_prefix: str,
    keep_artifacts: bool,
) -> dict[str, Any]:
    columnar: dict[str, Any] = dict(existing or {})
    columnar["mode"] = mode
    columnar["provider"] = _columnar_provider(route_id)
    if route_id != "direct_push_columnar":
        object_storage = _mapping(columnar.get("object_storage"))
        object_storage["uri_prefix"] = object_prefix
        object_storage["cleanup_policy"] = "keep_on_failure" if keep_artifacts else "eager"
        columnar["object_storage"] = object_storage
    else:
        columnar.pop("object_storage", None)
    return columnar


def _columnar_provider(route_id: str) -> str:
    if "object_storage_pull" in route_id:
        return "object_storage_pull"
    if route_id == "direct_push_columnar":
        return "direct_push_columnar"
    return "auto"


def _mapping(value: object | None) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _deepcopy_mapping(value: Mapping[str, Any]) -> dict[str, Any]:
    return json.loads(json.dumps(value))


def _manifest_context() -> Any:
    from dpone.manifest.loader import ManifestLoaderRouter
    from dpone.services.manifest import ManifestCommandContext

    return ManifestCommandContext(registry_paths=(), loader=ManifestLoaderRouter(registry_paths=()))


def _collect_target_quality(path: Path, request: CertificationRouteRequest) -> dict[str, object]:
    manifest_ctx = _manifest_context()
    loaded = manifest_ctx.loader.load(path, metadata_only=False)
    spec = resolve_single_process(loaded, selector=request.selector)
    spec.config.ensure_runtime_bindings()
    return collect_target_quality(sink=spec.config.sink_obj, load_config=spec.config.load_config)


def _merge_quality(base: Mapping[str, object], extra: Mapping[str, object]) -> dict[str, object]:
    return {**dict(base), **dict(extra)}


def _extract_route_decisions(payload: Mapping[str, object]) -> list[dict[str, object]]:
    result = _mapping(payload.get("result"))
    route = result.get("route_capabilities")
    return [route] if isinstance(route, Mapping) else []


def _extract_load_steps(payload: Mapping[str, object]) -> list[dict[str, object]]:
    result = _mapping(payload.get("result"))
    steps = result.get("load_steps")
    return [dict(step) for step in steps] if isinstance(steps, list) else []


def _extract_quality(payload: Mapping[str, object], key: str) -> dict[str, object]:
    result = _mapping(payload.get("result"))
    value = result.get(key)
    return dict(value) if isinstance(value, Mapping) else {}


def _errors(payload: Mapping[str, object]) -> list[str]:
    result = _mapping(payload.get("result"))
    errors = result.get("errors")
    return [str(item) for item in errors] if isinstance(errors, list) else []


__all__ = ["RunManifestRouteCertificationRunner"]
