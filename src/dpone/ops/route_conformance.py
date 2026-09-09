"""Route Conformance Lab service facade."""

from __future__ import annotations

import json
from collections.abc import Mapping
from importlib import import_module
from pathlib import Path
from typing import Any

from dpone.ops.certification_artifacts import artifact_payload_passed
from dpone.ops.checksums import sha256_file
from dpone.ops.routes.catalog import RouteProfileCatalog
from dpone.ops.routes.conformance_models import (
    ROUTE_CONFORMANCE_RELEASE_GATE_SCHEMA_VERSION,
    ROUTE_CONFORMANCE_SUMMARY_SCHEMA_VERSION,
    ConformanceStatus,
    RouteConformanceAggregateReport,
    RouteConformanceArtifactSummary,
    RouteConformanceDatasetProfile,
    RouteConformanceReport,
)
from dpone.ops.routes.models import RouteKey


class RouteConformanceService:
    """Run deterministic route conformance checks and release gates."""

    def __init__(
        self,
        *,
        catalog: RouteProfileCatalog | None = None,
        dataset_factory: Any | None = None,
        verifier: Any | None = None,
        policy: Any | None = None,
    ) -> None:
        self._catalog = catalog or RouteProfileCatalog.default()
        self._dataset_factory = dataset_factory or _default_dataset_factory()
        self._verifier = verifier or _default_verifier()
        self._policy = policy or _default_policy()

    def run(
        self,
        *,
        output_dir: str | Path,
        source: str,
        sink: str,
        strategy: str,
        dataset_profile: RouteConformanceDatasetProfile,
        min_rows: int = 0,
        min_columns: int = 0,
        require_schema_evolution: bool = False,
    ) -> RouteConformanceReport:
        directory = Path(output_dir)
        route = RouteKey.of(source, sink, strategy)
        profile = self._catalog.get(route)
        dataset = self._dataset_factory.generate(dataset_profile)
        source_snapshot = dataset.to_snapshot("source")
        sink_snapshot = dataset.to_snapshot("sink")
        source_snapshot_path = directory / "source_snapshot.json"
        sink_snapshot_path = directory / "sink_snapshot.json"
        _write_json(source_snapshot_path, source_snapshot.to_dict())
        _write_json(sink_snapshot_path, sink_snapshot.to_dict())
        verification = self._verifier.verify(
            source_snapshot=source_snapshot,
            sink_snapshot=sink_snapshot,
            chunk_size=dataset_profile.chunk_size,
        )
        schema_evolution = _schema_evolution(
            required=require_schema_evolution,
            plan=dataset.schema_evolution_plan,
        )
        decision = self._policy.evaluate(
            route=route,
            profile_exists=profile is not None,
            dataset=dataset_profile,
            verification=verification,
            schema_evolution=schema_evolution,
            min_rows=min_rows,
            min_columns=min_columns,
            require_schema_evolution=require_schema_evolution,
        )
        report = RouteConformanceReport(
            route=route,
            profile=profile,
            dataset=dataset_profile,
            verification=verification,
            schema_evolution=schema_evolution,
            decision=decision,
            artifacts={
                "source_snapshot": str(source_snapshot_path),
                "sink_snapshot": str(sink_snapshot_path),
                "report": str(directory / "route_conformance.json"),
            },
            output_dir=str(directory),
            json_path=str(directory / "route_conformance.json"),
            markdown_path=str(directory / "route_conformance.md"),
        )
        report.write()
        return report

    def summarize(
        self,
        *,
        output_dir: str | Path,
        artifacts: Mapping[str, str | Path],
    ) -> RouteConformanceAggregateReport:
        return _aggregate_report(
            output_dir=output_dir,
            artifacts=artifacts,
            schema_version=ROUTE_CONFORMANCE_SUMMARY_SCHEMA_VERSION,
            title="Route Conformance Summary",
            release="",
            json_name="route_conformance_summary.json",
            markdown_name="route_conformance_summary.md",
            gate_mode=False,
        )

    def release_gate(
        self,
        *,
        output_dir: str | Path,
        release: str,
        artifacts: Mapping[str, str | Path],
    ) -> RouteConformanceAggregateReport:
        return _aggregate_report(
            output_dir=output_dir,
            artifacts=artifacts,
            schema_version=ROUTE_CONFORMANCE_RELEASE_GATE_SCHEMA_VERSION,
            title="Route Conformance Release Gate",
            release=release,
            json_name="route_conformance_release_gate.json",
            markdown_name="route_conformance_release_gate.md",
            gate_mode=True,
        )


def _schema_evolution(*, required: bool, plan: Mapping[str, object]) -> dict[str, object]:
    raw_operations = plan.get("operations")
    operations = [str(item) for item in raw_operations] if isinstance(raw_operations, list) else []
    passed = bool(operations) if required else True
    return {
        "required": required,
        "passed": passed,
        "status": "verified" if passed else "blocked",
        "operations": operations,
        "plan": dict(plan),
    }


def _default_dataset_factory() -> Any:
    module = import_module("dpone.ops.routes.conformance_dataset")
    return module.SyntheticRouteDatasetFactory()


def _default_verifier() -> Any:
    module = import_module("dpone.ops.routes.conformance_verifier")
    return module.RouteConformanceVerifier()


def _default_policy() -> Any:
    module = import_module("dpone.ops.routes.conformance_policy")
    return module.RouteConformancePolicy()


def _write_json(path: str | Path, payload: Mapping[str, object]) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _aggregate_report(
    *,
    output_dir: str | Path,
    artifacts: Mapping[str, str | Path],
    schema_version: str,
    title: str,
    release: str,
    json_name: str,
    markdown_name: str,
    gate_mode: bool,
) -> RouteConformanceAggregateReport:
    directory = Path(output_dir)
    summaries = tuple(_artifact_summary(name=name, path=path) for name, path in artifacts.items())
    blockers = _aggregate_blockers(summaries, gate_mode=gate_mode)
    warnings = tuple(warning for summary in summaries for warning in summary.warnings)
    passed_count = sum(1 for summary in summaries if summary.passed)
    status = _aggregate_status(tuple(blockers), warnings)
    report = RouteConformanceAggregateReport(
        schema_version=schema_version,
        title=title,
        release=release,
        passed=status != "blocked",
        status=status,
        score=_aggregate_score(len(summaries), passed_count, tuple(blockers)),
        routes=summaries,
        blockers=tuple(blockers),
        warnings=warnings,
        next_actions=_aggregate_next_actions(tuple(blockers)),
        output_dir=str(directory),
        json_path=str(directory / json_name),
        markdown_path=str(directory / markdown_name),
    )
    report.write()
    return report


def _artifact_summary(*, name: str, path: str | Path) -> RouteConformanceArtifactSummary:
    artifact_path = Path(path)
    if not artifact_path.is_file():
        return RouteConformanceArtifactSummary(
            name=name,
            path=str(path),
            passed=False,
            missing=True,
            sha256="0" * 64,
            route_case_id="",
            dataset={},
            blockers=(f"{name}.missing",),
            warnings=tuple(),
        )
    payload = _read_payload(artifact_path)
    blockers = (
        tuple(str(item) for item in payload.get("blockers", ()) if str(item))
        if _is_sequence(payload.get("blockers"))
        else tuple()
    )
    warnings = (
        tuple(str(item) for item in payload.get("warnings", ()) if str(item))
        if _is_sequence(payload.get("warnings"))
        else tuple()
    )
    return RouteConformanceArtifactSummary(
        name=name,
        path=str(artifact_path),
        passed=artifact_payload_passed(payload, name=name) and not blockers,
        missing=False,
        sha256=sha256_file(artifact_path),
        route_case_id=_route_case_id(payload),
        dataset=dict(payload.get("dataset", {})) if isinstance(payload.get("dataset"), Mapping) else {},
        blockers=blockers,
        warnings=warnings,
    )


def _read_payload(path: Path) -> Mapping[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {"passed": False, "blockers": ["invalid_json"]}
    return payload if isinstance(payload, Mapping) else {"passed": False, "blockers": ["invalid_shape"]}


def _route_case_id(payload: Mapping[str, Any]) -> str:
    route = payload.get("route")
    if isinstance(route, Mapping):
        return str(route.get("case_id", ""))
    return ""


def _aggregate_blockers(
    summaries: tuple[RouteConformanceArtifactSummary, ...],
    *,
    gate_mode: bool,
) -> tuple[str, ...]:
    blockers: list[str] = []
    if not summaries:
        blockers.append("route_conformance_artifacts.missing")
    for summary in summaries:
        blockers.extend(summary.blockers)
        if gate_mode and not summary.passed and not summary.missing:
            blockers.append(f"{summary.name}.not_passed")
    return tuple(dict.fromkeys(blockers))


def _aggregate_next_actions(blockers: tuple[str, ...]) -> tuple[str, ...]:
    if not blockers:
        return (
            "Attach route_conformance_summary.json or route_conformance_release_gate.json to release evidence.",
            "Keep generated source/sink snapshots immutable for audit review.",
        )
    return (
        "Open each failed route_conformance.json and inspect blockers plus mismatch_samples.",
        "Regenerate conformance evidence after fixing route conversion, schema evolution, or dataset scale gaps.",
    )


def _aggregate_status(blockers: tuple[str, ...], warnings: tuple[str, ...]) -> ConformanceStatus:
    if blockers:
        return "blocked"
    if warnings:
        return "warning"
    return "verified"


def _aggregate_score(total: int, passed: int, blockers: tuple[str, ...]) -> float:
    if total <= 0:
        return 0.0
    if blockers:
        return round((passed / total) * 100.0, 2)
    return 100.0


def _is_sequence(value: object) -> bool:
    return isinstance(value, list | tuple)


__all__ = ["RouteConformanceService"]
