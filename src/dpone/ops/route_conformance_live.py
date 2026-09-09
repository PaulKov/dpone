"""Live Route Conformance Lab service facade."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from importlib import import_module
from pathlib import Path
from typing import Any

from dpone.ops.routes.catalog import RouteProfileCatalog
from dpone.ops.routes.conformance_live_models import (
    RouteConformanceLiveConfig,
    RouteConformanceLiveReport,
    RouteConformanceLiveStep,
)
from dpone.ops.routes.conformance_models import (
    RouteConformanceDecision,
    RouteConformanceReport,
    RouteConformanceVerificationResult,
)
from dpone.ops.routes.models import RouteKey, RouteProfile


class RouteConformanceLiveService:
    """Run live adapter orchestration and exact conformance verification."""

    def __init__(
        self,
        *,
        catalog: RouteProfileCatalog | None = None,
        dataset_factory: Any | None = None,
        verifier: Any | None = None,
        policy: Any | None = None,
        adapter_registry: Mapping[str, Any] | None = None,
    ) -> None:
        self._catalog = catalog or RouteProfileCatalog.default()
        self._dataset_factory = dataset_factory or _default_dataset_factory()
        self._verifier = verifier or _default_verifier()
        self._policy = policy or _default_policy()
        self._adapter_registry = adapter_registry if adapter_registry is not None else _default_adapter_registry()

    def run(
        self,
        *,
        output_dir: str | Path,
        source: str,
        sink: str,
        strategy: str,
        config: RouteConformanceLiveConfig,
    ) -> RouteConformanceLiveReport:
        directory = Path(output_dir)
        route = RouteKey.of(source, sink, strategy)
        profile = self._catalog.get(route)
        dataset = self._dataset_factory.generate(config.dataset)
        adapter = self._adapter_registry.get(config.adapter)
        if adapter is None:
            return _missing_adapter_report(
                directory=directory,
                route=route,
                profile=profile,
                config=config,
            )

        steps: list[RouteConformanceLiveStep] = [adapter.seed_source(route=route, dataset=dataset, config=config)]
        if _step_blockers(steps):
            return _blocked_step_report(directory=directory, route=route, profile=profile, config=config, steps=steps)

        if config.require_schema_evolution or config.dataset.include_schema_evolution:
            steps.append(adapter.apply_schema_evolution(route=route, dataset=dataset, config=config))
            if _step_blockers(steps):
                return _blocked_step_report(
                    directory=directory, route=route, profile=profile, config=config, steps=steps
                )

        steps.append(adapter.execute_route(route=route, dataset=dataset, config=config))
        if _step_blockers(steps):
            return _blocked_step_report(directory=directory, route=route, profile=profile, config=config, steps=steps)

        source_snapshot = adapter.read_source_snapshot(route=route, dataset=dataset, config=config)
        sink_snapshot = adapter.read_sink_snapshot(route=route, dataset=dataset, config=config)
        source_snapshot_path = directory / "live_source_snapshot.json"
        sink_snapshot_path = directory / "live_sink_snapshot.json"
        _write_json(source_snapshot_path, source_snapshot.to_dict())
        _write_json(sink_snapshot_path, sink_snapshot.to_dict())
        verification = self._verifier.verify(
            source_snapshot=source_snapshot,
            sink_snapshot=sink_snapshot,
            chunk_size=config.dataset.chunk_size,
        )
        schema_evolution = _schema_evolution(
            required=config.require_schema_evolution, plan=dataset.schema_evolution_plan
        )
        decision = self._policy.evaluate(
            route=route,
            profile_exists=profile is not None,
            dataset=config.dataset,
            verification=verification,
            schema_evolution=schema_evolution,
            min_rows=config.min_rows,
            min_columns=config.min_columns,
            require_schema_evolution=config.require_schema_evolution,
        )
        steps.append(
            RouteConformanceLiveStep(
                name="verify_exact",
                status=decision.status,
                summary="exact verification passed" if decision.passed else "exact verification blocked",
                rows=verification.sink_rows,
                blockers=decision.blockers,
            )
        )
        conformance = _embedded_conformance_report(
            directory=directory,
            route=route,
            profile=profile,
            config=config,
            verification=verification,
            schema_evolution=schema_evolution,
            decision=decision,
            source_snapshot_path=source_snapshot_path,
            sink_snapshot_path=sink_snapshot_path,
        )
        conformance.write()
        report = RouteConformanceLiveReport(
            route=route,
            profile=profile,
            adapter=config.adapter,
            config=config,
            conformance=conformance,
            verification=verification,
            decision=decision,
            live_steps=tuple(steps),
            schema_evolution=schema_evolution,
            artifacts={
                "live_source_snapshot": str(source_snapshot_path),
                "live_sink_snapshot": str(sink_snapshot_path),
                "route_conformance": conformance.json_path,
                "report": str(directory / "route_conformance_live.json"),
            },
            output_dir=str(directory),
            json_path=str(directory / "route_conformance_live.json"),
            markdown_path=str(directory / "route_conformance_live.md"),
        )
        report.write()
        return report


def _blocked_step_report(
    *,
    directory: Path,
    route: RouteKey,
    profile: RouteProfile | None,
    config: RouteConformanceLiveConfig,
    steps: Sequence[RouteConformanceLiveStep],
) -> RouteConformanceLiveReport:
    blockers = _step_blockers(steps)
    decision = RouteConformanceDecision(
        passed=False,
        status="blocked",
        score=0.0,
        blockers=blockers,
        warnings=tuple(),
        next_actions=("Fix the blocked live adapter step, then rerun live-run.",),
    )
    verification = RouteConformanceVerificationResult(
        passed=False,
        source_rows=0,
        sink_rows=0,
        chunk_count=0,
        source_hash="0" * 64,
        sink_hash="0" * 64,
        chunks=tuple(),
        physical_contract_mismatches=tuple(),
        mismatch_samples=tuple(),
        blockers=decision.blockers,
        warnings=tuple(),
    )
    report = RouteConformanceLiveReport(
        route=route,
        profile=profile,
        adapter=config.adapter,
        config=config,
        conformance=None,
        verification=verification,
        decision=decision,
        live_steps=tuple(steps),
        schema_evolution=_blocked_schema_evolution(config),
        artifacts={"report": str(directory / "route_conformance_live.json")},
        output_dir=str(directory),
        json_path=str(directory / "route_conformance_live.json"),
        markdown_path=str(directory / "route_conformance_live.md"),
    )
    report.write()
    return report


def _embedded_conformance_report(
    *,
    directory: Path,
    route: RouteKey,
    profile: RouteProfile | None,
    config: RouteConformanceLiveConfig,
    verification: RouteConformanceVerificationResult,
    schema_evolution: Mapping[str, object],
    decision: RouteConformanceDecision,
    source_snapshot_path: Path,
    sink_snapshot_path: Path,
) -> RouteConformanceReport:
    return RouteConformanceReport(
        route=route,
        profile=profile,
        dataset=config.dataset,
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


def _missing_adapter_report(
    *,
    directory: Path,
    route: RouteKey,
    profile: RouteProfile | None,
    config: RouteConformanceLiveConfig,
) -> RouteConformanceLiveReport:
    decision = RouteConformanceDecision(
        passed=False,
        status="blocked",
        score=0.0,
        blockers=(f"adapter.{config.adapter}.missing",),
        warnings=tuple(),
        next_actions=("Install or register the requested live conformance adapter, then rerun live-run.",),
    )
    verification = RouteConformanceVerificationResult(
        passed=False,
        source_rows=0,
        sink_rows=0,
        chunk_count=0,
        source_hash="0" * 64,
        sink_hash="0" * 64,
        chunks=tuple(),
        physical_contract_mismatches=tuple(),
        mismatch_samples=tuple(),
        blockers=decision.blockers,
        warnings=tuple(),
    )
    report = RouteConformanceLiveReport(
        route=route,
        profile=profile,
        adapter=config.adapter,
        config=config,
        conformance=None,
        verification=verification,
        decision=decision,
        live_steps=(
            RouteConformanceLiveStep(
                name="resolve_adapter",
                status="blocked",
                summary=f"adapter {config.adapter} is not registered",
                blockers=decision.blockers,
            ),
        ),
        schema_evolution=_blocked_schema_evolution(config),
        artifacts={"report": str(directory / "route_conformance_live.json")},
        output_dir=str(directory),
        json_path=str(directory / "route_conformance_live.json"),
        markdown_path=str(directory / "route_conformance_live.md"),
    )
    report.write()
    return report


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


def _blocked_schema_evolution(config: RouteConformanceLiveConfig) -> dict[str, object]:
    return {
        "required": config.require_schema_evolution,
        "passed": False,
        "status": "blocked",
        "operations": [],
        "plan": {},
    }


def _step_blockers(steps: Sequence[RouteConformanceLiveStep]) -> tuple[str, ...]:
    return tuple(dict.fromkeys(blocker for step in steps for blocker in step.blockers))


def _default_dataset_factory() -> Any:
    module = import_module("dpone.ops.routes.conformance_dataset")
    return module.SyntheticRouteDatasetFactory()


def _default_verifier() -> Any:
    module = import_module("dpone.ops.routes.conformance_verifier")
    return module.RouteConformanceVerifier()


def _default_policy() -> Any:
    module = import_module("dpone.ops.routes.conformance_policy")
    return module.RouteConformancePolicy()


def _default_adapter_registry() -> Mapping[str, Any]:
    module = import_module("dpone.ops.routes.conformance_live_adapters")
    return module.default_live_adapter_registry()


def _write_json(path: str | Path, payload: Mapping[str, object]) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


__all__ = ["RouteConformanceLiveService"]
