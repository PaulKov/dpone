"""Route-scoped live certification harness and evidence bundle."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from dpone.ops.certification_artifacts import artifact_payload_passed
from dpone.ops.checksums import sha256_file
from dpone.ops.routes.catalog import RouteProfileCatalog
from dpone.ops.routes.live_certification_models import (
    RouteLiveCertificationEvidence,
    RouteLiveCertificationReport,
    RouteLiveCertificationStep,
)
from dpone.ops.routes.live_certification_policy import RouteLiveCertificationPolicy
from dpone.ops.routes.models import RouteKey, RouteProfile

DEFAULT_ROUTE_LIVE_EVIDENCE: tuple[str, ...] = (
    "service_markers",
    "route_readiness",
    "route_certification_pack",
    "route_execution_ledger",
    "state_promotion",
    "benchmark_slo",
    "performance_certification",
    "live_state_reconciliation",
    "evidence_chain",
)

ROUTE_LIVE_REQUIRED_EVIDENCE: Mapping[str, tuple[str, ...]] = {
    "mssql_to_clickhouse": (
        "cdc_handoff",
        "cdc_apply_certification",
        "cdc_observability_evidence",
        "cdc_recovery_evidence",
        "cdc_schema_evolution_evidence",
        "cdc_promotion_gate",
    ),
    "postgres_to_mssql": (
        "native_transfer_evidence",
        "lossless_transport_contract",
        "resume_checkpoint",
        "type_matrix",
    ),
}

PROFILE_REQUIRED_EVIDENCE: Mapping[str, tuple[str, ...]] = {
    "native_transfer": ("native_transfer_transport_certification", "native_transfer_route_certification"),
}

ROUTE_LIVE_TEST_TARGETS: Mapping[str, tuple[str, ...]] = {
    "mssql_to_clickhouse": (
        "tests/integration/mssql/test_mssql_clickhouse_live_cdc_runtime_integration.py",
        "tests/integration/mssql/test_mssql_to_clickhouse_native_transfer_integration.py",
    ),
    "postgres_to_mssql": ("tests/integration/mssql/test_postgres_to_mssql_native_transfer_integration.py",),
}

LOCAL_DOCKER_SERVICES: tuple[str, ...] = ("postgres", "mssql", "clickhouse", "kafka", "schema-registry", "minio")
LIVE_PROFILES: frozenset[str] = frozenset(
    ("local_live", "real_local", "type_matrix_certification", "native_transfer", "vendor_live")
)


class RouteLiveCertificationService:
    """Build route live-certification plans and evidence bundles without running heavy checks."""

    def __init__(
        self,
        *,
        catalog: RouteProfileCatalog | None = None,
        policy: RouteLiveCertificationPolicy | None = None,
    ) -> None:
        self._catalog = catalog or RouteProfileCatalog.default()
        self._policy = policy or RouteLiveCertificationPolicy()

    def build(
        self,
        *,
        output_dir: str | Path,
        release: str,
        source: str,
        sink: str,
        strategy: str,
        artifacts: Mapping[str, str | Path],
        profile: str = "real_local",
        row_count: int = 10000,
        required_evidence: Sequence[str] = (),
    ) -> RouteLiveCertificationReport:
        directory = Path(output_dir)
        directory.mkdir(parents=True, exist_ok=True)
        route = RouteKey.of(source, sink, strategy)
        route_profile = self._catalog.get(route)
        live_profile = _profile(profile)
        normalized_row_count = max(1, min(int(row_count), 10000000))
        required = _required_evidence(
            route=route,
            route_profile=route_profile,
            live_profile=live_profile,
            extra=tuple(required_evidence),
        )
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
        base_blockers = tuple() if route_profile is not None else (f"route.unsupported:{route.colon_id}",)
        decision = self._policy.evaluate(
            required_evidence=required,
            evidence=evidence,
            base_blockers=base_blockers,
        )
        report = RouteLiveCertificationReport(
            release=release,
            profile=live_profile,
            row_count=normalized_row_count,
            route=route,
            route_profile=route_profile,
            passed=decision.passed,
            level=decision.level,
            score=decision.score,
            blockers=decision.blockers,
            warnings=decision.warnings,
            next_actions=decision.next_actions,
            required_evidence=required,
            evidence=evidence,
            harness_steps=_harness_steps(
                route=route,
                release=release,
                live_profile=live_profile,
                row_count=normalized_row_count,
                output_dir=directory,
            ),
            output_dir=str(directory),
            json_path=str(directory / "route_live_certification.json"),
            markdown_path=str(directory / "route_live_certification.md"),
        )
        report.write()
        return report


def _profile(value: str) -> str:
    normalized = str(value).strip().lower().replace("-", "_")
    return normalized if normalized in LIVE_PROFILES else "real_local"


def _required_evidence(
    *,
    route: RouteKey,
    route_profile: RouteProfile | None,
    live_profile: str,
    extra: tuple[str, ...],
) -> tuple[str, ...]:
    profile_evidence = tuple(route_profile.required_evidence) if route_profile else tuple()
    return tuple(
        dict.fromkeys(
            (
                *DEFAULT_ROUTE_LIVE_EVIDENCE,
                *profile_evidence,
                *ROUTE_LIVE_REQUIRED_EVIDENCE.get(route.pair_id, ()),
                *PROFILE_REQUIRED_EVIDENCE.get(live_profile, ()),
                *extra,
            )
        )
    )


def _harness_steps(
    *,
    route: RouteKey,
    release: str,
    live_profile: str,
    row_count: int,
    output_dir: Path,
) -> tuple[RouteLiveCertificationStep, ...]:
    json_path = output_dir / "route_live_certification.json"
    release_gate_dir = output_dir / "route-release-gate"
    env = _env(live_profile=live_profile, row_count=row_count)
    test_targets = " ".join(ROUTE_LIVE_TEST_TARGETS.get(route.pair_id, ("tests/integration -q",)))
    route_artifacts = tuple(
        dict.fromkeys(
            (
                *ROUTE_LIVE_REQUIRED_EVIDENCE.get(route.pair_id, ()),
                *PROFILE_REQUIRED_EVIDENCE.get(live_profile, ()),
            )
        )
    )
    return (
        RouteLiveCertificationStep(
            name="start_docker_services",
            command="docker compose -f docker/docker-compose.integration.yml up -d " + " ".join(LOCAL_DOCKER_SERVICES),
            required=live_profile != "vendor_live",
            artifacts=("service_markers",),
            environment=env,
            services=LOCAL_DOCKER_SERVICES,
            summary="Start disposable Docker services for local live certification.",
        ),
        RouteLiveCertificationStep(
            name="run_route_live_tests",
            command=f"{' '.join(env)} uv run pytest {test_targets} -q",
            required=True,
            artifacts=route_artifacts,
            environment=env,
            services=LOCAL_DOCKER_SERVICES if live_profile != "vendor_live" else tuple(),
            summary="Run the route-specific live certification tests.",
        ),
        RouteLiveCertificationStep(
            name="build_route_live_evidence_bundle",
            command=(
                "uv run dpone ops route-live-certification "
                f"--release {release} --source {route.source} --sink {route.sink} --strategy {route.strategy} "
                f"--profile {live_profile} --row-count {row_count} --output-dir {output_dir} --format json"
            ),
            required=True,
            artifacts=("route_live_evidence_bundle",),
            environment=env,
            summary="Bundle the produced live route evidence into a stable route-scoped receipt.",
        ),
        RouteLiveCertificationStep(
            name="build_route_release_gate",
            command=(
                "uv run dpone ops route-release-gate "
                f"--release {release} --source {route.source} --sink {route.sink} --strategy {route.strategy} "
                f"--artifact route_live_evidence_bundle={json_path} --require route_live_evidence_bundle "
                f"--output-dir {release_gate_dir} --format json"
            ),
            required=True,
            artifacts=("route_release_gate",),
            environment=env,
            summary="Feed the live bundle into the final route release gate.",
        ),
        RouteLiveCertificationStep(
            name="stop_docker_services",
            command="docker compose -f docker/docker-compose.integration.yml down -v",
            required=False,
            artifacts=tuple(),
            environment=env,
            services=LOCAL_DOCKER_SERVICES,
            summary="Stop disposable local services after artifacts have been collected.",
        ),
    )


def _env(*, live_profile: str, row_count: int) -> tuple[str, ...]:
    if live_profile == "vendor_live":
        return ("DPONE_RUN_INTEGRATION_LIVE=1", f"DPONE_MATRIX_ROW_COUNT={row_count}")
    matrix_mode = "real_local" if live_profile in {"real_local", "native_transfer"} else "mock_local"
    return (
        "DPONE_RUN_INTEGRATION=1",
        "DPONE_RUN_INTEGRATION_MATRIX=1",
        f"DPONE_MATRIX_RUN_MODE={matrix_mode}",
        f"DPONE_MATRIX_ROW_COUNT={row_count}",
    )


def _read_evidence(
    *,
    name: str,
    path_value: str | Path | None,
    required: bool,
    route: RouteKey,
) -> RouteLiveCertificationEvidence:
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
    return RouteLiveCertificationEvidence(
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


def _missing(*, name: str, path: str, required: bool) -> RouteLiveCertificationEvidence:
    return RouteLiveCertificationEvidence(
        name=name,
        kind=_kind(name),
        path=path,
        required=required,
        missing=True,
        passed=not required,
        sha256="0" * 64,
        summary="required route live evidence is missing" if required else "optional route live evidence is missing",
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


__all__ = [
    "DEFAULT_ROUTE_LIVE_EVIDENCE",
    "ROUTE_LIVE_REQUIRED_EVIDENCE",
    "ROUTE_LIVE_TEST_TARGETS",
    "RouteLiveCertificationService",
]
