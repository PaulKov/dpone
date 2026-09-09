"""Route release candidate orchestration over existing route evidence services."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path

from dpone.ops.checksums import sha256_file
from dpone.ops.release_evidence_pack import ReleaseEvidencePackService
from dpone.ops.route_live_certification import RouteLiveCertificationService
from dpone.ops.route_release_gate import RouteReleaseGateService
from dpone.ops.routes.catalog import RouteProfileCatalog
from dpone.ops.routes.certification import RouteCertificationPackService
from dpone.ops.routes.models import RouteKey
from dpone.ops.routes.rc_orchestrator_models import RouteRcOrchestrationReport, RouteRcOrchestrationStep
from dpone.ops.routes.rc_orchestrator_policy import RouteRcOrchestrationPolicy

DEFAULT_ROUTE_RC_RELEASE_EVIDENCE: tuple[str, ...] = (
    "service_markers",
    "route_readiness",
    "route_certification_pack",
    "route_execution_ledger",
    "route_refresh_verification",
    "state_promotion",
    "route_live_evidence_bundle",
    "route_release_gate",
    "performance_certification",
    "live_state_reconciliation",
    "pre_release_checklist",
    "evidence_chain",
)


class RouteReleaseCandidateOrchestratorService:
    """Compose route release candidate evidence without running live systems."""

    def __init__(
        self,
        *,
        catalog: RouteProfileCatalog | None = None,
        route_certification_pack: RouteCertificationPackService | None = None,
        route_live_certification: RouteLiveCertificationService | None = None,
        route_release_gate: RouteReleaseGateService | None = None,
        release_evidence_pack: ReleaseEvidencePackService | None = None,
        policy: RouteRcOrchestrationPolicy | None = None,
    ) -> None:
        self._catalog = catalog or RouteProfileCatalog.default()
        self._route_certification_pack = route_certification_pack or RouteCertificationPackService()
        self._route_live_certification = route_live_certification or RouteLiveCertificationService(
            catalog=self._catalog
        )
        self._route_release_gate = route_release_gate or RouteReleaseGateService(catalog=self._catalog)
        self._release_evidence_pack = release_evidence_pack or ReleaseEvidencePackService()
        self._policy = policy or RouteRcOrchestrationPolicy()

    def run(
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
    ) -> RouteRcOrchestrationReport:
        directory = Path(output_dir)
        directory.mkdir(parents=True, exist_ok=True)
        route = RouteKey.of(source, sink, strategy)
        route_profile = self._catalog.get(route)
        normalized_row_count = max(1, min(int(row_count), 10000000))
        artifact_paths = dict(artifacts)

        certification_pack = self._route_certification_pack.build(
            output_dir=directory / "route-certification-pack",
            source=route.source,
            sink=route.sink,
            strategy=route.strategy,
            artifacts=artifact_paths,
        )
        artifact_paths.update(
            {
                "route_readiness": certification_pack.readiness_json_path,
                "route_certification_pack": certification_pack.json_path,
            }
        )

        live_bundle = self._route_live_certification.build(
            output_dir=directory / "route-live-certification",
            release=release,
            source=route.source,
            sink=route.sink,
            strategy=route.strategy,
            profile=profile,
            row_count=normalized_row_count,
            artifacts=artifact_paths,
        )
        artifact_paths["route_live_evidence_bundle"] = live_bundle.json_path

        release_gate = self._route_release_gate.evaluate(
            output_dir=directory / "route-release-gate",
            release=release,
            source=route.source,
            sink=route.sink,
            strategy=route.strategy,
            artifacts=artifact_paths,
            required_evidence=("route_live_evidence_bundle",),
        )
        artifact_paths["route_release_gate"] = release_gate.json_path

        release_evidence_pack = self._release_evidence_pack.build(
            output_dir=directory / "release-evidence-pack",
            release=release,
            profile="route_release_candidate",
            artifacts=artifact_paths,
            required=tuple(dict.fromkeys((*DEFAULT_ROUTE_RC_RELEASE_EVIDENCE, *required_evidence))),
        )
        artifact_paths["release_evidence_pack"] = release_evidence_pack.json_path

        steps = (
            _step(
                name="route_certification_pack",
                command=_route_certification_pack_command(route=route, output_dir=directory),
                path=Path(certification_pack.json_path),
                passed=certification_pack.passed,
                summary=f"readiness_level={certification_pack.readiness_level}",
                blockers=certification_pack.blockers,
            ),
            _step(
                name="route_live_certification",
                command=_route_live_certification_command(
                    route=route,
                    release=release,
                    profile=profile,
                    row_count=normalized_row_count,
                    output_dir=directory,
                ),
                path=Path(live_bundle.json_path),
                passed=live_bundle.passed,
                summary=f"level={live_bundle.level}",
                blockers=live_bundle.blockers,
            ),
            _step(
                name="route_release_gate",
                command=_route_release_gate_command(route=route, release=release, output_dir=directory),
                path=Path(release_gate.json_path),
                passed=release_gate.passed,
                summary=f"level={release_gate.level}",
                blockers=release_gate.blockers,
            ),
            _step(
                name="release_evidence_pack",
                command=_release_evidence_pack_command(release=release, output_dir=directory),
                path=Path(release_evidence_pack.json_path),
                passed=release_evidence_pack.passed,
                summary=f"profile={release_evidence_pack.profile}",
                blockers=release_evidence_pack.blockers,
            ),
        )
        decision = self._policy.evaluate(steps=steps)
        report = RouteRcOrchestrationReport(
            release=release,
            profile=profile,
            row_count=normalized_row_count,
            route=route,
            route_profile=route_profile,
            passed=decision.passed,
            level=decision.level,
            score=decision.score,
            blockers=decision.blockers,
            warnings=decision.warnings,
            next_actions=decision.next_actions,
            steps=steps,
            artifact_index={name: str(path) for name, path in artifact_paths.items()},
            output_dir=str(directory),
            json_path=str(directory / "route_rc_orchestration.json"),
            markdown_path=str(directory / "route_rc_orchestration.md"),
        )
        report.write()
        return report


def _step(
    *,
    name: str,
    command: str,
    path: Path,
    passed: bool,
    summary: str,
    blockers: Sequence[str] = (),
) -> RouteRcOrchestrationStep:
    return RouteRcOrchestrationStep(
        name=name,
        command=command,
        path=str(path),
        sha256=sha256_file(path) if path.exists() else "0" * 64,
        passed=passed,
        required=True,
        summary=summary,
        blockers=tuple(str(item) for item in blockers),
    )


def _route_certification_pack_command(*, route: RouteKey, output_dir: Path) -> str:
    return (
        "uv run dpone ops route-certification-pack "
        f"--source {route.source} --sink {route.sink} --strategy {route.strategy} "
        f"--output-dir {output_dir / 'route-certification-pack'} --format json"
    )


def _route_live_certification_command(
    *,
    route: RouteKey,
    release: str,
    profile: str,
    row_count: int,
    output_dir: Path,
) -> str:
    return (
        "uv run dpone ops route-live-certification "
        f"--release {release} --source {route.source} --sink {route.sink} --strategy {route.strategy} "
        f"--profile {profile} --row-count {row_count} "
        f"--output-dir {output_dir / 'route-live-certification'} --format json"
    )


def _route_release_gate_command(*, route: RouteKey, release: str, output_dir: Path) -> str:
    return (
        "uv run dpone ops route-release-gate "
        f"--release {release} --source {route.source} --sink {route.sink} --strategy {route.strategy} "
        "--artifact route_live_evidence_bundle=<route_live_certification.json> "
        f"--require route_live_evidence_bundle --output-dir {output_dir / 'route-release-gate'} --format json"
    )


def _release_evidence_pack_command(*, release: str, output_dir: Path) -> str:
    return (
        "uv run dpone ops release-evidence-pack "
        f"--release {release} --profile route_release_candidate "
        "--artifact route_release_gate=<route_release_gate.json> "
        "--artifact route_live_evidence_bundle=<route_live_certification.json> "
        f"--output-dir {output_dir / 'release-evidence-pack'} --format json"
    )


__all__ = ["DEFAULT_ROUTE_RC_RELEASE_EVIDENCE", "RouteReleaseCandidateOrchestratorService"]
