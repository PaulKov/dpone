"""Route-scoped release certification bundle and promotion gate."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime  # type: ignore[attr-defined]
from pathlib import Path

from dpone.ops.checksums import sha256_file
from dpone.ops.release_evidence_pack import ReleaseEvidencePackService
from dpone.ops.route_certification_matrix_claim import RouteCertificationMatrixClaimBuilder
from dpone.ops.route_release_gate import RouteReleaseGateService
from dpone.ops.routes.catalog import RouteProfileCatalog
from dpone.ops.routes.certification import RouteCertificationPackService
from dpone.ops.routes.certify_models import RouteCertificationBundleReport, RouteCertificationStage
from dpone.ops.routes.certify_policy import RouteCertificationPolicy
from dpone.ops.routes.models import RouteKey

OSS_SAFE_PROFILE = "oss_ci"
VENDOR_LIVE_PROFILE = "vendor_live"

DEFAULT_ROUTE_CERTIFY_EVIDENCE: tuple[str, ...] = (
    "route_refresh_execution",
    "route_refresh_snapshot_capture",
    "route_refresh_verification",
    "route_execution_ledger",
    "state_promotion",
    "benchmark_slo",
    "pre_release_checklist",
    "evidence_chain",
)

DEFAULT_ROUTE_CERTIFY_RELEASE_EVIDENCE: tuple[str, ...] = (
    "route_certification_pack",
    "route_readiness",
    "route_promotion_gate",
    *DEFAULT_ROUTE_CERTIFY_EVIDENCE,
)


class RouteCertificationService:
    """Build one release-ready certification bundle from immutable route evidence."""

    def __init__(
        self,
        *,
        catalog: RouteProfileCatalog | None = None,
        route_certification_pack: RouteCertificationPackService | None = None,
        route_promotion_gate: RouteReleaseGateService | None = None,
        release_evidence_pack: ReleaseEvidencePackService | None = None,
        policy: RouteCertificationPolicy | None = None,
        matrix_claim_builder: RouteCertificationMatrixClaimBuilder | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._catalog = catalog or RouteProfileCatalog.default()
        self._route_certification_pack = route_certification_pack or RouteCertificationPackService(
            catalog=self._catalog
        )
        self._route_promotion_gate = route_promotion_gate or RouteReleaseGateService(catalog=self._catalog)
        self._release_evidence_pack = release_evidence_pack or ReleaseEvidencePackService()
        self._policy = policy or RouteCertificationPolicy()
        self._matrix_claim_builder = matrix_claim_builder or RouteCertificationMatrixClaimBuilder()
        self._clock = clock or (lambda: datetime.now(UTC))

    def certify(
        self,
        *,
        output_dir: str | Path,
        release: str,
        source: str,
        sink: str,
        strategy: str,
        artifacts: Mapping[str, str | Path],
        profile: str = OSS_SAFE_PROFILE,
        required_evidence: Sequence[str] = (),
        release_set: str | Path | None = None,
    ) -> RouteCertificationBundleReport:
        directory = Path(output_dir)
        route = RouteKey.of(source, sink, strategy)
        route_profile = self._catalog.get(route)
        normalized_profile = _profile(profile)
        artifact_paths: dict[str, str | Path] = dict(artifacts)
        matrix_claim = (
            self._matrix_claim_builder.build(
                release_set_path=release_set,
                source=route.source,
                sink=route.sink,
                strategy=route.strategy,
                certified_at=self._clock(),
            )
            if release_set is not None
            else None
        )
        directory.mkdir(parents=True, exist_ok=True)

        certification_pack = self._route_certification_pack.build(
            output_dir=directory / "route-certification-pack",
            source=route.source,
            sink=route.sink,
            strategy=route.strategy,
            artifacts=artifact_paths,
        )
        for name, path in certification_pack.artifacts.items():
            artifact_paths.setdefault(name, path)
        artifact_paths.update(
            {
                "route_certification_pack": certification_pack.json_path,
                "route_readiness": certification_pack.readiness_json_path,
            }
        )

        required = _required_evidence(profile=normalized_profile, extra=tuple(required_evidence))
        promotion_gate = self._route_promotion_gate.evaluate(
            output_dir=directory / "route-promotion-gate",
            release=release,
            source=route.source,
            sink=route.sink,
            strategy=route.strategy,
            artifacts=artifact_paths,
            required_evidence=required,
        )
        artifact_paths.update(
            {
                "route_promotion_gate": promotion_gate.json_path,
                "route_release_gate": promotion_gate.json_path,
            }
        )

        release_required = _release_evidence_required(profile=normalized_profile, extra=tuple(required_evidence))
        release_pack = self._release_evidence_pack.build(
            output_dir=directory / "release-evidence-pack",
            release=release,
            profile=f"route_certification_{normalized_profile}",
            artifacts=artifact_paths,
            required=release_required,
        )
        artifact_paths["release_evidence_pack"] = release_pack.json_path

        stages = (
            _stage(
                name="route_certification_pack",
                path=Path(certification_pack.json_path),
                passed=certification_pack.passed,
                summary=f"readiness_level={certification_pack.readiness_level}",
                blockers=certification_pack.blockers,
            ),
            _stage(
                name="route_promotion_gate",
                path=Path(promotion_gate.json_path),
                passed=promotion_gate.passed,
                summary=f"level={promotion_gate.level}",
                blockers=promotion_gate.blockers,
            ),
            _stage(
                name="release_evidence_pack",
                path=Path(release_pack.json_path),
                passed=release_pack.passed,
                summary=f"profile={release_pack.profile}",
                blockers=release_pack.blockers,
            ),
        )
        decision = self._policy.evaluate(stages=stages)
        report = RouteCertificationBundleReport(
            release=release,
            profile=normalized_profile,
            route=route,
            route_profile=route_profile,
            passed=decision.passed,
            level=decision.level,
            score=decision.score,
            blockers=decision.blockers,
            warnings=decision.warnings,
            next_actions=decision.next_actions,
            required_evidence=required,
            stages=stages,
            artifact_index={name: str(path) for name, path in artifact_paths.items()},
            output_dir=str(directory),
            json_path=str(directory / "route_certification_bundle.json"),
            markdown_path=str(directory / "route_certification_bundle.md"),
            matrix_claim=matrix_claim,
        )
        report.write()
        return report


def _profile(value: str) -> str:
    normalized = str(value or OSS_SAFE_PROFILE).strip().lower().replace("-", "_")
    return normalized or OSS_SAFE_PROFILE


def _required_evidence(*, profile: str, extra: tuple[str, ...]) -> tuple[str, ...]:
    values = [*DEFAULT_ROUTE_CERTIFY_EVIDENCE, *extra]
    if profile == VENDOR_LIVE_PROFILE:
        values.append("route_live_evidence_bundle")
    return tuple(dict.fromkeys(item for item in values if item))


def _release_evidence_required(*, profile: str, extra: tuple[str, ...]) -> tuple[str, ...]:
    values = [*DEFAULT_ROUTE_CERTIFY_RELEASE_EVIDENCE, *extra]
    if profile == VENDOR_LIVE_PROFILE:
        values.append("route_live_evidence_bundle")
    return tuple(dict.fromkeys(item for item in values if item))


def _stage(
    *,
    name: str,
    path: Path,
    passed: bool,
    summary: str,
    blockers: Sequence[str] = (),
) -> RouteCertificationStage:
    return RouteCertificationStage(
        name=name,
        path=str(path),
        sha256=sha256_file(path) if path.exists() else "0" * 64,
        passed=passed,
        required=True,
        summary=summary,
        blockers=tuple(str(item) for item in blockers),
    )


__all__ = [
    "DEFAULT_ROUTE_CERTIFY_EVIDENCE",
    "DEFAULT_ROUTE_CERTIFY_RELEASE_EVIDENCE",
    "OSS_SAFE_PROFILE",
    "RouteCertificationService",
    "VENDOR_LIVE_PROFILE",
]
