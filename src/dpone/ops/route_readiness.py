"""Route-level readiness facade over matrix, evidence, and policy services."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path

from dpone.ops.routes.catalog import RouteProfileCatalog
from dpone.ops.routes.evidence import RouteEvidenceReader
from dpone.ops.routes.models import RouteEvidenceItem, RouteKey, RouteProfile, RouteReadinessReport
from dpone.ops.routes.policy import RouteReadinessPolicy


class RouteReadinessService:
    """Build source -> sink route readiness reports without executing heavy checks."""

    def __init__(
        self,
        *,
        catalog: RouteProfileCatalog | None = None,
        evidence_reader: RouteEvidenceReader | None = None,
        policy: RouteReadinessPolicy | None = None,
    ) -> None:
        self._catalog = catalog or RouteProfileCatalog.default()
        self._evidence_reader = evidence_reader or RouteEvidenceReader()
        self._policy = policy or RouteReadinessPolicy()

    def evaluate(
        self,
        *,
        output_dir: str | Path,
        source: str,
        sink: str,
        strategy: str,
        artifacts: Mapping[str, str | Path],
    ) -> RouteReadinessReport:
        directory = Path(output_dir)
        key = RouteKey.of(source, sink, strategy)
        profile = self._catalog.get(key)
        if profile is None:
            blocker = f"route.unsupported:{key.colon_id}"
            report = RouteReadinessReport(
                route=key,
                profile=None,
                passed=False,
                level="unknown",
                score=0.0,
                blockers=(blocker,),
                warnings=tuple(),
                next_actions=("Add the route to the integration matrix before requesting readiness.",),
                evidence=tuple(),
                output_dir=str(directory),
                json_path=str(directory / "route_readiness.json"),
                markdown_path=str(directory / "route_readiness.md"),
            )
            report.write()
            return report

        evidence = self._evidence(profile=profile, artifacts=artifacts)
        decision = self._policy.evaluate(profile=profile, evidence=evidence)
        report = RouteReadinessReport(
            route=key,
            profile=profile,
            passed=decision.passed,
            level=decision.level,
            score=decision.score,
            blockers=decision.blockers,
            warnings=decision.warnings,
            next_actions=decision.next_actions,
            evidence=evidence,
            output_dir=str(directory),
            json_path=str(directory / "route_readiness.json"),
            markdown_path=str(directory / "route_readiness.md"),
        )
        report.write()
        return report

    def _evidence(
        self,
        *,
        profile: RouteProfile,
        artifacts: Mapping[str, str | Path],
    ) -> tuple[RouteEvidenceItem, ...]:
        required = tuple(profile.required_evidence)
        names = tuple(dict.fromkeys((*required, *sorted(artifacts))))
        return tuple(
            self._evidence_reader.read(name=name, path=artifacts.get(name), required=name in required) for name in names
        )
