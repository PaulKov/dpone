"""Connector-neutral route capability taxonomy and planner."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from enum import Enum

SCHEMA_VERSION = "dpone.runtime.route_capabilities.v1"
SUPPORTED_MODES = {"auto", "required", "warn_only"}


class RuntimeCapability(str, Enum):  # noqa: UP042 - mypy target lacks enum.StrEnum support in this repo.
    SOURCE_READ = "source_read"
    SOURCE_SNAPSHOT = "source_snapshot"
    INTERMEDIATE_STORAGE = "intermediate_storage"
    FILE_FORMAT = "file_format"
    TRANSPORT = "transport"
    SINK_STAGE = "sink_stage"
    SINK_FINALIZE = "sink_finalize"
    AUTH = "auth"
    CLUSTER = "cluster"
    GOVERNANCE = "governance"


@dataclass(frozen=True, slots=True)
class CapabilityRequirement:
    id: str
    domain: RuntimeCapability
    required_for: str
    severity: str = "error"
    probe: str | None = None
    blocker_code: str | None = None
    alternatives: tuple[str, ...] = ()

    def blocker(self) -> str:
        return self.blocker_code or f"{self.id}.unsupported"


class CapabilityEvidence:
    def __init__(
        self,
        *,
        requirement_id: str,
        domain: RuntimeCapability,
        passed: bool,
        connector_id: str | None = None,
        connector_version: str | None = None,
        server_version: str | None = None,
        checks: Sequence[str] = (),
        warnings: Sequence[str] = (),
        blockers: Sequence[str] = (),
        alternatives: Sequence[str] = (),
        details: Mapping[str, object] | None = None,
    ) -> None:
        self.requirement_id = requirement_id
        self.domain = domain
        self.passed = passed
        self.connector_id = connector_id
        self.connector_version = connector_version
        self.server_version = server_version
        self.checks = tuple(checks)
        self.warnings = tuple(warnings)
        self.blockers = tuple(blockers)
        self.alternatives = tuple(alternatives)
        self.details = dict(details or {})

    @classmethod
    def success(
        cls,
        *,
        requirement_id: str,
        domain: RuntimeCapability,
        checks: Sequence[str] = (),
        connector_id: str | None = None,
        connector_version: str | None = None,
        server_version: str | None = None,
        warnings: Sequence[str] = (),
        alternatives: Sequence[str] = (),
        details: Mapping[str, object] | None = None,
    ) -> CapabilityEvidence:
        return cls(
            requirement_id=requirement_id,
            domain=domain,
            passed=True,
            connector_id=connector_id,
            connector_version=connector_version,
            server_version=server_version,
            checks=checks,
            warnings=warnings,
            alternatives=alternatives,
            details=details,
        )

    @classmethod
    def failure(
        cls,
        *,
        requirement_id: str,
        domain: RuntimeCapability,
        blockers: Sequence[str],
        connector_id: str | None = None,
        connector_version: str | None = None,
        server_version: str | None = None,
        checks: Sequence[str] = (),
        warnings: Sequence[str] = (),
        alternatives: Sequence[str] = (),
        details: Mapping[str, object] | None = None,
    ) -> CapabilityEvidence:
        return cls(
            requirement_id=requirement_id,
            domain=domain,
            passed=False,
            connector_id=connector_id,
            connector_version=connector_version,
            server_version=server_version,
            checks=checks,
            warnings=warnings,
            blockers=blockers,
            alternatives=alternatives,
            details=details,
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "requirement_id": self.requirement_id,
            "domain": self.domain.value,
            "passed": self.passed,
            "connector_id": self.connector_id,
            "connector_version": self.connector_version,
            "server_version": self.server_version,
            "checks": list(self.checks),
            "warnings": list(self.warnings),
            "blockers": list(self.blockers),
            "alternatives": list(self.alternatives),
            "details": dict(self.details),
        }


@dataclass(frozen=True, slots=True)
class RouteCandidate:
    route_id: str
    requirements: tuple[CapabilityRequirement, ...]
    priority: int
    expected_performance_class: str = "standard"
    fallback_priority: int | None = None


@dataclass(frozen=True, slots=True)
class RouteCapabilityDecision:
    selected_route_id: str
    requested_mode: str
    requested_route_id: str | None
    should_start_source_io: bool
    rejected_routes: tuple[dict[str, object], ...] = ()
    fallback_reason: str | None = None
    warnings: tuple[str, ...] = ()
    blockers: tuple[str, ...] = ()
    recommendations: tuple[str, ...] = ()
    evidence: tuple[CapabilityEvidence, ...] = ()
    details: dict[str, object] = field(default_factory=dict)

    def to_evidence(self) -> dict[str, object]:
        return {
            "schema_version": SCHEMA_VERSION,
            "decision_id": "runtime.route_capabilities",
            "requested_mode": self.requested_mode,
            "requested_route_id": self.requested_route_id,
            "selected_route_id": self.selected_route_id,
            "should_start_source_io": self.should_start_source_io,
            "fallback_reason": self.fallback_reason,
            "warnings": list(self.warnings),
            "blockers": list(self.blockers),
            "recommendations": list(self.recommendations),
            "rejected_routes": list(self.rejected_routes),
            "evidence": [item.to_dict() for item in self.evidence],
            "details": dict(self.details),
        }


class RouteAlternativeAdvisor:
    _DEFAULTS = {
        "storage.runtime_put_denied": "Fix the runtime object-storage writer connection permissions.",
        "sink.auth.named_collection_missing": (
            "Create the required named collection or allow a non-production credential mode explicitly."
        ),
        "sink.cluster_pull_unsupported": "Use single-node object-storage pull or a streaming fallback.",
        "format.parquet_read_unsupported": "Use typed binary/native streaming until Parquet pull is supported.",
        "source.columnar_writer_missing": "Install dpone[columnar] or use a streaming fallback.",
    }

    def recommend(self, blockers: Sequence[str]) -> tuple[str, ...]:
        recommendations = [self._DEFAULTS[blocker] for blocker in blockers if blocker in self._DEFAULTS]
        return tuple(dict.fromkeys(recommendations))


CapabilityProbe = Callable[[CapabilityRequirement, Mapping[str, object]], CapabilityEvidence]


class CapabilityProbeRegistry:
    def __init__(self) -> None:
        self._by_requirement: dict[str, CapabilityProbe] = {}
        self._by_domain: dict[RuntimeCapability, CapabilityProbe] = {}

    def register(self, requirement_id: str, probe: CapabilityProbe) -> None:
        self._by_requirement[requirement_id] = probe

    def register_domain(self, domain: RuntimeCapability, probe: CapabilityProbe) -> None:
        self._by_domain[domain] = probe

    def probe(self, requirement: CapabilityRequirement, *, context: Mapping[str, object]) -> CapabilityEvidence:
        probe = self._by_requirement.get(requirement.id) or self._by_domain.get(requirement.domain)
        if probe is None:
            return CapabilityEvidence.failure(
                requirement_id=requirement.id,
                domain=requirement.domain,
                blockers=(requirement.blocker(),),
            )
        return probe(requirement, context)

    def probe_all(
        self,
        requirements: Sequence[CapabilityRequirement],
        *,
        context: Mapping[str, object],
    ) -> dict[str, CapabilityEvidence]:
        return {requirement.id: self.probe(requirement, context=context) for requirement in requirements}


class CapabilityEvidencePublisher:
    def __init__(self) -> None:
        self.records: list[dict[str, object]] = []

    def publish(self, decision: RouteCapabilityDecision) -> None:
        self.records.append(decision.to_evidence())


class RouteCapabilityPlanner:
    def __init__(self, *, advisor: RouteAlternativeAdvisor | None = None) -> None:
        self._advisor = advisor or RouteAlternativeAdvisor()

    def decide(
        self,
        *,
        candidates: Sequence[RouteCandidate],
        evidence: Mapping[str, CapabilityEvidence],
        mode: str = "auto",
        requested_route_id: str | None = None,
    ) -> RouteCapabilityDecision:
        normalized_mode = mode.strip().lower()
        if normalized_mode not in SUPPORTED_MODES:
            raise ValueError(f"Unsupported runtime.capabilities.mode: {mode}")
        ordered = sorted(candidates, key=lambda item: (item.priority, item.fallback_priority or item.priority))
        evaluated = [_evaluate_candidate(candidate, evidence) for candidate in ordered]
        requested = _find_requested(evaluated, requested_route_id)

        if normalized_mode == "warn_only":
            target = requested or (evaluated[0] if evaluated else None)
            if target is None:
                return RouteCapabilityDecision(
                    "blocked", normalized_mode, requested_route_id, False, blockers=("route_missing",)
                )
            return _warn_only_decision(target, normalized_mode, requested_route_id)

        if normalized_mode == "required":
            target = requested or (evaluated[0] if evaluated else None)
            return self._required_decision(target, normalized_mode, requested_route_id)

        selected = next((item for item in evaluated if not item.blockers), None)
        if selected is None:
            all_blockers = _unique(blocker for item in evaluated for blocker in item.blockers)
            return RouteCapabilityDecision(
                selected_route_id="blocked",
                requested_mode=normalized_mode,
                requested_route_id=requested_route_id,
                should_start_source_io=False,
                rejected_routes=tuple(item.rejected_payload() for item in evaluated),
                blockers=all_blockers,
                recommendations=self._advisor.recommend(all_blockers),
                evidence=tuple(_evidence_items(evaluated)),
            )
        rejected_before_selected = tuple(
            item.rejected_payload() for item in evaluated if item.route_id != selected.route_id
        )
        fallback_reason = _first_rejection_reason(evaluated, selected.route_id)
        return RouteCapabilityDecision(
            selected_route_id=selected.route_id,
            requested_mode=normalized_mode,
            requested_route_id=requested_route_id,
            should_start_source_io=True,
            rejected_routes=rejected_before_selected,
            fallback_reason=fallback_reason,
            recommendations=self._advisor.recommend(
                _unique(blocker for item in evaluated for blocker in item.blockers)
            ),
            evidence=tuple(_evidence_items(evaluated)),
        )

    def _required_decision(
        self,
        target: _EvaluatedRoute | None,
        mode: str,
        requested_route_id: str | None,
    ) -> RouteCapabilityDecision:
        if target is None:
            return RouteCapabilityDecision("blocked", mode, requested_route_id, False, blockers=("route_missing",))
        if target.blockers:
            return RouteCapabilityDecision(
                selected_route_id="blocked",
                requested_mode=mode,
                requested_route_id=requested_route_id or target.route_id,
                should_start_source_io=False,
                rejected_routes=(target.rejected_payload(),),
                blockers=target.blockers,
                recommendations=self._advisor.recommend(target.blockers),
                evidence=target.evidence,
            )
        return RouteCapabilityDecision(
            selected_route_id=target.route_id,
            requested_mode=mode,
            requested_route_id=requested_route_id or target.route_id,
            should_start_source_io=True,
            evidence=target.evidence,
        )


@dataclass(frozen=True, slots=True)
class _EvaluatedRoute:
    route_id: str
    blockers: tuple[str, ...]
    warnings: tuple[str, ...]
    evidence: tuple[CapabilityEvidence, ...]

    def rejected_payload(self) -> dict[str, object]:
        return {
            "route_id": self.route_id,
            "blockers": list(self.blockers),
            "warnings": list(self.warnings),
        }


def _evaluate_candidate(candidate: RouteCandidate, evidence: Mapping[str, CapabilityEvidence]) -> _EvaluatedRoute:
    items: list[CapabilityEvidence] = []
    blockers: list[str] = []
    warnings: list[str] = []
    for requirement in candidate.requirements:
        item = evidence.get(requirement.id)
        if item is None:
            item = CapabilityEvidence.failure(
                requirement_id=requirement.id,
                domain=requirement.domain,
                blockers=(requirement.blocker(),),
            )
        items.append(item)
        warnings.extend(item.warnings)
        if not item.passed and requirement.severity == "error":
            blockers.extend(item.blockers or (requirement.blocker(),))
    return _EvaluatedRoute(candidate.route_id, _unique(blockers), _unique(warnings), tuple(items))


def _warn_only_decision(
    target: _EvaluatedRoute,
    mode: str,
    requested_route_id: str | None,
) -> RouteCapabilityDecision:
    warnings = _unique((*target.warnings, *target.blockers))
    return RouteCapabilityDecision(
        selected_route_id=target.route_id,
        requested_mode=mode,
        requested_route_id=requested_route_id or target.route_id,
        should_start_source_io=True,
        warnings=warnings,
        evidence=target.evidence,
    )


def _find_requested(evaluated: Sequence[_EvaluatedRoute], requested_route_id: str | None) -> _EvaluatedRoute | None:
    if requested_route_id is None:
        return None
    return next((item for item in evaluated if item.route_id == requested_route_id), None)


def _first_rejection_reason(evaluated: Sequence[_EvaluatedRoute], selected_route_id: str) -> str | None:
    for item in evaluated:
        if item.route_id == selected_route_id:
            return None
        if item.blockers:
            return item.blockers[0]
    return None


def _evidence_items(evaluated: Sequence[_EvaluatedRoute]) -> list[CapabilityEvidence]:
    return [evidence for item in evaluated for evidence in item.evidence]


def _unique(values: Iterable[str]) -> tuple[str, ...]:
    return tuple(dict.fromkeys(str(value) for value in values))
