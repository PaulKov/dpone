"""Route readiness value objects and public report contracts."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict, dataclass

from dpone.contracts.connector_declarations import canonical_connector_id
from dpone.ops.routes.report_rendering import (
    report_json,
    route_readiness_markdown,
    route_reconciliation_repair_markdown,
    route_schema_evolution_markdown,
    write_report,
)

SCHEMA_VERSION = "dpone.route_readiness.v1"
ROUTE_SCHEMA_EVOLUTION_SCHEMA_VERSION = "dpone.route_schema_evolution.v1"
ROUTE_RECONCILIATION_REPAIR_SCHEMA_VERSION = "dpone.route_reconciliation_repair.v1"


@dataclass(frozen=True, slots=True)
class RouteKey:
    """Canonical identity for one source -> sink -> strategy route."""

    source: str
    sink: str
    strategy: str

    @classmethod
    def of(cls, source: str, sink: str, strategy: str) -> RouteKey:
        return cls(_normalize(source), _normalize(sink), _normalize(strategy))

    @property
    def pair_id(self) -> str:
        return f"{self.source}_to_{self.sink}"

    @property
    def case_id(self) -> str:
        return f"{self.pair_id}__{self.strategy}"

    @property
    def colon_id(self) -> str:
        return f"{self.source}:{self.sink}:{self.strategy}"

    def to_dict(self) -> dict[str, str]:
        return {
            "source": self.source,
            "sink": self.sink,
            "strategy": self.strategy,
            "pair_id": self.pair_id,
            "case_id": self.case_id,
            "colon_id": self.colon_id,
        }


@dataclass(frozen=True, slots=True)
class RouteProfile:
    """Static route metadata assembled from matrix and strategy catalogs."""

    key: RouteKey
    docs_link: str
    install_extras: tuple[str, ...]
    required_profiles: tuple[str, ...]
    live_profiles: tuple[str, ...]
    local_service_supported: bool
    external_credentials_required: bool
    certification_status: str
    native_fast_path: str
    required_evidence: tuple[str, ...]
    default_slo_hints: Mapping[str, object]

    def to_dict(self) -> dict[str, object]:
        return {
            "key": self.key.to_dict(),
            "docs_link": self.docs_link,
            "install_extras": list(self.install_extras),
            "required_profiles": list(self.required_profiles),
            "live_profiles": list(self.live_profiles),
            "local_service_supported": self.local_service_supported,
            "external_credentials_required": self.external_credentials_required,
            "certification_status": self.certification_status,
            "native_fast_path": self.native_fast_path,
            "required_evidence": list(self.required_evidence),
            "default_slo_hints": dict(self.default_slo_hints),
        }


@dataclass(frozen=True, slots=True)
class RouteEvidenceItem:
    """Normalized evidence artifact for route-level readiness decisions."""

    name: str
    kind: str
    path: str
    required: bool
    passed: bool
    sha256: str
    summary: str
    blockers: tuple[str, ...]
    missing: bool = False
    evidence_status: str | None = None

    def to_dict(self) -> dict[str, object]:
        payload = asdict(self)
        payload["blockers"] = list(self.blockers)
        if self.evidence_status is None:
            payload.pop("evidence_status")
        return payload


@dataclass(frozen=True, slots=True)
class RouteReadinessDecision:
    """Pure policy decision before report file paths are attached."""

    passed: bool
    level: str
    score: float
    blockers: tuple[str, ...]
    warnings: tuple[str, ...]
    next_actions: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "passed": self.passed,
            "level": self.level,
            "score": self.score,
            "blockers": list(self.blockers),
            "warnings": list(self.warnings),
            "next_actions": list(self.next_actions),
        }


@dataclass(frozen=True, slots=True)
class RouteReadinessReport:
    """Stable JSON/Markdown contract for route readiness CLI and CI gates."""

    route: RouteKey
    profile: RouteProfile | None
    passed: bool
    level: str
    score: float
    blockers: tuple[str, ...]
    warnings: tuple[str, ...]
    next_actions: tuple[str, ...]
    evidence: tuple[RouteEvidenceItem, ...]
    output_dir: str
    json_path: str
    markdown_path: str

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": SCHEMA_VERSION,
            "route": self.route.to_dict(),
            "profile": self.profile.to_dict() if self.profile else None,
            "passed": self.passed,
            "level": self.level,
            "score": self.score,
            "blockers": list(self.blockers),
            "warnings": list(self.warnings),
            "next_actions": list(self.next_actions),
            "evidence": [item.to_dict() for item in self.evidence],
            "output_dir": self.output_dir,
            "json_path": self.json_path,
            "markdown_path": self.markdown_path,
        }

    def to_json(self) -> str:
        return report_json(self)

    def to_markdown(self) -> str:
        return route_readiness_markdown(self)

    def write(self) -> None:
        write_report(self)


@dataclass(frozen=True, slots=True)
class RouteSchemaApplyDecision:
    """Route-level target DDL/apply decision for one schema evolution artifact."""

    mode: str
    safe_to_apply: bool
    requires_approval: bool
    ddl_preview: str
    reason: str

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class RouteSchemaEvolutionReport:
    """Stable JSON/Markdown contract for route-level schema evolution gates."""

    route: RouteKey
    profile: RouteProfile | None
    passed: bool
    level: str
    blockers: tuple[str, ...]
    warnings: tuple[str, ...]
    next_actions: tuple[str, ...]
    change: Mapping[str, object]
    plan: Mapping[str, object]
    apply_decision: RouteSchemaApplyDecision
    upstream_artifacts: Mapping[str, str]
    output_dir: str
    json_path: str
    markdown_path: str

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": ROUTE_SCHEMA_EVOLUTION_SCHEMA_VERSION,
            "route": self.route.to_dict(),
            "profile": self.profile.to_dict() if self.profile else None,
            "passed": self.passed,
            "level": self.level,
            "blockers": list(self.blockers),
            "warnings": list(self.warnings),
            "next_actions": list(self.next_actions),
            "change": dict(self.change),
            "plan": dict(self.plan),
            "apply_decision": self.apply_decision.to_dict(),
            "upstream_artifacts": dict(self.upstream_artifacts),
            "output_dir": self.output_dir,
            "json_path": self.json_path,
            "markdown_path": self.markdown_path,
        }

    def to_json(self) -> str:
        return report_json(self)

    def to_markdown(self) -> str:
        return route_schema_evolution_markdown(self)

    def write(self) -> None:
        write_report(self)


@dataclass(frozen=True, slots=True)
class RouteRepairAction:
    """Route-level repair action derived from reconciliation evidence."""

    action: str
    key: Mapping[str, object]
    reason: str
    source_action: str

    def to_dict(self) -> dict[str, object]:
        return {
            "action": self.action,
            "key": dict(self.key),
            "reason": self.reason,
            "source_action": self.source_action,
        }


@dataclass(frozen=True, slots=True)
class RouteRepairPlan:
    """Bounded route repair plan for one reconciliation window."""

    source_boundary: str
    target_boundary: str
    actions: tuple[RouteRepairAction, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "source_boundary": self.source_boundary,
            "target_boundary": self.target_boundary,
            "actions": [item.to_dict() for item in self.actions],
        }


@dataclass(frozen=True, slots=True)
class RouteReconciliationRepairReport:
    """Stable JSON/Markdown contract for route reconciliation repair gates."""

    route: RouteKey
    profile: RouteProfile | None
    passed: bool
    level: str
    blockers: tuple[str, ...]
    warnings: tuple[str, ...]
    next_actions: tuple[str, ...]
    repair_plan: RouteRepairPlan
    reconciliation: Mapping[str, object]
    output_dir: str
    json_path: str
    markdown_path: str

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": ROUTE_RECONCILIATION_REPAIR_SCHEMA_VERSION,
            "route": self.route.to_dict(),
            "profile": self.profile.to_dict() if self.profile else None,
            "passed": self.passed,
            "level": self.level,
            "blockers": list(self.blockers),
            "warnings": list(self.warnings),
            "next_actions": list(self.next_actions),
            "repair_plan": self.repair_plan.to_dict(),
            "reconciliation": dict(self.reconciliation),
            "output_dir": self.output_dir,
            "json_path": self.json_path,
            "markdown_path": self.markdown_path,
        }

    def to_json(self) -> str:
        return report_json(self)

    def to_markdown(self) -> str:
        return route_reconciliation_repair_markdown(self)

    def write(self) -> None:
        write_report(self)


def _normalize(value: str) -> str:
    return canonical_connector_id(value)
