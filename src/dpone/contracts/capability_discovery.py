"""Immutable public DTOs for self-service capability discovery."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Protocol

from dpone.contracts.airflow_deployment import canonical_fingerprint
from dpone.contracts.connector_declarations import (
    ConnectorDeclaration,
    built_in_connector_declarations,
    canonical_connector_id,
)
from dpone.contracts.route_attestation import parse_aware_datetime

CAPABILITY_DISCOVERY_SCHEMA = "dpone.capability-discovery.v1"
EVIDENCE_STATUSES = frozenset({"PASS", "FAIL", "SKIP", "UNVERIFIED"})
ROUTE_CERTIFICATION_LEVELS = (
    "experimental",
    "route-certified",
    "production-certified",
    "enterprise-certified",
)


@dataclass(frozen=True, slots=True)
class CapabilityIssue:
    """Stable non-secret issue produced while building the discovery projection."""

    code: str
    entity_kind: str
    entity_id: str
    message: str

    def to_dict(self) -> dict[str, str]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class RouteSupport:
    status: str
    limitations: tuple[str, ...]
    docs_link: str
    install_extras: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "limitations": list(self.limitations),
            "docs_link": self.docs_link,
            "install_extras": list(self.install_extras),
        }


@dataclass(frozen=True, slots=True)
class RouteCertificationVariant:
    """Evidence-backed certification for one complete six-dimensional route."""

    id: str
    route_id: str
    transport: str
    schema_evolution: str
    airflow_runtime_mode: str
    level: str
    evidence_status: str
    evidence_refs: tuple[str, ...]
    reason_codes: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "route_id": self.route_id,
            "transport": self.transport,
            "schema_evolution": self.schema_evolution,
            "airflow_runtime_mode": self.airflow_runtime_mode,
            "level": self.level,
            "evidence_status": self.evidence_status,
            "evidence_refs": list(self.evidence_refs),
            "reason_codes": list(self.reason_codes),
        }


@dataclass(frozen=True, slots=True)
class RouteCertificationCandidate:
    """Canonical matrix candidate used to revalidate one proof directory."""

    certification_id: str
    source: str
    sink: str
    strategy: str
    transport: str
    schema_evolution: str
    airflow_runtime_mode: str
    sampling_mode: str

    @property
    def key(self) -> tuple[str, str, str]:
        return (self.source, self.sink, self.strategy)


class RouteCertificationCandidateProtocol(Protocol):
    @property
    def certification_id(self) -> str: ...

    @property
    def source(self) -> str: ...

    @property
    def sink(self) -> str: ...

    @property
    def strategy(self) -> str: ...

    @property
    def transport(self) -> str: ...

    @property
    def schema_evolution(self) -> str: ...

    @property
    def airflow_runtime_mode(self) -> str: ...

    @property
    def sampling_mode(self) -> str: ...

    @property
    def key(self) -> tuple[str, str, str]: ...


@dataclass(frozen=True, slots=True)
class RouteCertification:
    level: str
    evidence_status: str
    reason_codes: tuple[str, ...]
    variants: tuple[RouteCertificationVariant, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "level": self.level,
            "evidence_status": self.evidence_status,
            "reason_codes": list(self.reason_codes),
            "variants": [item.to_dict() for item in self.variants],
        }


@dataclass(frozen=True, slots=True)
class BeginnerRouteSupport:
    recipe_available: bool
    recipe_refs: tuple[str, ...]
    default_recipe_ref: str | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "recipe_available": self.recipe_available,
            "recipe_refs": list(self.recipe_refs),
            "default_recipe_ref": self.default_recipe_ref,
        }


@dataclass(frozen=True, slots=True)
class RouteCapability:
    id: str
    source: str
    sink: str
    strategy: str
    support: RouteSupport
    certification: RouteCertification
    beginner: BeginnerRouteSupport

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "source": self.source,
            "sink": self.sink,
            "strategy": self.strategy,
            "support": self.support.to_dict(),
            "certification": self.certification.to_dict(),
            "beginner": self.beginner.to_dict(),
        }


@dataclass(frozen=True, slots=True)
class RecipeDiscoveryEntry:
    """Authority-neutral recipe input injected into the capability projector."""

    ref: str
    origin: str
    status: str
    route_id: str | None
    scaffoldable: bool
    default_for_route: bool = False
    reason_codes: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class RecipeCapability:
    ref: str
    origin: str
    status: str
    source: str | None
    sink: str | None
    strategy: str | None
    route_id: str | None
    support_status: str
    certification_level: str
    evidence_status: str
    install_extras: tuple[str, ...]
    limitations: tuple[str, ...]
    scaffoldable: bool
    reason_codes: tuple[str, ...]
    scaffold_argv: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["install_extras"] = list(self.install_extras)
        payload["limitations"] = list(self.limitations)
        payload["reason_codes"] = list(self.reason_codes)
        payload["scaffold_argv"] = list(self.scaffold_argv)
        return payload


@dataclass(frozen=True, slots=True)
class CapabilityDiscoverySnapshot:
    connectors: tuple[ConnectorDeclaration, ...]
    routes: tuple[RouteCapability, ...]
    recipes: tuple[RecipeCapability, ...]
    issues: tuple[CapabilityIssue, ...] = ()

    @property
    def snapshot_id(self) -> str:
        return canonical_fingerprint(self._semantic_payload())

    def _semantic_payload(self) -> dict[str, Any]:
        return {
            "schema": CAPABILITY_DISCOVERY_SCHEMA,
            "connectors": [item.to_dict() for item in self.connectors],
            "routes": [item.to_dict() for item in self.routes],
            "recipes": [item.to_dict() for item in self.recipes],
            "issues": [item.to_dict() for item in self.issues],
        }

    def to_dict(self) -> dict[str, Any]:
        return {
            **self._semantic_payload(),
            "snapshot_id": self.snapshot_id,
        }


__all__ = [
    "CAPABILITY_DISCOVERY_SCHEMA",
    "EVIDENCE_STATUSES",
    "ROUTE_CERTIFICATION_LEVELS",
    "BeginnerRouteSupport",
    "CapabilityDiscoverySnapshot",
    "CapabilityIssue",
    "ConnectorDeclaration",
    "RecipeCapability",
    "RecipeDiscoveryEntry",
    "RouteCapability",
    "RouteCertification",
    "RouteCertificationCandidate",
    "RouteCertificationCandidateProtocol",
    "RouteCertificationVariant",
    "RouteSupport",
    "built_in_connector_declarations",
    "canonical_connector_id",
    "parse_aware_datetime",
]
