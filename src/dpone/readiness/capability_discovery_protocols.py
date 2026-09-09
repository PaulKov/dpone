"""Narrow structural inputs for the capability readiness projector."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, Protocol


class RouteKeyProtocol(Protocol):
    @property
    def colon_id(self) -> str: ...

    @property
    def source(self) -> str: ...

    @property
    def sink(self) -> str: ...

    @property
    def strategy(self) -> str: ...


class RouteProfileProtocol(Protocol):
    @property
    def key(self) -> RouteKeyProtocol: ...

    @property
    def certification_status(self) -> str: ...

    @property
    def docs_link(self) -> str: ...

    @property
    def install_extras(self) -> tuple[str, ...]: ...


class ConnectorCapabilityProtocol(Protocol):
    @property
    def id(self) -> str: ...

    @property
    def roles(self) -> tuple[str, ...]: ...

    @property
    def maturity(self) -> str: ...

    @property
    def release_phase(self) -> str: ...

    @property
    def docs_link(self) -> str: ...

    @property
    def capability_ids(self) -> tuple[str, ...]: ...

    def to_dict(self) -> dict[str, Any]: ...


class RouteSupportProtocol(Protocol):
    @property
    def status(self) -> str: ...

    def to_dict(self) -> dict[str, Any]: ...


class RouteCertificationProtocol(Protocol):
    @property
    def level(self) -> str: ...

    @property
    def evidence_status(self) -> str: ...

    @property
    def reason_codes(self) -> tuple[str, ...]: ...

    @property
    def variants(self) -> Sequence[RouteCertificationVariantProtocol]: ...

    def to_dict(self) -> dict[str, Any]: ...


class RouteCertificationVariantProtocol(Protocol):
    @property
    def id(self) -> str: ...

    @property
    def route_id(self) -> str: ...

    @property
    def transport(self) -> str: ...

    @property
    def schema_evolution(self) -> str: ...

    @property
    def airflow_runtime_mode(self) -> str: ...

    @property
    def level(self) -> str: ...

    @property
    def evidence_status(self) -> str: ...

    @property
    def evidence_refs(self) -> tuple[str, ...]: ...

    @property
    def reason_codes(self) -> tuple[str, ...]: ...


class BeginnerRouteProtocol(Protocol):
    @property
    def recipe_available(self) -> bool: ...


class RouteCapabilityProtocol(Protocol):
    @property
    def id(self) -> str: ...

    @property
    def source(self) -> str: ...

    @property
    def sink(self) -> str: ...

    @property
    def strategy(self) -> str: ...

    @property
    def support(self) -> RouteSupportProtocol: ...

    @property
    def certification(self) -> RouteCertificationProtocol: ...

    @property
    def beginner(self) -> BeginnerRouteProtocol: ...

    def to_dict(self) -> dict[str, Any]: ...


class RecipeCapabilityProtocol(Protocol):
    def to_dict(self) -> dict[str, Any]: ...


class CapabilityIssueProtocol(Protocol):
    def to_dict(self) -> dict[str, Any]: ...


class CapabilitySnapshotProtocol(Protocol):
    @property
    def snapshot_id(self) -> str: ...

    @property
    def connectors(self) -> Sequence[ConnectorCapabilityProtocol]: ...

    @property
    def routes(self) -> Sequence[RouteCapabilityProtocol]: ...

    @property
    def recipes(self) -> Sequence[RecipeCapabilityProtocol]: ...

    @property
    def issues(self) -> Sequence[CapabilityIssueProtocol]: ...

    def to_dict(self) -> dict[str, Any]: ...


class CapabilitySnapshotProvider(Protocol):
    """Return one immutable, freshness-checked snapshot for a use-case call."""

    def __call__(self) -> CapabilitySnapshotProtocol: ...


__all__ = [
    "BeginnerRouteProtocol",
    "CapabilityIssueProtocol",
    "CapabilitySnapshotProtocol",
    "CapabilitySnapshotProvider",
    "ConnectorCapabilityProtocol",
    "RecipeCapabilityProtocol",
    "RouteCapabilityProtocol",
    "RouteCertificationProtocol",
    "RouteCertificationVariantProtocol",
    "RouteKeyProtocol",
    "RouteProfileProtocol",
    "RouteSupportProtocol",
]
