"""Registry for certified safe-sample source data copiers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from dpone.services.safe_sample_runtime_executor import FailClosedSafeSampleDataCopier, SafeSampleDataCopier
from dpone.services.sample_route_certifications import CertifiedSamplingRoute, certified_sampling_route_catalog

if TYPE_CHECKING:
    from collections.abc import Mapping

    from dpone.services.safe_sample_execution_plan import SafeSampleExecutionPlan
    from dpone.services.safe_sample_policy import TemporaryTargetPlan


@dataclass(frozen=True, slots=True)
class SafeSampleDataCopierRegistration:
    """A copier implementation tied to one route-certified sampling contract."""

    certification_id: str
    route: CertifiedSamplingRoute
    copier: SafeSampleDataCopier

    def to_dict(self) -> dict[str, Any]:
        return {
            "certification_id": self.certification_id,
            "status": self.route.status,
            "source": self.route.source,
            "sink": self.route.sink,
            "strategy": self.route.strategy,
            "transport": self.route.transport,
            "schema_evolution": self.route.schema_evolution,
            "airflow_runtime_mode": self.route.airflow_runtime_mode,
            "sampling_mode": self.route.sampling_mode,
        }


class SafeSampleDataCopierRegistry:
    """Resolve source-copy implementations only through known route certifications."""

    def __init__(self, registrations: tuple[SafeSampleDataCopierRegistration, ...] = ()) -> None:
        self._registrations = {registration.certification_id: registration for registration in registrations}

    @classmethod
    def empty(cls) -> SafeSampleDataCopierRegistry:
        return cls()

    @classmethod
    def with_copiers(cls, copiers: Mapping[str, SafeSampleDataCopier]) -> SafeSampleDataCopierRegistry:
        routes = _routes_by_certification_id()
        registrations: list[SafeSampleDataCopierRegistration] = []
        for certification_id, copier in sorted(copiers.items()):
            route = routes.get(certification_id)
            if route is None:
                raise ValueError(f"unknown route certification: {certification_id}")
            registrations.append(
                SafeSampleDataCopierRegistration(
                    certification_id=certification_id,
                    route=route,
                    copier=copier,
                )
            )
        return cls(tuple(registrations))

    def has_copier_for(self, plan: SafeSampleExecutionPlan) -> bool:
        return self.resolve(plan) is not None

    def resolve(self, plan: SafeSampleExecutionPlan) -> SafeSampleDataCopier | None:
        certification_id = route_certification_id_for_plan(plan)
        if certification_id is None:
            return None
        registration = self._registrations.get(certification_id)
        return registration.copier if registration is not None else None

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": "dpone.safe-sample-data-copier-registry.v1",
            "registered_copiers": [registration.to_dict() for registration in self._registrations.values()],
        }


class RegistryBackedSafeSampleDataCopier:
    """Select a certified copier for the plan, otherwise keep the runtime fail-closed."""

    def __init__(
        self,
        registry: SafeSampleDataCopierRegistry,
        *,
        fallback: SafeSampleDataCopier | None = None,
    ) -> None:
        self._registry = registry
        self._fallback = fallback or FailClosedSafeSampleDataCopier()

    def copy(
        self,
        *,
        plan: SafeSampleExecutionPlan,
        target_plan: TemporaryTargetPlan,
        init_fetch: dict[str, Any],
    ) -> Mapping[str, Any]:
        copier = self._registry.resolve(plan)
        if copier is None:
            return self._fallback.copy(plan=plan, target_plan=target_plan, init_fetch=init_fetch)
        return copier.copy(plan=plan, target_plan=target_plan, init_fetch=init_fetch)


def route_certification_id_for_plan(plan: SafeSampleExecutionPlan) -> str | None:
    proof = str(plan.policy_result.capabilities.proof or "")
    prefix = "route_certification:"
    if not proof.startswith(prefix):
        return None
    certification_id = proof.removeprefix(prefix).strip()
    return certification_id or None


def _routes_by_certification_id() -> dict[str, CertifiedSamplingRoute]:
    return {route.certification_id: route for route in certified_sampling_route_catalog()}


__all__ = [
    "RegistryBackedSafeSampleDataCopier",
    "SafeSampleDataCopierRegistration",
    "SafeSampleDataCopierRegistry",
    "route_certification_id_for_plan",
]
