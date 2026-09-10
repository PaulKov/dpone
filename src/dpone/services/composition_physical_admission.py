"""Merge resolved aliases before one complete physical collision observation."""

from __future__ import annotations

from typing import TYPE_CHECKING

from dpone.contracts.composition_control import (
    CompositionActivationRequest,
    CompositionAdmissionError,
    CompositionExecutionPlan,
    CompositionOccurrenceContext,
    CompositionPhysicalDomain,
    CompositionPhysicalResource,
    DbtRelationWrite,
    dbt_relation_write_subject,
)

if TYPE_CHECKING:
    from dpone.ports.composition_physical import CompositionPhysicalBackend


class CompositionPhysicalAdmissionService:
    """Backend effects are explicit; this service owns complete-union grouping."""

    def __init__(self, *, backend: CompositionPhysicalBackend) -> None:
        self._backend = backend

    @property
    def execution_cells(self) -> frozenset[str]:
        return self._backend.execution_cells

    def observe(
        self,
        plan: CompositionExecutionPlan,
        context: CompositionOccurrenceContext,
    ) -> tuple[CompositionPhysicalResource, ...]:
        plan.__post_init__()
        plan.require_installed_cells(self.execution_cells)
        self._backend.require_execution(plan, context)
        return observe_composition_writes(plan.writes, context=context, backend=self._backend)

    def require_stable_bindings(self, request: CompositionActivationRequest, plan: CompositionExecutionPlan) -> None:
        """Check physical continuity without replacing original catalog evidence."""
        plan.require_installed_cells(self.execution_cells)
        self._backend.require_execution(plan, request.context)
        groups = _resolve_domains(plan.writes, context=request.context, backend=self._backend)
        expected = {resource.guard_id: resource for resource in request.resources}
        if set(groups) != set(expected):
            raise CompositionAdmissionError("physical_binding_drift")
        for guard, (domain, writes) in groups.items():
            resource = expected[guard]
            if (resource.connector, resource.service_id, resource.physical_subject_sha256) != (
                domain.connector,
                domain.service_id,
                domain.physical_subject_sha256,
            ) or resource.write_subjects != tuple(sorted(dbt_relation_write_subject(write) for write in writes)):
                raise CompositionAdmissionError("physical_binding_drift")


def observe_composition_writes(
    writes: tuple[DbtRelationWrite, ...],
    *,
    context: CompositionOccurrenceContext,
    backend: CompositionPhysicalBackend,
) -> tuple[CompositionPhysicalResource, ...]:
    """Use one complete comparison per physical domain, never per connection alias."""
    groups = _resolve_domains(writes, context=context, backend=backend)
    resources = []
    for guard, (domain, group) in sorted(groups.items()):
        observation = backend.observe_domain(domain, tuple(group), context)
        observation.__post_init__()
        expected = tuple(sorted(dbt_relation_write_subject(write) for write in group))
        if observation.domain != domain or tuple(sorted(subject for subject, _ in observation.slots)) != expected:
            raise CompositionAdmissionError("physical_observation_closure")
        equivalences = [equivalence for _, equivalence in observation.slots]
        if len(set(equivalences)) != len(equivalences):
            raise CompositionAdmissionError("physical_target_collision")
        resources.append(
            CompositionPhysicalResource(
                guard_id=guard,
                connector=domain.connector,
                service_id=domain.service_id,
                physical_subject_sha256=domain.physical_subject_sha256,
                observation_sha256=observation.catalog_sha256,
                write_subjects=expected,
            )
        )
    return tuple(resources)


def _resolve_domains(
    writes: tuple[DbtRelationWrite, ...],
    *,
    context: CompositionOccurrenceContext,
    backend: CompositionPhysicalBackend,
) -> dict[str, tuple[CompositionPhysicalDomain, list[DbtRelationWrite]]]:
    if not isinstance(writes, tuple) or not 1 <= len(writes) <= 8192:
        raise CompositionAdmissionError("physical_write_budget")
    groups: dict[str, tuple[CompositionPhysicalDomain, list[DbtRelationWrite]]] = {}
    for write in writes:
        domain = backend.resolve_domain(write, context)
        domain.__post_init__()
        if domain.connector != write.connector:
            raise CompositionAdmissionError("physical_binding_connector")
        existing = groups.setdefault(domain.guard_id, (domain, []))
        if existing[0] != domain:
            raise CompositionAdmissionError("physical_identity_conflict")
        existing[1].append(write)
    return groups
