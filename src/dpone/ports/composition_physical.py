"""Environment-bound physical observation capabilities for the full parent."""

from typing import Protocol

from dpone.contracts.composition_control import (
    CompositionDomainObservation,
    CompositionExecutionPlan,
    CompositionOccurrenceContext,
    CompositionPhysicalDomain,
    DbtRelationWrite,
)


class CompositionPhysicalBackend(Protocol):
    """Explicitly composed backend, never a caller-populated capability registry."""

    @property
    def execution_cells(self) -> frozenset[str]: ...

    def require_execution(self, plan: CompositionExecutionPlan, context: CompositionOccurrenceContext) -> None:
        """Verify all route options, effects, protected enrollment and writer gates."""

    def resolve_domain(
        self, write: DbtRelationWrite, context: CompositionOccurrenceContext
    ) -> CompositionPhysicalDomain:
        """Resolve bindings and verify protected physical pins and enrollment."""

    def observe_domain(
        self,
        domain: CompositionPhysicalDomain,
        writes: tuple[DbtRelationWrite, ...],
        context: CompositionOccurrenceContext,
    ) -> CompositionDomainObservation:
        """Observe all aliases' writes together, including helpers and dependencies."""
