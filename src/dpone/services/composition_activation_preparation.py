"""Complete source/capability preparation before any occurrence mutation."""

from __future__ import annotations

from typing import TYPE_CHECKING

from dpone.contracts.composition_control import (
    CompositionActivationRequest,
    CompositionAdmissionError,
    CompositionOccurrenceContext,
    CompositionSourceSnapshot,
)
from dpone.manifest.composition_execution_plan import plan_composition_execution

if TYPE_CHECKING:
    from dpone.ports.composition_activation import CompositionPhysicalAdmission


class CompositionActivationPreparation:
    """One full-parent plan, explicit installed cells and complete physical proof."""

    def __init__(self, *, physical: CompositionPhysicalAdmission) -> None:
        self._physical = physical

    def prepare(
        self,
        *,
        sources: CompositionSourceSnapshot,
        context: CompositionOccurrenceContext,
    ) -> CompositionActivationRequest:
        """Finish all checks before the caller may drain or reserve resources."""
        if sources.release_id != context.release_id:
            raise CompositionAdmissionError("parent_context")
        plan = plan_composition_execution(sources)
        plan.require_installed_cells(self._physical.execution_cells)
        resources = self._physical.observe(plan, context)
        return CompositionActivationRequest(
            context=context,
            source_subject_sha256=sources.subject_sha256,
            workloads=plan.workloads,
            resources=resources,
        )

    def require_existing(
        self,
        request: CompositionActivationRequest,
        *,
        sources: CompositionSourceSnapshot,
        context: CompositionOccurrenceContext,
    ) -> None:
        """Validate immutable original admission while allowing legitimate DDL.

        Do not reconstruct a request from current create/modify timestamps. The
        protected binding verifier must still reject service/database drift.
        """
        request.__post_init__()
        plan = plan_composition_execution(sources)
        plan.require_installed_cells(self._physical.execution_cells)
        if (
            sources.release_id != context.release_id
            or request.context != context
            or request.source_subject_sha256 != sources.subject_sha256
            or request.workloads != plan.workloads
        ):
            raise CompositionAdmissionError("immutable_admission_subject")
        self._physical.require_stable_bindings(request, plan)
