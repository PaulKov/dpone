"""Parent activation saga with immutable preparation and exact protected readback."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from dpone.contracts.composition_control import (
    CompositionActivationOccurrence,
    CompositionAdmissionError,
    CompositionOccurrenceContext,
)

if TYPE_CHECKING:
    from dpone.ports.composition_activation import CompositionActivationInputs, CompositionActivationStoreFactory
    from dpone.services.composition_activation_preparation import CompositionActivationPreparation


class CompositionActivationCoordinator:
    """Coordinate full admission without changing native-v2 authority or source bytes.

    All I/O is behind supplied capabilities. Production composition must install
    protected backends for every cell; this service is not itself a SQL gate.
    """

    def __init__(
        self,
        *,
        inputs: CompositionActivationInputs,
        preparation: CompositionActivationPreparation,
        stores: CompositionActivationStoreFactory,
    ) -> None:
        self._inputs, self._preparation, self._stores = inputs, preparation, stores

    def prepare(
        self,
        *,
        projection_root: Path,
        activation_id: str,
        environment: str,
        release_id: str,
        deployment_id: str,
        previous_deployment_id: str | None,
    ) -> CompositionActivationOccurrence:
        """Admit the complete union before protected reservation or predecessor drain."""
        context = self._context(
            projection_root=projection_root,
            activation_id=activation_id,
            environment=environment,
            release_id=release_id,
            deployment_id=deployment_id,
            previous_deployment_id=previous_deployment_id,
        )
        sources = self._inputs.load_sources(projection_root=projection_root, release_id=release_id)
        store = self._stores.build(projection_root=projection_root, context=context)
        existing = store.read(activation_id)
        if existing is not None:
            existing.require_state("PREPARED")
            self._preparation.require_existing(existing.request, sources=sources, context=context)
            return existing
        request = self._preparation.prepare(sources=sources, context=context)
        prepared = store.prepare(request).require_state("PREPARED")
        if prepared.request != request:
            raise CompositionAdmissionError("prepared_readback")
        return prepared

    def activate(
        self,
        prepared: CompositionActivationOccurrence,
        *,
        projection_root: Path,
    ) -> CompositionActivationOccurrence:
        """Acknowledge ACTIVE only after the caller's current-pointer CAS commit."""
        return self._transition(prepared, projection_root=projection_root, before="PREPARED", after="ACTIVE")

    def require_active(
        self,
        *,
        projection_root: Path,
        activation_id: str,
        environment: str,
        release_id: str,
        deployment_id: str,
        previous_deployment_id: str | None,
    ) -> CompositionActivationOccurrence:
        """Read the existing occurrence without reacquiring or recreating its request."""
        context = self._context(
            projection_root=projection_root,
            activation_id=activation_id,
            environment=environment,
            release_id=release_id,
            deployment_id=deployment_id,
            previous_deployment_id=previous_deployment_id,
        )
        sources = self._inputs.load_sources(projection_root=projection_root, release_id=release_id)
        store = self._stores.build(projection_root=projection_root, context=context)
        active = store.read(activation_id)
        if active is None:
            raise CompositionAdmissionError("occurrence_missing")
        active.require_state("ACTIVE")
        self._preparation.require_existing(active.request, sources=sources, context=context)
        return active

    def begin_retirement(
        self,
        active: CompositionActivationOccurrence,
        *,
        projection_root: Path,
    ) -> CompositionActivationOccurrence:
        """Close new attempt admission while retaining all in-flight resource epochs."""
        return self._transition(active, projection_root=projection_root, before="ACTIVE", after="RETIRING")

    def finalize_retirement(
        self,
        retiring: CompositionActivationOccurrence,
        *,
        projection_root: Path,
    ) -> CompositionActivationOccurrence:
        """Require protected gate closure, quiescence and resolved outcomes before release."""
        return self._transition(retiring, projection_root=projection_root, before="RETIRING", after="RETIRED")

    def _transition(
        self,
        occurrence: CompositionActivationOccurrence,
        *,
        projection_root: Path,
        before: str,
        after: str,
    ) -> CompositionActivationOccurrence:
        occurrence.require_state(before)
        context = occurrence.request.context
        checked = self._context(
            projection_root=projection_root,
            activation_id=context.activation_id,
            environment=context.environment,
            release_id=context.release_id,
            deployment_id=context.deployment_id,
            previous_deployment_id=context.previous_deployment_id,
        )
        sources = self._inputs.load_sources(projection_root=projection_root, release_id=context.release_id)
        self._preparation.require_existing(occurrence.request, sources=sources, context=checked)
        store = self._stores.build(projection_root=projection_root, context=checked)
        transitions = {
            "ACTIVE": store.activate,
            "RETIRING": store.begin_retirement,
            "RETIRED": store.finalize_retirement,
        }
        result = transitions[after](occurrence.request).require_state(after)
        if result.request != occurrence.request or result.receipt.guard_epochs != occurrence.receipt.guard_epochs:
            raise CompositionAdmissionError("transition_readback")
        return result

    def _context(
        self,
        *,
        projection_root: Path,
        activation_id: str,
        environment: str,
        release_id: str,
        deployment_id: str,
        previous_deployment_id: str | None,
    ) -> CompositionOccurrenceContext:
        context = self._inputs.load_context(
            projection_root=projection_root,
            activation_id=activation_id,
            environment=environment,
            release_id=release_id,
            deployment_id=deployment_id,
            previous_deployment_id=previous_deployment_id,
        )
        context.__post_init__()
        if (
            context.activation_id,
            context.environment,
            context.release_id,
            context.deployment_id,
            context.previous_deployment_id,
        ) != (
            activation_id,
            environment,
            release_id,
            deployment_id,
            previous_deployment_id,
        ):
            raise CompositionAdmissionError("projection_context")
        return context
