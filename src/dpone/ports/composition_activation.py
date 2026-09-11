"""Capabilities for complete parent activation; no native-only substitutions."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.contracts.composition_control import (
        CompositionActivationOccurrence,
        CompositionActivationRequest,
        CompositionExecutionPlan,
        CompositionOccurrenceContext,
        CompositionPhysicalResource,
        CompositionSourceSnapshot,
    )
from pathlib import Path
from typing import Protocol


class CompositionActivationInputs(Protocol):
    """Reconstruct source/context authority from a confined sealed projection."""

    def load_sources(self, *, projection_root: Path, release_id: str) -> CompositionSourceSnapshot: ...

    def load_context(
        self,
        *,
        projection_root: Path,
        activation_id: str,
        environment: str,
        release_id: str,
        deployment_id: str,
        previous_deployment_id: str | None,
    ) -> CompositionOccurrenceContext: ...


class CompositionPhysicalAdmission(Protocol):
    """Provide live, protected all-backend authority after binding resolution.

    Observe each entire physical collision domain in one catalog comparison,
    merging aliases before comparisons. Include helpers, staging and publication
    effects. Reject missing enrollment/permissions/session gates and unknown
    capabilities. Never derive global authority from endpoint names or principals.
    """

    @property
    def execution_cells(self) -> frozenset[str]: ...

    def observe(
        self, plan: CompositionExecutionPlan, context: CompositionOccurrenceContext
    ) -> tuple[CompositionPhysicalResource, ...]: ...

    def require_stable_bindings(self, request: CompositionActivationRequest, plan: CompositionExecutionPlan) -> None:
        """Recheck protected physical pins, without rehashing changed table catalogs."""


class CompositionActivationStore(Protocol):
    """Protected occurrence/guard transactions, never filesystem-only receipts.

    prepare reserves the complete closure without expiration. Before ownership
    transfer, it closes predecessor admission and requires all exact attempt
    gates closed, server-quiescent and outcomes reconciled. Every transition
    independently reads back exact request, state and full guard epochs. Unknown
    outcomes retain ownership. The store must enforce this atomically across
    its protected control rows; live data changes are not globally atomic.
    """

    def read(self, activation_id: str) -> CompositionActivationOccurrence | None: ...

    def prepare(self, request: CompositionActivationRequest) -> CompositionActivationOccurrence: ...

    def activate(self, request: CompositionActivationRequest) -> CompositionActivationOccurrence: ...

    def begin_retirement(self, request: CompositionActivationRequest) -> CompositionActivationOccurrence: ...

    def finalize_retirement(self, request: CompositionActivationRequest) -> CompositionActivationOccurrence: ...


class CompositionActivationStoreFactory(Protocol):
    """Rebuild authority from the verified projection for every lifecycle phase."""

    def build(self, *, projection_root: Path, context: CompositionOccurrenceContext) -> CompositionActivationStore: ...


class CompositionActivationCoordinatorPort(Protocol):
    """Separate parent capability; native-only coordination cannot satisfy it."""

    def prepare(
        self,
        *,
        projection_root: Path,
        activation_id: str,
        environment: str,
        release_id: str,
        deployment_id: str,
        previous_deployment_id: str | None,
    ) -> CompositionActivationOccurrence: ...

    def activate(
        self, prepared: CompositionActivationOccurrence, *, projection_root: Path
    ) -> CompositionActivationOccurrence: ...

    def require_active(
        self,
        *,
        projection_root: Path,
        activation_id: str,
        environment: str,
        release_id: str,
        deployment_id: str,
        previous_deployment_id: str | None,
    ) -> CompositionActivationOccurrence: ...
