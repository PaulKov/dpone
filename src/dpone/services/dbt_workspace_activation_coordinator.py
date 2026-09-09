"""Application coordinator for exact workspace activation occurrence readback."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from dpone.contracts.dbt_workspace_control import (
    DbtWorkspaceActivationError,
    DbtWorkspaceActivationRequest,
    DbtWorkspaceActiveActivation,
    DbtWorkspacePreparedActivation,
    DbtWorkspaceRetiredActivation,
    DbtWorkspaceRetiringActivation,
    DbtWorkspaceRuntimeAuthority,
    ResolvedBindingConnection,
    require_activation_receipt,
)

if TYPE_CHECKING:
    from dpone.ports.dbt_workspace_activation import (
        DbtWorkspaceActivationAdmissionFactoryPort,
        DbtWorkspaceActivationAdmissionPort,
        DbtWorkspaceActivationInputPort,
    )
    from dpone.services.dbt_workspace_activation_preparation import DbtWorkspaceActivationPreparation


class _Resolver:
    def __init__(self, inputs: DbtWorkspaceActivationInputPort, authority: DbtWorkspaceRuntimeAuthority) -> None:
        self._inputs = inputs
        self._authority = authority

    def resolve(self, connection_ref: str) -> ResolvedBindingConnection:
        return self._inputs.resolve_connection(self._authority, connection_ref)


@dataclass(frozen=True, slots=True)
class _RuntimeContext:
    environment: str
    release_id: str
    deployment_id: str
    authority_subject_sha256: str
    resolver: _Resolver


class DbtWorkspaceActivationCoordinator:
    """Rebuild exact requests for prepare, activation and audit reconciliation."""

    def __init__(
        self,
        *,
        inputs: DbtWorkspaceActivationInputPort,
        preparation: DbtWorkspaceActivationPreparation,
        admission: DbtWorkspaceActivationAdmissionPort | None = None,
        admission_factory: DbtWorkspaceActivationAdmissionFactoryPort | None = None,
    ) -> None:
        if (admission is None) == (admission_factory is None):
            raise ValueError("exactly one workspace admission or admission_factory is required")
        self._inputs = inputs
        self._preparation = preparation
        self._admission = admission
        self._admission_factory = admission_factory

    def prepare(
        self,
        *,
        projection_root: Path,
        activation_id: str,
        environment: str,
        release_id: str,
        deployment_id: str,
        previous_deployment_id: str | None,
    ) -> DbtWorkspacePreparedActivation:
        request, admission = self._request_and_admission(
            projection_root=projection_root,
            activation_id=activation_id,
            environment=environment,
            release_id=release_id,
            deployment_id=deployment_id,
            previous_deployment_id=previous_deployment_id,
        )
        receipt = require_activation_receipt(admission.prepare(request), request, state="PREPARED")
        return DbtWorkspacePreparedActivation(request, receipt)

    def activate(
        self,
        prepared: DbtWorkspacePreparedActivation,
        *,
        projection_root: Path,
    ) -> DbtWorkspaceActiveActivation:
        prepared.__post_init__()
        request, admission = self._rebuild(prepared.request, projection_root=projection_root)
        receipt = require_activation_receipt(
            admission.activate(request),
            request,
            state="ACTIVE",
        )
        return DbtWorkspaceActiveActivation(request, receipt)

    def require_active(
        self,
        *,
        projection_root: Path,
        activation_id: str,
        environment: str,
        release_id: str,
        deployment_id: str,
        previous_deployment_id: str | None,
    ) -> DbtWorkspaceActiveActivation:
        request, admission = self._request_and_admission(
            projection_root=projection_root,
            activation_id=activation_id,
            environment=environment,
            release_id=release_id,
            deployment_id=deployment_id,
            previous_deployment_id=previous_deployment_id,
        )
        receipt = require_activation_receipt(
            admission.require_active(request),
            request,
            state="ACTIVE",
        )
        return DbtWorkspaceActiveActivation(request, receipt)

    def begin_retirement(
        self,
        active: DbtWorkspaceActiveActivation,
        *,
        projection_root: Path,
    ) -> DbtWorkspaceRetiringActivation:
        """Fence new task attempts without interrupting already admitted work."""

        active.__post_init__()
        request, admission = self._rebuild(active.request, projection_root=projection_root)
        receipt = require_activation_receipt(
            admission.begin_retirement(request),
            request,
            state="RETIRING",
        )
        return DbtWorkspaceRetiringActivation(request, receipt)

    def finalize_retirement(
        self,
        retiring: DbtWorkspaceRetiringActivation,
        *,
        projection_root: Path,
    ) -> DbtWorkspaceRetiredActivation:
        """Release exact epochs only after the persistence adapter proves quiescence."""

        retiring.__post_init__()
        request, admission = self._rebuild(retiring.request, projection_root=projection_root)
        receipt = require_activation_receipt(
            admission.finalize_retirement(request),
            request,
            state="RETIRED",
        )
        return DbtWorkspaceRetiredActivation(request, receipt)

    def _request_and_admission(
        self,
        *,
        projection_root: Path,
        activation_id: str,
        environment: str,
        release_id: str,
        deployment_id: str,
        previous_deployment_id: str | None,
    ) -> tuple[DbtWorkspaceActivationRequest, DbtWorkspaceActivationAdmissionPort]:
        sources = self._inputs.load_sources(projection_root=projection_root, release_id=release_id)
        authority = self._inputs.load_runtime_authority(
            projection_root=projection_root,
            environment=environment,
            release_id=release_id,
            deployment_id=deployment_id,
        )
        authority.__post_init__()
        if (
            authority.environment != environment
            or authority.release_id != release_id
            or authority.deployment_id != deployment_id
        ):
            raise DbtWorkspaceActivationError("runtime_context")
        resolver = _Resolver(self._inputs, authority)
        request = self._preparation.prepare(
            activation_id=activation_id,
            previous_deployment_id=previous_deployment_id,
            sources=sources,
            runtime_context=_RuntimeContext(
                environment=authority.environment,
                release_id=authority.release_id,
                deployment_id=authority.deployment_id,
                authority_subject_sha256=authority.authority_subject_sha256,
                resolver=resolver,
            ),
        )
        admission = self._admission
        if admission is None:
            factory = self._admission_factory
            if factory is None:
                raise DbtWorkspaceActivationError("admission_factory")
            admission = factory.build(resolver)
        return request, admission

    def _rebuild(
        self,
        expected: DbtWorkspaceActivationRequest,
        *,
        projection_root: Path,
    ) -> tuple[DbtWorkspaceActivationRequest, DbtWorkspaceActivationAdmissionPort]:
        request, admission = self._request_and_admission(
            projection_root=projection_root,
            activation_id=expected.activation_id,
            environment=expected.environment,
            release_id=expected.release_id,
            deployment_id=expected.deployment_id,
            previous_deployment_id=expected.previous_deployment_id,
        )
        if request != expected:
            raise DbtWorkspaceActivationError("occurrence_rebuild")
        return request, admission


__all__ = ["DbtWorkspaceActivationCoordinator"]
