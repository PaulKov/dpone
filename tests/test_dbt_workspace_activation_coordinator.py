"""Coordinator rebuilds and validates exact durable activation occurrences."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from dpone.contracts.dbt_workspace_activation import (
    DbtWorkspaceActivationError,
    DbtWorkspaceActivationReceipt,
    DbtWorkspaceActivationRequest,
    DbtWorkspaceGuardEpoch,
    DbtWorkspacePhysicalResource,
)
from dpone.contracts.dbt_workspace_runtime_authority import DbtWorkspaceRuntimeAuthority
from dpone.services.dbt_workspace_activation_coordinator import DbtWorkspaceActivationCoordinator

ACTIVATION_ID = "164a3c74-cf85-4a4a-a087-07c9b07050ff"


def _digest(character: str) -> str:
    return "sha256:" + character * 64


def _authority(*, environment: str = "prod") -> DbtWorkspaceRuntimeAuthority:
    return DbtWorkspaceRuntimeAuthority.build(
        environment=environment,
        release_id=_digest("a"),
        deployment_id=_digest("b"),
        release_sha256=_digest("c"),
        deployment_sha256=_digest("d"),
        binding_set_sha256=_digest("e"),
        connection_registry_sha256=_digest("f"),
        credential_runtime_sha256=_digest("1"),
    )


class _Inputs:
    def __init__(self) -> None:
        self.sources = SimpleNamespace(release_id=_digest("a"), inventory=SimpleNamespace(snapshot_sha256=_digest("2")))
        self.authority = _authority()
        self.source_calls: list[tuple[Path, str]] = []
        self.authority_calls: list[tuple[Path, str, str, str]] = []

    def load_sources(self, *, projection_root: Path, release_id: str):
        self.source_calls.append((projection_root, release_id))
        return self.sources

    def load_runtime_authority(
        self,
        *,
        projection_root: Path,
        environment: str,
        release_id: str,
        deployment_id: str,
    ) -> DbtWorkspaceRuntimeAuthority:
        self.authority_calls.append((projection_root, environment, release_id, deployment_id))
        return self.authority

    def resolve_connection(self, authority: DbtWorkspaceRuntimeAuthority, connection_ref: str):
        raise AssertionError("fake preparation must not resolve credentials")


class _Preparation:
    def __init__(self) -> None:
        self.calls = []

    def prepare(self, *, activation_id, previous_deployment_id, sources, runtime_context):
        self.calls.append((activation_id, previous_deployment_id, sources, runtime_context))
        resource = DbtWorkspacePhysicalResource(
            guard_id="mssql://service/warehouse/mart/orders",
            connector="mssql",
            service_authority_sha256=_digest("3"),
            target_authority_sha256=_digest("4"),
            observation_sha256=_digest("5"),
            write_subjects=(_digest("6"),),
        )
        return DbtWorkspaceActivationRequest.build(
            activation_id=activation_id,
            environment=runtime_context.environment,
            release_id=runtime_context.release_id,
            deployment_id=runtime_context.deployment_id,
            previous_deployment_id=previous_deployment_id,
            source_inventory_sha256=sources.inventory.snapshot_sha256,
            runtime_context_sha256=runtime_context.authority_subject_sha256,
            write_subjects=resource.write_subjects,
            resources=(resource,),
        )


class _Admission:
    def __init__(self) -> None:
        self.prepare_calls: list[DbtWorkspaceActivationRequest] = []
        self.activate_calls: list[DbtWorkspaceActivationRequest] = []
        self.active_calls: list[DbtWorkspaceActivationRequest] = []
        self.retiring_calls: list[DbtWorkspaceActivationRequest] = []
        self.retired_calls: list[DbtWorkspaceActivationRequest] = []
        self.mutate_receipt = False

    def _receipt(self, request: DbtWorkspaceActivationRequest, state: str) -> DbtWorkspaceActivationReceipt:
        receipt = DbtWorkspaceActivationReceipt.build(
            activation_id=request.activation_id,
            request_sha256=request.request_sha256,
            state=state,
            guard_epochs=tuple(DbtWorkspaceGuardEpoch(resource.guard_id, 1) for resource in request.resources),
        )
        if not self.mutate_receipt:
            return receipt
        return DbtWorkspaceActivationReceipt.build(
            activation_id=request.activation_id,
            request_sha256=_digest("9"),
            state=state,
            guard_epochs=receipt.guard_epochs,
        )

    def prepare(self, request: DbtWorkspaceActivationRequest) -> DbtWorkspaceActivationReceipt:
        self.prepare_calls.append(request)
        return self._receipt(request, "PREPARED")

    def activate(self, request: DbtWorkspaceActivationRequest) -> DbtWorkspaceActivationReceipt:
        self.activate_calls.append(request)
        return self._receipt(request, "ACTIVE")

    def require_active(self, request: DbtWorkspaceActivationRequest) -> DbtWorkspaceActivationReceipt:
        self.active_calls.append(request)
        return self._receipt(request, "ACTIVE")

    def begin_retirement(self, request: DbtWorkspaceActivationRequest) -> DbtWorkspaceActivationReceipt:
        self.retiring_calls.append(request)
        return self._receipt(request, "RETIRING")

    def finalize_retirement(self, request: DbtWorkspaceActivationRequest) -> DbtWorkspaceActivationReceipt:
        self.retired_calls.append(request)
        return self._receipt(request, "RETIRED")


class _AdmissionFactory:
    def __init__(self, admission: _Admission) -> None:
        self.admission = admission
        self.resolvers = []

    def build(self, resolver):
        self.resolvers.append(resolver)
        return self.admission


def _coordinator():
    inputs, preparation, admission = _Inputs(), _Preparation(), _Admission()
    coordinator = DbtWorkspaceActivationCoordinator(inputs=inputs, preparation=preparation, admission=admission)
    return coordinator, inputs, preparation, admission


def _coordinates() -> dict[str, object]:
    return {
        "projection_root": Path("/sealed/projection"),
        "activation_id": ACTIVATION_ID,
        "environment": "prod",
        "release_id": _digest("a"),
        "deployment_id": _digest("b"),
        "previous_deployment_id": None,
    }


def test_prepare_then_activate_use_one_exact_request() -> None:
    coordinator, inputs, _, admission = _coordinator()

    prepared = coordinator.prepare(**_coordinates())
    active = coordinator.activate(prepared, projection_root=Path("/sealed/projection"))

    assert active.request == prepared.request
    assert admission.prepare_calls == [prepared.request]
    assert admission.activate_calls == [prepared.request]
    assert inputs.source_calls == [
        (Path("/sealed/projection"), _digest("a")),
        (Path("/sealed/projection"), _digest("a")),
    ]


def test_retirement_preserves_one_exact_request_through_quiescent_release() -> None:
    coordinator, _, _, admission = _coordinator()
    prepared = coordinator.prepare(**_coordinates())
    active = coordinator.activate(prepared, projection_root=Path("/sealed/projection"))

    retiring = coordinator.begin_retirement(active, projection_root=Path("/sealed/projection"))
    retired = coordinator.finalize_retirement(retiring, projection_root=Path("/sealed/projection"))

    assert retired.request == active.request
    assert retiring.receipt.state == "RETIRING"
    assert retired.receipt.state == "RETIRED"
    assert admission.retiring_calls == [active.request]
    assert admission.retired_calls == [active.request]


def test_audit_rebuilds_request_and_only_reads_existing_active_occurrence() -> None:
    coordinator, _, preparation, admission = _coordinator()

    active = coordinator.require_active(**_coordinates())

    assert active.receipt.state == "ACTIVE"
    assert admission.prepare_calls == []
    assert admission.activate_calls == []
    assert admission.active_calls == [active.request]
    assert len(preparation.calls) == 1


def test_foreign_or_stale_readback_fails_closed() -> None:
    coordinator, _, _, admission = _coordinator()
    admission.mutate_receipt = True

    with pytest.raises(DbtWorkspaceActivationError, match="receipt_subject|occurrence_mismatch"):
        coordinator.prepare(**_coordinates())


def test_runtime_authority_must_match_requested_coordinates() -> None:
    coordinator, inputs, preparation, admission = _coordinator()
    inputs.authority = _authority(environment="dev")

    with pytest.raises(DbtWorkspaceActivationError, match="runtime_context"):
        coordinator.prepare(**_coordinates())

    assert preparation.calls == []
    assert admission.prepare_calls == []


def test_admission_factory_is_rebuilt_from_sealed_authority_for_each_phase() -> None:
    inputs, preparation, admission = _Inputs(), _Preparation(), _Admission()
    factory = _AdmissionFactory(admission)
    coordinator = DbtWorkspaceActivationCoordinator(
        inputs=inputs,
        preparation=preparation,
        admission_factory=factory,
    )

    prepared = coordinator.prepare(**_coordinates())
    coordinator.activate(prepared, projection_root=Path("/sealed/projection"))

    assert len(factory.resolvers) == 2
    assert factory.resolvers[0] is not factory.resolvers[1]
