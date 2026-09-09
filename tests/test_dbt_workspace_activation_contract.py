"""Workspace activation identities cannot substitute for durable authority."""

from __future__ import annotations

from dataclasses import replace

import pytest

from dpone.contracts.dbt_workspace_activation import (
    DbtWorkspaceActivationError,
    DbtWorkspaceActivationReceipt,
    DbtWorkspaceActivationRequest,
    DbtWorkspaceGuardEpoch,
    DbtWorkspacePhysicalResource,
    require_activation_receipt,
)

ACTIVATION_ID = "164a3c74-cf85-4a4a-a087-07c9b07050ff"


def _digest(character: str) -> str:
    return "sha256:" + character * 64


def _resource(guard_id: str = "mssql://service/warehouse/mart/orders") -> DbtWorkspacePhysicalResource:
    return DbtWorkspacePhysicalResource(
        guard_id=guard_id,
        connector="mssql",
        service_authority_sha256=_digest("1"),
        target_authority_sha256=_digest("2"),
        observation_sha256=_digest("3"),
        write_subjects=(_digest("4" if guard_id.endswith("orders") else "5"),),
    )


def _request(*resources: DbtWorkspacePhysicalResource) -> DbtWorkspaceActivationRequest:
    return _build(resources or (_resource(),))


def _build(resources, *, write_subjects=None) -> DbtWorkspaceActivationRequest:
    return DbtWorkspaceActivationRequest.build(
        activation_id=ACTIVATION_ID,
        environment="prod",
        release_id=_digest("a"),
        deployment_id=_digest("b"),
        previous_deployment_id=_digest("c"),
        source_inventory_sha256=_digest("d"),
        runtime_context_sha256=_digest("e"),
        write_subjects=write_subjects
        or tuple(sorted(subject for resource in resources for subject in resource.write_subjects)),
        resources=resources,
    )


def _receipt(request: DbtWorkspaceActivationRequest, state: str = "PREPARED") -> DbtWorkspaceActivationReceipt:
    return DbtWorkspaceActivationReceipt.build(
        activation_id=request.activation_id,
        request_sha256=request.request_sha256,
        state=state,
        guard_epochs=tuple(
            DbtWorkspaceGuardEpoch(item.guard_id, index + 1) for index, item in enumerate(request.resources)
        ),
    )


def test_request_and_receipt_bind_complete_sorted_resource_closure():
    request = _request(
        _resource("mssql://service/warehouse/mart/orders"),
        _resource("mssql://service/warehouse/mart/payments"),
    )
    receipt = _receipt(request)

    assert require_activation_receipt(receipt, request, state="PREPARED") is receipt
    assert request.request_sha256.startswith("sha256:")
    assert receipt.receipt_sha256.startswith("sha256:")


@pytest.mark.parametrize(
    "resources",
    [
        (),
        (_resource("z"), _resource("a")),
        (_resource(), _resource()),
        [_resource()],
    ],
)
def test_partial_unsorted_duplicate_or_non_tuple_resource_closure_is_rejected(resources):
    with pytest.raises(DbtWorkspaceActivationError, match="(?:write|resource)_closure"):
        _build(resources)


def test_resource_partition_must_cover_each_write_exactly_once():
    with pytest.raises(DbtWorkspaceActivationError, match="resource_partition"):
        _build((_resource(),), write_subjects=(_digest("4"), _digest("5")))


def test_self_consistent_copy_with_changed_context_has_a_different_subject():
    request = _request()
    changed = DbtWorkspaceActivationRequest.build(
        activation_id=request.activation_id,
        environment=request.environment,
        release_id=request.release_id,
        deployment_id=request.deployment_id,
        previous_deployment_id=request.previous_deployment_id,
        source_inventory_sha256=request.source_inventory_sha256,
        runtime_context_sha256=_digest("f"),
        write_subjects=request.write_subjects,
        resources=request.resources,
    )
    assert changed.request_sha256 != request.request_sha256


@pytest.mark.parametrize("state", ["PREPARED", "ACTIVE", "RETIRING", "RETIRED"])
def test_all_saga_states_require_exact_durable_readback(state):
    request = _request()
    receipt = _receipt(request, state)
    assert require_activation_receipt(receipt, request, state=state) == receipt


def test_stale_foreign_or_partial_receipt_is_rejected():
    request = _request()
    receipt = _receipt(request)
    mutations = (
        DbtWorkspaceActivationReceipt.build(
            activation_id="53fdb2bf-2245-43bd-b8bd-04769012508a",
            request_sha256=receipt.request_sha256,
            state=receipt.state,
            guard_epochs=receipt.guard_epochs,
        ),
        DbtWorkspaceActivationReceipt.build(
            activation_id=receipt.activation_id,
            request_sha256=_digest("9"),
            state=receipt.state,
            guard_epochs=receipt.guard_epochs,
        ),
        DbtWorkspaceActivationReceipt.build(
            activation_id=receipt.activation_id,
            request_sha256=receipt.request_sha256,
            state="ACTIVE",
            guard_epochs=receipt.guard_epochs,
        ),
        DbtWorkspaceActivationReceipt.build(
            activation_id=request.activation_id,
            request_sha256=request.request_sha256,
            state="PREPARED",
            guard_epochs=(DbtWorkspaceGuardEpoch("mssql://foreign", 1),),
        ),
    )
    for mutation in mutations:
        with pytest.raises(DbtWorkspaceActivationError):
            require_activation_receipt(mutation, request, state="PREPARED")


def test_receipt_digest_cannot_be_reused_after_epoch_change():
    receipt = _receipt(_request())
    with pytest.raises(DbtWorkspaceActivationError, match="receipt_subject"):
        replace(receipt, guard_epochs=(replace(receipt.guard_epochs[0], fencing_epoch=2),))
