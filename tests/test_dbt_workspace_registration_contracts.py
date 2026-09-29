"""An explicit retained baseline/empty attestation is required for registration."""

import json
from dataclasses import replace

import pytest

from dpone.contracts.airflow_desired_state import AirflowDesiredDeployment
from dpone.contracts.dbt_workspace_channel import WorkspaceHandoverError
from dpone.contracts.dbt_workspace_lifecycle import workspace_request_payload
from dpone.contracts.dbt_workspace_registration import (
    WorkspaceEmptyAttestation,
    WorkspaceRegistrationActor,
    WorkspaceRegistrationInput,
    WorkspaceRegistrationInventory,
    WorkspaceRegistrationInventoryItem,
    WorkspaceRegistrationReceipt,
)
from dpone.contracts.dbt_workspace_registration_baseline import WorkspaceAdoptedCurrent
from tests.test_airflow_desired_state import _desired_payload
from tests.test_dbt_workspace_handover_contracts import _channel
from tests.test_dbt_workspace_lifecycle import _readback
from tests.test_dbt_workspace_mssql_activation_admission import _request


def _baseline():
    request = _request()
    payload = _desired_payload(occurrence_id=request.activation_id)
    payload["environment"] = request.environment
    desired = AirflowDesiredDeployment.from_mapping(payload)
    return WorkspaceAdoptedCurrent(
        activation_id=request.activation_id,
        state="ACTIVE",
        desired_state_json=desired.to_json_bytes().decode(),
        desired_state_sha256=desired.sha256,
        observed_remote_revision="old-revision",
        request_json=json.dumps(workspace_request_payload(request)),
        request_sha256=request.request_sha256,
        guard_epochs=_readback().guards,
        authorization_subject_sha256="sha256:" + "7" * 64,
    )


def _registration():
    return WorkspaceRegistrationInput(
        registration_id="923e4567-e89b-42d3-a456-426614174000",
        channel=_channel(),
        mode="empty",
        reason="Register a new channel after reviewing the complete protected inventory.",
        operator_reference="migration-review",
        inventory=WorkspaceRegistrationInventory(()),
        empty_attestation=WorkspaceEmptyAttestation("channel_never_applied", ()),
        adopted_current=None,
    )


def test_empty_registration_roundtrip_retains_explicit_attestation():
    registration = _registration()
    assert WorkspaceRegistrationInput.from_mapping(registration.to_dict()) == registration
    with pytest.raises(WorkspaceHandoverError):
        replace(registration, empty_attestation=None)


@pytest.mark.parametrize("state", ["PREPARED", "ACTIVE", "RETIRING"])
def test_even_disjoint_unbound_nonretired_occurrence_blocks_empty_registration(state):
    item = WorkspaceRegistrationInventoryItem(_request().activation_id, _request().request_sha256, state, None)
    with pytest.raises(WorkspaceHandoverError):
        replace(_registration(), inventory=WorkspaceRegistrationInventory((item,)))


def test_explicitly_bound_foreign_channel_does_not_block_empty_registration():
    item = WorkspaceRegistrationInventoryItem(
        _request().activation_id, _request().request_sha256, "ACTIVE", "sha256:" + "8" * 64
    )
    assert replace(_registration(), inventory=WorkspaceRegistrationInventory((item,))).mode == "empty"


def test_baseline_roundtrip_contains_original_request_not_only_hash():
    baseline = _baseline()
    assert WorkspaceAdoptedCurrent.from_mapping(baseline.to_dict()) == baseline
    assert baseline.request == _request()


@pytest.mark.parametrize(
    "change",
    [
        {"state": "RETIRING"},
        {"request_json": "{}"},
        {"guard_epochs": ()},
        {"request_sha256": "sha256:" + "9" * 64},
        {"desired_state_sha256": "sha256:" + "9" * 64},
    ],
)
def test_adoption_refuses_missing_original_payload_and_changed_coordinates(change):
    with pytest.raises(WorkspaceHandoverError):
        replace(_baseline(), **change)


def test_adoption_requires_exact_unbound_inventory_and_channel():
    baseline = _baseline()
    item = WorkspaceRegistrationInventoryItem(baseline.activation_id, baseline.request_sha256, "ACTIVE", None)
    registered = replace(
        _registration(),
        channel=replace(_channel(), environment="prod"),
        mode="adopt_active",
        inventory=WorkspaceRegistrationInventory((item,)),
        empty_attestation=None,
        adopted_current=baseline,
    )
    assert WorkspaceRegistrationInput.from_mapping(registered.to_dict()) == registered
    with pytest.raises(WorkspaceHandoverError):
        replace(registered, inventory=WorkspaceRegistrationInventory(()))
    with pytest.raises(WorkspaceHandoverError):
        replace(registered, channel=_channel())


@pytest.mark.parametrize("target", ["input", "inventory", "baseline"])
def test_registration_documents_reject_unknown_keys(target):
    value = {"input": _registration(), "inventory": WorkspaceRegistrationInventory(()), "baseline": _baseline()}[target]
    payload = value.to_dict()
    payload["extra"] = True
    with pytest.raises(WorkspaceHandoverError):
        type(value).from_mapping(payload)


def test_complete_registration_receipt_roundtrip_pins_input_actor_and_time():
    receipt = WorkspaceRegistrationReceipt(
        _registration(), WorkspaceRegistrationActor("operator", "registrar", 12), "2026-09-23T12:00:00.000001Z"
    )
    assert WorkspaceRegistrationReceipt.from_json(json.dumps(receipt.to_dict())) == receipt
    altered = receipt.to_dict()
    altered["database_actor"]["original_login"] = "another-operator"
    with pytest.raises(WorkspaceHandoverError):
        WorkspaceRegistrationReceipt.from_mapping(altered)
    with pytest.raises(WorkspaceHandoverError):
        replace(receipt, registered_at_utc="2026-09-23T12:00:00Z")
    with pytest.raises(WorkspaceHandoverError):
        WorkspaceRegistrationActor("operator", "registrar", True)


def test_registration_input_json_rejects_duplicate_operator_keys():
    encoded = json.dumps(_registration().to_dict())
    assert WorkspaceRegistrationInput.from_json(encoded) == _registration()
    with pytest.raises(WorkspaceHandoverError):
        WorkspaceRegistrationInput.from_json('{"reason":"replace",' + encoded[1:])


@pytest.mark.parametrize(("field", "limit"), [("reason", 1024), ("operator_reference", 256)])
def test_operator_text_limits_count_utf8_bytes_not_code_points(field, limit):
    assert replace(_registration(), **{field: "é" * (limit // 2)})
    with pytest.raises(WorkspaceHandoverError):
        replace(_registration(), **{field: "é" * (limit // 2 + 1)})
