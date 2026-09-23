"""Durable channel/claim identities are closed and independent of local caches."""

import json
from dataclasses import replace

import pytest

from dpone.contracts.airflow_desired_state import AirflowDesiredDeployment
from dpone.contracts.dbt_workspace_channel import WorkspaceChannel, WorkspaceHandoverError
from dpone.contracts.dbt_workspace_handover import WorkspaceHandoverClaim
from tests.test_airflow_desired_state import _desired_payload


def _channel() -> WorkspaceChannel:
    return WorkspaceChannel(
        "s3://bucket/control/dev/desired.json", "sha256:" + "e" * 64, "dev", "group/repository", "master"
    )


def _claim() -> WorkspaceHandoverClaim:
    desired = AirflowDesiredDeployment.from_mapping(_desired_payload())
    return WorkspaceHandoverClaim(
        channel=_channel(),
        expected_channel_revision=0,
        claim_revision=1,
        predecessor_activation_id=None,
        predecessor_deployment_id=None,
        predecessor_request_sha256=None,
        successor_activation_id=desired.source.occurrence_id,
        successor_release_id=desired.promotion.release_id,
        successor_deployment_id=desired.promotion.deployment_id,
        desired_state_sha256=desired.sha256,
        desired_state_json=desired.to_json_bytes().decode(),
        observed_remote_revision="revision-1",
        source_inventory_sha256="sha256:" + "1" * 64,
        runtime_context_sha256="sha256:" + "2" * 64,
        authorization_subject_sha256="sha256:" + "3" * 64,
    )


def test_channel_roundtrip_and_different_desired_object_are_distinct():
    channel = _channel()
    assert WorkspaceChannel.from_mapping(channel.to_dict()) == channel
    assert (
        replace(channel, desired_state_uri="s3://bucket/control/dev/another.json").channel_sha256
        != channel.channel_sha256
    )
    assert "watcher_identity" not in channel.to_dict()


@pytest.mark.parametrize("mutation", ["extra", "missing", "digest", "schema"])
def test_channel_closed_shape(mutation):
    payload = _channel().to_dict()
    if mutation == "extra":
        payload["watcher_identity"] = "replica"
    elif mutation == "missing":
        del payload["source_ref"]
    else:
        payload["channel_sha256" if mutation == "digest" else "schema"] = "invalid"
    with pytest.raises(WorkspaceHandoverError):
        WorkspaceChannel.from_mapping(payload)


@pytest.mark.parametrize(
    "uri",
    [
        "S3://bucket/control/x",
        "s3://bucket/a/../b",
        "s3://bucket/a?token=x",
        "s3://user@bucket/a",
        "s3://bucket/a/",
        "s3://bucket//a",
    ],
)
def test_channel_rejects_noncanonical_uri(uri):
    with pytest.raises(WorkspaceHandoverError):
        replace(_channel(), desired_state_uri=uri)


def test_exact_claim_roundtrip_keeps_authoritative_uuid_and_raw_desired():
    claim = _claim()
    assert WorkspaceHandoverClaim.from_mapping(claim.to_dict()) == claim
    assert (
        claim.successor_activation_id
        == AirflowDesiredDeployment.from_json(claim.desired_state_json).source.occurrence_id
    )


@pytest.mark.parametrize(
    "change",
    [
        {"expected_channel_revision": True},
        {"claim_revision": 2},
        {"expected_channel_revision": 2**63 - 1, "claim_revision": 2**63},
        {"predecessor_deployment_id": "sha256:" + "4" * 64},
        {"successor_activation_id": "223e4567-e89b-42d3-a456-426614174000"},
        {"desired_state_sha256": "sha256:" + "4" * 64},
        {"source_inventory_sha256": "invalid"},
        {"observed_remote_revision": "x" * 1025},
    ],
)
def test_claim_refuses_partial_predecessor_or_changed_immutable_coordinates(change):
    with pytest.raises(WorkspaceHandoverError):
        replace(_claim(), **change)


def test_previous_publication_is_not_the_applied_predecessor():
    claim = replace(
        _claim(),
        predecessor_activation_id="223e4567-e89b-42d3-a456-426614174000",
        predecessor_deployment_id="sha256:" + "5" * 64,
        predecessor_request_sha256="sha256:" + "6" * 64,
    )
    assert WorkspaceHandoverClaim.from_mapping(claim.to_dict()) == claim


@pytest.mark.parametrize("field", ["claim_sha256", "runtime_context_sha256", "authorization_subject_sha256"])
def test_claim_does_not_accept_modified_fields_under_old_digest(field):
    payload = _claim().to_dict()
    payload[field] = "sha256:" + "f" * 64
    with pytest.raises(WorkspaceHandoverError):
        WorkspaceHandoverClaim.from_mapping(payload)


@pytest.mark.parametrize("model", [_channel, _claim])
def test_closed_json_boundary_rejects_duplicate_keys_and_nonfinite_values(model):
    value = model()
    encoded = json.dumps(value.to_dict())
    assert type(value).from_json(encoded) == value
    for invalid in ('{"schema":"ignored",' + encoded[1:], '{"value":NaN}', b"\xff"):
        with pytest.raises(WorkspaceHandoverError):
            type(value).from_json(invalid)
