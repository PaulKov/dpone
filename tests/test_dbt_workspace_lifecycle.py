"""Closed historical identity and recovery payload validation."""

import json
from dataclasses import replace

import pytest

from dpone.contracts.airflow_deployment import canonical_fingerprint
from dpone.contracts.dbt_workspace_activation import DbtWorkspaceActivationError
from dpone.contracts.dbt_workspace_lifecycle import (
    DbtWorkspaceHistoricalGuard,
    DbtWorkspaceLifecycleIdentity,
    DbtWorkspaceLifecycleReadback,
    parse_workspace_request,
    workspace_request_payload,
)
from tests.test_dbt_workspace_mssql_activation_admission import _digest, _request


def _readback():
    request = _request()
    resource = request.resources[0]
    return DbtWorkspaceLifecycleReadback(
        DbtWorkspaceLifecycleIdentity.from_request(request),
        request.request_sha256,
        "ACTIVE",
        (
            DbtWorkspaceHistoricalGuard(
                resource.guard_id, canonical_fingerprint(resource.to_dict()), 1, resource.write_subjects
            ),
        ),
    )


def test_saved_request_roundtrip_preserves_original_subject():
    request = _request()
    assert parse_workspace_request(json.loads(json.dumps(workspace_request_payload(request)))) == request


@pytest.mark.parametrize("mutation", ["extra", "missing", "schema", "resource", "digest", "subjects"])
def test_saved_request_rejects_open_or_mutated_shape(mutation):
    payload = json.loads(json.dumps(workspace_request_payload(_request())))
    if mutation == "extra":
        payload["unknown"] = True
    elif mutation == "missing":
        del payload["activation_id"]
    elif mutation == "schema":
        payload["schema"] = "unknown"
    elif mutation == "resource":
        payload["resources"][0]["unknown"] = True
    elif mutation == "digest":
        payload["request_sha256"] = _digest("f")
    else:
        payload["write_subjects"] = "invalid"
    with pytest.raises(DbtWorkspaceActivationError):
        parse_workspace_request(payload)


@pytest.mark.parametrize("change", [{"activation_id": "invalid"}, {"release_id": "invalid"}, {"write_subjects": ()}])
def test_historical_identity_rejects_invalid_coordinates(change):
    with pytest.raises(DbtWorkspaceActivationError):
        replace(_readback().identity, **change)


@pytest.mark.parametrize("mutation", ["resource", "epoch", "request", "membership", "partial"])
def test_historical_readback_cannot_change_owned_subjects_or_epochs(mutation):
    previous = _readback()
    with pytest.raises(DbtWorkspaceActivationError):
        if mutation == "request":
            current = replace(previous, request_sha256=_digest("e"))
        elif mutation == "partial":
            current = replace(previous, guards=())
        else:
            changes = {
                "resource": {"resource_sha256": _digest("e")},
                "epoch": {"fencing_epoch": 2},
                "membership": {"write_subjects": (_digest("e"),)},
            }
            current = replace(previous, guards=(replace(previous.guards[0], **changes[mutation]),))
        current.require_same_ownership(previous)


def test_terminal_readback_may_preserve_historical_epochs():
    active = _readback()
    replace(active, state="RETIRED").require_same_ownership(active)
