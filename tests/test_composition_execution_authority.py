"""Wire authority for composition execution: release, supervisor and evidence bytes.

These tests pin pure value semantics only. They grant no activation authority and
perform no filesystem or environment access.
"""

from __future__ import annotations

import base64
import json

import pytest
from dpone_airflow_pack.run_identity import encode_composition_supervisor

from dpone.contracts.composition_execution_authority import (
    COMPOSITION_AUTHORITY_REASONS,
    MAX_SUPERVISOR_TRANSPORT_CHARS,
    composition_evidence_object,
    deployment_supervisor_transport,
    is_composition_admission,
    release_composition_admission,
    supervisor_from_transport,
    supervisor_transport,
)
from dpone.contracts.release_composition import COMPOSITION_ADMISSION

SUPERVISOR = {
    "schema": "dpone.composition-supervisor.v1",
    "persistent_volume_claim": "dpone-composition-supervisor",
    "child_uid_start": 1_000_000_000,
    "child_gid_start": 1_000_000_000,
    "child_identity_count": 1_000_000,
}


def deployment_bytes(**overrides) -> bytes:
    payload = {
        "schema": "dpone.deployment-set.v3",
        "deployment_id": "sha256:" + "1" * 64,
        "environment": "prod",
        "composition_supervisor": dict(SUPERVISOR),
        **overrides,
    }
    return json.dumps(payload).encode("utf-8")


def test_only_the_exact_marker_is_composition_admission():
    assert is_composition_admission(COMPOSITION_ADMISSION)
    for value in ("", None, 1, COMPOSITION_ADMISSION.upper(), COMPOSITION_ADMISSION + " ", "dpone.release-set.v3"):
        assert not is_composition_admission(value)


def test_v3_release_bytes_without_constituent_authority_reject():
    release = {
        "schema": "dpone.release-set.v3",
        "producer": {"wire_contract": "dpone.release-composition.v1"},
    }
    with pytest.raises(ValueError, match="composition_release_authority"):
        release_composition_admission(json.dumps(release).encode("utf-8"))


def test_legacy_release_bytes_project_no_marker():
    release = {"schema": "dpone.release-set.v1", "artifacts": {}}
    assert release_composition_admission(json.dumps(release).encode("utf-8")) is None


@pytest.mark.parametrize("payload", [b"", b"{", b"[]", b'{"schema": "dpone.release-set.v3", "schema": "x"}'])
def test_unparsable_release_bytes_reject(payload):
    with pytest.raises(ValueError, match="composition_release_authority"):
        release_composition_admission(payload)


def test_v3_deployment_bytes_pin_the_canonical_supervisor_transport():
    transport = deployment_supervisor_transport(deployment_bytes(), admission=COMPOSITION_ADMISSION)

    assert transport == encode_composition_supervisor(SUPERVISOR)
    assert supervisor_from_transport(transport).child_uid_stop == 1_001_000_000


def test_legacy_deployment_wire_v3_carries_no_command_authority():
    assert deployment_supervisor_transport(deployment_bytes(composition_supervisor=None), admission=None) is None
    payload = json.dumps({"schema": "dpone.deployment-set.v3", "environment": "prod"}).encode("utf-8")
    assert deployment_supervisor_transport(payload, admission=None) is None


def test_legacy_release_with_sealed_supervisor_rejects():
    with pytest.raises(ValueError, match="composition_supervisor_forbidden"):
        deployment_supervisor_transport(deployment_bytes(), admission=None)


@pytest.mark.parametrize(
    ("payload", "reason"),
    [
        (deployment_bytes(composition_supervisor=None), "composition_supervisor_authority_missing"),
        (json.dumps({"schema": "dpone.deployment-set.v3"}).encode("utf-8"), "composition_supervisor_authority_missing"),
        (
            deployment_bytes(composition_supervisor={**SUPERVISOR, "extra": 1}),
            "composition_supervisor_authority_invalid",
        ),
        (deployment_bytes(composition_supervisor="forged"), "composition_supervisor_authority_invalid"),
        (
            deployment_bytes(composition_supervisor={**SUPERVISOR, "child_identity_count": 999_999}),
            "composition_supervisor_authority_invalid",
        ),
        (deployment_bytes(schema="dpone.deployment-set.v2"), "composition_supervisor_authority_invalid"),
        (b"{", "composition_supervisor_authority_invalid"),
    ],
)
def test_v3_deployment_supervisor_mirror_must_be_exact(payload, reason):
    with pytest.raises(ValueError, match=reason):
        deployment_supervisor_transport(payload, admission=COMPOSITION_ADMISSION)


@pytest.mark.parametrize(
    ("value", "reason"),
    [
        (None, "composition_supervisor_authority_missing"),
        ("", "composition_supervisor_authority_missing"),
        ("   ", "composition_supervisor_authority_invalid"),
        ("not base64 at all", "composition_supervisor_authority_invalid"),
        (base64.b64encode(b"{").decode("ascii"), "composition_supervisor_authority_invalid"),
        (base64.b64encode(b'["dpone"]').decode("ascii"), "composition_supervisor_authority_invalid"),
        (
            base64.b64encode(json.dumps(SUPERVISOR).encode("ascii")).decode("ascii"),
            "composition_supervisor_authority_noncanonical",
        ),
        (encode_composition_supervisor(SUPERVISOR).rstrip("="), "composition_supervisor_authority_invalid"),
        (
            encode_composition_supervisor(SUPERVISOR)[:8] + "\n" + encode_composition_supervisor(SUPERVISOR)[8:],
            "composition_supervisor_authority_invalid",
        ),
        (
            encode_composition_supervisor({**SUPERVISOR, "child_uid_start": 1}),
            "composition_supervisor_authority_invalid",
        ),
        ("A" * (MAX_SUPERVISOR_TRANSPORT_CHARS + 4), "composition_supervisor_authority_oversize"),
    ],
)
def test_supervisor_transport_rejects_unpinned_values(value, reason):
    with pytest.raises(ValueError, match=reason):
        supervisor_from_transport(value)


def test_supervisor_transport_round_trip_is_canonical():
    projection = supervisor_from_transport(encode_composition_supervisor(SUPERVISOR))

    assert supervisor_transport(projection) == encode_composition_supervisor(SUPERVISOR)
    assert supervisor_from_transport(supervisor_transport(projection)) == projection


def test_evidence_bytes_must_be_one_json_object():
    assert composition_evidence_object(b'{"status":"passed"}') == {"status": "passed"}
    for payload in (b"", b"   ", b"[]", b'{"a":1,"a":2}', b"not json"):
        with pytest.raises(ValueError, match="composition_evidence_invalid"):
            composition_evidence_object(payload)


def test_every_public_reason_is_a_declared_token():
    assert "composition_supervisor_authority_invalid" in COMPOSITION_AUTHORITY_REASONS
    assert all(reason.startswith("composition_") for reason in COMPOSITION_AUTHORITY_REASONS)
