"""Custody wiring uses authenticated host facts and the supplied SQL ledger.

Host, enrollment rows and process identity are explicit doubles; these checks do
not certify deployed Docker/Linux isolation.
"""

import time
from dataclasses import replace
from types import SimpleNamespace

import pytest

from dpone.app import composition_dispatcher_capture_custody as module
from dpone.contracts.composition_identity import CompositionAdmissionError
from dpone.contracts.composition_snapshot_capture import SnapshotCaptureSubject, attempt_snapshot_target
from tests.composition_snapshot_helpers import digest, intent
from tests.test_composition_clickhouse_custody_enrollment import v2_body
from tests.test_composition_clickhouse_supervisor_enrollment import enrolled


def configured(monkeypatch):
    body = v2_body()
    value = intent()
    body.update(
        service_id=value.target.service_id,
        database_uuid=value.target.database_id,
        target_enrollment_sha256=value.target.enrollment_sha256,
    )
    body["facts"]["linux"]["capture_custody"] = {
        "identity_maps": {"uid_map": [[0, 0, 4294967295]], "gid_map": [[0, 0, 4294967295]]},
        "root_identity": {"device": 1, "inode": 8, "mode": 0o700, "uid": 101, "gid": 101},
    }
    enrollment = enrolled(body)
    calls = []
    client = SimpleNamespace(
        capture=lambda reference, *, deadline: calls.append((reference, deadline)) or body["facts"]
    )
    monkeypatch.setattr(module, "CaptureSupervisorFactsClient", lambda *args, **kwargs: client)
    custody = module.DispatcherCaptureCustody(
        enrollment=enrollment, socket_path="/run/dpone/host.sock", deadline=time.monotonic() + 30
    )
    return custody, enrollment, calls, client


def test_host_custody_reopens_twice_with_one_deadline_and_no_sql(monkeypatch):
    custody, enrollment, calls, _ = configured(monkeypatch)
    monkeypatch.setattr(module, "read_service_enrollment", lambda *args: pytest.fail("host callback opened SQL"))
    assert custody.require_host() is None
    assert len(calls) == 2 and calls[0] == calls[1]
    assert calls[0][0] == enrollment.enrollment_sha256


def test_host_drift_rejects_even_boolean_integer_equivalence(monkeypatch):
    custody, enrollment, _, client = configured(monkeypatch)
    facts = enrollment.body["facts"]
    facts["linux"]["capture_custody"]["root_identity"]["device"] = True
    client.capture = lambda *args, **kwargs: facts
    with pytest.raises(CompositionAdmissionError, match="dispatcher_capture_custody"):
        custody.require_host()


def test_file_construction_uses_only_enrolled_volume_and_fresh_callback(monkeypatch):
    custody, _, _, _ = configured(monkeypatch)
    calls = []
    monkeypatch.setattr(module, "ServiceSnapshotFiles", lambda root, **kwargs: calls.append((root, kwargs)) or "files")
    assert custody.open_files() == "files"
    root, kwargs = calls[0]
    assert str(root) == "/capture"
    assert (kwargs["dispatcher_uid"], kwargs["dispatcher_gid"], kwargs["root_device"], kwargs["root_inode"]) == (
        101,
        101,
        1,
        8,
    )
    assert kwargs["require_custody"] == custody.require_host


def test_expired_deadline_cannot_call_host(monkeypatch):
    custody, _, calls, _ = configured(monkeypatch)
    monkeypatch.setattr(module.time, "monotonic", lambda: custody.deadline + 1)
    with pytest.raises(CompositionAdmissionError, match="dispatcher_capture_custody"):
        custody.require_host()
    assert calls == []


def test_enrollment_validation_reuses_supplied_transaction(monkeypatch):
    custody, enrollment, _, _ = configured(monkeypatch)
    value = intent()
    target = replace(
        attempt_snapshot_target(value.target, value.attempt),
        service_id=enrollment.body["service_id"],
        database_id=enrollment.body["database_uuid"],
        enrollment_sha256=enrollment.body["target_enrollment_sha256"],
    )
    target = attempt_snapshot_target(target, value.attempt)
    subject = SnapshotCaptureSubject(value.attempt, target, digest("source"), ("db", "dbo", "data"), value.limits)
    ledger = SimpleNamespace(
        cursor=object(),
        schema="control",
        expected_service_id="control-service",
        require_transaction=lambda expected=None: 17,
    )
    calls = []
    monkeypatch.setattr(
        module, "read_service_enrollment", lambda current, service: calls.append((current, service)) or enrollment
    )
    custody.require_enrollment_in(ledger, subject)
    assert calls == [(ledger, target.service_id)]


def test_enrollment_change_or_transaction_replacement_rejects(monkeypatch):
    custody, enrollment, _, _ = configured(monkeypatch)
    value = intent()
    target = replace(
        attempt_snapshot_target(value.target, value.attempt),
        service_id=enrollment.body["service_id"],
        database_id=enrollment.body["database_uuid"],
        enrollment_sha256=enrollment.body["target_enrollment_sha256"],
    )
    target = attempt_snapshot_target(target, value.attempt)
    subject = SnapshotCaptureSubject(value.attempt, target, digest("source"), ("db", "dbo", "data"), value.limits)
    ledger = SimpleNamespace(
        cursor=object(),
        schema="control",
        expected_service_id="control-service",
        require_transaction=lambda expected=None: 17,
    )

    def changed(current, service):
        current.cursor = object()
        return enrollment

    monkeypatch.setattr(module, "read_service_enrollment", changed)
    with pytest.raises(CompositionAdmissionError, match="dispatcher_capture_custody"):
        custody.require_enrollment_in(ledger, subject)


@pytest.mark.parametrize("kind", ["v1", "expired", "infinite", "root_owner"])
def test_invalid_profile_cannot_construct_host_client(monkeypatch, kind):
    _, enrollment, _, _ = configured(monkeypatch)
    body = enrollment.body
    deadline = time.monotonic() + 10
    if kind == "v1":
        body = v2_body()
        body["schema"] = "dpone.composition-clickhouse-supervisor-enrollment.v1"
        del body["policy"]["capture_custody"]
    elif kind == "expired":
        deadline = time.monotonic() - 1
    elif kind == "infinite":
        deadline = float("inf")
    else:
        body["facts"]["linux"]["capture_custody"]["root_identity"]["uid"] = 0
    monkeypatch.setattr(
        module, "CaptureSupervisorFactsClient", lambda *args, **kwargs: pytest.fail("client before validation")
    )
    with pytest.raises(CompositionAdmissionError, match="dispatcher_capture_custody"):
        module.DispatcherCaptureCustody(
            enrollment=enrolled(body), socket_path="/run/dpone/host.sock", deadline=deadline
        )


def test_second_host_observation_must_match_the_first_original(monkeypatch):
    custody, enrollment, _, client = configured(monkeypatch)
    observations = iter([enrollment.body["facts"], {"drift": True}])
    client.capture = lambda *args, **kwargs: next(observations)
    with pytest.raises(CompositionAdmissionError, match="dispatcher_capture_custody"):
        custody.require_host()
