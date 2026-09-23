"""Six phases remain observations; synthetic links are never producer ACKs."""

import json
from dataclasses import fields, replace
from hashlib import sha256
from uuid import UUID

import pytest

from dpone.contracts import mssql_sqlclient_observe_departure_evidence as evidence
from dpone.contracts.mssql_sqlclient_departure_evidence_types import EVIDENCE_LIMITS
from dpone.contracts.mssql_sqlclient_departure_evidence_types import SqlClientDepartureEvidenceKind as Kind
from dpone.contracts.mssql_sqlclient_departure_execution_evidence import SqlClientDepartureLocalExit, encode_local_exit
from dpone.contracts.mssql_sqlclient_departure_registration import SqlClientDepartureRegistration, encode_registration
from dpone.contracts.mssql_sqlclient_observation import session_authority_digest
from dpone.contracts.mssql_sqlclient_observe_departure_codec import (
    make_observe_departure_result,
    observe_departure_request_digest,
)
from dpone.contracts.mssql_sqlclient_observe_departure_evidence import encode_observe_departure_evidence
from dpone.contracts.mssql_tds_coordinator import coordinator_identity_digest
from dpone.contracts.mssql_tds_result import attempt_identity_digest
from dpone.contracts.mssql_tds_worker import TdsChildExit
from dpone.contracts.strict_json import canonical_json_bytes
from tests.mssql_sqlclient_departure_v2_fixtures import alias, corrupt, leaves, sample
from tests.test_mssql_sqlclient_observe_departure_contract import request


def chain(r=None, d=None):
    r = request() if r is None else r
    d = sample() if d is None else d
    kw = dict(request=r, observer_admission=r.plan.observer_admission)
    objects = [r.plan]
    payloads = []

    def put(v, k):
        raw = evidence.encode_observe_departure_evidence(v, kind=k, **kw)
        payloads.append(raw)
        return sha256(raw).hexdigest()

    h0 = put(r.plan, Kind.LAUNCH_INTENT)
    a = attempt_identity_digest(r.plan.attempt)
    helper = r.plan.helper_id
    registration = SqlClientDepartureRegistration(helper, a, h0, r.startup, r.plan.admission_sha256)
    objects.append(registration)
    h1 = put(registration, Kind.REGISTRATION)
    credential = evidence.SqlClientObserveDepartureCredentialIntent(helper, a, h1, r)
    objects.append(credential)
    h2 = put(credential, Kind.CREDENTIAL_INTENT)
    result = make_observe_departure_result(r, d, observer_admission=r.plan.observer_admission)
    result_evidence = evidence.SqlClientObserveDepartureResultEvidence(helper, a, h2, result)
    objects.append(result_evidence)
    h3 = put(result_evidence, Kind.RESULT)
    local = SqlClientDepartureLocalExit(helper, a, h1, h3, TdsChildExit(r.startup.process, 0, True))
    objects.append(local)
    h4 = put(local, Kind.LOCAL_EXIT)
    exclusion = evidence.SqlClientObserveDepartureExclusion(
        helper,
        a,
        coordinator_identity_digest(r.plan.observe_operation),
        r.plan.original_registration_artifact_sha256,
        r.plan.original_authority_artifact_sha256,
        r.plan.original_authority_sha256,
        r.plan.original_containment_artifact_sha256,
        r.plan.preparation_artifact_sha256,
        h0,
        h1,
        h2,
        observe_departure_request_digest(r),
        h3,
        h4,
        session_authority_digest(d.observer.authority).hex(),
    )
    objects.append(exclusion)
    put(exclusion, Kind.EXCLUSION)
    return r, objects, tuple(payloads)


def test_six_phase_canonical_chain_and_common_bytes():
    r, objects, payloads = chain()
    kw = dict(request=r, observer_admission=r.plan.observer_admission)
    evidence.validate_observe_exclusion_chain(objects[-1], payloads=payloads[:5], **kw)
    for kind, obj, raw in zip(Kind, objects, payloads, strict=True):
        assert evidence.decode_observe_departure_evidence(raw, kind=kind, **kw) == obj
        receipt = evidence.observe_departure_evidence_receipt(raw, kind=kind, **kw)
        assert receipt.payload_sha256 == sha256(raw).hexdigest() and receipt.byte_count == len(raw)
        assert receipt.relative_name.startswith("tds-sqlclient-departure-")
    assert payloads[1] == encode_registration(objects[1])
    assert payloads[4] == encode_local_exit(objects[4])


@pytest.mark.parametrize("field", [f.name for f in fields(evidence.SqlClientObserveDepartureExclusion)])
def test_each_exclusion_link_is_bound(field):
    r, objects, payloads = chain()
    v = objects[-1]
    bad = replace(v, **{field: UUID(int=15) if field == "helper_id" else "9" * 64})
    with pytest.raises(ValueError):
        evidence.validate_observe_exclusion_chain(
            bad, request=r, observer_admission=r.plan.observer_admission, payloads=payloads[:5]
        )


def test_every_evidence_scalar_and_caps():
    r, objects, payloads = chain()
    kw = dict(request=r, observer_admission=r.plan.observer_admission)
    for kind, obj, raw in zip(Kind, objects, payloads, strict=True):
        for path, value in leaves(obj):
            with pytest.raises(ValueError):
                evidence.encode_observe_departure_evidence(corrupt(obj, path, alias(value)), kind=kind, **kw)
        with pytest.raises(ValueError):
            evidence.decode_observe_departure_evidence(b" " * (EVIDENCE_LIMITS[kind] + 1), kind=kind, **kw)
        for key in ("unknown", "connection_material"):
            data = json.loads(raw)
            data[key] = {"password": "SECRET_CANARY"}
            with pytest.raises(ValueError) as exc:
                evidence.observe_departure_evidence_receipt(canonical_json_bytes(data), kind=kind, **kw)
            assert "SECRET_CANARY" not in str(exc.value)


def test_helper_local_exit_never_original_containment():
    r, objects, _ = chain()
    kw = dict(request=r, observer_admission=r.plan.observer_admission)
    with pytest.raises(ValueError):
        replace(objects[4], exit=TdsChildExit(r.plan.observe_process, -9, True))
    with pytest.raises(ValueError):
        evidence.encode_observe_departure_evidence(
            replace(objects[4], exit=TdsChildExit(r.plan.observe_process, 0, True)), kind=Kind.LOCAL_EXIT, **kw
        )
    with pytest.raises(ValueError):
        evidence.encode_observe_departure_evidence(
            replace(objects[1], startup=replace(r.startup, process=r.plan.observe_process)),
            kind=Kind.REGISTRATION,
            **kw,
        )


def test_launch_observe_schema():
    r = request()
    raw = encode_observe_departure_evidence(
        r.plan, kind=Kind.LAUNCH_INTENT, request=r, observer_admission=r.plan.observer_admission
    )
    assert b"dpone.sqlclient.observe-departure-launch-intent-evidence.v1" in raw
    with pytest.raises(ValueError):
        encode_observe_departure_evidence(
            {"password": "SECRET_CANARY"},
            kind=Kind.LAUNCH_INTENT,
            request=r,
            observer_admission=r.plan.observer_admission,
        )


def test_raw_uuid_boolean_alias_rejects_even_when_equal():
    from copy import deepcopy

    r = replace(request(), plan=replace(request().plan, helper_id=UUID(int=1)))
    r, objects, payloads = chain(r)
    kw = dict(request=r, observer_admission=r.plan.observer_admission)
    for kind, obj in zip(tuple(Kind)[1:], objects[1:], strict=True):
        bad = deepcopy(obj)
        object.__setattr__(bad.helper_id, "int", True)
        with pytest.raises(ValueError):
            evidence.encode_observe_departure_evidence(bad, kind=kind, **kw)


@pytest.mark.parametrize("invalid", ["request", "admission", "different_admission"])
def test_chain_rejects_raw_expectations_before_hash(monkeypatch, invalid):
    """Even a finally rejected chain must never hash unvalidated expectations."""
    r, objects, payloads = chain()
    admission = r.plan.observer_admission
    if invalid == "request":
        cases = [(corrupt(r, path, alias(value)), admission) for path, value in leaves(r)]
    elif invalid == "admission":
        cases = [(r, corrupt(admission, path, alias(value))) for path, value in leaves(admission)]
    else:
        other = replace(admission, login=replace(admission.login, name="other", original_name="other"))
        cases = [(r, other)]
    calls = []

    def trace(name, original):
        def invoke(*args, **kwargs):
            calls.append(name)
            return original(*args, **kwargs)

        return invoke

    for name in (
        "attempt_identity_digest",
        "coordinator_identity_digest",
        "observe_departure_request_digest",
        "session_authority_digest",
        "sha256",
    ):
        monkeypatch.setattr(evidence, name, trace(name, getattr(evidence, name)))
    for malformed_request, malformed_admission in cases:
        with pytest.raises(ValueError):
            evidence.validate_observe_exclusion_chain(
                objects[-1], request=malformed_request, observer_admission=malformed_admission, payloads=payloads[:5]
            )
        assert calls == []
