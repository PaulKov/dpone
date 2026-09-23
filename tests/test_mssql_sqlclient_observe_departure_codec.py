"""Canonical nonsecret OBSERVE wire vectors and strict original bindings."""

import json
from dataclasses import replace
from hashlib import sha256

import pytest

from dpone.contracts import mssql_sqlclient_observe_departure_codec as codec
from dpone.contracts.mssql_sqlclient_create_departure_v2_codec import (
    decode_create_departure_v2,
    encode_create_departure_v2,
)
from dpone.contracts.mssql_sqlclient_observe_departure import SqlClientObserveContainment
from dpone.contracts.mssql_tds_result import attempt_identity_digest
from dpone.contracts.mssql_tds_worker import TdsChildExit
from dpone.contracts.strict_json import canonical_json_bytes
from tests.mssql_sqlclient_departure_v2_fixtures import alias, corrupt, leaves, sample
from tests.test_mssql_sqlclient_observe_departure_contract import request


def containment():
    p = request().plan
    return SqlClientObserveContainment(
        attempt_sha256=attempt_identity_digest(p.attempt),
        observe_operation=p.observe_operation,
        registration_artifact_sha256=p.original_registration_artifact_sha256,
        authority_artifact_sha256=p.original_authority_artifact_sha256,
        original_authority_sha256=p.original_authority_sha256,
        preparation_artifact_sha256=p.preparation_artifact_sha256,
        exit=TdsChildExit(p.observe_process, -9, True),
    )


def test_roundtrips_and_exact_digest():
    r = request()
    raw = codec.encode_observe_departure_request(r)
    assert codec.decode_observe_departure_request(raw) == r
    assert codec.observe_departure_request_digest(r) == sha256(raw).hexdigest()
    assert codec.decode_observe_departure_plan(codec.encode_observe_departure_plan(r.plan)) == r.plan
    d = sample()
    raw = codec.encode_observe_departure_observation(d)
    assert codec.decode_observe_departure_observation(raw) == d
    with pytest.raises(ValueError):
        decode_create_departure_v2(raw)
    with pytest.raises(ValueError):
        codec.decode_observe_departure_observation(encode_create_departure_v2(d))
    kw = dict(request=r, observer_admission=r.plan.observer_admission)
    result = codec.make_observe_departure_result(r, d, observer_admission=r.plan.observer_admission)
    assert codec.decode_observe_departure_result(codec.encode_observe_departure_result(result, **kw), **kw) == result
    c = containment()
    kw = dict(process=r.plan.observe_process)
    assert codec.decode_observe_containment(codec.encode_observe_containment(c, **kw), **kw) == c
    with pytest.raises(ValueError):
        codec.encode_observe_containment(c, process=r.startup.process)


@pytest.mark.parametrize(
    "mutation", ["unknown", "missing", "duplicate", "spacing", "schema", "bool", "upper_uuid", "secret"]
)
def test_closed_request_rejects(mutation):
    raw = codec.encode_observe_departure_request(request())
    body = json.loads(raw)
    if mutation == "unknown":
        body["extra"] = 0
    elif mutation == "missing":
        body.pop("startup")
    elif mutation == "schema":
        body["schema"] = "dpone.sqlclient.departure-request.v2"
    elif mutation == "bool":
        body["startup"]["process"]["pid"] = True
    elif mutation == "upper_uuid":
        body["plan"]["helper_id"] = "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA"
    elif mutation == "secret":
        body["connection_material"] = {"password": "SECRET_CANARY"}
    raw = canonical_json_bytes(body)
    if mutation == "duplicate":
        raw = raw[:-1] + b',"schema":"duplicate"}'
    elif mutation == "spacing":
        raw = b" " + raw
    with pytest.raises(ValueError) as exc:
        codec.decode_observe_departure_request(raw)
    assert "SECRET_CANARY" not in str(exc.value)


@pytest.mark.parametrize(
    "limit,decode",
    [
        (65536, codec.decode_observe_departure_plan),
        (65536, codec.decode_observe_departure_request),
        (16384, codec.decode_observe_departure_observation),
    ],
)
def test_caps_before_json(limit, decode):
    for raw in (b"", b" " * (limit + 1), bytearray(b"{}")):
        with pytest.raises(ValueError):
            decode(raw)


def test_original_scalar_alias_before_hash(monkeypatch):
    calls = []
    monkeypatch.setattr(codec, "sha256", lambda *args: calls.append(args))
    for path, value in leaves(request()):
        with pytest.raises(ValueError):
            codec.observe_departure_request_digest(corrupt(request(), path, alias(value)))
    assert not calls


def test_wrong_request_and_independent_verifier_reject():
    r = request()
    kw = dict(request=r, observer_admission=r.plan.observer_admission)
    result = codec.make_observe_departure_result(r, sample(), observer_admission=r.plan.observer_admission)
    raw = codec.encode_observe_departure_result(result, **kw)
    for changed in (replace(r, plan=replace(r.plan, preparation_artifact_sha256="a" * 64)),):
        with pytest.raises(ValueError):
            codec.decode_observe_departure_result(raw, request=changed, observer_admission=r.plan.observer_admission)
    other = replace(
        r.plan.observer_admission, login=replace(r.plan.observer_admission.login, name="other", original_name="other")
    )
    with pytest.raises(ValueError):
        codec.decode_observe_departure_result(raw, request=r, observer_admission=other)


def maximum_request(character):
    from tests.test_mssql_sqlclient_departure_ipc_v2 import maximum_request as old_maximum
    from tests.test_mssql_sqlclient_observe_departure_contract import from_create_request

    old, d = old_maximum(character)
    return from_create_request(old), d


@pytest.mark.parametrize("character", ['"', "\\", "\uffff", "\U0010ffff"])
def test_joint_largest_valid_fixture(character):
    from tests.test_mssql_sqlclient_observe_departure_evidence import chain

    r, d = maximum_request(character)
    p = codec.encode_observe_departure_plan(r.plan)
    raw = codec.encode_observe_departure_request(r)
    result = codec.make_observe_departure_result(r, d, observer_admission=r.plan.observer_admission)
    result_raw = codec.encode_observe_departure_result(result, request=r, observer_admission=r.plan.observer_admission)
    observation = codec.encode_observe_departure_observation(d)
    assert len(p) <= 65536 and len(raw) <= 65536 and len(result_raw) <= 32768 and len(observation) <= 16384
    assert codec.decode_observe_departure_request(raw) == r
    _, _, payloads = chain(r, d)
    assert all(len(b) <= cap for b, cap in zip(payloads, (131072, 32768, 131072, 65536, 16384, 16384), strict=True))
    print("sizes", repr(character), len(p), len(raw), len(observation), len(result_raw), [len(b) for b in payloads])


def test_process_domain_and_containment_receipt_shape():
    from dpone.contracts.mssql_sqlclient_observe_departure import (
        SqlClientObserveContainmentObservation,
        SqlClientObserveContainmentReceipt,
    )
    from dpone.contracts.mssql_tds_directory_codec import process_identity_digest
    from dpone.contracts.mssql_tds_worker import TdsProcessIdentity

    process = TdsProcessIdentity("a" * 64, "11111111-1111-4111-8111-111111111111", 123, 456)
    assert process_identity_digest(process) == "72173b1530fb18595d0c9de57a00df21c0c6e942646dd03cc2fba42532a63137"
    receipt = SqlClientObserveContainmentReceipt(
        "a" * 64, "tds-sqlclient-observe-containment-" + "a" * 64 + "-" + "b" * 64 + ".json", "b" * 64, 16384
    )
    assert SqlClientObserveContainmentObservation("a" * 64, receipt).receipt is receipt
    assert SqlClientObserveContainmentObservation("a" * 64, None).receipt is None
    for change in ({"byte_count": 16385}, {"byte_count": True}, {"relative_name": "../secret"}):
        with pytest.raises(ValueError):
            replace(receipt, **change)
    with pytest.raises(ValueError):
        SqlClientObserveContainmentObservation("c" * 64, receipt)


def test_nested_uuid_raw_integer_corruption_rejected():
    from copy import deepcopy

    r = deepcopy(request())
    object.__setattr__(r.plan.helper_id, "int", True)
    with pytest.raises(ValueError):
        codec.encode_observe_departure_request(r)
    r = deepcopy(request())
    object.__setattr__(r.plan.observe_operation.operation_id, "int", True)
    with pytest.raises(ValueError):
        codec.encode_observe_departure_request(r)


def test_every_census_scalar_rejects_alias():
    for path, value in leaves(sample()):
        with pytest.raises(ValueError):
            codec.encode_observe_departure_observation(corrupt(sample(), path, alias(value)))


@pytest.mark.parametrize("code", [-255, -9, 0, 255])
def test_original_exit_range_preserved(code):
    c = replace(containment(), exit=replace(containment().exit, exit_code=code))
    raw = codec.encode_observe_containment(c, process=request().plan.observe_process)
    assert json.loads(raw)["exit"]["exit_code"] == code
    with pytest.raises(ValueError):
        replace(c, exit=replace(c.exit, reaped=False))
    with pytest.raises(ValueError):
        replace(c, exit=replace(c.exit, exit_code=True))


def test_actual_containment_receipt_hash_and_secret_rejection(monkeypatch):
    p = request().plan.observe_process
    raw = codec.encode_observe_containment(containment(), process=p)
    receipt = codec.observe_containment_receipt(raw, process=p)
    assert receipt.payload_sha256 == sha256(raw).hexdigest() and receipt.byte_count == len(raw)
    calls = []
    monkeypatch.setattr(codec, "sha256", lambda *args: calls.append(args))
    body = json.loads(raw)
    body["connection_material"] = {"password": "SECRET_CANARY"}
    for invalid in (canonical_json_bytes(body), b" " * 16385):
        with pytest.raises(ValueError) as exc:
            codec.observe_containment_receipt(invalid, process=p)
        assert "SECRET_CANARY" not in str(exc.value)
    assert not calls
