"""Contextual helper results and success-only local shape imply no observation."""

import json
from dataclasses import replace
from uuid import UUID

import pytest

from dpone.contracts.mssql_sqlclient_departure_execution_evidence import (
    SqlClientDepartureExclusion,
    SqlClientDepartureLocalExit,
    SqlClientDepartureResultEvidence,
    decode_exclusion,
    decode_local_exit,
    decode_result_evidence,
    encode_exclusion,
    encode_local_exit,
    encode_result_evidence,
)
from dpone.contracts.mssql_sqlclient_departure_ipc_codec import make_departure_result
from dpone.contracts.mssql_tds_result import attempt_identity_digest
from dpone.contracts.mssql_tds_worker import TdsChildExit
from tests.test_mssql_sqlclient_create_departure_codec import sample
from tests.test_mssql_sqlclient_departure_ipc import request
from tests.test_mssql_sqlclient_departure_registration import ERROR


def execution_values(r=None, departure=None):
    departure = sample() if departure is None else departure
    r = request(departure) if r is None else r
    subject = r.plan.helper_id, attempt_identity_digest(r.plan.attempt)
    return (
        SqlClientDepartureResultEvidence(*subject, "3" * 64, make_departure_result(r, departure)),
        SqlClientDepartureLocalExit(*subject, "2" * 64, "4" * 64, TdsChildExit(r.startup.process, 0, True)),
        SqlClientDepartureExclusion(*subject, *[str(i) * 64 for i in range(5)]),
    )


def test_execution_roundtrips():
    r = request()
    result, local, exclusion = execution_values(r)
    assert decode_result_evidence(encode_result_evidence(result, request=r), request=r) == result
    assert decode_local_exit(encode_local_exit(local)) == local
    assert decode_exclusion(encode_exclusion(exclusion)) == exclusion


@pytest.mark.parametrize(
    "field,value",
    [
        ("operation_deadline", 21.0),
        ("max_address_space_bytes", 123),
        ("helper_id", UUID(int=7)),
        ("create_result_sha256", "9" * 64),
        ("create_local_exit_sha256", "8" * 64),
        ("admission_sha256", "7" * 64),
    ],
)
def test_result_rejects_valid_changed_independent_request(field, value):
    r = request()
    result = execution_values(r)[0]
    payload = encode_result_evidence(result, request=r)
    changed = replace(request(), plan=replace(request().plan, **{field: value}))
    with pytest.raises(ValueError, match=ERROR):
        encode_result_evidence(result, request=changed)
    with pytest.raises(ValueError, match=ERROR):
        decode_result_evidence(payload, request=changed)


def test_result_rejects_wrong_creator_even_with_expected_request_hash():
    from tests.test_mssql_sqlclient_create_departure_codec import maximum

    r = request()
    result = execution_values(r)[0]
    altered = replace(result, result=replace(result.result, departure=maximum("x")))
    with pytest.raises(ValueError, match=ERROR):
        encode_result_evidence(altered, request=r)


@pytest.mark.parametrize(
    "path,field,alias",
    [
        ((), "request_sha256", "A" * 64),
        (("departure", "original"), "session_id", 72.0),
        (("departure", "admission", "login"), "is_sysadmin", 0),
        (("departure", "principal"), "principal_id", 5.0),
        (("departure",), "counts", [0] * 6),
    ],
)
def test_result_rejects_forged_nested_original(path, field, alias):
    r = request()
    value = execution_values(r)[0]
    nested = value.result
    for key in path:
        nested = getattr(nested, key)
    object.__setattr__(nested, field, alias)
    with pytest.raises(ValueError, match=ERROR):
        encode_result_evidence(value, request=r)


def test_result_rejects_mutated_original_request_before_hashing(monkeypatch):
    from dpone.contracts import mssql_sqlclient_departure_ipc_codec as ipc

    r = request()
    value = execution_values(r)[0]
    raw = encode_result_evidence(value, request=r)
    object.__setattr__(r.startup.process, "pid", 124.0)
    calls = []
    monkeypatch.setattr(ipc, "sha256", lambda *args: calls.append(args))
    for operation in (lambda: encode_result_evidence(value, request=r), lambda: decode_result_evidence(raw, request=r)):
        with pytest.raises(ValueError, match=ERROR):
            operation()
    assert not calls


@pytest.mark.parametrize(
    "field,value",
    [
        ("exit_code", False),
        ("exit_code", 0.0),
        ("exit_code", 1),
        ("exit_code", -1),
        ("reaped", 1),
        ("reaped", False),
        ("identity", {}),
    ],
)
def test_local_requires_exact_success_scalars(field, value):
    original = execution_values()[1]
    object.__setattr__(original.exit, field, value)
    with pytest.raises(ValueError, match=ERROR):
        encode_local_exit(original)


@pytest.mark.parametrize("field,value", [("pid", 124.0), ("start_ticks", True)])
def test_local_reconstructs_original_process_before_asdict(field, value, monkeypatch):
    from dpone.contracts import mssql_sqlclient_departure_execution_evidence as module

    local = execution_values()[1]
    object.__setattr__(local.exit.identity, field, value)
    calls = []
    monkeypatch.setattr(module, "asdict", lambda *args: calls.append(args))
    with pytest.raises(ValueError, match=ERROR):
        encode_local_exit(local)
    assert not calls


@pytest.mark.parametrize("codec", ["result", "local", "exclusion"])
@pytest.mark.parametrize("mutation", ["missing", "extra", "duplicate"])
def test_every_execution_outer_field_is_closed(codec, mutation):
    from dpone.contracts.strict_json import canonical_json_bytes

    r = request()
    result, local, exclusion = execution_values(r)
    raw, decode = {
        "result": (encode_result_evidence(result, request=r), lambda p: decode_result_evidence(p, request=r)),
        "local": (encode_local_exit(local), decode_local_exit),
        "exclusion": (encode_exclusion(exclusion), decode_exclusion),
    }[codec]
    original = json.loads(raw)
    for field in original:
        data = original.copy()
        if mutation == "missing":
            del data[field]
            payload = canonical_json_bytes(data)
        elif mutation == "extra":
            data[field + "_extra"] = "private-canary"
            payload = canonical_json_bytes(data)
        else:
            payload = raw[:-1] + b"," + canonical_json_bytes({field: data[field]})[1:-1] + b"}"
        with pytest.raises(ValueError, match=ERROR):
            decode(payload)
