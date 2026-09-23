"""Closed persistence structure and concrete local reaping receipt contracts."""

from hashlib import sha256

import pytest

from dpone.contracts.mssql_tds_coordinator_evidence import (
    TdsCoordinatorEvidenceKind as Kind,
)
from dpone.contracts.mssql_tds_coordinator_evidence import (
    TdsCoordinatorEvidenceRecord,
    TdsCoordinatorLocalExit,
    decode_local_exit,
    encode_local_exit,
    local_exit_observation,
)
from dpone.contracts.mssql_tds_worker import TdsChildExit, TdsProcessIdentity
from dpone.contracts.strict_json import canonical_json_bytes

H = "a" * 64
PROCESS = TdsProcessIdentity(H, "11111111-1111-1111-1111-111111111111", 234, 1)


def test_local_exit_roundtrip_and_actual_proof_binding():
    local = TdsCoordinatorLocalExit(H, PROCESS, TdsChildExit(PROCESS, -9, True), "b" * 64)
    payload = encode_local_exit(local)
    assert decode_local_exit(payload) == local
    observation = local_exit_observation(local)
    assert observation.process == PROCESS and observation.authority_sha256 == "b" * 64
    assert observation.proof_sha256 == sha256(payload).hexdigest()
    record = TdsCoordinatorEvidenceRecord(H, Kind.LOCAL_EXIT, payload)
    assert record.receipt.payload_sha256 == observation.proof_sha256
    assert record.receipt.relative_name == f"tds-coordinator-{H}-local_exit-{observation.proof_sha256}.json"


def test_extra_top_level_field_is_not_persistable():
    payload = canonical_json_bytes(
        {"schema": "dpone.tds.coordinator-build.v1", "build": {}, "profile": "synthetic", "credentials": "not allowed"}
    )
    with pytest.raises(ValueError, match="evidence_record_invalid"):
        TdsCoordinatorEvidenceRecord(H, Kind.ADMISSION, payload)


# These are deliberately structural fixtures, not valid SQL/domain observations.
OUTERS = {
    Kind.CREATE_REQUEST: {"schema": "dpone.tds.create-request.v1", "parent": {}, "object_nonce": None, "columns": []},
    Kind.ADMISSION: {"schema": "dpone.tds.coordinator-build.v1", "build": {}, "profile": None},
    Kind.REGISTRATION: {"schema": "dpone.tds.coordinator-registration.v1", "startup": {}, "admission_sha256": H},
    Kind.AUTHORITY: {
        "schema": "dpone.tds.coordinator-authority.v1",
        "operation_sha256": H,
        "execution_owner": {},
        "process": {},
        "implementation_sha256": H,
        "session": {},
        "database": {},
        "schema_observation": {},
        "lock": {},
        "transaction_count": 0,
        "implicit_transactions": False,
    },
    Kind.RESULT: {"schema": "dpone.tds.coordinator-create-result.v1", "result": {}, "evidence": None, "failure": None},
    Kind.LOCAL_EXIT: {
        "schema": "dpone.tds.coordinator-local-exit.v1",
        "operation_sha256": H,
        "process": {},
        "exit": {},
        "authority_sha256": H,
    },
}


@pytest.mark.parametrize("kind", list(OUTERS))
def test_exact_top_level_shapes_and_receipts(kind):
    from dataclasses import replace

    payload = canonical_json_bytes(OUTERS[kind])
    record = TdsCoordinatorEvidenceRecord(H, kind, payload)
    assert record.receipt.byte_count == len(payload)
    assert record.receipt.payload_sha256 == sha256(payload).hexdigest()
    assert "payload=" not in repr(record)
    with pytest.raises(ValueError):
        replace(record.receipt, relative_name="../escape.json")
    for key in OUTERS[kind]:
        missing = dict(OUTERS[kind])
        del missing[key]
        with pytest.raises(ValueError, match="record_invalid"):
            TdsCoordinatorEvidenceRecord(H, kind, canonical_json_bytes(missing))
    extra = dict(OUTERS[kind], secret="forbidden")
    with pytest.raises(ValueError, match="record_invalid"):
        TdsCoordinatorEvidenceRecord(H, kind, canonical_json_bytes(extra))


@pytest.mark.parametrize(
    "kind,limit,field",
    [
        (Kind.CREATE_REQUEST, 131072, "parent"),
        (Kind.ADMISSION, 16384, "build"),
        (Kind.REGISTRATION, 32768, "startup"),
        (Kind.AUTHORITY, 16384, "session"),
        (Kind.RESULT, 262144, "result"),
        (Kind.LOCAL_EXIT, 16384, "exit"),
    ],
)
def test_exact_payload_cap_and_first_byte_over(kind, limit, field):
    body = dict(OUTERS[kind])
    body[field] = ""
    body[field] = "x" * (limit - len(canonical_json_bytes(body)))
    payload = canonical_json_bytes(body)
    assert len(payload) == limit
    assert TdsCoordinatorEvidenceRecord(H, kind, payload).receipt.byte_count == limit
    body[field] += "x"
    with pytest.raises(ValueError, match="record_invalid"):
        TdsCoordinatorEvidenceRecord(H, kind, canonical_json_bytes(body))


@pytest.mark.parametrize(
    "payload",
    [
        b"{}",
        b"[]",
        b"null",
        b'{"schema":"x","schema":"y"}',
        b'{"schema":NaN}',
        b"\xff",
        b" " + canonical_json_bytes(OUTERS[Kind.ADMISSION]),
    ],
)
def test_ambiguous_noncanonical_or_wrong_shape_is_rejected_without_payload_echo(payload):
    with pytest.raises(ValueError) as caught:
        TdsCoordinatorEvidenceRecord(H, Kind.ADMISSION, payload)
    assert str(caught.value) == "mssql_native.tds_evidence_record_invalid"


def test_kind_scalar_alias_and_operation_mismatch_rejected():
    with pytest.raises(ValueError):
        TdsCoordinatorEvidenceRecord(H, "admission", canonical_json_bytes(OUTERS[Kind.ADMISSION]))
    with pytest.raises(ValueError):
        TdsCoordinatorEvidenceRecord("b" * 64, Kind.AUTHORITY, canonical_json_bytes(OUTERS[Kind.AUTHORITY]))
    with pytest.raises(ValueError):
        TdsCoordinatorEvidenceRecord(H, Kind.RESULT, canonical_json_bytes(OUTERS[Kind.ADMISSION]))


@pytest.mark.parametrize(
    "change", ["process", "nested_process", "reaped", "exit_bool", "pid_bool", "extra", "authority"]
)
def test_local_exit_rejects_mismatches_scalar_aliases_and_extra_fields(change):
    from dpone.contracts.strict_json import strict_json_object

    value = TdsCoordinatorLocalExit(H, PROCESS, TdsChildExit(PROCESS, 0, True), H)
    body = strict_json_object(encode_local_exit(value))
    if change == "process":
        body["process"]["pid"] += 1
    if change == "nested_process":
        body["exit"]["identity"]["pid"] += 1
    if change == "reaped":
        body["exit"]["reaped"] = False
    if change == "exit_bool":
        body["exit"]["exit_code"] = False
    if change == "pid_bool":
        body["process"]["pid"] = True
    if change == "extra":
        body["exit"]["unexpected"] = True
    if change == "authority":
        body["authority_sha256"] = "invalid"
    with pytest.raises(ValueError, match="local_exit_invalid"):
        decode_local_exit(canonical_json_bytes(body))


def test_no_unreaped_or_mismatching_typed_exit_can_produce_contained():
    other = TdsProcessIdentity(H, PROCESS.boot_id, 235, 1)
    for exit in (TdsChildExit(PROCESS, 0, False), TdsChildExit(other, 0, True)):
        with pytest.raises(ValueError, match="local_exit_invalid"):
            TdsCoordinatorLocalExit(H, PROCESS, exit, H)
