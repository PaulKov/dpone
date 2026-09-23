"""Pure permission parent evidence checks; no SQL or process authority."""

from dataclasses import asdict, replace
from hashlib import sha256

import pytest

from dpone.contracts.mssql_sqlclient_permission_grant import (
    encode_permission_grant_evidence,
    encode_permission_grant_request,
)
from dpone.contracts.mssql_sqlclient_permission_grant_parent_evidence import (
    CAPS,
    PermissionGrantEvidenceSubject,
    PermissionGrantHeldReadyEvidence,
    PermissionGrantParentEvidenceContext,
    PermissionGrantParentEvidenceObservation,
    PermissionGrantParentEvidenceReceipt,
    PermissionGrantParentEvidenceRecord,
    decode_permission_grant_held_ready_evidence,
    encode_permission_grant_held_ready_evidence,
)
from dpone.contracts.mssql_sqlclient_permission_grant_parent_evidence import (
    PermissionGrantParentEvidenceKind as K,
)
from dpone.contracts.mssql_sqlclient_permission_grant_wire import (
    PermissionBoundary,
    PermissionWireBinding,
    PermissionWireKind,
    encode_permission_message,
)
from dpone.contracts.mssql_tds_coordinator import coordinator_identity_digest
from dpone.contracts.mssql_tds_coordinator_authority import encode_authority
from dpone.contracts.mssql_tds_coordinator_codec import coordinator_identity_body
from dpone.contracts.mssql_tds_coordinator_ipc import encode_registration
from dpone.contracts.mssql_tds_result import attempt_identity_digest
from dpone.contracts.mssql_tds_worker import TdsAttemptOwnership
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object
from tests.test_mssql_sqlclient_permission_grant_wire import fixture, grant_body

ERROR = "mssql_native.sqlclient_permission_parent_evidence_invalid"


def evidence_fixture():
    binding, authority, grant, evidence = fixture()
    subject = PermissionGrantEvidenceSubject(
        attempt_identity_digest(binding.request.parent), coordinator_identity_digest(binding.operation)
    )
    admission = canonical_json_bytes(
        {
            "schema": "dpone.tds.coordinator-build.v1",
            "build": {
                name: {"path": f"/verified/{name}", "sha256": sha256(name.encode()).hexdigest()}
                for name in ("interpreter", "pyodbc", "driver", "driver_manager")
            },
            "profile": "pyodbc-raw-verified-tls-v1",
        }
    )
    semantic_request = encode_permission_grant_request(binding.request)
    wire_request = encode_permission_message(
        binding,
        PermissionWireKind.REQUEST,
        1,
        {
            "operation": strict_json_object(canonical_json_bytes(coordinator_identity_body(binding.operation))),
            "execution_owner": asdict(binding.execution_owner),
            "request": strict_json_object(semantic_request),
        },
    )
    authority_payload = encode_permission_message(
        binding,
        PermissionWireKind.AUTHORITY,
        2,
        {"authority": strict_json_object(encode_authority(authority))},
    )
    result = encode_permission_grant_evidence(evidence)
    result_digest = sha256(result).hexdigest()
    check = encode_permission_message(
        binding,
        PermissionWireKind.CHECK_HELD,
        4,
        {"boundary": PermissionBoundary.READY.value, "evidence_sha256": result_digest},
    )
    held = encode_permission_message(
        binding,
        PermissionWireKind.HELD,
        4,
        {
            "boundary": PermissionBoundary.READY.value,
            "evidence_sha256": result_digest,
            "authority": strict_json_object(encode_authority(authority)),
        },
    )
    ready = encode_permission_grant_held_ready_evidence(
        PermissionGrantHeldReadyEvidence(
            subject.operation_sha256,
            result_digest,
            sha256(check).hexdigest(),
            check,
            sha256(held).hexdigest(),
            held,
        ),
        binding=binding,
    )
    payloads = {
        K.REQUEST: semantic_request,
        K.ADMISSION: admission,
        K.REGISTRATION: encode_registration(binding.startup, sha256(admission).hexdigest()),
        K.REQUEST_ACCEPTED: encode_permission_message(
            binding,
            PermissionWireKind.REQUEST_ACCEPTED,
            1,
            {"request_payload_sha256": sha256(wire_request).hexdigest()},
        ),
        K.AUTHORITY: authority_payload,
        K.EXECUTION_INTENT: encode_permission_message(
            binding, PermissionWireKind.EXECUTE, 3, {"grant": grant_body(grant)}
        ),
        K.RESULT: result,
        K.HELD_READY: ready,
    }
    return binding, subject, payloads


def records():
    binding, subject, payloads = evidence_fixture()
    return tuple(PermissionGrantParentEvidenceRecord(subject, kind, payloads[kind], binding=binding) for kind in K)


def test_all_eight_kinds_caps_names_and_receipts_are_closed():
    assert [kind.value for kind in K] == [
        "request",
        "admission",
        "registration",
        "request_accepted",
        "authority",
        "execution_intent",
        "result",
        "held_ready",
    ]
    assert list(CAPS.values()) == [131072, 16384, 32768, 16384, 16384, 16384, 1048572, 65536]
    for record in records():
        receipt = record.receipt
        assert receipt.payload_sha256 == sha256(record.payload).hexdigest()
        assert receipt.byte_count == len(record.payload)
        assert receipt.relative_name.startswith(
            f"tds-permission-v1-{record.subject.attempt_sha256}-{record.subject.operation_sha256}-{record.kind.value}-"
        )
        assert len(receipt.relative_name.encode()) <= 234
        assert PermissionGrantParentEvidenceObservation(record.subject, receipt).receipt is receipt


def test_request_accepted_binds_wire_request_not_semantic_request():
    binding, subject, payloads = evidence_fixture()
    semantic_digest = sha256(payloads[K.REQUEST]).hexdigest()
    accepted = strict_json_object(payloads[K.REQUEST_ACCEPTED])
    assert accepted["body"]["request_payload_sha256"] != semantic_digest
    PermissionGrantParentEvidenceRecord(subject, K.REQUEST_ACCEPTED, payloads[K.REQUEST_ACCEPTED], binding=binding)


@pytest.mark.parametrize("kind", list(K))
def test_non_bytes_empty_and_oversize_reject_before_content(kind):
    binding, subject, _ = evidence_fixture()
    for payload in (b"", bytearray(b"{}"), memoryview(b"{}"), b"x" * (CAPS[kind] + 1)):
        with pytest.raises(ValueError, match=ERROR):
            PermissionGrantParentEvidenceRecord(subject, kind, payload, binding=binding)


@pytest.mark.parametrize("secret_key", ["password", "token", "connection_string", "credentials", "secret"])
@pytest.mark.parametrize("level", ["outer", "build", "pin"])
def test_admission_rejects_credential_canaries_at_every_shape(secret_key, level):
    binding, subject, payloads = evidence_fixture()
    value = strict_json_object(payloads[K.ADMISSION])
    target = value if level == "outer" else value["build"]
    if level == "pin":
        target = value["build"]["driver"]
    target[secret_key] = "PRIVATE_CANARY"
    with pytest.raises(ValueError, match=ERROR) as caught:
        PermissionGrantParentEvidenceRecord(subject, K.ADMISSION, canonical_json_bytes(value), binding=binding)
    assert "PRIVATE_CANARY" not in str(caught.value)


def test_held_ready_roundtrip_and_digest_tamper_reject():
    binding, subject, payloads = evidence_fixture()
    value = decode_permission_grant_held_ready_evidence(payloads[K.HELD_READY], binding=binding)
    assert encode_permission_grant_held_ready_evidence(value, binding=binding) == payloads[K.HELD_READY]
    forged = replace(value, permission_evidence_sha256="f" * 64)
    with pytest.raises(ValueError, match=ERROR):
        encode_permission_grant_held_ready_evidence(forged, binding=binding)
    assert "payload" not in repr(records()[-1])


def test_result_rejects_self_consistent_binding_with_different_execution_owner():
    binding, subject, payloads = evidence_fixture()
    alternative = TdsAttemptOwnership(
        "another-owner", binding.execution_owner.fence, "00000000-0000-0000-0000-000000000043"
    )
    other_binding = PermissionWireBinding(
        binding.request,
        binding.operation,
        binding.startup,
        alternative,
        binding.operation_deadline_ns,
    )
    with pytest.raises(ValueError, match=ERROR):
        PermissionGrantParentEvidenceRecord(subject, K.RESULT, payloads[K.RESULT], binding=other_binding)


def test_kind_and_scalar_aliases_are_rejected():
    from enum import StrEnum

    class KindAlias(StrEnum):
        REQUEST = "request"

    binding, subject, payloads = evidence_fixture()
    with pytest.raises(ValueError, match=ERROR):
        PermissionGrantParentEvidenceRecord(subject, KindAlias.REQUEST, payloads[K.REQUEST], binding=binding)
    receipt = PermissionGrantParentEvidenceRecord(subject, K.REQUEST, payloads[K.REQUEST], binding=binding).receipt
    alias = type("TextAlias", (str,), {})(receipt.relative_name)
    with pytest.raises(ValueError, match=ERROR):
        PermissionGrantParentEvidenceReceipt(
            receipt.subject, receipt.kind, alias, receipt.payload_sha256, receipt.byte_count
        )


def test_receipt_observation_and_frozen_mutation_revalidate():
    record = records()[0]
    receipt = record.receipt
    object.__setattr__(receipt.subject, "attempt_sha256", "f" * 64)
    with pytest.raises(ValueError, match=ERROR):
        PermissionGrantParentEvidenceReceipt(
            receipt.subject, receipt.kind, receipt.relative_name, receipt.payload_sha256, receipt.byte_count
        )


def test_context_reconstructs_exact_record_and_excludes_binding_from_repr():
    binding, subject, payloads = evidence_fixture()
    context = PermissionGrantParentEvidenceContext(subject, binding)
    record = PermissionGrantParentEvidenceRecord(subject, K.REQUEST, payloads[K.REQUEST], binding=binding)
    assert context.record(record) == record
    assert "binding" not in repr(context)


def test_context_rejects_aliases_subject_mismatch_and_binding_snapshot_drift():
    binding, subject, payloads = evidence_fixture()
    with pytest.raises(ValueError, match=ERROR):
        PermissionGrantParentEvidenceContext(subject, object())
    with pytest.raises(ValueError, match=ERROR):
        PermissionGrantParentEvidenceContext(object(), binding)

    context = PermissionGrantParentEvidenceContext(subject, binding)
    different = PermissionGrantEvidenceSubject("f" * 64, subject.operation_sha256)
    with pytest.raises(ValueError, match=ERROR):
        PermissionGrantParentEvidenceContext(different, binding)

    record = PermissionGrantParentEvidenceRecord(subject, K.REQUEST, payloads[K.REQUEST], binding=binding)
    original_deadline = binding.operation_deadline_ns
    object.__setattr__(binding, "operation_deadline_ns", original_deadline + 1)
    try:
        with pytest.raises(ValueError, match=ERROR):
            context.record(record)
    finally:
        object.__setattr__(binding, "operation_deadline_ns", original_deadline)
