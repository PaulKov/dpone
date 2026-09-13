"""Closed dispatcher wire rejects changed identities and ambiguous documents."""

from dataclasses import asdict
from uuid import uuid4

import pytest

from dpone.contracts.composition_dispatch_rpc import (
    DispatchRpcError,
    DispatchRpcRequest,
    DispatchRpcResponse,
    decode_request,
    decode_response,
    encode_request,
)
from dpone.contracts.composition_persistence import encode_attempt_identity
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object
from tests.composition_snapshot_helpers import intent
from tests.test_composition_clickhouse_dispatch import insert


def request(operation="READINESS", subject=None):
    return DispatchRpcRequest(
        str(uuid4()),
        str(uuid4()),
        "sha256:" + "1" * 64,
        operation,
        canonical_json_bytes(
            subject
            or {
                "nonce": str(uuid4()),
                "configuration_sha256": "sha256:" + "2" * 64,
                "plan_sha256": "sha256:" + "3" * 64,
                "target_binding_ref": "warehouse/snapshot",
            }
        ),
    )


def test_readiness_roundtrip_and_identity_bound_response():
    value = request()
    assert decode_request(encode_request(value)) == (value, b"")
    reply = DispatchRpcResponse.for_request(value, b'{"ready":true}')
    assert decode_response(reply.to_bytes(), value) == reply
    with pytest.raises(DispatchRpcError):
        decode_response(reply.to_bytes(), request())


def test_native_payload_exact_digest_and_length():
    dispatch = insert()
    value = request(
        "DISPATCH",
        {"dispatch_document": strict_json_object(dispatch.to_bytes()), "dispatch_sha256": dispatch.dispatch_sha256},
    )
    assert decode_request(encode_request(value, b"native")) == (value, b"native")
    for payload in (b"changed", b"nativE", b"", b"native!"):
        with pytest.raises(DispatchRpcError):
            encode_request(value, payload)


@pytest.mark.parametrize("change", [{"sql": "SELECT 1"}, {"operation": "QUERY"}, {"schema": "v2"}])
def test_closed_metadata(change):
    value = request()
    metadata = strict_json_object(value.to_bytes())
    metadata.update(change)
    document = canonical_json_bytes(metadata)
    with pytest.raises(DispatchRpcError):
        decode_request(len(document).to_bytes(4, "big") + document)


def test_noncanonical_metadata_and_extra_body_rejected():
    document = request().to_bytes() + b" "
    with pytest.raises(DispatchRpcError):
        decode_request(len(document).to_bytes(4, "big") + document)
    with pytest.raises(DispatchRpcError):
        decode_request(encode_request(request()) + b"extra")


@pytest.mark.parametrize("kind", ["dispatch", "publication"])
def test_status_has_closed_journal_reference(kind):
    value = request(
        "READ_STATUS",
        {"reference_kind": kind, "reference_sha256": "sha256:" + "4" * 64, "attempt_sha256": "sha256:" + "5" * 64},
    )
    assert decode_request(encode_request(value))[0] == value


@pytest.mark.parametrize("operation", ["OPEN_GATE", "CLOSE_GATE", "OBSERVE_CATALOG"])
def test_attempt_bound_operation_documents(operation):
    original = intent()
    subject = {
        "attempt_document": strict_json_object(encode_attempt_identity(original.attempt)),
        "attempt_sha256": original.attempt.attempt_sha256,
    }
    if operation == "OBSERVE_CATALOG":
        subject.update(target=asdict(original.target), generation_uuid=original.generation.new_generation_uuid)
    else:
        subject["purpose"] = "INGEST"
    value = request(operation, subject)
    assert decode_request(encode_request(value))[0] == value
    subject["attempt_sha256"] = "sha256:" + "f" * 64
    with pytest.raises(DispatchRpcError):
        request(operation, subject)


@pytest.mark.parametrize(
    "field",
    ["request_id", "dispatcher_id", "runtime_authority_sha256", "operation", "subject_sha256", "evidence_sha256"],
)
def test_response_requires_all_identity_echoes_and_evidence_hash(field):
    value = request()
    response = strict_json_object(DispatchRpcResponse.for_request(value, b'{"observed":true}').to_bytes())
    response[field] = str(uuid4()) if field.endswith("_id") else "sha256:" + "f" * 64
    with pytest.raises(DispatchRpcError):
        decode_response(canonical_json_bytes(response), value)


@pytest.mark.parametrize("document", [b'{"schema":"a","schema":"b"}', b'{"number":NaN}', b'{"number":Infinity}'])
def test_duplicate_and_nonfinite_metadata_rejected(document):
    with pytest.raises(DispatchRpcError):
        decode_request(len(document).to_bytes(4, "big") + document)


def test_local_payload_cap_and_metadata_length_prefix():
    dispatch = insert()
    value = request(
        "DISPATCH",
        {"dispatch_document": strict_json_object(dispatch.to_bytes()), "dispatch_sha256": dispatch.dispatch_sha256},
    )
    frame = encode_request(value, b"native")
    with pytest.raises(DispatchRpcError):
        decode_request(frame, max_payload_bytes=5)
    for prefix in (0, 1024 * 1024 + 1, 2**32 - 1):
        with pytest.raises(DispatchRpcError):
            decode_request(prefix.to_bytes(4, "big") + b"{}")
