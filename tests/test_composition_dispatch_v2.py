"""Whole-cell v2 carries scheduler identity, never rows or caller execution inputs."""

from dataclasses import asdict

import pytest

from dpone.contracts.airflow_correlation import AirflowAttemptCorrelation
from dpone.contracts.airflow_run_identity import AirflowArtifactIdentity, AirflowRunIdentity
from dpone.contracts.composition_dispatch_v2 import (
    DispatchV2Request,
    decode_request,
    encode_request,
)
from dpone.contracts.composition_identity import CompositionAdmissionError
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object
from tests.test_composition_clickhouse_dispatch import create

SHA = "sha256:" + "a" * 64
UUID = "11223344-1122-4122-8122-112233445566"


def request():
    attempt = create().attempt
    run = AirflowRunIdentity(SHA, SHA, AirflowArtifactIdentity(attempt.workload_id, attempt.pack_sha256))
    scheduler = AirflowAttemptCorrelation(
        "dag", attempt.task_id, attempt.dag_run_id, attempt.try_number, attempt.map_index
    )
    subject = canonical_json_bytes(
        {
            "attempt_document": {"schema": "dpone.composition-attempt.v1", **asdict(attempt)},
            "attempt_sha256": attempt.attempt_sha256,
            "airflow_run_identity": run.to_dict(),
            "airflow_attempt": scheduler.to_dict(),
        }
    )
    return DispatchV2Request(UUID, UUID, SHA, "EXECUTE_TRANSFER", subject)


def test_exact_request_roundtrip_without_payload():
    original = request()
    assert decode_request(encode_request(original)) == original
    assert original.attempt == create().attempt
    assert original.payload_spec == (0, None)


@pytest.mark.parametrize("change", ["payload", "extra", "scheduler", "pack", "missing_null", "version"])
def test_request_rejects_open_noncanonical_and_contradictory_documents(change):
    original = request()
    body = strict_json_object(original.to_bytes())
    if change == "payload":
        frame = encode_request(original) + b"native"
    else:
        if change == "extra":
            body["subject"]["manifest"] = {}
        elif change == "scheduler":
            body["subject"]["airflow_attempt"]["try_number"] += 1
        elif change == "pack":
            body["subject"]["airflow_run_identity"]["workload_pack"]["id"] = "foreign"
        elif change == "missing_null":
            del body["subject"]["airflow_run_identity"]["dag_spec"]
        else:
            body["schema"] = "dpone.composition-dispatch-rpc.v1"
        metadata = canonical_json_bytes(body)
        frame = len(metadata).to_bytes(4, "big") + metadata
    with pytest.raises(CompositionAdmissionError):
        decode_request(frame)


def test_noncanonical_or_oversized_metadata_rejects():
    for metadata in (b" " + request().to_bytes(), b"x" * (1024 * 1024 + 1)):
        with pytest.raises(CompositionAdmissionError):
            decode_request(len(metadata).to_bytes(4, "big") + metadata)


@pytest.mark.parametrize(
    "field,value",
    [
        ("request_id", "11223344-1122-1122-8122-112233445566"),
        ("dispatcher_id", "00000000-0000-0000-0000-000000000000"),
        ("runtime_authority_sha256", "SHA256:" + "A" * 64),
        ("operation", "OPEN_GATE"),
    ],
)
def test_request_common_identity_is_closed(field, value):
    from dataclasses import replace

    with pytest.raises(CompositionAdmissionError):
        replace(request(), **{field: value})


def test_scheduler_unknown_field_and_duplicate_json_key_reject():
    body = strict_json_object(request().to_bytes())
    body["subject"]["airflow_attempt"]["extra"] = "secret"
    metadata = canonical_json_bytes(body)
    with pytest.raises(CompositionAdmissionError):
        decode_request(len(metadata).to_bytes(4, "big") + metadata)
    metadata = request().to_bytes().replace(b'"operation":', b'"operation":"EXECUTE_TRANSFER","operation":')
    with pytest.raises(CompositionAdmissionError):
        decode_request(len(metadata).to_bytes(4, "big") + metadata)


def test_response_correlates_and_decodes_retained_status():
    from dpone.contracts.composition_dispatch_v2 import DispatchV2Response, decode_response
    from tests.test_composition_remote_transfer_result import status_body

    _, body = status_body()
    original = request()
    response = DispatchV2Response.for_request(original, canonical_json_bytes(body), status="IN_PROGRESS")
    assert decode_response(response.to_bytes(), original) == response
    wire = strict_json_object(response.to_bytes())
    wire["request_id"] = "22334455-2233-4233-8233-223344556677"
    with pytest.raises(CompositionAdmissionError):
        decode_response(canonical_json_bytes(wire), original)
    wire = strict_json_object(response.to_bytes())
    wire["status"] = "SUCCEEDED"
    with pytest.raises(CompositionAdmissionError):
        decode_response(canonical_json_bytes(wire), original)


def test_response_validates_entire_success_envelope():
    from dpone.contracts.composition_dispatch_v2 import DispatchV2Response, decode_response
    from tests.test_composition_remote_transfer_result import result_body

    _, body = result_body()
    original = request()
    response = DispatchV2Response.for_request(original, canonical_json_bytes(body), status="SUCCEEDED")
    assert decode_response(response.to_bytes(), original).status == "SUCCEEDED"


def test_pinned_dag_spec_must_match_scheduler_dag_with_other_identity_unchanged():
    from dataclasses import replace

    original = request()
    subject = strict_json_object(original.subject)
    subject["airflow_run_identity"]["dag_spec"] = {"id": subject["airflow_attempt"]["dag_id"], "sha256": SHA}
    matched = replace(original, subject=canonical_json_bytes(subject))
    assert decode_request(encode_request(matched)) == matched
    subject["airflow_attempt"]["dag_id"] = "forged-dag"
    with pytest.raises(CompositionAdmissionError):
        replace(original, subject=canonical_json_bytes(subject))


def test_read_status_uses_same_closed_metadata():
    from dataclasses import replace

    original = replace(request(), operation="READ_STATUS")
    assert decode_request(encode_request(original)) == original
    assert original.payload_spec == (0, None)


@pytest.mark.parametrize(
    "operation,status,mutation,valid",
    [
        ("READ_STATUS", "UNKNOWN", None, True),
        ("EXECUTE_TRANSFER", "UNKNOWN", None, False),
        ("READ_STATUS", "IN_PROGRESS", None, False),
        ("READ_STATUS", "UNKNOWN", "foreign", False),
        ("READ_STATUS", "UNKNOWN", "extra", False),
        ("READ_STATUS", "UNKNOWN", "observation", False),
    ],
)
def test_absence_is_only_status_observation(operation, status, mutation, valid):
    from dataclasses import replace

    from dpone.contracts.composition_dispatch_v2 import DispatchV2Response, decode_response

    original = replace(request(), operation=operation)
    body = {
        "schema": "dpone.composition-remote-transfer-absence.v1",
        "attempt_sha256": original.attempt.attempt_sha256,
        "observation": "ABSENT",
    }
    if mutation == "foreign":
        body["attempt_sha256"] = SHA
    elif mutation == "extra":
        body["receipt"] = None
    elif mutation == "observation":
        body["observation"] = "NEVER_EXECUTED"
    if valid:
        response = DispatchV2Response.for_request(original, canonical_json_bytes(body), status=status)
        assert decode_response(response.to_bytes(), original) == response
    else:
        with pytest.raises(CompositionAdmissionError):
            DispatchV2Response.for_request(original, canonical_json_bytes(body), status=status)
