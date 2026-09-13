"""Explicit remote observations preserve execution state and validated evidence."""

from dataclasses import replace

import pytest

from dpone.contracts.composition_dispatch_v2 import DispatchV2Response
from dpone.contracts.composition_identity import CompositionAdmissionError
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object
from tests.composition_snapshot_helpers import digest
from tests.test_composition_remote_clickhouse_worker import direct_root
from tests.test_composition_remote_transfer_result import result_body


def test_absence_before_execute_is_one_observation_without_consuming_submission():
    calls = []

    def call(wire):
        calls.append(wire)
        return DispatchV2Response.for_request(
            wire,
            canonical_json_bytes(
                {
                    "schema": "dpone.composition-remote-transfer-absence.v1",
                    "attempt_sha256": wire.attempt.attempt_sha256,
                    "observation": "ABSENT",
                }
            ),
            status="UNKNOWN",
        )

    root, typed = direct_root(call)
    for _ in range(2):
        response = root.read_status(typed)
        assert response.status == "UNKNOWN"
        assert strict_json_object(response.evidence_document)["observation"] == "ABSENT"
        assert root.can_execute_attempt()
    assert [wire.operation for wire in calls] == ["READ_STATUS", "READ_STATUS"]
    assert calls[0].request_id != calls[1].request_id
    assert calls[0].subject == calls[1].subject


def test_status_after_execute_reopens_without_another_execution_or_state_reset():
    _, body = result_body()
    calls = []

    def call(wire):
        calls.append(wire)
        return DispatchV2Response.for_request(wire, canonical_json_bytes(body), status="SUCCEEDED")

    root, typed = direct_root(call)
    root.execute(typed)
    assert root.read_status(typed).status == "SUCCEEDED"
    with pytest.raises(CompositionAdmissionError):
        root.execute(typed)
    assert [wire.operation for wire in calls] == ["EXECUTE_TRANSFER", "READ_STATUS"]
    assert not root.can_execute_attempt()


@pytest.mark.parametrize("field,value", [("manifest", {}), ("plan_sha256", digest("foreign"))])
def test_invalid_typed_status_never_sends_or_consumes_execution(field, value):
    root, typed = direct_root(lambda wire: pytest.fail("invalid status opened HTTP"))
    with pytest.raises(CompositionAdmissionError):
        root.read_status(replace(typed, **{field: value}))
    assert root.can_execute_attempt()


@pytest.mark.parametrize("failure", ["foreign", "transport"])
def test_status_errors_are_correlated_sanitized_and_never_automatically_retried(failure):
    _, body = result_body()
    calls = []

    def call(wire):
        calls.append(wire)
        if failure == "transport":
            raise RuntimeError("private-token-detail")
        response = DispatchV2Response.for_request(wire, canonical_json_bytes(body), status="SUCCEEDED")
        return replace(response, subject_sha256=digest("foreign"))

    root, typed = direct_root(call)
    with pytest.raises(CompositionAdmissionError) as error:
        root.read_status(typed)
    assert "private-token-detail" not in str(error.value)
    assert len(calls) == 1 and root.can_execute_attempt()
