from __future__ import annotations

from collections.abc import Mapping

import pytest

from dpone.adapters.airflow_runtime_pod_retention_durable import (
    AcknowledgedAirflowRuntimePodRetentionEvidencePublisher,
)
from dpone.contracts.airflow_runtime_pod_retention_ack import (
    AirflowRuntimePodRetentionEvidenceAcknowledgement,
    runtime_pod_retention_event_sha256,
)
from dpone.ports.airflow_runtime_pod_retention import AirflowRuntimePodRetentionEvidenceError


def _event() -> dict[str, object]:
    return {
        "schema": "dpone.airflow-runtime-pod-retention-event.v1",
        "operation_id": "sha256:" + "a" * 64,
        "sequence": 1,
        "event": "operation_started",
    }


class _Sink:
    def __init__(self, *, digest: str | None = None, fail: bool = False) -> None:
        self.digest = digest
        self.fail = fail
        self.events: list[dict[str, object]] = []

    def append_and_ack(self, event: Mapping[str, object]) -> AirflowRuntimePodRetentionEvidenceAcknowledgement:
        if self.fail:
            raise OSError("secret vendor failure")
        exact = dict(event)
        self.events.append(exact)
        return AirflowRuntimePodRetentionEvidenceAcknowledgement(
            operation_id=str(exact["operation_id"]),
            sequence=int(exact["sequence"]),
            event_sha256=self.digest or runtime_pod_retention_event_sha256(exact),
            sink_ref="urn:dpone-evidence:test",
        )


def test_publisher_reports_durable_only_after_exact_acknowledgement() -> None:
    sink = _Sink()
    publisher = AcknowledgedAirflowRuntimePodRetentionEvidencePublisher(sink)
    event = _event()

    publisher.publish(event)

    assert publisher.durability == "durable_acknowledged"
    assert sink.events == [event]


@pytest.mark.parametrize("sink", (_Sink(digest="sha256:" + "b" * 64), _Sink(fail=True)))
def test_publisher_maps_mismatch_or_sink_failure_to_safe_evidence_error(sink: _Sink) -> None:
    publisher = AcknowledgedAirflowRuntimePodRetentionEvidencePublisher(sink)

    with pytest.raises(AirflowRuntimePodRetentionEvidenceError, match="not acknowledged") as exc_info:
        publisher.publish(_event())

    assert "secret" not in str(exc_info.value)
