"""Publisher adapter that accepts only byte-bound durable sink acknowledgements."""

from __future__ import annotations

from typing import Literal

from dpone.ports.airflow_runtime_pod_retention import (
    AirflowRuntimePodRetentionDurableSink,
    AirflowRuntimePodRetentionEvidenceError,
)


class AcknowledgedAirflowRuntimePodRetentionEvidencePublisher:
    durability: Literal["durable_acknowledged"] = "durable_acknowledged"

    def __init__(self, sink: AirflowRuntimePodRetentionDurableSink) -> None:
        self._sink = sink

    def publish(self, event: dict[str, object]) -> None:
        exact_event = dict(event)
        try:
            acknowledgement = self._sink.append_and_ack(exact_event)
            acknowledgement.require_matches(exact_event)
        except Exception:  # noqa: BLE001 - vendor sink failures are redacted here.
            raise AirflowRuntimePodRetentionEvidenceError(
                "runtime Pod retention durable evidence was not acknowledged"
            ) from None


__all__ = ["AcknowledgedAirflowRuntimePodRetentionEvidencePublisher"]
