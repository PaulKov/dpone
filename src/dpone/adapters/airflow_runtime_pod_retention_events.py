"""JSONL evidence adapter for runtime Pod retention mutation events."""

from __future__ import annotations

import json
from typing import Literal, TextIO

from dpone.ports.airflow_runtime_pod_retention import AirflowRuntimePodRetentionEvidenceError

_MAX_EVENT_BYTES = 16 * 1024


class JsonLinesAirflowRuntimePodRetentionEvidencePublisher:
    """Flush one bounded event per line to an infrastructure-owned stream."""

    durability: Literal["process_ordered"] = "process_ordered"

    def __init__(self, stream: TextIO) -> None:
        self._stream = stream

    def publish(self, event: dict[str, object]) -> None:
        try:
            encoded = json.dumps(event, ensure_ascii=True, separators=(",", ":"), sort_keys=True)
            if len(encoded.encode("utf-8")) > _MAX_EVENT_BYTES:
                raise ValueError("event exceeds byte capacity")
            self._stream.write(f"{encoded}\n")
            self._stream.flush()
        except (OSError, TypeError, ValueError) as exc:
            raise AirflowRuntimePodRetentionEvidenceError(
                "Airflow runtime Pod retention evidence publisher is unavailable."
            ) from exc


__all__ = ["JsonLinesAirflowRuntimePodRetentionEvidencePublisher"]
