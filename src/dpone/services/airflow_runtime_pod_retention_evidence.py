"""Pure event sequencing for crash-visible runtime Pod deletion evidence."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from dataclasses import dataclass

_SCHEMA = "dpone.airflow-runtime-pod-retention-event.v1"


@dataclass(slots=True)
class AirflowRuntimePodRetentionEvidenceSequence:
    publish_event: Callable[[dict[str, object]], None]
    operation_id: str
    namespace: str
    observed_at: str
    actor: str
    credential_mode: str
    selected_count: int
    _sequence: int = 0

    @classmethod
    def start(
        cls,
        *,
        publish_event: Callable[[dict[str, object]], None],
        namespace: str,
        observed_at: str,
        actor: str,
        credential_mode: str,
        credential_context: str | None,
        selected_preconditions: tuple[str, ...],
    ) -> AirflowRuntimePodRetentionEvidenceSequence:
        identity = {
            "actor": actor,
            "credential_mode": credential_mode,
            "credential_context": credential_context,
            "namespace": namespace,
            "observed_at": observed_at,
            "selected_preconditions": list(selected_preconditions),
        }
        digest = hashlib.sha256(
            json.dumps(identity, ensure_ascii=True, separators=(",", ":"), sort_keys=True).encode("utf-8")
        ).hexdigest()
        sequence = cls(
            publish_event=publish_event,
            operation_id=f"sha256:{digest}",
            namespace=namespace,
            observed_at=observed_at,
            actor=actor,
            credential_mode=credential_mode,
            selected_count=len(selected_preconditions),
        )
        sequence.publish(event="operation_started", outcome="started")
        return sequence

    def intent(self, item: dict[str, object]) -> None:
        self.publish(event="delete_intent", outcome="intent_recorded", item=item)

    def outcome(self, item: dict[str, object], *, outcome: str) -> None:
        self.publish(
            event="delete_outcome",
            outcome=outcome,
            item=item,
            error_code=str(item["error_code"]) if item.get("error_code") else None,
        )

    def complete(self, status: str) -> None:
        self.publish(event="operation_completed", outcome=status)

    def publish(
        self,
        *,
        event: str,
        outcome: str,
        item: dict[str, object] | None = None,
        error_code: str | None = None,
    ) -> None:
        self._sequence += 1
        self.publish_event(
            {
                "schema": _SCHEMA,
                "operation_id": self.operation_id,
                "sequence": self._sequence,
                "event": event,
                "namespace": self.namespace,
                "observed_at": self.observed_at,
                "actor": self.actor,
                "credential_mode": self.credential_mode,
                "selected_count": self.selected_count,
                "pod_ref": item.get("pod_ref") if item else None,
                "precondition_ref": item.get("precondition_ref") if item else None,
                "pod_name": item.get("pod_name") if item else None,
                "outcome": outcome,
                "error_code": error_code,
            }
        )


__all__ = ["AirflowRuntimePodRetentionEvidenceSequence"]
