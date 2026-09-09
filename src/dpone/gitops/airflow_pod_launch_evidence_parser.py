from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class GitOpsAirflowPodContainerEvidence:
    name: str
    image: str
    image_id: str
    ready: bool
    restart_count: int
    state: str
    exit_code: int | None
    reason: str | None
    message: str | None

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "image": self.image,
            "image_id": self.image_id,
            "ready": self.ready,
            "restart_count": self.restart_count,
            "state": self.state,
            "exit_code": self.exit_code,
            "reason": self.reason,
            "message": self.message,
        }


@dataclass(frozen=True, slots=True)
class GitOpsAirflowPodEventEvidence:
    type: str
    reason: str
    message: str
    count: int

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "type": self.type,
            "reason": self.reason,
            "message": self.message,
            "count": self.count,
        }


@dataclass(frozen=True, slots=True)
class GitOpsAirflowObservedPod:
    pod_name: str
    namespace: str
    phase: str
    service_account: str
    node_name: str | None
    containers: tuple[GitOpsAirflowPodContainerEvidence, ...]
    events: tuple[GitOpsAirflowPodEventEvidence, ...]
    logs_tail: str

    def container(self, name: str) -> GitOpsAirflowPodContainerEvidence | None:
        return next((item for item in self.containers if item.name == name), None)

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "pod_name": self.pod_name,
            "namespace": self.namespace,
            "phase": self.phase,
            "service_account": self.service_account,
            "node_name": self.node_name,
            "containers": [container.to_jsonable() for container in self.containers],
            "events": [event.to_jsonable() for event in self.events],
            "logs_tail": self.logs_tail,
        }


def parse_observed_pod(
    *,
    pod: Mapping[str, Any],
    events: Mapping[str, Any] | None = None,
    logs_tail: str = "",
) -> GitOpsAirflowObservedPod:
    metadata = _mapping(pod.get("metadata"))
    spec = _mapping(pod.get("spec"))
    status = _mapping(pod.get("status"))
    return GitOpsAirflowObservedPod(
        pod_name=_text(metadata.get("name")),
        namespace=_text(metadata.get("namespace")),
        phase=_text(status.get("phase")),
        service_account=_text(spec.get("serviceAccountName")),
        node_name=_optional_text(spec.get("nodeName")),
        containers=_parse_containers(status),
        events=parse_pod_events(events or {}),
        logs_tail=logs_tail,
    )


def parse_pod_events(raw_events: Mapping[str, Any]) -> tuple[GitOpsAirflowPodEventEvidence, ...]:
    items = raw_events.get("items")
    if not isinstance(items, list):
        return ()
    return tuple(_event(item) for item in items if isinstance(item, Mapping))


def _parse_containers(status: Mapping[str, Any]) -> tuple[GitOpsAirflowPodContainerEvidence, ...]:
    statuses = status.get("containerStatuses")
    if not isinstance(statuses, list):
        return ()
    return tuple(_container(item) for item in statuses if isinstance(item, Mapping))


def _container(raw: Mapping[str, Any]) -> GitOpsAirflowPodContainerEvidence:
    state, details = _state(raw.get("state"))
    return GitOpsAirflowPodContainerEvidence(
        name=_text(raw.get("name")),
        image=_text(raw.get("image")),
        image_id=_text(raw.get("imageID")),
        ready=bool(raw.get("ready")),
        restart_count=_int(raw.get("restartCount")),
        state=state,
        exit_code=_optional_int(details.get("exitCode")),
        reason=_optional_text(details.get("reason")),
        message=_optional_text(details.get("message")),
    )


def _event(raw: Mapping[str, Any]) -> GitOpsAirflowPodEventEvidence:
    return GitOpsAirflowPodEventEvidence(
        type=_text(raw.get("type")),
        reason=_text(raw.get("reason")),
        message=_text(raw.get("message")),
        count=_int(raw.get("count") or _mapping(raw.get("series")).get("count") or 1),
    )


def _state(raw_state: object) -> tuple[str, Mapping[str, Any]]:
    state = _mapping(raw_state)
    for name in ("terminated", "waiting", "running"):
        details = state.get(name)
        if isinstance(details, Mapping):
            return name, details
    return "unknown", {}


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _int(value: object) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return 0
    try:
        return int(str(value))
    except ValueError:
        return 0


def _optional_int(value: object) -> int | None:
    if value is None:
        return None
    return _int(value)


def _text(value: object) -> str:
    return str(value or "").strip()


def _optional_text(value: object) -> str | None:
    text = _text(value)
    return text or None


__all__ = [
    "GitOpsAirflowObservedPod",
    "GitOpsAirflowPodContainerEvidence",
    "GitOpsAirflowPodEventEvidence",
    "parse_observed_pod",
    "parse_pod_events",
]
