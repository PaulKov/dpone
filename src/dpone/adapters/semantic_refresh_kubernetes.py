"""Kubernetes CoreV1 adapter for trusted semantic-refresh termination evidence."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Protocol

from dpone.ports.semantic_refresh_kubernetes import (
    ObservedContainerTermination,
    ObservedTerminalPod,
    SemanticRefreshPodObservationAuthority,
)

_UTC = timezone.utc  # noqa: UP017 - package supports Python 3.10


class KubernetesCoreV1PodPort(Protocol):
    """Minimal lazy Kubernetes client surface used by the adapter."""

    def read_namespaced_pod(self, *, name: str, namespace: str) -> Any:
        """Read one exact pod."""


class KubernetesCoreV1TerminationObserver:
    """Read terminal pod state without treating absence/deletion as termination."""

    def __init__(self, *, api: KubernetesCoreV1PodPort, cluster_id: str) -> None:
        if not cluster_id:
            raise ValueError("cluster_id is required")
        self._api = api
        self._cluster_id = cluster_id

    def observe_terminal_pod(
        self,
        authority: SemanticRefreshPodObservationAuthority,
    ) -> ObservedTerminalPod:
        """Return all terminated init, regular, and ephemeral containers."""

        if authority.cluster_id != self._cluster_id:
            raise ValueError("protected cluster differs from configured Kubernetes client")
        pod = self._api.read_namespaced_pod(name=authority.pod_name, namespace=authority.namespace)
        metadata, status = _required(pod, "metadata"), _required(pod, "status")
        uid = _text(metadata, "uid")
        if uid != authority.pod_uid:
            raise ValueError("Kubernetes pod UID differs from protected authority")
        phase = _text(status, "phase")
        if phase not in {"Succeeded", "Failed"}:
            raise ValueError("Kubernetes pod has not reached a terminal phase")
        statuses = tuple(
            item
            for field in ("init_container_statuses", "container_statuses", "ephemeral_container_statuses")
            for item in (getattr(status, field, None) or ())
        )
        containers = tuple(sorted((_termination(item) for item in statuses), key=lambda item: item.name))
        if not containers or len({item.name for item in containers}) != len(containers):
            raise ValueError("Kubernetes container termination closure is missing or ambiguous")
        return ObservedTerminalPod(
            cluster_id=self._cluster_id,
            namespace=_text(metadata, "namespace"),
            pod_name=_text(metadata, "name"),
            pod_uid=uid,
            pod_resource_version=_text(metadata, "resource_version"),
            terminal_phase=phase,
            container_terminations=containers,
        )


def _termination(status: object) -> ObservedContainerTermination:
    state = _required(status, "state")
    terminated = getattr(state, "terminated", None)
    if terminated is None:
        raise ValueError("terminal pod contains a non-terminated container")
    exit_code = getattr(terminated, "exit_code", None)
    if exit_code is not None and (isinstance(exit_code, bool) or not isinstance(exit_code, int)):
        raise ValueError("container exit code is invalid")
    return ObservedContainerTermination(
        name=_text(status, "name"),
        container_id=_text(status, "container_id"),
        reason=_text(terminated, "reason"),
        finished_at=_timestamp(getattr(terminated, "finished_at", None), "container finished_at"),
        exit_code=exit_code,
    )


def _required(value: object, field: str) -> Any:
    result = getattr(value, field, None)
    if result is None:
        raise ValueError(f"Kubernetes {field} is missing")
    return result


def _text(value: object, field: str) -> str:
    result = getattr(value, field, None)
    if not isinstance(result, str) or not result:
        raise ValueError(f"Kubernetes {field} is missing")
    return result


def _timestamp(value: object, field: str) -> str:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"Kubernetes {field} must be timezone-aware")
    return value.astimezone(_UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


__all__ = ["KubernetesCoreV1PodPort", "KubernetesCoreV1TerminationObserver"]
