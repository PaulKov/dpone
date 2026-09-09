"""Kubernetes observation and signing ports for semantic-refresh takeover."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol


class SemanticRefreshPodObservationAuthority(Protocol):
    """Protected pod coordinates required by a Kubernetes observer."""

    @property
    def cluster_id(self) -> str: ...

    @property
    def namespace(self) -> str: ...

    @property
    def pod_name(self) -> str: ...

    @property
    def pod_uid(self) -> str: ...


@dataclass(frozen=True, order=True, slots=True)
class ObservedContainerTermination:
    """One container termination returned by the Kubernetes control plane."""

    name: str
    container_id: str
    reason: str
    finished_at: str
    exit_code: int | None = None


@dataclass(frozen=True, slots=True)
class ObservedTerminalPod:
    """Exact terminal pod identity and the complete container closure."""

    cluster_id: str
    namespace: str
    pod_name: str
    pod_uid: str
    pod_resource_version: str
    terminal_phase: str
    container_terminations: tuple[ObservedContainerTermination, ...]


class SemanticRefreshKubernetesObservationPort(Protocol):
    """Read one exact pod directly from a protected Kubernetes API client."""

    def observe_terminal_pod(
        self,
        authority: SemanticRefreshPodObservationAuthority,
    ) -> ObservedTerminalPod:
        """Return a terminal observation or fail closed."""


class SemanticRefreshObserverSignaturePort(Protocol):
    """Sign a canonical termination-observation subject."""

    def sign(self, *, subject_sha256: str, authority_id: str) -> str:
        """Return the protected signature digest for the exact subject."""

    def verify(self, *, subject_sha256: str, authority_id: str, signature_sha256: str) -> bool:
        """Verify the exact signature against protected observer authority."""


__all__ = [
    "ObservedContainerTermination",
    "ObservedTerminalPod",
    "SemanticRefreshKubernetesObservationPort",
    "SemanticRefreshObserverSignaturePort",
    "SemanticRefreshPodObservationAuthority",
]
