"""Immutable metadata contract used by Airflow runtime Pod policies."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import UTC, datetime  # type: ignore[attr-defined]

from dpone.contracts.kubernetes_metadata import KubernetesMetadataSnapshot


@dataclass(frozen=True, slots=True)
class TerminalPodMetadataSnapshot:
    metadata: KubernetesMetadataSnapshot
    phase: str


@dataclass(frozen=True, slots=True)
class AirflowRuntimePodOwnershipContract:
    """Injected label authority shared by inventory and pure classification."""

    managed_by_key: str
    managed_by_value: str
    runtime_contract_key: str
    runtime_contract_value: str
    workload_id_key: str


def require_aware_utc(value: datetime) -> datetime:
    """Normalize a Pod timestamp without accepting ambiguous wall time."""

    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamp must include an explicit timezone")
    return value.astimezone(UTC)


def iso_utc(value: datetime | None) -> str | None:
    if value is None:
        return None
    try:
        return require_aware_utc(value).isoformat().replace("+00:00", "Z")
    except ValueError:
        return None


def runtime_pod_ref(namespace: str, uid: str) -> str:
    identity = f"{namespace}\0{uid}".encode()
    return f"sha256:{hashlib.sha256(identity).hexdigest()}"


def runtime_pod_precondition_ref(namespace: str, uid: str, resource_version: str) -> str:
    identity = f"{namespace}\0{uid}\0{resource_version}".encode()
    return f"sha256:{hashlib.sha256(identity).hexdigest()}"


__all__ = [
    "AirflowRuntimePodOwnershipContract",
    "TerminalPodMetadataSnapshot",
    "iso_utc",
    "require_aware_utc",
    "runtime_pod_precondition_ref",
    "runtime_pod_ref",
]
