"""Pure classification policy for Airflow Connection Secret metadata."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from dpone_airflow_pack.connection_names import require_kubernetes_dns_label
from dpone_airflow_pack.connection_secret_identity import secret_name_reference
from dpone_airflow_pack.connection_secret_lifecycle import (
    ATTEMPT_REF_ANNOTATION,
    CLEANUP_POLICY_LABEL,
    LIFECYCLE_VERSION,
    LIFECYCLE_VERSION_LABEL,
    MANAGED_BY_LABEL,
    MANAGED_BY_VALUE,
    POD_RESOURCE_KIND,
    RESOURCE_KIND_LABEL,
    SECRET_REF_ANNOTATION,
    SECRET_RESOURCE_KIND,
    is_sha256_reference,
)

from dpone.services.airflow_connection_secret_gc_models import (
    AirflowConnectionSecretGcError,
    invalid_input,
)

if TYPE_CHECKING:
    from dpone.ports.airflow_connection_secret_gc import KubernetesMetadataSnapshot

_FUTURE_SKEW_SECONDS = 300


@dataclass(frozen=True, slots=True)
class AirflowConnectionSecretGcCandidate:
    snapshot: KubernetesMetadataSnapshot
    secret_ref: str
    attempt_ref: str
    cleanup_policy: str
    age_seconds: int
    reason: str


@dataclass(frozen=True, slots=True)
class AirflowConnectionSecretGcClassification:
    status: str
    inventory: dict[str, int]
    items: tuple[dict[str, object], ...]
    candidates: tuple[AirflowConnectionSecretGcCandidate, ...]


def classify_airflow_connection_secret_inventory(
    *,
    secrets: tuple[KubernetesMetadataSnapshot, ...],
    pods: tuple[KubernetesMetadataSnapshot, ...],
    observed_at: datetime,
    minimum_age_seconds: int,
) -> AirflowConnectionSecretGcClassification:
    """Classify one complete metadata inventory without side effects."""

    active_pairs = _active_pairs(pods)
    valid_secret_pairs: set[tuple[str, str]] = set()
    items: list[dict[str, object]] = []
    candidates: list[AirflowConnectionSecretGcCandidate] = []
    seen_refs: set[str] = set()
    for secret in secrets:
        candidate, item = _classify_secret(
            secret,
            active_pairs=active_pairs,
            observed_at=observed_at,
            minimum_age_seconds=minimum_age_seconds,
        )
        secret_ref = str(item["secret_ref"])
        if secret_ref in seen_refs:
            raise AirflowConnectionSecretGcError(
                "DPONE_AIRFLOW_SECRET_GC_INVENTORY_INVALID",
                "Managed Secret inventory contains duplicate digest references.",
            )
        seen_refs.add(secret_ref)
        items.append(item)
        if candidate is not None:
            candidates.append(candidate)
            valid_secret_pairs.add((candidate.secret_ref, candidate.attempt_ref))
        elif item["action"] != "quarantine":
            valid_secret_pairs.add((secret_ref, str(item.get("attempt_ref") or "")))
    quarantined = sum(item["action"] == "quarantine" for item in items)
    status = "needs_attention" if quarantined else "needs_cleanup" if candidates else "ok"
    return AirflowConnectionSecretGcClassification(
        status=status,
        inventory={
            "managed_secrets": len(secrets),
            "managed_pods": len(pods),
            "active_references": len(active_pairs & valid_secret_pairs),
            "orphan_pod_references": len(active_pairs - valid_secret_pairs),
            "quarantined": quarantined,
        },
        items=tuple(sorted(items, key=lambda item: str(item["secret_ref"]))),
        candidates=tuple(sorted(candidates, key=lambda item: item.secret_ref)),
    )


def require_aware_utc(value: datetime | None) -> datetime:
    """Normalize one aware timestamp or reject ambiguous local time."""

    if value is None or value.tzinfo is None or value.utcoffset() is None:
        raise invalid_input("observation timestamps must be timezone-aware")
    return value.astimezone(UTC)


def iso_utc(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _classify_secret(
    secret: KubernetesMetadataSnapshot,
    *,
    active_pairs: set[tuple[str, str]],
    observed_at: datetime,
    minimum_age_seconds: int,
) -> tuple[AirflowConnectionSecretGcCandidate | None, dict[str, object]]:
    safe_ref = _safe_name_ref(secret.name)
    validated = _validated_secret(secret)
    if validated is None:
        return None, _plan_item(safe_ref, None, "quarantine", "invalid_metadata", None, None)
    secret_ref, attempt_ref, cleanup_policy = validated
    age_seconds = int((observed_at - require_aware_utc(secret.creation_timestamp)).total_seconds())
    if age_seconds < -_FUTURE_SKEW_SECONDS:
        return None, _plan_item(
            secret_ref,
            attempt_ref,
            "quarantine",
            "future_timestamp",
            cleanup_policy,
            age_seconds,
        )
    age_seconds = max(0, age_seconds)
    pair = (secret_ref, attempt_ref)
    if pair in active_pairs:
        return None, _plan_item(secret_ref, attempt_ref, "protect", "active_pod", cleanup_policy, age_seconds)
    if age_seconds < minimum_age_seconds:
        return None, _plan_item(secret_ref, attempt_ref, "protect", "minimum_age", cleanup_policy, age_seconds)
    reason = "retained_expired" if cleanup_policy == "retain" else "synchronous_cleanup_orphaned"
    candidate = AirflowConnectionSecretGcCandidate(
        secret,
        secret_ref,
        attempt_ref,
        cleanup_policy,
        age_seconds,
        reason,
    )
    return candidate, _plan_item(secret_ref, attempt_ref, "delete", reason, cleanup_policy, age_seconds)


def _active_pairs(pods: tuple[KubernetesMetadataSnapshot, ...]) -> set[tuple[str, str]]:
    pairs: set[tuple[str, str]] = set()
    for pod in pods:
        refs = _validated_pod_refs(pod)
        if refs is None:
            raise AirflowConnectionSecretGcError(
                "DPONE_AIRFLOW_SECRET_GC_POD_METADATA_INVALID",
                "Managed Pod lifecycle metadata is incomplete or invalid; deletion is blocked.",
            )
        pairs.add(refs)
    return pairs


def _validated_pod_refs(pod: KubernetesMetadataSnapshot) -> tuple[str, str] | None:
    expected = {
        MANAGED_BY_LABEL: MANAGED_BY_VALUE,
        RESOURCE_KIND_LABEL: POD_RESOURCE_KIND,
        LIFECYCLE_VERSION_LABEL: LIFECYCLE_VERSION,
    }
    secret_ref = pod.annotations.get(SECRET_REF_ANNOTATION)
    attempt_ref = pod.annotations.get(ATTEMPT_REF_ANNOTATION)
    if any(pod.labels.get(key) != value for key, value in expected.items()):
        return None
    if not is_sha256_reference(secret_ref) or not is_sha256_reference(attempt_ref):
        return None
    return str(secret_ref), str(attempt_ref)


def _validated_secret(secret: KubernetesMetadataSnapshot) -> tuple[str, str, str] | None:
    cleanup_policy = secret.labels.get(CLEANUP_POLICY_LABEL)
    expected = {
        MANAGED_BY_LABEL: MANAGED_BY_VALUE,
        RESOURCE_KIND_LABEL: SECRET_RESOURCE_KIND,
        LIFECYCLE_VERSION_LABEL: LIFECYCLE_VERSION,
    }
    secret_ref = secret.annotations.get(SECRET_REF_ANNOTATION)
    attempt_ref = secret.annotations.get(ATTEMPT_REF_ANNOTATION)
    if any(secret.labels.get(key) != value for key, value in expected.items()):
        return None
    if cleanup_policy not in {"retain", "after_execute"}:
        return None
    if not all((secret.name, secret.uid, secret.resource_version)) or secret.creation_timestamp is None:
        return None
    try:
        require_kubernetes_dns_label(secret.name, context="managed Secret name")
    except ValueError:
        return None
    if not is_sha256_reference(secret_ref) or not is_sha256_reference(attempt_ref):
        return None
    if str(secret_ref) != secret_name_reference(secret.name):
        return None
    try:
        require_aware_utc(secret.creation_timestamp)
    except AirflowConnectionSecretGcError:
        return None
    return str(secret_ref), str(attempt_ref), str(cleanup_policy)


def _plan_item(
    secret_ref: str,
    attempt_ref: str | None,
    action: str,
    reason: str,
    cleanup_policy: str | None,
    age_seconds: int | None,
) -> dict[str, object]:
    return {
        "secret_ref": secret_ref,
        "attempt_ref": attempt_ref,
        "action": action,
        "reason": reason,
        "cleanup_policy": cleanup_policy,
        "age_seconds": age_seconds,
    }


def _safe_name_ref(name: object) -> str:
    try:
        return secret_name_reference(name)
    except (TypeError, ValueError):
        return "sha256:" + hashlib.sha256(str(name).encode("utf-8", errors="replace")).hexdigest()


__all__ = [
    "AirflowConnectionSecretGcCandidate",
    "AirflowConnectionSecretGcClassification",
    "classify_airflow_connection_secret_inventory",
    "iso_utc",
    "require_aware_utc",
]
