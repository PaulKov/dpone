"""Digest-only lifecycle metadata for temporary Airflow Connection Secrets."""

from __future__ import annotations

import re
from dataclasses import dataclass

MANAGED_BY_LABEL = "app.kubernetes.io/managed-by"
RESOURCE_KIND_LABEL = "dpone.dev/resource-kind"
LIFECYCLE_VERSION_LABEL = "dpone.dev/lifecycle-version"
CLEANUP_POLICY_LABEL = "dpone.dev/cleanup-policy"
SECRET_REF_ANNOTATION = "dpone.dev/secret-ref"
ATTEMPT_REF_ANNOTATION = "dpone.dev/attempt-ref"

SECRET_RESOURCE_KIND = "airflow-connection-secret"
POD_RESOURCE_KIND = "airflow-connection-pod"
LIFECYCLE_VERSION = "v1"
MANAGED_BY_VALUE = "dpone"

SECRET_LABEL_SELECTOR = ",".join(
    (
        f"{MANAGED_BY_LABEL}={MANAGED_BY_VALUE}",
        f"{RESOURCE_KIND_LABEL}={SECRET_RESOURCE_KIND}",
        f"{LIFECYCLE_VERSION_LABEL}={LIFECYCLE_VERSION}",
    )
)
POD_LABEL_SELECTOR = ",".join(
    (
        f"{MANAGED_BY_LABEL}={MANAGED_BY_VALUE}",
        f"{RESOURCE_KIND_LABEL}={POD_RESOURCE_KIND}",
        f"{LIFECYCLE_VERSION_LABEL}={LIFECYCLE_VERSION}",
    )
)

_SHA256_PATTERN = re.compile(r"^sha256:[0-9a-f]{64}$")
_CLEANUP_POLICIES = frozenset({"after_execute", "retain"})


@dataclass(frozen=True, slots=True)
class AirflowConnectionSecretLifecycle:
    """Validated safe references shared by one attempt Secret and its Pod."""

    secret_ref: str
    attempt_ref: str
    cleanup_policy: str

    def __post_init__(self) -> None:
        _require_sha256(self.secret_ref, field="secret_ref")
        _require_sha256(self.attempt_ref, field="attempt_ref")
        if self.cleanup_policy not in _CLEANUP_POLICIES:
            raise ValueError("airflow_connection Secret lifecycle cleanup_policy is invalid")

    def secret_metadata(self) -> dict[str, dict[str, str]]:
        """Return Secret labels and annotations without credential material."""

        return {
            "labels": {
                MANAGED_BY_LABEL: MANAGED_BY_VALUE,
                RESOURCE_KIND_LABEL: SECRET_RESOURCE_KIND,
                LIFECYCLE_VERSION_LABEL: LIFECYCLE_VERSION,
                CLEANUP_POLICY_LABEL: self.cleanup_policy,
            },
            "annotations": self._reference_annotations(),
        }

    def pod_metadata(self) -> dict[str, dict[str, str]]:
        """Return Pod metadata used as a metadata-only active-use lease."""

        return {
            "labels": {
                MANAGED_BY_LABEL: MANAGED_BY_VALUE,
                RESOURCE_KIND_LABEL: POD_RESOURCE_KIND,
                LIFECYCLE_VERSION_LABEL: LIFECYCLE_VERSION,
            },
            "annotations": self._reference_annotations(),
        }

    def _reference_annotations(self) -> dict[str, str]:
        return {
            SECRET_REF_ANNOTATION: self.secret_ref,
            ATTEMPT_REF_ANNOTATION: self.attempt_ref,
        }


def is_sha256_reference(value: object) -> bool:
    """Return whether a value is a canonical lowercase SHA-256 reference."""

    return isinstance(value, str) and _SHA256_PATTERN.fullmatch(value) is not None


def _require_sha256(value: object, *, field: str) -> str:
    if not is_sha256_reference(value):
        raise ValueError(f"airflow_connection Secret lifecycle {field} must be a SHA-256 reference")
    return str(value)


__all__ = [
    "ATTEMPT_REF_ANNOTATION",
    "CLEANUP_POLICY_LABEL",
    "LIFECYCLE_VERSION",
    "LIFECYCLE_VERSION_LABEL",
    "MANAGED_BY_LABEL",
    "MANAGED_BY_VALUE",
    "POD_LABEL_SELECTOR",
    "POD_RESOURCE_KIND",
    "RESOURCE_KIND_LABEL",
    "SECRET_LABEL_SELECTOR",
    "SECRET_REF_ANNOTATION",
    "SECRET_RESOURCE_KIND",
    "AirflowConnectionSecretLifecycle",
    "is_sha256_reference",
]
