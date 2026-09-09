"""Pure task-attempt identity policy for temporary Airflow Connection Secrets."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass

from dpone_airflow_pack.connection_names import (
    KUBERNETES_DNS_LABEL_MAX_LENGTH,
    require_kubernetes_dns_label,
)

_ATTEMPT_SUFFIX_HEX_LENGTH = 16
_ATTEMPT_SEPARATOR = "-"
_IDENTITY_ERROR_CODE = "DPONE_AIRFLOW_TASK_ATTEMPT_IDENTITY_INVALID"
ATTEMPT_VOLUME_NAME_KEY = "_dpone_attempt_volume_name"


class AirflowTaskAttemptIdentityError(ValueError):
    """Report an unusable Airflow execution identity without echoing its values."""

    code = _IDENTITY_ERROR_CODE

    def __init__(self) -> None:
        super().__init__(f"{self.code}: Airflow task attempt identity is unavailable or invalid")


@dataclass(frozen=True, slots=True)
class AirflowTaskAttemptIdentity:
    """Public Airflow task-instance key fields used for resource isolation."""

    dag_id: str
    task_id: str
    run_id: str
    try_number: int
    map_index: int

    def canonical_bytes(self) -> bytes:
        payload = {
            "dag_id": self.dag_id,
            "map_index": self.map_index,
            "run_id": self.run_id,
            "task_id": self.task_id,
            "try_number": self.try_number,
        }
        return json.dumps(payload, ensure_ascii=True, separators=(",", ":"), sort_keys=True).encode("utf-8")


@dataclass(frozen=True, slots=True)
class AirflowConnectionSecretAttempt:
    """Attempt-scoped physical name plus non-secret correlation references."""

    secret_name: str
    secret_ref: str
    attempt_ref: str


def require_airflow_task_attempt_identity(context: object) -> AirflowTaskAttemptIdentity:
    """Extract the Airflow 2/3 task-instance key or fail before credential I/O."""

    if not isinstance(context, Mapping):
        raise AirflowTaskAttemptIdentityError
    task_instance = context.get("task_instance")
    if task_instance is None:
        task_instance = context.get("ti")
    if task_instance is None:
        raise AirflowTaskAttemptIdentityError
    try:
        return AirflowTaskAttemptIdentity(
            dag_id=_require_text(getattr(task_instance, "dag_id")),
            task_id=_require_text(getattr(task_instance, "task_id")),
            run_id=_require_text(getattr(task_instance, "run_id")),
            try_number=_require_integer(getattr(task_instance, "try_number")),
            map_index=_require_integer(getattr(task_instance, "map_index")),
        )
    except (AttributeError, TypeError, ValueError):
        raise AirflowTaskAttemptIdentityError from None


def derive_airflow_connection_secret_attempt(
    base_secret_name: object,
    identity: AirflowTaskAttemptIdentity,
) -> AirflowConnectionSecretAttempt:
    """Derive one bounded, deterministic Secret name for a task attempt."""

    base = require_kubernetes_dns_label(
        base_secret_name,
        context="airflow_connection projection secret_name",
    )
    attempt_digest = hashlib.sha256(identity.canonical_bytes()).hexdigest()
    suffix = attempt_digest[:_ATTEMPT_SUFFIX_HEX_LENGTH]
    prefix_limit = KUBERNETES_DNS_LABEL_MAX_LENGTH - len(_ATTEMPT_SEPARATOR) - len(suffix)
    prefix = base[:prefix_limit].rstrip("-")
    secret_name = require_kubernetes_dns_label(
        f"{prefix}{_ATTEMPT_SEPARATOR}{suffix}",
        context="airflow_connection attempt Secret name",
    )
    return AirflowConnectionSecretAttempt(
        secret_name=secret_name,
        secret_ref=secret_name_reference(secret_name),
        attempt_ref=f"sha256:{attempt_digest}",
    )


def secret_name_reference(secret_name: object) -> str:
    """Return a digest-only reference suitable for redacted diagnostics."""

    normalized = require_kubernetes_dns_label(
        secret_name,
        context="airflow_connection projected Secret metadata.name",
    )
    return f"sha256:{hashlib.sha256(normalized.encode('utf-8')).hexdigest()}"


def bind_attempt_secret_name(
    projection: Mapping[str, object],
    attempt: AirflowConnectionSecretAttempt,
) -> dict[str, object]:
    """Bind a physical Secret while retaining one stable logical volume slot."""

    base_name = require_kubernetes_dns_label(
        projection.get("secret_name"),
        context="airflow_connection projection secret_name",
    )
    return {
        **projection,
        ATTEMPT_VOLUME_NAME_KEY: base_name,
        "secret_name": attempt.secret_name,
    }


def _require_text(value: object) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError
    return value.strip()


def _require_integer(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError
    return value


__all__ = [
    "AirflowConnectionSecretAttempt",
    "AirflowTaskAttemptIdentity",
    "AirflowTaskAttemptIdentityError",
    "ATTEMPT_VOLUME_NAME_KEY",
    "bind_attempt_secret_name",
    "derive_airflow_connection_secret_attempt",
    "require_airflow_task_attempt_identity",
    "secret_name_reference",
]
