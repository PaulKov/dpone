"""Bounded validation helpers for Airflow desired-state contracts."""

from __future__ import annotations

import re
import unicodedata
from collections.abc import Mapping
from datetime import datetime
from typing import Any
from uuid import UUID

from dpone.contracts.airflow_deployment import is_canonical_sha256_digest

MAX_AIRFLOW_DESIRED_STATE_BYTES = 72 * 1024
MAX_DESIRED_STATE_REVISION_BYTES = 1024
MAX_AIRFLOW_DESIRED_DAGS = 5000

ROOT_FIELDS = frozenset({"schema", "environment", "source", "promotion", "previous", "promoted_at"})
SOURCE_FIELDS = frozenset({"project", "ref", "pipeline_id", "job_id", "occurrence_id", "git_sha"})
PROMOTION_FIELDS = frozenset(
    {
        "registry_scope_id",
        "release_id",
        "deployment_id",
        "airflow_index_sha256",
        "runtime_image_digest",
        "runtime_image_dbt_digest",
        "expected_dag_ids",
        "publication_evidence_sha256",
    }
)
PREVIOUS_FIELDS = frozenset({"revision", "deployment_id"})
_ENVIRONMENT_PATTERN = re.compile(r"[a-z0-9](?:[a-z0-9._-]{0,62}[a-z0-9])?")
_PROJECT_SEGMENT_PATTERN = re.compile(r"[A-Za-z0-9](?:[A-Za-z0-9._-]{0,126}[A-Za-z0-9])?")
_DAG_ID_PATTERN = re.compile(r"[A-Za-z0-9](?:[A-Za-z0-9_.-]{0,248}[A-Za-z0-9])?")
GIT_SHA_PATTERN = re.compile(r"(?:[0-9a-f]{40}|[0-9a-f]{64})")
_CI_ID_PATTERN = re.compile(r"[0-9]{1,32}")
_RFC3339_UTC_PATTERN = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]{1,6})?Z")
_SECRET_KEY_FRAGMENTS = (
    "access_key",
    "authorization",
    "connection_payload",
    "credential",
    "password",
    "passwd",
    "secret",
    "signed_url",
    "token",
    "vault",
)


class AirflowDesiredStateError(ValueError):
    """A desired-state value is unsafe, malformed, or non-canonical."""

    def __init__(self, code: str, message: str) -> None:
        self.code = code
        super().__init__(f"{code}: {message}")


def mapping(value: object, *, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping) or any(not isinstance(key, str) for key in value):
        raise invalid(f"{field} must be an object with string keys")
    return value


def reject_unknown(payload: Mapping[str, Any], allowed: frozenset[str], *, field: str) -> None:
    unknown = sorted(key for key in payload if key not in allowed)
    if unknown:
        raise invalid(f"{field} contains unsupported fields: {', '.join(unknown)}")


def reject_secret_like_fields(value: object) -> None:
    if isinstance(value, Mapping):
        for key, item in value.items():
            normalized = str(key).lower().replace("-", "_")
            if any(fragment in normalized for fragment in _SECRET_KEY_FRAGMENTS):
                raise AirflowDesiredStateError(
                    "DPONE_AIRFLOW_DESIRED_STATE_SECRET_FIELD",
                    "desired state contains a secret-like field",
                )
            reject_secret_like_fields(item)
    elif isinstance(value, list):
        for item in value:
            reject_secret_like_fields(item)


def text(value: object, *, field: str, maximum: int) -> str:
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or len(value) > maximum
        or any(unicodedata.category(char).startswith("C") for char in value)
    ):
        raise invalid(f"{field} must be a bounded canonical string")
    return value


def bounded_revision(value: object) -> str:
    if (
        not isinstance(value, str)
        or not value
        or len(value.encode("utf-8")) > MAX_DESIRED_STATE_REVISION_BYTES
        or any(unicodedata.category(char).startswith("C") for char in value)
    ):
        raise invalid("desired-state revision must be a bounded opaque printable token")
    return value


def environment(value: object) -> str:
    result = text(value, field="environment", maximum=64)
    if _ENVIRONMENT_PATTERN.fullmatch(result) is None:
        raise invalid("environment must be a lowercase safe identifier")
    return result


def project(value: object) -> str:
    result = text(value, field="source.project", maximum=512)
    parts = result.split("/")
    if len(parts) < 2 or any(_PROJECT_SEGMENT_PATTERN.fullmatch(part) is None for part in parts):
        raise invalid("source.project must be a safe repository path")
    return result


def ci_id(value: object, *, field: str) -> str:
    result = text(value, field=field, maximum=32)
    if _CI_ID_PATTERN.fullmatch(result) is None:
        raise invalid(f"{field} must be a decimal CI identifier")
    return result


def canonical_uuid(value: object, *, field: str) -> str:
    result = text(value, field=field, maximum=36)
    try:
        parsed = UUID(result)
    except ValueError as exc:
        raise invalid(f"{field} must be a canonical UUID") from exc
    if str(parsed) != result:
        raise invalid(f"{field} must be a canonical UUID")
    return result


def digest(value: object, *, field: str) -> str:
    if not is_canonical_sha256_digest(value):
        raise invalid(f"{field} must be a canonical lowercase sha256 digest")
    return str(value)


def optional_digest(value: object, *, field: str) -> str | None:
    return None if value is None else digest(value, field=field)


def dag_ids(value: object) -> tuple[str, ...]:
    if not isinstance(value, (list, tuple)):
        raise invalid("promotion.expected_dag_ids must be an array")
    ids = tuple(text(item, field="promotion.expected_dag_ids", maximum=250) for item in value)
    if not ids or len(ids) > MAX_AIRFLOW_DESIRED_DAGS:
        raise invalid(f"promotion.expected_dag_ids must contain 1..{MAX_AIRFLOW_DESIRED_DAGS} items")
    if any(_DAG_ID_PATTERN.fullmatch(item) is None for item in ids):
        raise invalid("promotion.expected_dag_ids contains an invalid DAG identifier")
    if len(set(ids)) != len(ids):
        raise invalid("promotion.expected_dag_ids must not contain duplicates")
    return tuple(sorted(ids))


def utc_timestamp(value: object, *, field: str) -> str:
    result = text(value, field=field, maximum=40)
    if _RFC3339_UTC_PATTERN.fullmatch(result) is None:
        raise invalid(f"{field} must be an RFC3339 UTC timestamp")
    try:
        parsed = datetime.fromisoformat(result[:-1] + "+00:00")
    except ValueError as exc:
        raise invalid(f"{field} must be an RFC3339 UTC timestamp") from exc
    offset = parsed.utcoffset()
    if offset is None or offset.total_seconds() != 0:
        raise invalid(f"{field} must be an RFC3339 UTC timestamp")
    return result


def bounded(value: bytes) -> None:
    if len(value) > MAX_AIRFLOW_DESIRED_STATE_BYTES:
        raise AirflowDesiredStateError(
            "DPONE_AIRFLOW_DESIRED_STATE_TOO_LARGE",
            "serialized desired state exceeds 72 KiB",
        )


def unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise invalid("desired state contains a duplicate JSON key")
        result[key] = value
    return result


def reject_constant(value: str) -> None:
    raise invalid(f"desired state contains unsupported JSON constant {value}")


def invalid(message: str) -> AirflowDesiredStateError:
    return AirflowDesiredStateError("DPONE_AIRFLOW_DESIRED_STATE_INVALID", message)


__all__ = [
    "GIT_SHA_PATTERN",
    "MAX_AIRFLOW_DESIRED_STATE_BYTES",
    "MAX_AIRFLOW_DESIRED_DAGS",
    "MAX_DESIRED_STATE_REVISION_BYTES",
    "PREVIOUS_FIELDS",
    "PROMOTION_FIELDS",
    "ROOT_FIELDS",
    "SOURCE_FIELDS",
    "AirflowDesiredStateError",
    "bounded",
    "bounded_revision",
    "canonical_uuid",
    "ci_id",
    "dag_ids",
    "digest",
    "environment",
    "invalid",
    "mapping",
    "optional_digest",
    "project",
    "reject_constant",
    "reject_secret_like_fields",
    "reject_unknown",
    "text",
    "unique_object",
    "utc_timestamp",
]
