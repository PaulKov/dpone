"""Validation helpers for the compact Airflow run identity."""

from __future__ import annotations

import json
from collections.abc import Mapping

_RUN_IDENTITY_FIELDS = frozenset(
    "schema release_id deployment_id dag_spec workload_pack runtime_image_digest binding_set_ref "
    "connection_registry_ref credential_runtime_ref airflow_bundle".split()
)
_ARTIFACT_IDENTITY_FIELDS = frozenset({"id", "sha256"})
_AIRFLOW_BUNDLE_FIELDS = frozenset({"backend", "ref", "versioned", "version", "snapshot_ref"})


def run_identity_error(value: object) -> str:
    """Return a safe diagnostic when a run identity is invalid."""

    if not isinstance(value, Mapping):
        return "run_identity must be an object"
    try:
        encoded = json.dumps(dict(value), allow_nan=False, ensure_ascii=True, separators=(",", ":"), sort_keys=True)
    except (TypeError, ValueError, RecursionError):
        return "run_identity must be finite JSON"
    if len(encoded.encode("utf-8")) > 16 * 1024:
        return "run_identity exceeds 16 KiB"
    unknown = sorted(str(key) for key in value if key not in _RUN_IDENTITY_FIELDS)
    missing = sorted(_RUN_IDENTITY_FIELDS.difference(value))
    if unknown or missing:
        return _field_problem(unknown=unknown, missing=missing)
    if value.get("schema") != "dpone.airflow-run-identity.v1":
        return "run_identity.schema is invalid"
    if not is_sha256(value.get("release_id")) or not is_sha256(value.get("deployment_id")):
        return "release_id and deployment_id must be canonical sha256 digests"
    for field in ("runtime_image_digest", "binding_set_ref", "connection_registry_ref", "credential_runtime_ref"):
        if value.get(field) is not None and not is_sha256(value.get(field)):
            return f"run_identity.{field} must be null or a canonical sha256 digest"
    dag_spec = value.get("dag_spec")
    if dag_spec is not None and (error := _artifact_identity_error(dag_spec, "dag_spec")):
        return error
    if error := _artifact_identity_error(value.get("workload_pack"), "workload_pack"):
        return error
    return _airflow_bundle_error(value.get("airflow_bundle"))


def is_sha256(value: object) -> bool:
    """Return whether *value* is the canonical ``sha256:<hex>`` form."""

    if not isinstance(value, str) or not value.startswith("sha256:"):
        return False
    digest = value.removeprefix("sha256:")
    return len(digest) == 64 and all(character in "0123456789abcdef" for character in digest)


def _artifact_identity_error(value: object, field: str) -> str:
    if not isinstance(value, Mapping):
        return f"run_identity.{field} must be an object"
    unknown = sorted(str(key) for key in value if key not in _ARTIFACT_IDENTITY_FIELDS)
    missing = sorted(_ARTIFACT_IDENTITY_FIELDS.difference(value))
    if unknown or missing:
        return f"run_identity.{field} {_field_problem(unknown=unknown, missing=missing)}"
    if not isinstance(value.get("id"), str) or not str(value["id"]).strip():
        return f"run_identity.{field}.id must be a non-empty string"
    if not is_sha256(value.get("sha256")):
        return f"run_identity.{field}.sha256 must be a canonical sha256 digest"
    return ""


def _airflow_bundle_error(value: object) -> str:
    if value is None:
        return ""
    if not isinstance(value, Mapping):
        return "run_identity.airflow_bundle must be null or an object"
    unknown = sorted(str(key) for key in value if key not in _AIRFLOW_BUNDLE_FIELDS)
    missing = sorted(_AIRFLOW_BUNDLE_FIELDS.difference(value))
    if unknown or missing:
        return f"run_identity.airflow_bundle {_field_problem(unknown=unknown, missing=missing)}"
    if not all(isinstance(value.get(field), str) and str(value[field]).strip() for field in ("backend", "ref")):
        return "run_identity.airflow_bundle backend and ref must be non-empty strings"
    versioned = value.get("versioned")
    version = value.get("version")
    if (
        not isinstance(versioned, bool)
        or (versioned and (not isinstance(version, str) or not version.strip()))
        or (not versioned and version is not None)
    ):
        return "run_identity.airflow_bundle version contract is invalid"
    ref = str(value["ref"])
    lowered_ref = ref.lower()
    authority = ref.partition("://")[2].partition("/")[0]
    if any(character.isspace() for character in ref) or any(
        marker in lowered_ref
        for marker in ("token=", "password=", "signature=", "credential=", "x-amz-signature=", "x-goog-signature=")
    ):
        return "run_identity.airflow_bundle.ref must be a non-secret logical reference"
    if authority and "@" in authority:
        return "run_identity.airflow_bundle.ref must not contain URI credentials"
    if value.get("snapshot_ref") is not None and not is_sha256(value.get("snapshot_ref")):
        return "run_identity.airflow_bundle.snapshot_ref must be null or a canonical sha256 digest"
    return ""


def _field_problem(*, unknown: list[str], missing: list[str]) -> str:
    parts = []
    if unknown:
        parts.append(f"contains unsupported fields: {', '.join(unknown)}")
    if missing:
        parts.append(f"is missing fields: {', '.join(missing)}")
    return "; ".join(parts)


__all__ = ["is_sha256", "run_identity_error"]
