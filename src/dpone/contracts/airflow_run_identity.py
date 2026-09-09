"""Canonical non-secret identity for one dpone Airflow workload attempt."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

AIRFLOW_RUN_IDENTITY_SCHEMA = "dpone.airflow-run-identity.v1"
AIRFLOW_RUN_IDENTITY_ENV = "DPONE_AIRFLOW_RUN_IDENTITY"
AIRFLOW_DEPLOYMENT_IDENTITY_SCHEMA = "dpone.airflow-deployment-identity.v1"
AIRFLOW_DEPLOYMENT_IDENTITY_ENV = "DPONE_AIRFLOW_DEPLOYMENT_IDENTITY"
MAX_AIRFLOW_RUN_IDENTITY_BYTES = 16 * 1024
MAX_AIRFLOW_DEPLOYMENT_IDENTITY_BYTES = 1024
_SHA256_PREFIX = "sha256:"
_IDENTITY_FIELDS = frozenset(
    {
        "schema",
        "release_id",
        "deployment_id",
        "dag_spec",
        "workload_pack",
        "runtime_image_digest",
        "binding_set_ref",
        "connection_registry_ref",
        "credential_runtime_ref",
        "airflow_bundle",
    }
)
_ARTIFACT_FIELDS = frozenset({"id", "sha256"})
_BUNDLE_FIELDS = frozenset({"backend", "ref", "versioned", "version", "snapshot_ref"})
_DEPLOYMENT_IDENTITY_FIELDS = frozenset({"schema", "release_id", "deployment_id", "activation_id"})
_UUID_V4_PATTERN = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$")


class AirflowRunIdentityError(ValueError):
    """A run identity is malformed, unsafe, or larger than the public limit."""

    code = "DPONE_AIRFLOW_RUN_IDENTITY_INVALID"

    def __init__(self, message: str) -> None:
        super().__init__(f"{self.code}: {message}")


class AirflowDeploymentIdentityError(ValueError):
    """An exact cache activation identity is malformed or unbounded."""

    code = "DPONE_AIRFLOW_DEPLOYMENT_IDENTITY_INVALID"

    def __init__(self, message: str) -> None:
        super().__init__(f"{self.code}: {message}")


@dataclass(frozen=True, slots=True)
class AirflowArtifactIdentity:
    id: str
    sha256: str

    @classmethod
    def from_mapping(cls, value: object, *, field: str) -> AirflowArtifactIdentity:
        payload = _mapping(value, field=field)
        _reject_unknown_fields(payload, allowed=_ARTIFACT_FIELDS, field=field)
        return cls(
            id=_required_text(payload.get("id"), field=f"{field}.id"),
            sha256=_required_digest(payload.get("sha256"), field=f"{field}.sha256"),
        )

    def to_dict(self) -> dict[str, str]:
        return {"id": self.id, "sha256": self.sha256}


@dataclass(frozen=True, slots=True)
class AirflowBundleRunIdentity:
    backend: str
    ref: str
    versioned: bool
    version: str | None = None
    snapshot_ref: str | None = None

    @classmethod
    def from_mapping(cls, value: object) -> AirflowBundleRunIdentity | None:
        if value is None:
            return None
        payload = _mapping(value, field="airflow_bundle")
        _reject_unknown_fields(payload, allowed=_BUNDLE_FIELDS, field="airflow_bundle")
        versioned = payload.get("versioned")
        if not isinstance(versioned, bool):
            raise AirflowRunIdentityError("airflow_bundle.versioned must be boolean")
        version = _optional_text(payload.get("version"), field="airflow_bundle.version")
        if versioned and version is None:
            raise AirflowRunIdentityError("airflow_bundle.version is required when versioned is true")
        if not versioned and version is not None:
            raise AirflowRunIdentityError("airflow_bundle.version must be null when versioned is false")
        return cls(
            backend=_required_text(payload.get("backend"), field="airflow_bundle.backend"),
            ref=_required_bundle_ref(payload.get("ref")),
            versioned=versioned,
            version=version,
            snapshot_ref=_optional_digest(payload.get("snapshot_ref"), "airflow_bundle.snapshot_ref"),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "backend": self.backend,
            "ref": self.ref,
            "versioned": self.versioned,
            "version": self.version,
            "snapshot_ref": self.snapshot_ref,
        }


@dataclass(frozen=True, slots=True)
class AirflowRunIdentity:
    release_id: str
    deployment_id: str
    workload_pack: AirflowArtifactIdentity
    dag_spec: AirflowArtifactIdentity | None = None
    runtime_image_digest: str | None = None
    binding_set_ref: str | None = None
    connection_registry_ref: str | None = None
    credential_runtime_ref: str | None = None
    airflow_bundle: AirflowBundleRunIdentity | None = None
    schema: str = AIRFLOW_RUN_IDENTITY_SCHEMA

    @classmethod
    def from_mapping(cls, value: object) -> AirflowRunIdentity:
        payload = _mapping(value, field="run_identity")
        _reject_unknown_fields(payload, allowed=_IDENTITY_FIELDS, field="run_identity")
        if payload.get("schema") != AIRFLOW_RUN_IDENTITY_SCHEMA:
            raise AirflowRunIdentityError(f"schema must be {AIRFLOW_RUN_IDENTITY_SCHEMA}")
        return cls(
            release_id=_required_digest(payload.get("release_id"), field="release_id"),
            deployment_id=_required_digest(payload.get("deployment_id"), field="deployment_id"),
            dag_spec=(
                None
                if payload.get("dag_spec") is None
                else AirflowArtifactIdentity.from_mapping(payload.get("dag_spec"), field="dag_spec")
            ),
            workload_pack=AirflowArtifactIdentity.from_mapping(
                payload.get("workload_pack"),
                field="workload_pack",
            ),
            runtime_image_digest=_optional_digest(payload.get("runtime_image_digest"), "runtime_image_digest"),
            binding_set_ref=_optional_digest(payload.get("binding_set_ref"), "binding_set_ref"),
            connection_registry_ref=_optional_digest(
                payload.get("connection_registry_ref"),
                "connection_registry_ref",
            ),
            credential_runtime_ref=_optional_digest(
                payload.get("credential_runtime_ref"),
                "credential_runtime_ref",
            ),
            airflow_bundle=AirflowBundleRunIdentity.from_mapping(payload.get("airflow_bundle")),
        )

    @property
    def semantic_fingerprint(self) -> str:
        return "sha256:" + hashlib.sha256(self.to_json().encode("utf-8")).hexdigest()

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": self.schema,
            "release_id": self.release_id,
            "deployment_id": self.deployment_id,
            "dag_spec": self.dag_spec.to_dict() if self.dag_spec is not None else None,
            "workload_pack": self.workload_pack.to_dict(),
            "runtime_image_digest": self.runtime_image_digest,
            "binding_set_ref": self.binding_set_ref,
            "connection_registry_ref": self.connection_registry_ref,
            "credential_runtime_ref": self.credential_runtime_ref,
            "airflow_bundle": self.airflow_bundle.to_dict() if self.airflow_bundle is not None else None,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=True, separators=(",", ":"), sort_keys=True)


@dataclass(frozen=True, slots=True)
class AirflowDeploymentIdentity:
    """One exact activation occurrence, separate from immutable artifact identity."""

    release_id: str
    deployment_id: str
    activation_id: str
    schema: str = AIRFLOW_DEPLOYMENT_IDENTITY_SCHEMA

    @classmethod
    def from_mapping(cls, value: object) -> AirflowDeploymentIdentity:
        try:
            payload = _mapping(value, field="deployment_identity")
            _reject_unknown_fields(
                payload,
                allowed=_DEPLOYMENT_IDENTITY_FIELDS,
                field="deployment_identity",
            )
            missing = sorted(_DEPLOYMENT_IDENTITY_FIELDS.difference(payload))
            if missing:
                raise AirflowDeploymentIdentityError("identity is missing fields: " + ", ".join(missing))
            if payload.get("schema") != AIRFLOW_DEPLOYMENT_IDENTITY_SCHEMA:
                raise AirflowDeploymentIdentityError("schema is unsupported")
            activation_id = _required_canonical_text(payload.get("activation_id"), field="activation_id")
            if _UUID_V4_PATTERN.fullmatch(activation_id) is None:
                raise AirflowDeploymentIdentityError("activation_id must be a canonical UUID v4")
            return cls(
                release_id=_required_canonical_digest(payload.get("release_id"), field="release_id"),
                deployment_id=_required_canonical_digest(payload.get("deployment_id"), field="deployment_id"),
                activation_id=activation_id,
            )
        except AirflowRunIdentityError as exc:
            raise AirflowDeploymentIdentityError(str(exc).partition(": ")[2]) from exc

    def to_dict(self) -> dict[str, str]:
        return {
            "schema": self.schema,
            "release_id": self.release_id,
            "deployment_id": self.deployment_id,
            "activation_id": self.activation_id,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def airflow_deployment_identity_schema() -> dict[str, Any]:
    """Return the closed JSON Schema for one exact activation occurrence."""

    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["schema", "release_id", "deployment_id", "activation_id"],
        "properties": {
            "schema": {"const": AIRFLOW_DEPLOYMENT_IDENTITY_SCHEMA},
            "release_id": {"type": "string", "pattern": r"^sha256:[0-9a-f]{64}$"},
            "deployment_id": {"type": "string", "pattern": r"^sha256:[0-9a-f]{64}$"},
            "activation_id": {
                "type": "string",
                "format": "uuid",
                "pattern": _UUID_V4_PATTERN.pattern,
            },
        },
    }


def parse_airflow_run_identity_json(value: str) -> AirflowRunIdentity:
    """Parse one bounded identity from the provider-owned runtime environment."""

    if len(value.encode("utf-8")) > MAX_AIRFLOW_RUN_IDENTITY_BYTES:
        raise AirflowRunIdentityError("serialized identity exceeds 16 KiB")
    try:
        payload = json.loads(value)
    except json.JSONDecodeError as exc:
        raise AirflowRunIdentityError(f"identity JSON is invalid: {exc.msg}") from exc
    identity = AirflowRunIdentity.from_mapping(payload)
    if len(identity.to_json().encode("utf-8")) > MAX_AIRFLOW_RUN_IDENTITY_BYTES:
        raise AirflowRunIdentityError("canonical identity exceeds 16 KiB")
    return identity


def parse_airflow_deployment_identity_json(value: str) -> AirflowDeploymentIdentity:
    if len(value.encode("utf-8")) > MAX_AIRFLOW_DEPLOYMENT_IDENTITY_BYTES:
        raise AirflowDeploymentIdentityError("serialized identity exceeds 1 KiB")
    try:
        payload = json.loads(value)
    except json.JSONDecodeError as exc:
        raise AirflowDeploymentIdentityError(f"identity JSON is invalid: {exc.msg}") from exc
    return AirflowDeploymentIdentity.from_mapping(payload)


def _mapping(value: object, *, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise AirflowRunIdentityError(f"{field} must be an object")
    return value


def _reject_unknown_fields(payload: Mapping[str, Any], *, allowed: frozenset[str], field: str) -> None:
    unknown = sorted(str(key) for key in payload if key not in allowed)
    if unknown:
        raise AirflowRunIdentityError(f"{field} contains unsupported fields: {', '.join(unknown)}")


def _required_text(value: object, *, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise AirflowRunIdentityError(f"{field} must be a non-empty string")
    return value.strip()


def _optional_text(value: object, *, field: str) -> str | None:
    if value is None:
        return None
    return _required_text(value, field=field)


def _required_canonical_text(value: object, *, field: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise AirflowRunIdentityError(f"{field} must be a canonical non-empty string")
    return value


def _required_canonical_digest(value: object, *, field: str) -> str:
    text = _required_canonical_text(value, field=field)
    if not _is_digest(text):
        raise AirflowRunIdentityError(f"{field} must be a canonical sha256 digest")
    return text


def _required_bundle_ref(value: object) -> str:
    text = _required_text(value, field="airflow_bundle.ref")
    lowered = text.lower()
    unsafe_markers = (
        "x-amz-signature=",
        "x-goog-signature=",
        "signature=",
        "credential=",
        "token=",
        "password=",
    )
    if any(char.isspace() for char in text) or any(marker in lowered for marker in unsafe_markers):
        raise AirflowRunIdentityError("airflow_bundle.ref must be a non-secret logical reference")
    authority = text.partition("://")[2].partition("/")[0]
    if authority and "@" in authority:
        raise AirflowRunIdentityError("airflow_bundle.ref must not contain URI credentials")
    return text


def _required_digest(value: object, *, field: str) -> str:
    text = _required_text(value, field=field)
    if not _is_digest(text):
        raise AirflowRunIdentityError(f"{field} must be a canonical sha256 digest")
    return text


def _optional_digest(value: object, field: str) -> str | None:
    if value is None:
        return None
    return _required_digest(value, field=field)


def _is_digest(value: str) -> bool:
    if not value.startswith(_SHA256_PREFIX):
        return False
    digest = value.removeprefix(_SHA256_PREFIX)
    return len(digest) == 64 and all(char in "0123456789abcdef" for char in digest)


__all__ = [
    "AIRFLOW_DEPLOYMENT_IDENTITY_ENV",
    "AIRFLOW_DEPLOYMENT_IDENTITY_SCHEMA",
    "AIRFLOW_RUN_IDENTITY_ENV",
    "AIRFLOW_RUN_IDENTITY_SCHEMA",
    "MAX_AIRFLOW_DEPLOYMENT_IDENTITY_BYTES",
    "MAX_AIRFLOW_RUN_IDENTITY_BYTES",
    "AirflowArtifactIdentity",
    "AirflowBundleRunIdentity",
    "AirflowDeploymentIdentity",
    "AirflowDeploymentIdentityError",
    "AirflowRunIdentity",
    "AirflowRunIdentityError",
    "airflow_deployment_identity_schema",
    "parse_airflow_deployment_identity_json",
    "parse_airflow_run_identity_json",
]
