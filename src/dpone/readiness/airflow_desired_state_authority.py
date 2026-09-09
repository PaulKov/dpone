"""Trusted composition authority for Airflow desired-state operations."""

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dpone.adapters.object_storage_artifact_registry import (
    canonical_object_storage_endpoint_authority,
    object_storage_registry_authority_scope_id,
    parse_artifact_registry_root,
)
from dpone.adapters.s3_airflow_desired_state import parse_s3_desired_state_uri
from dpone.contracts.airflow_desired_state_validation import environment, text
from dpone.contracts.dbt_workspace_attempt import require_workspace_authority_connection_ref
from dpone.ports.artifact_registry import ArtifactRegistryKeyError

AUTHORITY_FILE_ENV = "DPONE_AIRFLOW_DESIRED_STATE_AUTHORITY_FILE"
AIRFLOW_DESIRED_STATE_AUTHORITY_SCHEMA = "dpone.airflow-desired-state-authority.v1"
AIRFLOW_DESIRED_STATE_AUTHORITY_SCHEMA_V2 = "dpone.airflow-desired-state-authority.v2"
MAX_AUTHORITY_BYTES = 32 * 1024
_FIELDS_V1 = frozenset(
    {
        "schema",
        "environment",
        "desired_state_uri",
        "certified_s3_endpoint_url",
        "artifact_registry_uri",
        "artifact_registry_ref",
        "watcher_identity",
        "source_project",
        "source_ref",
    }
)
_FIELDS_V2 = _FIELDS_V1 | {"workspace_authority_connection_ref"}


@dataclass(frozen=True, slots=True)
class AirflowDesiredStateAuthority:
    """One protected environment's exact storage and activation authority."""

    environment: str
    desired_state_uri: str
    certified_s3_endpoint_url: str
    artifact_registry_uri: str
    artifact_registry_ref: str
    watcher_identity: str
    source_project: str
    source_ref: str
    workspace_authority_connection_ref: str | None = None

    def __post_init__(self) -> None:
        environment(self.environment)
        desired = parse_s3_desired_state_uri(self.desired_state_uri)
        registry = parse_artifact_registry_root(self.artifact_registry_uri)
        if desired.bucket != registry.bucket:
            raise ValueError("desired state and artifact registry must use one certified bucket")
        if (
            desired.key == registry.key
            or desired.key.startswith(f"{registry.key}/")
            or registry.key.startswith(f"{desired.key}/")
        ):
            raise ValueError("desired state must be outside the immutable artifact registry")
        text(self.certified_s3_endpoint_url, field="certified_s3_endpoint_url", maximum=512)
        try:
            endpoint = canonical_object_storage_endpoint_authority(self.certified_s3_endpoint_url)
        except ArtifactRegistryKeyError as exc:
            raise ValueError("certified S3 endpoint URL is invalid") from exc
        if not endpoint.startswith("https://") or endpoint != self.certified_s3_endpoint_url:
            raise ValueError("certified S3 endpoint URL must be canonical HTTPS")
        text(self.artifact_registry_ref, field="artifact_registry_ref", maximum=256)
        text(self.watcher_identity, field="watcher_identity", maximum=256)
        text(self.source_project, field="source_project", maximum=512)
        text(self.source_ref, field="source_ref", maximum=256)
        if self.workspace_authority_connection_ref is not None:
            require_workspace_authority_connection_ref(self.workspace_authority_connection_ref)

    @property
    def registry_scope_id(self) -> str:
        """Return the immutable registry authority identity used by promotion evidence."""

        return object_storage_registry_authority_scope_id(
            parse_artifact_registry_root(self.artifact_registry_uri),
            endpoint_authority=self.certified_s3_endpoint_url,
        )

    @property
    def publish_authority_sha256(self) -> str:
        """Return a credential-free identity for the complete write authority."""

        authority = {
            "artifact_registry_ref": self.artifact_registry_ref,
            "artifact_registry_uri": self.artifact_registry_uri,
            "certified_s3_endpoint_url": self.certified_s3_endpoint_url,
            "desired_state_uri": self.desired_state_uri,
            "environment": self.environment,
            "registry_scope_id": self.registry_scope_id,
            "source_project": self.source_project,
            "source_ref": self.source_ref,
            "watcher_identity": self.watcher_identity,
        }
        if self.workspace_authority_connection_ref is not None:
            authority["workspace_authority_connection_ref"] = self.workspace_authority_connection_ref
        body = json.dumps(
            authority,
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
        return "sha256:" + hashlib.sha256(body).hexdigest()


def load_airflow_desired_state_authority(
    path: Path | None = None,
) -> AirflowDesiredStateAuthority:
    """Load the protected authority file selected by the runtime environment."""

    selected = path
    if selected is None:
        configured = os.environ.get(AUTHORITY_FILE_ENV)
        if not configured:
            raise ValueError(f"{AUTHORITY_FILE_ENV} is required")
        selected = Path(configured)
    raw = selected.read_bytes()
    if not raw or len(raw) > MAX_AUTHORITY_BYTES:
        raise ValueError("desired-state authority is empty or exceeds its size limit")
    try:
        payload = json.loads(
            raw.decode("utf-8", errors="strict"),
            object_pairs_hook=_unique_object,
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("desired-state authority must be valid UTF-8 JSON") from exc
    if not isinstance(payload, dict):
        raise ValueError("desired-state authority fields do not match the v1 contract")
    schema = payload.get("schema")
    expected_fields = (
        _FIELDS_V1
        if schema == AIRFLOW_DESIRED_STATE_AUTHORITY_SCHEMA
        else _FIELDS_V2
        if schema == AIRFLOW_DESIRED_STATE_AUTHORITY_SCHEMA_V2
        else None
    )
    if expected_fields is None:
        raise ValueError("desired-state authority schema is unsupported")
    if set(payload) != expected_fields:
        raise ValueError("desired-state authority fields do not match its schema")
    return AirflowDesiredStateAuthority(
        environment=_text(payload, "environment"),
        desired_state_uri=_text(payload, "desired_state_uri"),
        certified_s3_endpoint_url=_text(payload, "certified_s3_endpoint_url"),
        artifact_registry_uri=_text(payload, "artifact_registry_uri"),
        artifact_registry_ref=_text(payload, "artifact_registry_ref"),
        watcher_identity=_text(payload, "watcher_identity"),
        source_project=_text(payload, "source_project"),
        source_ref=_text(payload, "source_ref"),
        workspace_authority_connection_ref=(
            _text(payload, "workspace_authority_connection_ref")
            if schema == AIRFLOW_DESIRED_STATE_AUTHORITY_SCHEMA_V2
            else None
        ),
    )


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("desired-state authority contains duplicate JSON keys")
        result[key] = value
    return result


def _text(payload: dict[str, Any], field: str) -> str:
    value = payload.get(field)
    if not isinstance(value, str) or not value:
        raise ValueError(f"desired-state authority {field} is invalid")
    return value


__all__ = [
    "AIRFLOW_DESIRED_STATE_AUTHORITY_SCHEMA",
    "AIRFLOW_DESIRED_STATE_AUTHORITY_SCHEMA_V2",
    "AUTHORITY_FILE_ENV",
    "AirflowDesiredStateAuthority",
    "load_airflow_desired_state_authority",
]
