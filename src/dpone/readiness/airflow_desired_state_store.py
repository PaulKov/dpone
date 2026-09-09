"""Composition helpers for the Airflow desired-state control object."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dpone.adapters.s3_airflow_desired_state import (
    S3AirflowDesiredStateStore,
    parse_s3_desired_state_uri,
)
from dpone.runtime.object_storage_access import (
    ObjectStorageConnectionRef,
    ObjectStorageConnectionResolver,
)
from dpone.storage import S3ObjectStorageClient

MAX_PROMOTION_EVIDENCE_BYTES = 256 * 1024
_PROMOTION_SCHEMA = "dwh.airflow_ci.dpone_deployment_promotion.v2"
_PROMOTION_REQUIRED_FIELDS = frozenset(
    {
        "schema_version",
        "release_id",
        "deployment_id",
        "environment",
        "registry_scope_id",
        "source_git_sha",
        "runtime_image_digest",
        "airflow_index_sha256",
        "expected_dag_ids",
        "expected_dag_count",
        "status",
        "warnings",
        "blockers",
        "created_at",
    }
)
_PROMOTION_OPTIONAL_FIELDS = frozenset({"runtime_image_dbt_digest"})
_PROMOTION_FIELDS = _PROMOTION_REQUIRED_FIELDS | _PROMOTION_OPTIONAL_FIELDS


@dataclass(frozen=True, slots=True)
class DesiredStateStoreOptions:
    desired_uri: str
    certified_endpoint_url: str
    identity_mode: str | None = None
    connection_type: str | None = None
    connection_id: str | None = None

    def build(self) -> S3AirflowDesiredStateStore:
        uri = parse_s3_desired_state_uri(self.desired_uri)
        if (self.identity_mode is None) == (self.connection_id is None):
            raise ValueError("exactly one desired-state access mode is required")
        if self.identity_mode is not None:
            if self.identity_mode != "workload_identity":
                raise ValueError("desired-state identity mode is unsupported")
            storage_client = S3ObjectStorageClient()
        else:
            ref = ObjectStorageConnectionRef(
                connection_type=self.connection_type or "vault",
                connection_id=str(self.connection_id),
            )
            storage_client = ObjectStorageConnectionResolver().build_client(ref=ref, uri=uri)
            if not isinstance(storage_client, S3ObjectStorageClient):
                raise ValueError("desired-state connection must resolve to S3")
        return S3AirflowDesiredStateStore(
            client=storage_client.conditional_object_client,
            uri=uri,
            certified_endpoint_url=self.certified_endpoint_url,
        )


@dataclass(frozen=True, slots=True)
class PromotionInput:
    environment: str
    registry_scope_id: str
    release_id: str
    deployment_id: str
    source_git_sha: str
    runtime_image_digest: str
    airflow_index_sha256: str
    expected_dag_ids: tuple[str, ...]
    evidence_sha256: str
    runtime_image_dbt_digest: str | None = None


def load_promotion_input(path: Path) -> PromotionInput:
    raw = path.read_bytes()
    if not raw or len(raw) > MAX_PROMOTION_EVIDENCE_BYTES:
        raise ValueError("promotion evidence is empty or exceeds its size limit")
    try:
        payload = json.loads(
            raw.decode("utf-8", errors="strict"),
            object_pairs_hook=_unique_object,
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("promotion evidence must be valid UTF-8 JSON") from exc
    if not isinstance(payload, dict):
        raise ValueError("promotion evidence fields do not match the v2 contract")
    keys = set(payload)
    if not _PROMOTION_REQUIRED_FIELDS.issubset(keys) or keys - _PROMOTION_FIELDS:
        raise ValueError("promotion evidence fields do not match the v2 contract")
    if payload.get("schema_version") != _PROMOTION_SCHEMA:
        raise ValueError("promotion evidence schema is unsupported")
    blockers = payload.get("blockers")
    dag_ids = payload.get("expected_dag_ids")
    if payload.get("status") != "passed" or blockers != []:
        raise ValueError("promotion evidence is not passed")
    if not isinstance(dag_ids, list) or any(not isinstance(item, str) for item in dag_ids):
        raise ValueError("promotion evidence expected_dag_ids is invalid")
    if dag_ids != sorted(set(dag_ids)) or payload.get("expected_dag_count") != len(dag_ids):
        raise ValueError("promotion evidence DAG identities are inconsistent")
    return PromotionInput(
        environment=_text(payload, "environment"),
        registry_scope_id=_text(payload, "registry_scope_id"),
        release_id=_text(payload, "release_id"),
        deployment_id=_text(payload, "deployment_id"),
        source_git_sha=_text(payload, "source_git_sha"),
        runtime_image_digest=_text(payload, "runtime_image_digest"),
        airflow_index_sha256=_text(payload, "airflow_index_sha256"),
        expected_dag_ids=tuple(dag_ids),
        evidence_sha256="sha256:" + hashlib.sha256(raw).hexdigest(),
        runtime_image_dbt_digest=(
            _text(payload, "runtime_image_dbt_digest") if "runtime_image_dbt_digest" in payload else None
        ),
    )


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("promotion evidence contains duplicate JSON keys")
        result[key] = value
    return result


def _text(payload: dict[str, Any], field: str) -> str:
    value = payload.get(field)
    if not isinstance(value, str) or not value:
        raise ValueError(f"promotion evidence {field} is invalid")
    return value


__all__ = [
    "DesiredStateStoreOptions",
    "MAX_PROMOTION_EVIDENCE_BYTES",
    "PromotionInput",
    "load_promotion_input",
]
