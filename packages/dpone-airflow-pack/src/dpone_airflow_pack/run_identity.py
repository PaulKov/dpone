"""Parse-safe run identity construction for verified deployment artifacts."""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any

from dpone_airflow_pack.deployment_index_contract import AirflowDeploymentIndex

AIRFLOW_RUN_IDENTITY_ENV = "DPONE_AIRFLOW_RUN_IDENTITY"
AIRFLOW_RUN_IDENTITY_SCHEMA = "dpone.airflow-run-identity.v1"
AIRFLOW_ACTIVATION_CONTEXT_KEY = "_activation_id"
WORKSPACE_AUTHORITY_CONNECTION_REF_CONTEXT_KEY = "_workspace_authority_connection_ref"
AIRFLOW_ACTIVATION_TAG_PREFIX = "dpone_activation:"
AIRFLOW_RELEASE_TAG_PREFIX = "dpone_release:"
AIRFLOW_DEPLOYMENT_TAG_PREFIX = "dpone_deployment:"
MAX_AIRFLOW_RUN_IDENTITY_BYTES = 16 * 1024


def build_dag_run_identity_context(
    index: AirflowDeploymentIndex,
    *,
    dag_spec_id: str,
    dag_spec_sha256: str,
) -> dict[str, Any]:
    """Build the deployment and DAG portion of one workload attempt identity."""

    return _build_run_identity_context(
        index,
        dag_spec={"id": dag_spec_id, "sha256": dag_spec_sha256},
    )


def build_task_group_run_identity_context(index: AirflowDeploymentIndex) -> dict[str, Any]:
    """Build deployment identity for a workload embedded in a custom DAG."""

    return _build_run_identity_context(index, dag_spec=None)


def _build_run_identity_context(
    index: AirflowDeploymentIndex,
    *,
    dag_spec: Mapping[str, str] | None,
) -> dict[str, Any]:
    context = {
        "schema": AIRFLOW_RUN_IDENTITY_SCHEMA,
        "release_id": index.release_id,
        "deployment_id": index.deployment_id,
        "dag_spec": dict(dag_spec) if dag_spec is not None else None,
        "runtime_image_digest": index.runtime_image_digest,
        "binding_set_ref": index.binding_set_ref,
        "connection_registry_ref": index.connection_registry_ref,
        "credential_runtime_ref": index.credential_runtime_ref,
        "airflow_bundle": _airflow_bundle(index.airflow_bundle_ref),
        "_workload_pack_sha256": {artifact.id: artifact.sha256 for artifact in index.workload_packs},
    }
    if index.activation_id is not None:
        context[AIRFLOW_ACTIVATION_CONTEXT_KEY] = index.activation_id
    if index.workspace_authority_connection_ref is not None:
        context[WORKSPACE_AUTHORITY_CONNECTION_REF_CONTEXT_KEY] = index.workspace_authority_connection_ref
    return context


def build_workload_run_identity(
    dag_context: Mapping[str, Any],
    *,
    workload_id: str,
    pack_sha256: str,
) -> dict[str, Any]:
    """Add one verified workload pack to a DAG identity context."""

    canonical_pack_sha256 = _canonical_sha256(pack_sha256)
    expected = dag_context.get("_workload_pack_sha256")
    if isinstance(expected, Mapping) and expected.get(workload_id) != canonical_pack_sha256:
        raise ValueError("DPONE_CACHE_CHECKSUM_MISMATCH: workload pack changed after deployment index validation")
    identity = {key: value for key, value in dag_context.items() if not str(key).startswith("_")}
    identity["workload_pack"] = {"id": workload_id, "sha256": canonical_pack_sha256}
    serialize_run_identity(identity)
    return identity


def serialize_run_identity(identity: Mapping[str, Any]) -> str:
    """Return deterministic bounded JSON suitable for a KPO environment value."""

    encoded = json.dumps(dict(identity), ensure_ascii=True, separators=(",", ":"), sort_keys=True)
    if len(encoded.encode("utf-8")) > MAX_AIRFLOW_RUN_IDENTITY_BYTES:
        raise ValueError("DPONE_AIRFLOW_RUN_IDENTITY_INVALID: serialized identity exceeds 16 KiB")
    return encoded


def _airflow_bundle(ref: str | None) -> dict[str, Any] | None:
    text = _safe_bundle_ref(ref)
    if not text:
        return None
    if text.startswith("git:"):
        version = text.removeprefix("git:").strip() or None
        return {
            "backend": "git",
            "ref": text,
            "versioned": version is not None,
            "version": version,
            "snapshot_ref": None,
        }
    if text.startswith("s3://"):
        backend = "s3"
    elif text.startswith(("gs://", "gcs://")):
        backend = "gcs"
    elif text.startswith("local:") or text.startswith("/"):
        backend = "local"
    else:
        backend = "unknown"
    return {
        "backend": backend,
        "ref": text,
        "versioned": False,
        "version": None,
        "snapshot_ref": None,
    }


def _safe_bundle_ref(value: str | None) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    lowered = text.lower()
    unsafe_markers = (
        "x-amz-signature=",
        "x-goog-signature=",
        "signature=",
        "credential=",
        "token=",
        "password=",
    )
    authority = text.partition("://")[2].partition("/")[0]
    if (
        any(char.isspace() for char in text)
        or any(marker in lowered for marker in unsafe_markers)
        or (authority and "@" in authority)
    ):
        raise ValueError("DPONE_AIRFLOW_RUN_IDENTITY_INVALID: Airflow bundle ref is unsafe")
    return text


def _canonical_sha256(value: str) -> str:
    digest = value.removeprefix("sha256:")
    if len(digest) != 64 or any(char not in "0123456789abcdef" for char in digest):
        raise ValueError("DPONE_AIRFLOW_RUN_IDENTITY_INVALID: pack sha256 is invalid")
    return "sha256:" + digest


__all__ = [
    "AIRFLOW_ACTIVATION_CONTEXT_KEY",
    "AIRFLOW_ACTIVATION_TAG_PREFIX",
    "AIRFLOW_DEPLOYMENT_TAG_PREFIX",
    "AIRFLOW_RELEASE_TAG_PREFIX",
    "AIRFLOW_RUN_IDENTITY_ENV",
    "AIRFLOW_RUN_IDENTITY_SCHEMA",
    "MAX_AIRFLOW_RUN_IDENTITY_BYTES",
    "build_dag_run_identity_context",
    "build_task_group_run_identity_context",
    "build_workload_run_identity",
    "serialize_run_identity",
]
