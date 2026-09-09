"""Immutable file-set writer for an Airflow deployment projection."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from pathlib import Path

from dpone.readiness.airflow_deployment_artifacts import digest_dir, json_bytes


def write_projection(
    *,
    root: Path,
    cache_root: Path,
    environment: str,
    deployment_id: str,
    deployment_bytes: bytes,
    airflow_index: dict[str, object],
    binding_set: dict[str, object],
    connection_registry_fingerprint: str,
    credential_runtime_fingerprint: str,
    runtime_connection_snapshots: Mapping[str, bytes] | None,
    additional_files: Mapping[str, bytes] | None,
    publisher: Callable[..., Path],
) -> Path:
    """Write or exactly reconcile one immutable deployment directory."""

    parent = cache_root / "deployments" / environment
    registry_snapshot = (
        runtime_connection_snapshots["connection_registry"]
        if runtime_connection_snapshots is not None
        else (connection_registry_fingerprint + "\n").encode()
    )
    credential_snapshot = (
        runtime_connection_snapshots["credential_runtime"]
        if runtime_connection_snapshots is not None
        else (credential_runtime_fingerprint + "\n").encode()
    )
    return publisher(
        root=root,
        parent=parent,
        final_name=digest_dir(deployment_id),
        expected_files={
            "deployment.json": deployment_bytes,
            "airflow-index.json": json_bytes(airflow_index),
            "binding-set.json": (
                runtime_connection_snapshots["binding_set"]
                if runtime_connection_snapshots is not None
                else json_bytes(binding_set)
            ),
            "connection-registry.ref": registry_snapshot,
            "credential-runtime.ref": credential_snapshot,
            **dict(additional_files or {}),
            "_SUCCESS": b"ok\n",
        },
    )
