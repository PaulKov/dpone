"""Materialize physical MSSQL Asset URIs for deployment from logical asset_ref.

Canonical resolution path (same as ``BindingCredentialResolver``):

1. logical ``asset_ref.connection_ref``
2. ``binding_set.bindings[logical_ref].connection_ref`` (exact bound registry ref)
3. ``connection_registry.connections[bound_ref].asset_authority``
4. physical AIP-60 ``mssql://`` URI

The closed wire document is ``dpone.mssql-asset-outlet-projection.v1`` and is
part of deployment identity (hashed into ``deployment_id``).
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from typing import Any

from dpone_airflow_pack.mssql_asset_ref_codec import require_asset_ref_sha256
from dpone_airflow_pack.mssql_outlet_projection_contract import (
    MSSQL_ASSET_OUTLET_PROJECTION_SCHEMA,
    PROJECTION_INVALID,
    compute_projection_sha256,
)

from dpone.gitops.airflow_mssql_asset_authority import (
    load_mssql_asset_authority_index,
)
from dpone.gitops.airflow_mssql_logical_asset import (
    MssqlLogicalAssetRef,
    materialize_mssql_logical_asset_uri,
    parse_mssql_logical_asset_ref,
)
from dpone.readiness.airflow_deployment_projection_errors import (
    AirflowDeploymentProjectionError,
)


def build_mssql_asset_outlet_projection(
    *,
    environment: str,
    binding_set: Mapping[str, Any],
    binding_set_ref: str,
    connection_registry: Mapping[str, Any],
    connection_registry_ref: str,
    workload_packs: Sequence[Mapping[str, Any]],
    pack_payloads: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any] | None:
    """Build a deployment-owned outlet projection, or ``None`` when unused.

    Release packs retain logical ``asset_ref`` entries. This projection binds
    those refs through the environment binding-set to registry authorities and
    attaches the closed document to the deployment (not the release).
    """

    authorities = load_mssql_asset_authority_index(connection_registry)
    entries_by_digest: dict[str, dict[str, Any]] = {}
    for pack_meta in workload_packs:
        workload_id = str(pack_meta.get("id") or "").strip()
        pack = pack_payloads.get(workload_id)
        if not workload_id or not isinstance(pack, Mapping):
            continue
        for item in _pack_outlets(pack):
            if not isinstance(item, Mapping) or not isinstance(item.get("asset_ref"), Mapping):
                continue
            asset_ref, issues = parse_mssql_logical_asset_ref(item.get("asset_ref"), path=workload_id)
            if issues or asset_ref is None:
                raise AirflowDeploymentProjectionError(
                    PROJECTION_INVALID,
                    issues[0].message if issues else "mssql asset_ref is invalid",
                    path=workload_id,
                )
            bound_ref = _bound_registry_ref(
                binding_set,
                logical_ref=asset_ref.connection_ref,
                path=workload_id,
            )
            authority = authorities.get(bound_ref)
            if authority is None:
                raise AirflowDeploymentProjectionError(
                    PROJECTION_INVALID,
                    (
                        f"bound registry connection_ref {bound_ref!r} for logical "
                        f"{asset_ref.connection_ref!r} has no deployment-owned "
                        "asset_authority in the connection registry"
                    ),
                    path=workload_id,
                )
            resolution = materialize_mssql_logical_asset_uri(
                asset_ref,
                authority=authority,
                path=workload_id,
            )
            if not resolution.ok or resolution.uri is None:
                message = (
                    resolution.issues[0].message if resolution.issues else "mssql asset_ref materialization failed"
                )
                raise AirflowDeploymentProjectionError(
                    PROJECTION_INVALID,
                    message,
                    path=workload_id,
                )
            digest = require_asset_ref_sha256(asset_ref.to_mapping())
            prior = entries_by_digest.get(digest)
            if prior is not None and prior["uri"] != resolution.uri:
                raise AirflowDeploymentProjectionError(
                    PROJECTION_INVALID,
                    f"conflicting physical URIs for logical asset_ref {digest}",
                    path=workload_id,
                )
            if prior is not None and prior["registry_connection_ref"] != bound_ref:
                raise AirflowDeploymentProjectionError(
                    PROJECTION_INVALID,
                    f"conflicting bound registry refs for logical asset_ref {digest}",
                    path=workload_id,
                )
            if prior is None:
                entries_by_digest[digest] = {
                    "workload_ids": [workload_id],
                    "asset_ref": asset_ref.to_mapping(),
                    "asset_ref_sha256": digest,
                    "registry_connection_ref": bound_ref,
                    "resolved_binding": {"registry_connection_ref": bound_ref},
                    "uri": resolution.uri,
                }
            elif workload_id not in prior["workload_ids"]:
                prior["workload_ids"] = sorted({*prior["workload_ids"], workload_id})
    if not entries_by_digest:
        return None
    entries = sorted(
        entries_by_digest.values(),
        key=lambda item: (item["asset_ref_sha256"], item["workload_ids"][0], item["uri"]),
    )
    projection: dict[str, Any] = {
        "schema": MSSQL_ASSET_OUTLET_PROJECTION_SCHEMA,
        "environment": environment,
        "binding_set_ref": binding_set_ref,
        "connection_registry_ref": connection_registry_ref,
        "entries": entries,
    }
    projection["projection_sha256"] = compute_projection_sha256(projection)
    return projection


def load_pack_payloads_from_descriptors(
    workload_packs: Sequence[Mapping[str, Any]],
    *,
    cache_root: Any,
    reader: Any,
    max_bytes: int = 16 * 1024 * 1024,
) -> dict[str, dict[str, Any]]:
    """Load release pack JSON objects keyed by workload id."""

    from pathlib import Path

    payloads: dict[str, dict[str, Any]] = {}
    root = Path(cache_root)
    for item in workload_packs:
        workload_id = str(item.get("id") or "").strip()
        artifact_ref = str(item.get("artifact_ref") or "").strip()
        if not workload_id or not artifact_ref.startswith("cache://"):
            continue
        relative = artifact_ref.removeprefix("cache://")
        raw = reader(root, relative, max_bytes=max_bytes)
        payload = json.loads(raw.decode("utf-8"))
        if isinstance(payload, dict):
            payloads[workload_id] = payload
    return payloads


def _bound_registry_ref(
    binding_set: Mapping[str, Any],
    *,
    logical_ref: str,
    path: str,
) -> str:
    """Resolve logical → exact bound registry ref (``BindingCredentialResolver`` path)."""

    bindings = binding_set.get("bindings")
    if not isinstance(bindings, Mapping):
        raise AirflowDeploymentProjectionError(
            PROJECTION_INVALID,
            "binding_set.bindings must be a mapping for mssql outlet projection",
            path=path,
        )
    binding = bindings.get(logical_ref)
    if not isinstance(binding, Mapping):
        raise AirflowDeploymentProjectionError(
            PROJECTION_INVALID,
            f"Connection ref is not bound: {logical_ref}",
            path=path,
        )
    ref = binding.get("connection_ref")
    if not isinstance(ref, str) or not ref.strip():
        raise AirflowDeploymentProjectionError(
            PROJECTION_INVALID,
            f"Connection ref is not bound: {logical_ref}",
            path=path,
        )
    return ref.strip()


def _pack_outlets(pack: Mapping[str, Any]) -> list[Any]:
    airflow = pack.get("airflow")
    if not isinstance(airflow, Mapping):
        return []
    execution = airflow.get("execution")
    if not isinstance(execution, Mapping):
        return []
    raw = execution.get("outlets")
    return list(raw) if isinstance(raw, list) else []


__all__ = [
    "MSSQL_ASSET_OUTLET_PROJECTION_SCHEMA",
    "MssqlLogicalAssetRef",
    "build_mssql_asset_outlet_projection",
    "load_pack_payloads_from_descriptors",
]
