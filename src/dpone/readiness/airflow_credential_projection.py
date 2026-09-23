"""Bounded build-plane source acquisition and pure credential metadata compiler."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from dpone_airflow_pack.credential_projection_contract import (
    CredentialProjection,
    CredentialProjectionError,
    canonical_projection_bytes,
    parse_credential_projection,
    projection_descriptor,
    require_registry_parity,
)
from dpone_airflow_pack.pack_identity import verify_pack_fingerprint

from dpone.contracts.configuration_errors import ETLConfigurationError
from dpone.contracts.dbt_execution_pack import DBT_EXECUTION_PACK_SCHEMA_V2, DbtExecutionPack
from dpone.contracts.dbt_release_workload_binding import runtime_payload_member
from dpone.gitops.airflow_connection_projection_closure import required_runtime_connection_refs
from dpone.manifest.confined_files import read_confined_file
from dpone.manifest.loader import ManifestLoaderRouter
from dpone.readiness.airflow_connection_bridge_report import airflow_connection_bridge_report
from dpone.readiness.airflow_deployment_artifacts import json_bytes
from dpone.readiness.airflow_deployment_projection_errors import AirflowDeploymentProjectionError
from dpone.readiness.airflow_desired_state_authority import AirflowDesiredStateAuthority
from dpone.runtime.init_fetch_contract import InitFetchError
from dpone.runtime.runtime_payload_archive import (
    extract_runtime_payload,
    runtime_payload_archive,
    verify_runtime_payload_tree,
)


def native_workload_requirements(packs: Mapping[str, Mapping[str, Any]]) -> dict[str, tuple[str, ...]]:
    """Read canonical bounded embedded sources; never guess recursively at refs.

    Native v2 enables the lane; every mixed ordinary workload then participates.
    Legacy-only releases keep their existing projection and wire unchanged.
    """
    requirements: dict[str, tuple[str, ...]] = {}
    for workload_id, pack in packs.items():
        verify_pack_fingerprint(pack)
        workload = pack.get("workload", {})
        if not isinstance(workload, Mapping) or workload.get("manifest") != "runtime/dbt-execution-pack.json":
            continue
        raw = runtime_payload_member(
            pack,
            expected_path="runtime/dbt-execution-pack.json",
            max_archive_bytes=8 * 1024 * 1024,
            max_member_bytes=1024 * 1024,
            label="execution pack",
        )
        execution = DbtExecutionPack.from_mapping(json.loads(raw))
        if execution.schema == DBT_EXECUTION_PACK_SCHEMA_V2:
            if workload_id != f"dbt__{execution.workflow_id}":
                raise CredentialProjectionError("MISMATCH")
            requirements[workload_id] = (execution.profile.connection_ref,)
    if not requirements:
        return {}
    for workload_id, pack in packs.items():
        if workload_id in requirements:
            continue
        try:
            requirements[workload_id] = _transfer_requirements(pack, workload_id)
        except (ValueError, TypeError, KeyError, OSError, InitFetchError, ETLConfigurationError):
            raise CredentialProjectionError("UNSUPPORTED") from None
    return requirements


def _transfer_requirements(pack: Mapping[str, Any], workload_id: str) -> tuple[str, ...]:
    """Read admitted runtime sources using the ordinary multi-file archive contract.

    Build-only staging is private and bounded. Metadata compilation resolves no
    credentials; ambient source registries are explicitly disabled.
    """
    if "runtime_payload_ids" in pack or pack["workload"]["workload_id"] != workload_id:
        raise CredentialProjectionError("MISMATCH")
    runtime = pack.get("runtime_manifest")
    if not isinstance(runtime, Mapping) or runtime.get("kind") not in {"source_manifest", "canonical_manifest"}:
        raise CredentialProjectionError("UNSUPPORTED")
    with TemporaryDirectory(prefix="dpone-credential-source-") as temporary:
        root = Path(temporary) / "source"
        archive = runtime_payload_archive(pack)
        extract_runtime_payload(archive, root)
        verify_runtime_payload_tree(archive, root)
        payload = read_confined_file(root, runtime["path"], max_bytes=8 * 1024 * 1024)
        if _sha(payload) != runtime.get("sha256"):
            raise CredentialProjectionError("MISMATCH")
        loaded = ManifestLoaderRouter(registry_paths=()).load(root / runtime["path"], metadata_only=True)
        if not loaded.processes:
            raise CredentialProjectionError("UNSUPPORTED")
        return required_runtime_connection_refs(loaded.processes)


def build_credential_projection(
    *,
    environment: str,
    release_id: str,
    artifact_registry_ref: str,
    requirements: Mapping[str, tuple[str, ...]],
    binding_set: Mapping[str, Any],
    source_registry: Mapping[str, Any],
    snapshots: Mapping[str, bytes],
    authority: AirflowDesiredStateAuthority | None,
) -> CredentialProjection | None:
    """Join verified native requirements to exact source keys, without secret I/O."""
    if not requirements:
        return None
    if authority is None or authority.workspace_authority_connection_ref is None:
        raise CredentialProjectionError("REQUIRED")
    if authority.environment != environment or authority.artifact_registry_ref != artifact_registry_ref:
        raise CredentialProjectionError("MISMATCH")
    control = authority.workspace_authority_connection_ref
    bindings, entries = binding_set.get("bindings"), source_registry.get("connections")
    if not isinstance(bindings, Mapping) or not isinstance(entries, Mapping):
        raise CredentialProjectionError()
    workloads, selected = [], set()
    for workload_id, refs in sorted(requirements.items()):
        members = []
        for ref, role in sorted({*((ref, "workload") for ref in refs), (control, "workspace_control")}):
            binding = bindings.get(ref)
            if not isinstance(binding, Mapping) or not isinstance(binding.get("connection_ref"), str):
                raise CredentialProjectionError("MISMATCH")
            registry_ref = binding["connection_ref"]
            selected.add(registry_ref)
            members.append({"connection_ref": ref, "registry_ref": registry_ref, "role": role})
        workloads.append({"workload_id": workload_id, "connections": members})
    for ref in selected:
        entry = entries.get(ref)
        credentials = entry.get("credentials") if isinstance(entry, Mapping) else None
        if not isinstance(credentials, Mapping) or not (
            credentials.get("resolver") == "vault_kv"
            or (
                credentials.get("resolver") == "airflow_connection"
                and credentials.get("execution_mode") == "operator_bridge"
            )
        ):
            raise CredentialProjectionError("UNSUPPORTED")
    report = airflow_connection_bridge_report(dict(source_registry), {ref: ref for ref in sorted(selected)})
    bridge = report["projection"]
    sources = []
    if bridge is not None:
        sources = [
            {
                "registry_ref": row["registry_connection_ref"],
                "connection_id": row["connection_id"],
                "secret_name": bridge["secret_name"],
                "secret_key": row["secret_key"],
                "mount_path": row["mount_path"],
                "filename": row["fields"]["uri"],
            }
            for row in bridge["connections"]
        ]
    raw: dict[str, Any] = {
        "schema": "dpone.runtime-credential-projection.v1",
        "environment": environment,
        "release_id": release_id,
        "binding_set_sha256": _sha(snapshots["binding_set"]),
        "source_registry_sha256": _sha(json_bytes(source_registry)),
        "runtime_registry_sha256": _sha(snapshots["connection_registry"]),
        "publish_authority_sha256": authority.publish_authority_sha256,
        "workspace_authority_connection_ref": control,
        "sources": sources,
        "workloads": workloads,
    }
    payload = canonical_projection_bytes(raw)
    projection = parse_credential_projection(
        payload,
        descriptor=projection_descriptor(payload),
        environment=environment,
        release_id=release_id,
        binding_set_sha256=raw["binding_set_sha256"],
        runtime_registry_sha256=raw["runtime_registry_sha256"],
    )
    require_registry_parity(projection, binding_set=binding_set, registry=json.loads(snapshots["connection_registry"]))
    return projection


def _sha(payload: bytes) -> str:
    return "sha256:" + hashlib.sha256(payload).hexdigest()


def compile_native_credential_projection(
    *,
    environment: str,
    release_id: str,
    artifact_registry_ref: str,
    packs: Mapping[str, Mapping[str, Any]],
    binding_set: Mapping[str, Any],
    source_registry: Mapping[str, Any],
    snapshots: Mapping[str, bytes],
    authority: AirflowDesiredStateAuthority | None,
) -> bytes | None:
    """Compile verified native sources and redact input failures at build boundary."""
    try:
        projection = build_credential_projection(
            environment=environment,
            release_id=release_id,
            artifact_registry_ref=artifact_registry_ref,
            requirements=native_workload_requirements(packs),
            binding_set=binding_set,
            source_registry=source_registry,
            snapshots=snapshots,
            authority=authority,
        )
        return canonical_projection_bytes(projection.to_dict()) if projection is not None else None
    except CredentialProjectionError as exc:
        raise AirflowDeploymentProjectionError(exc.code, str(exc)) from None
    except (ValueError, TypeError, KeyError):
        raise AirflowDeploymentProjectionError(
            "DPONE_RUNTIME_CREDENTIAL_PROJECTION_INVALID", "native credential projection source is invalid"
        ) from None
