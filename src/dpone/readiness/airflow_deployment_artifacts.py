"""Immutable, bounded inputs for Airflow deployment projections."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dpone.contracts.airflow_deployment import (
    canonical_fingerprint,
)
from dpone.manifest.confined_files import read_confined_file
from dpone.readiness.airflow_deployment_artifacts_io import (
    bytes_descriptor,
    capture_confined_source,
    digest_dir,
    json_bytes,
    normalize_environment_segment,
    parse_json_object,
    read_environment_yaml,
)
from dpone.readiness.airflow_deployment_projection_errors import (
    AirflowDeploymentProjectionError,
)
from dpone.readiness.airflow_release_artifact_index import (
    MAX_RELEASE_ARTIFACT_BYTES,
    MAX_RUNTIME_PAYLOAD_BYTES,
    ReleaseArtifactRules,
    index_release_artifacts,
    require_release_identity,
)
from dpone.readiness.airflow_release_schema_validation import (
    validate_release_set_schema,
)
from dpone.readiness.airflow_runtime_payload_refs import (
    RuntimePayloadRefError,
    require_runtime_payload_refs,
)

MAX_RELEASE_SET_BYTES = 8 * 1024 * 1024
MAX_ENVIRONMENT_INPUT_BYTES = 1024 * 1024


@dataclass(frozen=True, slots=True)
class DeploymentProjectionInputs:
    """One validated snapshot of all source material used by a projection."""

    environment: str
    release_bytes: bytes
    binding_set: dict[str, Any]
    connection_registry: dict[str, Any]
    credential_runtime: dict[str, Any]
    binding_fingerprint: str
    registry_fingerprint: str
    credential_runtime_fingerprint: str
    dag_specs: list[dict[str, Any]]
    workload_packs: list[dict[str, Any]]
    runtime_payloads: list[dict[str, Any]]
    release_schema: str = "dpone.release-set.v1"


_STRICT_V2_RULES = ReleaseArtifactRules(
    require_canonical_checksums=True,
    require_positive_bytes=True,
    require_pack_fingerprint=True,
)
_LOCAL_SAFE_SAMPLE_V1_RULES = ReleaseArtifactRules(
    require_canonical_checksums=False,
    require_positive_bytes=False,
    require_pack_fingerprint=False,
)


def load_strict_projection_inputs(
    *,
    root: Path,
    cache_root: Path,
    release_id: str,
    environment: str,
) -> DeploymentProjectionInputs:
    """Load the strict v2 source snapshots."""

    return _load_projection_inputs(
        root=root,
        cache_root=cache_root,
        release_id=release_id,
        environment=environment,
        rules=_STRICT_V2_RULES,
    )


def load_local_safe_sample_v1_inputs(
    *,
    root: Path,
    cache_root: Path,
    release_id: str,
    environment: str,
) -> DeploymentProjectionInputs:
    """Load only the explicit local v1 compatibility source snapshots."""

    return _load_projection_inputs(
        root=root,
        cache_root=cache_root,
        release_id=release_id,
        environment=environment,
        rules=_LOCAL_SAFE_SAMPLE_V1_RULES,
    )


def _load_projection_inputs(
    *,
    root: Path,
    cache_root: Path,
    release_id: str,
    environment: str,
    rules: ReleaseArtifactRules,
) -> DeploymentProjectionInputs:
    """Capture and validate the source snapshots shared by both projection lanes."""

    environment = normalize_environment_segment(environment)
    release_relative = f"releases/{digest_dir(release_id)}/release-set.json"
    release_path = cache_root / release_relative
    release_bytes = capture_confined_source(
        cache_root,
        release_relative,
        path=release_path,
        max_bytes=MAX_RELEASE_SET_BYTES,
        missing_code="DPONE_RELEASE_NOT_FOUND",
        too_large_code="DPONE_RELEASE_SET_TOO_LARGE",
        changed_code="DPONE_RELEASE_INPUT_CHANGED",
        unsafe_code="DPONE_RELEASE_INPUT_UNSAFE",
        reader=read_confined_file,
    )
    release = parse_json_object(
        release_bytes,
        path=release_path,
        invalid_code="DPONE_RELEASE_SET_INVALID",
        label="release-set",
    )
    validate_release_set_schema(release, path=release_path)
    require_release_identity(release, requested_release_id=release_id, path=release_path)
    if release.get("schema") == "dpone.release-set.v3":
        from dpone.manifest.release_composition_files import verify_composition_transport_files

        try:
            verify_composition_transport_files(release_path.parent, release)
        except (ValueError, OSError) as exc:
            raise AirflowDeploymentProjectionError(
                "DPONE_COMPOSITION_INVALID", "composition transport inventory is incomplete or corrupt"
            ) from exc

    binding_relative = f"environments/{environment}/binding-set.yaml"
    registry_relative = f"platform/connection-registries/{environment}.yaml"
    credential_relative = f"environments/{environment}/credential-runtime.yaml"
    binding_set = read_environment_yaml(
        root,
        binding_relative,
        code_prefix="DPONE_BINDING_SET",
        max_bytes=MAX_ENVIRONMENT_INPUT_BYTES,
        reader=read_confined_file,
    )
    connection_registry = read_environment_yaml(
        root,
        registry_relative,
        code_prefix="DPONE_CONNECTION_REGISTRY",
        max_bytes=MAX_ENVIRONMENT_INPUT_BYTES,
        reader=read_confined_file,
    )
    credential_runtime = read_environment_yaml(
        root,
        credential_relative,
        code_prefix="DPONE_CREDENTIAL_RUNTIME",
        max_bytes=MAX_ENVIRONMENT_INPUT_BYTES,
        reader=read_confined_file,
    )
    binding_set_path = root / binding_relative
    connection_registry_path = root / registry_relative
    credential_runtime_path = root / credential_relative
    for payload, path, code in (
        (binding_set, binding_set_path, "DPONE_BINDING_SET_ENVIRONMENT_MISMATCH"),
        (connection_registry, connection_registry_path, "DPONE_CONNECTION_REGISTRY_ENVIRONMENT_MISMATCH"),
        (credential_runtime, credential_runtime_path, "DPONE_CREDENTIAL_RUNTIME_ENVIRONMENT_MISMATCH"),
    ):
        if payload.get("environment") != environment:
            raise AirflowDeploymentProjectionError(
                code,
                "environment-owned file does not match requested deployment environment",
                path=path.as_posix(),
            )
    workload_packs = index_release_artifacts(
        release,
        release_id=release_id,
        cache_root=cache_root,
        section="workload_packs",
        rules=rules,
        max_release_artifact_bytes=MAX_RELEASE_ARTIFACT_BYTES,
        max_runtime_payload_bytes=MAX_RUNTIME_PAYLOAD_BYTES,
        reader=read_confined_file,
    )
    runtime_payloads = index_release_artifacts(
        release,
        release_id=release_id,
        cache_root=cache_root,
        section="runtime_payloads",
        rules=rules,
        max_release_artifact_bytes=MAX_RELEASE_ARTIFACT_BYTES,
        max_runtime_payload_bytes=MAX_RUNTIME_PAYLOAD_BYTES,
        reader=read_confined_file,
    )
    try:
        require_runtime_payload_refs(
            workload_packs=workload_packs,
            runtime_payloads=runtime_payloads,
            code="DPONE_RUNTIME_PAYLOAD_REFS_DANGLING",
        )
    except RuntimePayloadRefError as exc:
        raise AirflowDeploymentProjectionError(
            exc.code,
            str(exc),
            path=exc.path,
        ) from exc
    return DeploymentProjectionInputs(
        environment=environment,
        release_bytes=release_bytes,
        binding_set=binding_set,
        connection_registry=connection_registry,
        credential_runtime=credential_runtime,
        binding_fingerprint=canonical_fingerprint(binding_set),
        registry_fingerprint=canonical_fingerprint(connection_registry),
        credential_runtime_fingerprint=canonical_fingerprint(credential_runtime),
        dag_specs=index_release_artifacts(
            release,
            release_id=release_id,
            cache_root=cache_root,
            section="dag_specs",
            rules=rules,
            max_release_artifact_bytes=MAX_RELEASE_ARTIFACT_BYTES,
            max_runtime_payload_bytes=MAX_RUNTIME_PAYLOAD_BYTES,
            reader=read_confined_file,
        ),
        workload_packs=workload_packs,
        runtime_payloads=runtime_payloads,
        release_schema=str(release["schema"]),
    )


__all__ = [
    "DeploymentProjectionInputs",
    "MAX_ENVIRONMENT_INPUT_BYTES",
    "MAX_RELEASE_ARTIFACT_BYTES",
    "MAX_RELEASE_SET_BYTES",
    "MAX_RUNTIME_PAYLOAD_BYTES",
    "bytes_descriptor",
    "digest_dir",
    "json_bytes",
    "load_local_safe_sample_v1_inputs",
    "load_strict_projection_inputs",
    "normalize_environment_segment",
]
