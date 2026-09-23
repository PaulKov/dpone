"""Exact local/publication inventory for deployment-owned credential metadata."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any

from dpone_airflow_pack.credential_projection_contract import (
    PROJECTION_FILENAME,
    CredentialProjection,
    require_projection_descriptor,
)

from dpone.manifest.confined_files import read_confined_file
from dpone.runtime.airflow_runtime_connection_inventory import RuntimeConnectionPublicationSpec
from dpone.runtime.init_fetch_contract import cache_relative_path
from dpone.runtime.runtime_credential_projection import verify_deployment_credential_projection


def verify_credential_projection_files(
    deployment: Mapping[str, Any],
    *,
    deployment_dir: Path,
    root: Path,
) -> CredentialProjection | None:
    """Read bounded non-secret bytes from one immutable deployment directory."""
    if deployment.get("schema") != "dpone.deployment-set.v6":
        return None
    descriptor = require_projection_descriptor(deployment.get("credential_projection"))

    def read(name: str, maximum: int) -> bytes:
        return read_confined_file(root, (deployment_dir / name).relative_to(root).as_posix(), max_bytes=maximum)

    return verify_deployment_credential_projection(
        deployment=deployment,
        projection_payload=read(PROJECTION_FILENAME, int(descriptor["bytes"])),
        binding_set_payload=read("binding-set.json", int(deployment["binding_set"]["bytes"])),
        registry_payload=read("connection-registry.ref", int(deployment["connection_registry"]["bytes"])),
    )


def credential_projection_publication_file(
    deployment: Mapping[str, Any],
    *,
    deployment_dir: Path,
    root: Path,
) -> RuntimeConnectionPublicationSpec | None:
    """Publish the verified local file under its independent content address."""
    if verify_credential_projection_files(deployment, deployment_dir=deployment_dir, root=root) is None:
        return None
    descriptor = require_projection_descriptor(deployment["credential_projection"])
    return RuntimeConnectionPublicationSpec(
        key=cache_relative_path(descriptor["artifact_ref"]),
        path=deployment_dir / PROJECTION_FILENAME,
        source_root=root,
    )
