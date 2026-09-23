"""Verify sealed credential metadata against watcher authority before mutation."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any

from dpone_airflow_pack.credential_projection_contract import CredentialProjectionError

from dpone.runtime.airflow_credential_projection_inventory import verify_credential_projection_files
from dpone.runtime.deployment_cache_common import DeploymentCacheError


def require_activation_credential_authority(
    deployment: Mapping[str, Any],
    *,
    deployment_dir: Path,
    cache_root: Path,
    control_ref: str | None,
    authority_sha256: str | None,
) -> None:
    """Consume an already integrity-validated deployment; never resolve secrets."""
    try:
        projection = verify_credential_projection_files(deployment, deployment_dir=deployment_dir, root=cache_root)
        if projection is None:
            if control_ref is not None:
                raise CredentialProjectionError("REQUIRED")
            return
        if control_ref is None or authority_sha256 is None:
            raise CredentialProjectionError("REQUIRED")
        projection.require_authority(control_ref=control_ref, authority_sha256=authority_sha256)
    except CredentialProjectionError as exc:
        raise DeploymentCacheError(
            exc.code, "deployment credentials differ from protected workspace authority"
        ) from exc
