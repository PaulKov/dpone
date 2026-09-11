"""Fail-closed validation of one local Airflow deployment projection."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from dpone.contracts.airflow_deployment_projection import (
    deployment_projection_release_identity_violation,
    deployment_projection_violation,
)
from dpone.runtime.deployment_cache_common import (
    DeploymentCacheError,
    open_regular_file,
    read_regular_json_object,
    require_path_without_symlinks,
)
from dpone.runtime.deployment_cache_integrity import (
    DEFAULT_MAX_CACHE_ARTIFACT_BYTES,
    DeploymentCacheIntegrityVerifier,
)
from dpone.runtime.deployment_cache_models import ValidatedDeploymentProjection


class DeploymentCacheProjectionValidator:
    """Validate layout, identities, mirrors, release, and artifact bytes."""

    def __init__(
        self,
        cache_root: Path,
        *,
        max_artifact_bytes: int = DEFAULT_MAX_CACHE_ARTIFACT_BYTES,
    ) -> None:
        self._cache_root = cache_root
        self._integrity_verifier = DeploymentCacheIntegrityVerifier(
            cache_root,
            max_artifact_bytes=max_artifact_bytes,
        )

    def validate(self, deployment_dir: str | Path, *, environment: str) -> dict[str, Any]:
        """Return the verified content identities for one deployment."""

        return self.validate_details(deployment_dir, environment=environment).identity()

    def validate_details(
        self,
        deployment_dir: str | Path,
        *,
        environment: str,
    ) -> ValidatedDeploymentProjection:
        """Return deployment and index payloads from the verified file descriptors."""

        requested_path = Path(deployment_dir).absolute()
        if not _is_relative_to(requested_path, self._cache_root):
            raise DeploymentCacheError(
                "DPONE_DEPLOYMENT_PATH_OUTSIDE_CACHE_ROOT",
                "deployment directory must be inside the configured cache root",
                path=requested_path.as_posix(),
            )
        require_path_without_symlinks(requested_path, root=self._cache_root, error_path=requested_path)
        deployment_path = requested_path.resolve(strict=False)
        relative_parts = deployment_path.relative_to(self._cache_root).parts
        if len(relative_parts) != 3 or relative_parts[0] != "deployments":
            raise _invalid_layout(deployment_path)
        projection = self._validate_projection(deployment_path, environment=environment)
        expected_path = self._cache_root / "deployments" / environment / _deployment_dir_name(projection.deployment_id)
        if deployment_path != expected_path:
            raise _invalid_layout(deployment_path)
        return projection

    def validate_activation_details(
        self,
        deployment_dir: str | Path,
        *,
        environment: str,
    ) -> ValidatedDeploymentProjection:
        """Validate one sealed activation in its canonical activation layout."""

        deployment_path = self._confined_path(deployment_dir)
        relative_parts = deployment_path.relative_to(self._cache_root).parts
        if len(relative_parts) != 3 or relative_parts[:2] != ("activations", environment):
            raise _invalid_activation_layout(deployment_path)
        projection = self._validate_projection(deployment_path, environment=environment)
        expected_path = self._cache_root / "activations" / environment / _deployment_dir_name(projection.deployment_id)
        if deployment_path != expected_path:
            raise _invalid_activation_layout(deployment_path)
        return projection

    def validate_staged_activation_details(
        self,
        deployment_dir: str | Path,
        *,
        environment: str,
        expected_deployment_id: str,
    ) -> ValidatedDeploymentProjection:
        """Validate sealed bytes before their atomic activation rename."""

        deployment_path = self._confined_path(deployment_dir)
        expected_prefix = f".{_deployment_dir_name(expected_deployment_id)}.tmp."
        if (
            deployment_path.parent != self._cache_root / "activations" / environment
            or not deployment_path.name.startswith(expected_prefix)
        ):
            raise _invalid_activation_layout(deployment_path)
        projection = self._validate_projection(deployment_path, environment=environment)
        if projection.deployment_id != expected_deployment_id:
            raise DeploymentCacheError(
                "DPONE_DEPLOYMENT_ID_MISMATCH",
                "activation snapshot identity differs from the verified candidate",
                path=deployment_path.as_posix(),
            )
        return projection

    def validate_detached_details(
        self,
        deployment_dir: str | Path,
        *,
        environment: str,
        expected_deployment_id: str,
    ) -> ValidatedDeploymentProjection:
        """Validate reviewed bytes after an atomic retention detach."""

        deployment_path = self._confined_path(deployment_dir)
        expected_prefix = f"{_deployment_dir_name(expected_deployment_id)}."
        if deployment_path.parent != self._cache_root / ".retention-trash" or not deployment_path.name.startswith(
            expected_prefix
        ):
            raise _invalid_detached_layout(deployment_path)
        projection = self._validate_projection(deployment_path, environment=environment)
        if projection.deployment_id != expected_deployment_id:
            raise DeploymentCacheError(
                "DPONE_DEPLOYMENT_ID_MISMATCH",
                "detached deployment identity differs from the reviewed candidate",
                path=deployment_path.as_posix(),
            )
        return projection

    def validate_current_details(
        self,
        deployment_dir: str | Path,
        *,
        environment: str,
    ) -> ValidatedDeploymentProjection:
        """Validate current from the new activation or legacy deployment layout."""

        deployment_path = self._confined_path(deployment_dir)
        relative_parts = deployment_path.relative_to(self._cache_root).parts
        if relative_parts and relative_parts[0] == "activations":
            return self.validate_activation_details(deployment_path, environment=environment)
        return self.validate_details(deployment_path, environment=environment)

    def _confined_path(self, deployment_dir: str | Path) -> Path:
        requested_path = Path(deployment_dir).absolute()
        if not _is_relative_to(requested_path, self._cache_root):
            raise DeploymentCacheError(
                "DPONE_DEPLOYMENT_PATH_OUTSIDE_CACHE_ROOT",
                "deployment directory must be inside the configured cache root",
                path=requested_path.as_posix(),
            )
        require_path_without_symlinks(requested_path, root=self._cache_root, error_path=requested_path)
        return requested_path.resolve(strict=False)

    def _validate_projection(
        self,
        deployment_dir: Path,
        *,
        environment: str,
    ) -> ValidatedDeploymentProjection:
        success_marker = deployment_dir / "_SUCCESS"
        _require_regular_file(
            success_marker,
            missing_code="DPONE_DEPLOYMENT_INCOMPLETE",
            invalid_code="DPONE_DEPLOYMENT_INCOMPLETE",
            label="deployment _SUCCESS marker",
        )
        deployment_path = deployment_dir / "deployment.json"
        deployment = read_regular_json_object(
            deployment_path,
            missing_code="DPONE_DEPLOYMENT_NOT_FOUND",
            invalid_code="DPONE_DEPLOYMENT_INVALID",
            label="deployment",
            root=self._cache_root,
        )
        index_path = deployment_dir / "airflow-index.json"
        index = read_regular_json_object(
            index_path,
            missing_code="DPONE_AIRFLOW_INDEX_NOT_FOUND",
            invalid_code="DPONE_AIRFLOW_INDEX_INVALID",
            label="airflow deployment index",
            root=self._cache_root,
        )
        _validate_headers(
            deployment,
            index,
            environment=environment,
            deployment_path=deployment_path,
            index_path=index_path,
        )
        release_identity_violation = deployment_projection_release_identity_violation(
            deployment,
            index,
        )
        if release_identity_violation is not None:
            raise DeploymentCacheError(
                release_identity_violation.code,
                release_identity_violation.message,
                path=deployment_path.as_posix(),
            )
        release_id = str(deployment["release_ref"])
        release_authority = self._integrity_verifier.verify_projection_authority(
            index=index,
            index_path=index_path,
            release_id=release_id,
        )
        violation = deployment_projection_violation(
            deployment,
            index,
            release_schema=release_authority.schema,
        )
        if violation is not None:
            violation_path = (
                index_path
                if violation.code in {"DPONE_DEPLOYMENT_ID_MISMATCH", "DPONE_DEPLOYMENT_INDEX_MIRROR_MISMATCH"}
                else deployment_path
            )
            raise DeploymentCacheError(violation.code, violation.message, path=violation_path.as_posix())
        return ValidatedDeploymentProjection(
            deployment=deployment,
            airflow_index=index,
            dbt_runtime_wire_contract=release_authority.dbt_runtime_wire_contract,
        )


_DEPLOYMENT_SCHEMAS = frozenset(
    {
        "dpone.deployment-set.v1",
        "dpone.deployment-set.v2",
        "dpone.deployment-set.v3",
    }
)
_INDEX_SCHEMAS = frozenset(
    {
        "dpone.airflow-deployment-index.v1",
        "dpone.airflow-deployment-index.v2",
        "dpone.airflow-deployment-index.v3",
    }
)
_SCHEMA_WIRE_PAIRS = frozenset(
    {
        ("dpone.deployment-set.v1", "dpone.airflow-deployment-index.v1"),
        ("dpone.deployment-set.v2", "dpone.airflow-deployment-index.v2"),
        ("dpone.deployment-set.v3", "dpone.airflow-deployment-index.v3"),
    }
)


def _validate_headers(
    deployment: dict[str, Any],
    index: dict[str, Any],
    *,
    environment: str,
    deployment_path: Path,
    index_path: Path,
) -> None:
    deployment_schema = deployment.get("schema")
    index_schema = index.get("schema")
    if deployment_schema not in _DEPLOYMENT_SCHEMAS:
        raise DeploymentCacheError(
            "DPONE_DEPLOYMENT_SCHEMA_INVALID",
            "deployment schema is invalid",
            path=deployment_path.as_posix(),
        )
    if deployment.get("environment") != environment:
        raise DeploymentCacheError(
            "DPONE_DEPLOYMENT_ENVIRONMENT_MISMATCH",
            "deployment environment does not match requested promotion environment",
            path=deployment_path.as_posix(),
        )
    if index_schema not in _INDEX_SCHEMAS:
        raise DeploymentCacheError(
            "DPONE_AIRFLOW_INDEX_SCHEMA_INVALID",
            "airflow index schema is invalid",
            path=index_path.as_posix(),
        )
    if (deployment_schema, index_schema) not in _SCHEMA_WIRE_PAIRS:
        raise DeploymentCacheError(
            "DPONE_DEPLOYMENT_INDEX_MIRROR_MISMATCH",
            "deployment and airflow index schema wires do not match",
            path=index_path.as_posix(),
        )


def _require_regular_file(path: Path, *, missing_code: str, invalid_code: str, label: str) -> None:
    descriptor = open_regular_file(path, missing_code=missing_code, invalid_code=invalid_code, label=label)
    os.close(descriptor)


def _invalid_layout(path: Path) -> DeploymentCacheError:
    return DeploymentCacheError(
        "DPONE_DEPLOYMENT_PATH_INVALID",
        "deployment directory must match .dpone-cache/deployments/<environment>/<deployment_id>",
        path=path.as_posix(),
    )


def _invalid_activation_layout(path: Path) -> DeploymentCacheError:
    return DeploymentCacheError(
        "DPONE_DEPLOYMENT_ACTIVATION_PATH_INVALID",
        "activation must match .dpone-cache/activations/<environment>/<deployment_id>",
        path=path.as_posix(),
    )


def _invalid_detached_layout(path: Path) -> DeploymentCacheError:
    return DeploymentCacheError(
        "DPONE_DEPLOYMENT_RETENTION_DETACH_PATH_INVALID",
        "detached deployment must match .dpone-cache/.retention-trash/<deployment_id>.<nonce>",
        path=path.as_posix(),
    )


def _is_relative_to(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
    except ValueError:
        return False
    return True


def _deployment_dir_name(deployment_id: str) -> str:
    return deployment_id.replace(":", "-", 1)


__all__ = ["DeploymentCacheProjectionValidator"]
