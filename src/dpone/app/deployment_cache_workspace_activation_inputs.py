"""Compose verified sealed-cache inputs for workspace activation preparation."""

from __future__ import annotations

from pathlib import Path
from threading import Lock
from typing import TYPE_CHECKING, Any

from dpone.contracts.airflow_deployment import is_canonical_sha256_digest
from dpone.contracts.dbt_workspace_activation import DbtWorkspaceActivationError
from dpone.contracts.dbt_workspace_runtime_authority import DbtWorkspaceRuntimeAuthority
from dpone.runtime.airflow_runtime_connection_inventory import (
    RUNTIME_CONNECTION_FILES,
    validate_deployment_auxiliary_files,
)
from dpone.runtime.deployment_cache_common import read_regular_json_object, require_path_without_symlinks

if TYPE_CHECKING:
    from dpone.contracts.dbt_source_inventory_binding import DbtReleaseSources
    from dpone.contracts.runtime_connection import ResolvedBindingConnection
    from dpone.ports.dbt_workspace_activation import (
        DbtWorkspaceConnectionResolver,
        DbtWorkspaceRuntimeResolverFactory,
    )
    from dpone.services.dbt_release_source_reader import DbtReleaseSourceReader


class DeploymentCacheWorkspaceActivationInputs:
    """Load exact source/runtime authorities while keeping resolution injected."""

    def __init__(
        self,
        *,
        cache_root: Path,
        source_reader: DbtReleaseSourceReader,
        resolver_factory: DbtWorkspaceRuntimeResolverFactory,
    ) -> None:
        self._cache_root = cache_root.resolve(strict=False)
        self._source_reader = source_reader
        self._resolver_factory = resolver_factory
        self._resolvers: dict[str, DbtWorkspaceConnectionResolver] = {}
        self._resolver_lock = Lock()

    def load_sources(self, *, projection_root: Path, release_id: str) -> DbtReleaseSources:
        """Read the complete immutable release closure, not the authoring tree."""

        self._require_projection_root(projection_root)
        if not is_canonical_sha256_digest(release_id):
            raise DbtWorkspaceActivationError("release_id")
        release_root = self._cache_root / "releases" / release_id.replace(":", "-")
        require_path_without_symlinks(release_root, root=self._cache_root, error_path=release_root)
        try:
            return self._source_reader.read(release_root, expected_release_id=release_id)
        except Exception:
            raise DbtWorkspaceActivationError("source_inventory") from None

    def load_runtime_authority(
        self,
        *,
        projection_root: Path,
        environment: str,
        release_id: str,
        deployment_id: str,
    ) -> DbtWorkspaceRuntimeAuthority:
        """Verify mirrored descriptors and retain only an injected resolver capability."""

        root = self._require_projection_root(projection_root)
        try:
            deployment = self._payload(root, "deployment.json", "deployment")
            index = self._payload(root, "airflow-index.json", "airflow index")
            self._require_coordinates(
                deployment,
                index,
                environment=environment,
                release_id=release_id,
                deployment_id=deployment_id,
            )
            validate_deployment_auxiliary_files(deployment, root)
            snapshots = {
                name: self._payload(root, local_name, name.replace("_", "-"))
                for name, (_, local_name, _, _) in RUNTIME_CONNECTION_FILES.items()
            }
            authority = self._authority(deployment, index)
            resolver = self._resolver_factory.build(authority=authority, **snapshots)
        except DbtWorkspaceActivationError:
            raise
        except Exception:
            raise DbtWorkspaceActivationError("runtime_context") from None
        with self._resolver_lock:
            existing = self._resolvers.get(authority.authority_subject_sha256)
            if existing is None:
                self._resolvers[authority.authority_subject_sha256] = resolver
        return authority

    def resolve_connection(
        self,
        authority: DbtWorkspaceRuntimeAuthority,
        connection_ref: str,
    ) -> ResolvedBindingConnection:
        """Use only the resolver built from this exact verified authority."""

        authority.__post_init__()
        with self._resolver_lock:
            resolver = self._resolvers.get(authority.authority_subject_sha256)
        if resolver is None:
            raise DbtWorkspaceActivationError("runtime_context_not_loaded")
        try:
            return resolver.resolve(connection_ref)
        except Exception:
            raise DbtWorkspaceActivationError("connection_resolution") from None

    def _require_projection_root(self, value: Path) -> Path:
        root = value.absolute()
        try:
            relative = root.relative_to(self._cache_root)
        except ValueError:
            raise DbtWorkspaceActivationError("projection_root") from None
        if len(relative.parts) != 3 or relative.parts[0] != "activations":
            raise DbtWorkspaceActivationError("projection_root")
        require_path_without_symlinks(root, root=self._cache_root, error_path=root)
        return root

    def _payload(self, root: Path, filename: str, label: str) -> dict[str, Any]:
        return read_regular_json_object(
            root / filename,
            missing_code="DPONE_DBT_WORKSPACE_ADMISSION_UNAVAILABLE",
            invalid_code="DPONE_DBT_WORKSPACE_ADMISSION_UNAVAILABLE",
            label=label,
            root=self._cache_root,
        )

    @staticmethod
    def _require_coordinates(
        deployment: dict[str, Any],
        index: dict[str, Any],
        *,
        environment: str,
        release_id: str,
        deployment_id: str,
    ) -> None:
        if (
            deployment.get("environment") != environment
            or deployment.get("release_ref") != release_id
            or deployment.get("deployment_id") != deployment_id
            or index.get("release_id") != release_id
            or index.get("deployment_id") != deployment_id
        ):
            raise DbtWorkspaceActivationError("runtime_context")

    @staticmethod
    def _authority(deployment: dict[str, Any], index: dict[str, Any]) -> DbtWorkspaceRuntimeAuthority:
        descriptors = {
            name: _descriptor(deployment, index, name)
            for name in ("release", "deployment", "binding_set", "connection_registry", "credential_runtime")
        }
        return DbtWorkspaceRuntimeAuthority.build(
            environment=str(deployment["environment"]),
            release_id=str(deployment["release_ref"]),
            deployment_id=str(deployment["deployment_id"]),
            release_sha256=descriptors["release"],
            deployment_sha256=descriptors["deployment"],
            binding_set_sha256=descriptors["binding_set"],
            connection_registry_sha256=descriptors["connection_registry"],
            credential_runtime_sha256=descriptors["credential_runtime"],
        )


def _descriptor(deployment: dict[str, Any], index: dict[str, Any], name: str) -> str:
    value = index.get(name)
    if not isinstance(value, dict) or not is_canonical_sha256_digest(value.get("sha256")):
        raise DbtWorkspaceActivationError("runtime_descriptor")
    if name in {"binding_set", "connection_registry", "credential_runtime"} and deployment.get(name) != value:
        raise DbtWorkspaceActivationError("runtime_descriptor_mirror")
    return str(value["sha256"])


__all__ = ["DeploymentCacheWorkspaceActivationInputs"]
