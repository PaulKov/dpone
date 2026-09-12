"""Reopen sealed parent and runtime originals before activation or resolution."""

from __future__ import annotations

from pathlib import Path
from threading import Lock
from typing import TYPE_CHECKING

from dpone.contracts.airflow_deployment import is_canonical_sha256_digest
from dpone.contracts.composition_activation import CompositionOccurrenceContext
from dpone.contracts.composition_identity import CompositionAdmissionError
from dpone.contracts.dbt_contract_validation import sha256_bytes
from dpone.contracts.dbt_workspace_runtime_authority import DbtWorkspaceRuntimeAuthority
from dpone.contracts.strict_json import strict_json_object
from dpone.manifest.confined_files import read_confined_file
from dpone.runtime.airflow_runtime_connection_inventory import (
    RUNTIME_CONNECTION_FILES,
    validate_deployment_auxiliary_files,
)
from dpone.runtime.deployment_cache_common import require_path_without_symlinks
from dpone.runtime.deployment_cache_projection_validator import DeploymentCacheProjectionValidator

if TYPE_CHECKING:
    from dpone.contracts.composition_sources import CompositionSourceSnapshot
    from dpone.contracts.runtime_connection import ResolvedBindingConnection
    from dpone.ports.dbt_workspace_activation import DbtWorkspaceConnectionResolver, DbtWorkspaceRuntimeResolverFactory
    from dpone.services.release_composition import VerifiedCompositionReleaseCapture


class DeploymentCacheCompositionActivationInputs:
    """Bind producer verification and lazy credential resolution at the app root.

    The injected source reader must perform full producer readmission, including
    native-generated and ordinary transfer manifests. A snapshot's shape alone
    never establishes its authenticity. Runtime capabilities are retained only
    for exact locally loaded occurrences, and every use reopens their originals.
    No credentials or resolved connection values are persisted in this adapter.
    """

    def __init__(
        self,
        *,
        cache_root: Path,
        source_reader: VerifiedCompositionReleaseCapture,
        resolver_factory: DbtWorkspaceRuntimeResolverFactory,
    ) -> None:
        self._cache_root = cache_root.resolve(strict=False)
        self._source_reader = source_reader
        self._resolver_factory = resolver_factory
        self._validator = DeploymentCacheProjectionValidator(self._cache_root)
        self._lock = Lock()
        self._loaded: dict[
            CompositionOccurrenceContext,
            tuple[DbtWorkspaceConnectionResolver, Path, dict[str, bytes]],
        ] = {}

    def load_sources(self, *, projection_root: Path, release_id: str) -> CompositionSourceSnapshot:
        """Recapture the entire immutable source closure on every lifecycle phase."""
        self._require_projection_root(projection_root)
        if not is_canonical_sha256_digest(release_id):
            raise CompositionAdmissionError("release_id")
        root = self._cache_root / "releases" / release_id.replace(":", "-")
        try:
            require_path_without_symlinks(root, root=self._cache_root, error_path=root)
            return self._source_reader.read_sources(root, expected_release_id=release_id)
        except Exception:
            raise CompositionAdmissionError("source_inventory") from None

    def load_context(
        self,
        *,
        projection_root: Path,
        activation_id: str,
        environment: str,
        release_id: str,
        deployment_id: str,
        previous_deployment_id: str | None,
    ) -> CompositionOccurrenceContext:
        """Verify sealed deployment/index and exact lazy-resolver snapshot bytes.

        Activation and predecessor coordinates come from the coordinator's CAS
        operation; they are not asserted by the deployment file. The full context
        keys the retained capability, preventing substitution of another attempt.
        """
        root = self._require_projection_root(projection_root)
        try:
            projection = self._validator.validate_activation_details(root, environment=environment)
            deployment, index = projection.deployment, projection.airflow_index
            if deployment["release_ref"] != release_id or deployment["deployment_id"] != deployment_id:
                raise ValueError("coordinates")
            originals = self._originals(root, release_id)
            if (
                strict_json_object(originals["deployment.json"]) != deployment
                or strict_json_object(originals["airflow-index.json"]) != index
            ):
                raise ValueError("projection changed")
            validate_deployment_auxiliary_files(deployment, root)
            descriptors = {}
            for name in ("release", "deployment", *RUNTIME_CONNECTION_FILES):
                filename = {"release": self._release_path(release_id), "deployment": "deployment.json"}.get(name)
                if filename is None:
                    filename = RUNTIME_CONNECTION_FILES[name][1]
                    if deployment.get(name) != index.get(name):
                        raise ValueError("descriptor mirror")
                descriptor = index[name]
                body = originals[filename]
                if (
                    descriptor["sha256"] != sha256_bytes(body)
                    or type(descriptor["bytes"]) is not int
                    or descriptor["bytes"] != len(body)
                ):
                    raise ValueError("descriptor bytes")
                descriptors[name + "_sha256"] = descriptor["sha256"]
            authority = DbtWorkspaceRuntimeAuthority.build(
                environment=environment,
                release_id=release_id,
                deployment_id=deployment_id,
                **descriptors,
            )
            context = CompositionOccurrenceContext(
                activation_id=activation_id,
                environment=environment,
                release_id=release_id,
                deployment_id=deployment_id,
                previous_deployment_id=previous_deployment_id,
                runtime_context_sha256=authority.authority_subject_sha256,
            )
            with self._lock:
                existing = self._loaded.get(context)
                if existing is not None and (existing[1] != root or existing[2] != originals):
                    raise ValueError("loaded context changed")
            snapshots = {
                name: strict_json_object(originals[values[1]]) for name, values in RUNTIME_CONNECTION_FILES.items()
            }
            resolver = self._resolver_factory.build(authority=authority, **snapshots)
            if self._originals(root, release_id) != originals:
                raise ValueError("runtime originals changed")
            with self._lock:
                existing = self._loaded.get(context)
                if existing is not None and (existing[1] != root or existing[2] != originals):
                    raise ValueError("loaded context changed")
                self._loaded.setdefault(context, (resolver, root, originals))
            return context
        except Exception:
            raise CompositionAdmissionError("runtime_context") from None

    def resolve_connection(
        self,
        context: CompositionOccurrenceContext,
        connection_ref: str,
    ) -> ResolvedBindingConnection:
        """Resolve only with a locally loaded exact occurrence and unchanged files."""
        context.__post_init__()
        with self._lock:
            loaded = self._loaded.get(context)
        if loaded is None:
            raise CompositionAdmissionError("runtime_context_not_loaded")
        resolver, root, originals = loaded
        try:
            if self._originals(root, context.release_id) != originals:
                raise ValueError("changed runtime originals")
        except Exception:
            raise CompositionAdmissionError("runtime_context") from None
        try:
            return resolver.resolve(connection_ref)
        except Exception:
            raise CompositionAdmissionError("connection_resolution") from None

    def _require_projection_root(self, value: Path) -> Path:
        try:
            root = value.absolute()
            relative = root.relative_to(self._cache_root)
            if ".." in relative.parts or len(relative.parts) != 3 or relative.parts[0] != "activations":
                raise ValueError("layout")
            require_path_without_symlinks(root, root=self._cache_root, error_path=root)
            return root
        except Exception:
            raise CompositionAdmissionError("projection_root") from None

    def _release_path(self, release_id: str) -> str:
        return f"releases/{release_id.replace(':', '-')}/release-set.json"

    def _originals(self, root: Path, release_id: str) -> dict[str, bytes]:
        names = (
            "_SUCCESS",
            "deployment.json",
            "airflow-index.json",
            *(row[1] for row in RUNTIME_CONNECTION_FILES.values()),
        )
        relative = root.relative_to(self._cache_root).as_posix()
        result = {
            name: read_confined_file(self._cache_root, f"{relative}/{name}", max_bytes=8 * 1024 * 1024)
            for name in names
        }
        name = self._release_path(release_id)
        result[name] = read_confined_file(self._cache_root, name, max_bytes=8 * 1024 * 1024)
        return result
