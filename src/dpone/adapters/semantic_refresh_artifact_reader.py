"""Exact-version reader for canonical semantic-refresh sealed artifacts."""

from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from typing import TYPE_CHECKING

from dpone.contracts.semantic_refresh_artifact_manifest import (
    SemanticRefreshSealedArtifactManifest,
)
from dpone.ports.semantic_refresh_artifact_reader import VersionPinnedSealedArtifact
from dpone.ports.semantic_refresh_artifact_store import (
    CreateOnlyVersionedArtifactStore,
    SemanticRefreshArtifactStoreResolver,
    artifact_store_binding,
    operation_artifact_prefix,
)
from dpone.ports.semantic_refresh_seal_policy import semantic_refresh_artifact_authority_sha256

if TYPE_CHECKING:
    from dpone.contracts.semantic_refresh_seal_authorization import (
        SemanticRefreshSealAuthorizationReceipt,
    )
    from dpone.ports.semantic_refresh_mssql_authority import SemanticRefreshMssqlProtectedAuthorityPort


class SealedArtifactReadError(RuntimeError):
    """Raised when version-pinned manifest or chunk authority differs."""


class VersionPinnedSealedArtifactReader:
    """Read only manifest-pinned object versions and verify every independent SHA."""

    def __init__(self, store: CreateOnlyVersionedArtifactStore) -> None:
        self._store = store

    def read(
        self,
        *,
        manifest_key: str,
        manifest_version: str,
        artifact_manifest_sha256: str,
        operation_id: str,
        operation_plan_sha256: str,
        workflow_execution_binding_sha256: str,
        attempt_binding_sha256: str,
        seal_authorization: SemanticRefreshSealAuthorizationReceipt,
    ) -> VersionPinnedSealedArtifact:
        """Authenticate one exact canonical manifest and all referenced chunks."""

        try:
            raw = self._store.read_version(key=manifest_key, version=manifest_version)
            parsed = json.loads(raw)
            manifest = SemanticRefreshSealedArtifactManifest.from_mapping(parsed)
        except Exception as exc:
            raise SealedArtifactReadError("sealed artifact manifest is unavailable or invalid") from exc
        expected_identity = (
            artifact_manifest_sha256,
            operation_id,
            operation_plan_sha256,
            workflow_execution_binding_sha256,
            attempt_binding_sha256,
        )
        observed_identity = (
            manifest.artifact_manifest_sha256,
            manifest.operation_id,
            manifest.operation_plan_sha256,
            manifest.workflow_execution_binding_sha256,
            manifest.attempt_binding_sha256,
        )
        if observed_identity != expected_identity:
            raise SealedArtifactReadError("sealed artifact identity differs from publication plan")
        if not manifest.matches_seal_authorization(seal_authorization):
            raise SealedArtifactReadError("sealed artifact differs from canonical seal authorization")
        if manifest_key != f"{manifest.artifact_prefix}/manifest.json":
            raise SealedArtifactReadError("sealed artifact manifest key differs from canonical prefix")
        chunks: list[bytes] = []
        for descriptor in manifest.chunks:
            try:
                content = self._store.read_version(
                    key=descriptor.object_key,
                    version=descriptor.provider_version,
                )
            except Exception as exc:
                raise SealedArtifactReadError("sealed artifact chunk version is unavailable") from exc
            if (
                descriptor.object_key != f"{manifest.artifact_prefix}/chunks/{descriptor.ordinal:06d}.parquet"
                or len(content) != descriptor.byte_count
                or _digest(content) != descriptor.chunk_sha256
            ):
                raise SealedArtifactReadError("sealed artifact chunk authority differs")
            chunks.append(content)
        return VersionPinnedSealedArtifact(manifest=manifest, chunk_bytes=tuple(chunks))


class AuthorityBoundVersionPinnedSealedArtifactReader:
    """Resolve the exact protected operation store before version-pinned reads."""

    def __init__(
        self,
        *,
        protected_operation: SemanticRefreshMssqlProtectedAuthorityPort,
        artifact_stores: SemanticRefreshArtifactStoreResolver,
    ) -> None:
        self._protected_operation = protected_operation
        self._artifact_stores = artifact_stores

    def read(
        self,
        *,
        manifest_key: str,
        manifest_version: str,
        artifact_manifest_sha256: str,
        operation_id: str,
        operation_plan_sha256: str,
        workflow_execution_binding_sha256: str,
        attempt_binding_sha256: str,
        seal_authorization: SemanticRefreshSealAuthorizationReceipt,
    ) -> VersionPinnedSealedArtifact:
        """Load protected policy and delegate exact byte verification."""

        operation = self._protected_operation.load_operation(
            workflow_execution_binding_sha256=workflow_execution_binding_sha256,
            operation_id=operation_id,
        )
        if (
            operation.operation_plan_sha256 != operation_plan_sha256
            or operation.attempt_binding_sha256 != attempt_binding_sha256
            or operation.workflow_execution_binding_sha256 != workflow_execution_binding_sha256
            or operation.workflow_execution_id != seal_authorization.workflow_execution_id
            or operation.operation_id != seal_authorization.operation_id
            or operation.operation_plan_sha256 != seal_authorization.operation_plan_sha256
            or operation.workflow_execution_binding_sha256 != seal_authorization.workflow_execution_binding_sha256
            or operation.attempt_binding_sha256 != seal_authorization.attempt_binding_sha256
            or operation.fencing_epoch != seal_authorization.fencing_epoch
            or semantic_refresh_artifact_authority_sha256(operation.artifact_authority)
            != seal_authorization.artifact_authority_sha256
        ):
            raise SealedArtifactReadError("protected artifact reader identity differs")
        derived_prefix = operation_artifact_prefix(operation.artifact_authority.artifact_prefix, operation.operation_id)
        if manifest_key != f"{derived_prefix}/manifest.json":
            raise SealedArtifactReadError("manifest key differs from protected operation prefix")
        store = self._artifact_stores.resolve(
            replace(
                artifact_store_binding(operation.artifact_authority),
                artifact_prefix=derived_prefix,
            )
        )
        return VersionPinnedSealedArtifactReader(store).read(
            manifest_key=manifest_key,
            manifest_version=manifest_version,
            artifact_manifest_sha256=artifact_manifest_sha256,
            operation_id=operation_id,
            operation_plan_sha256=operation_plan_sha256,
            workflow_execution_binding_sha256=workflow_execution_binding_sha256,
            attempt_binding_sha256=attempt_binding_sha256,
            seal_authorization=seal_authorization,
        )


def _digest(content: bytes) -> str:
    return "sha256:" + hashlib.sha256(content).hexdigest()


__all__ = [
    "SealedArtifactReadError",
    "AuthorityBoundVersionPinnedSealedArtifactReader",
    "VersionPinnedSealedArtifactReader",
]
