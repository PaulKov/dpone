"""Manifest-last sealing service for semantic-refresh producer artifacts."""

from __future__ import annotations

import json
from hashlib import sha256

from dpone.contracts.semantic_refresh_artifact_manifest import (
    SemanticRefreshArtifactChunk,
    SemanticRefreshSealedArtifactManifest,
)
from dpone.ports.semantic_refresh_artifact_seal import (
    ArtifactSealPlan,
    SealedArtifactReceipt,
)
from dpone.ports.semantic_refresh_artifact_store import (
    ArtifactCreateConflict,
    ArtifactObjectRef,
    ArtifactStoreUnavailable,
    CreateOnlyVersionedArtifactStore,
    SemanticRefreshArtifactAuthority,
)


class ArtifactConflictError(RuntimeError):
    """Raised when an immutable artifact key has non-identical bytes."""


class ArtifactVerificationError(RuntimeError):
    """Raised when provider metadata or an exact-version read is inconsistent."""


class SealedArtifactService:
    """Create chunks first and publish their version-pinned manifest last."""

    def __init__(
        self,
        *,
        store: CreateOnlyVersionedArtifactStore,
        authority: SemanticRefreshArtifactAuthority,
    ) -> None:
        self._store = store
        self._authority = authority

    def seal(self, plan: ArtifactSealPlan) -> SealedArtifactReceipt:
        """Seal exact authorized chunks and publish the canonical manifest last."""

        self._authority.assert_authorized(inventory=plan.authority_inventory())
        chunk_refs = tuple(
            self._create_or_reconcile(
                key=f"{plan.artifact_prefix}/chunks/{chunk.ordinal:06d}.parquet",
                content=chunk.content,
                expected_sha256=chunk.sha256,
                plan=plan,
            )
            for chunk in plan.chunks
        )
        sealed_manifest = SemanticRefreshSealedArtifactManifest.build(
            seal_authorization=plan.seal_authorization,
            artifact_prefix=plan.artifact_prefix,
            provider=plan.provider,
            encryption_policy_sha256=plan.encryption_policy_sha256,
            retention_policy_sha256=plan.retention_policy_sha256,
            chunks=tuple(
                SemanticRefreshArtifactChunk(
                    ordinal=chunk.ordinal,
                    object_key=ref.key,
                    provider_version=ref.version,
                    chunk_sha256=ref.sha256,
                    byte_count=ref.size_bytes,
                    row_count=chunk.row_count,
                )
                for chunk, ref in zip(plan.chunks, chunk_refs, strict=True)
            ),
        )
        manifest_payload = sealed_manifest.to_dict()
        manifest_bytes = json.dumps(
            manifest_payload,
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        manifest_ref = self._create_or_reconcile(
            key=f"{plan.artifact_prefix}/manifest.json",
            content=manifest_bytes,
            expected_sha256=_digest(manifest_bytes),
            plan=plan,
        )
        return SealedArtifactReceipt(
            operation_id=plan.operation_id,
            status="SEALED",
            manifest=manifest_ref,
            chunks=chunk_refs,
            manifest_payload=manifest_payload,
            sealed_manifest=sealed_manifest,
        )

    def _create_or_reconcile(
        self,
        *,
        key: str,
        content: bytes,
        expected_sha256: str,
        plan: ArtifactSealPlan,
    ) -> ArtifactObjectRef:
        candidate: ArtifactObjectRef | None
        try:
            candidate = self._store.create(
                key=key,
                content=content,
                sha256=expected_sha256,
                encryption_scope=plan.encryption_scope,
                retention_until=plan.retention_until,
            )
        except (ArtifactCreateConflict, ArtifactStoreUnavailable):
            candidate = self._store.head(key=key)
        if candidate is None:
            raise ArtifactVerificationError(f"artifact create outcome cannot be reconciled: {key}")
        ref = candidate
        if (
            ref.key != key
            or not ref.version
            or ref.size_bytes != len(content)
            or ref.sha256 != expected_sha256
            or ref.encryption_scope != plan.encryption_scope
            or ref.retention_until != plan.retention_until
        ):
            raise ArtifactConflictError(f"immutable artifact metadata conflicts: {key}")
        try:
            observed = self._store.read_version(key=key, version=ref.version)
        except Exception as exc:
            raise ArtifactVerificationError(f"exact artifact version cannot be read: {key}@{ref.version}") from exc
        if len(observed) != len(content) or _digest(observed) != expected_sha256 or observed != content:
            raise ArtifactConflictError(f"immutable artifact bytes conflict: {key}")
        return ref


def _digest(content: bytes) -> str:
    return "sha256:" + sha256(content).hexdigest()


__all__ = [
    "ArtifactConflictError",
    "ArtifactVerificationError",
    "SealedArtifactService",
]
