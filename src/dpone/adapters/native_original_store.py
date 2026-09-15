"""Native subject-bound originals on an injected bounded immutable provider."""

from __future__ import annotations

from hashlib import sha256
from typing import cast

from dpone.contracts.native_delivery_json import (
    MAX_NATIVE_JSON_BYTES,
    decode_native_delivery_json,
    encode_native_delivery_json,
)
from dpone.contracts.native_identity import OriginalRef
from dpone.contracts.native_originals import (
    NativeOriginalKind,
    NativeOriginalStorageAuthority,
    NativeOriginalSubject,
    decode_native_original_storage_authority,
    decode_native_original_subject,
    encode_native_original_storage_authority,
    encode_native_original_subject,
    native_original_object_payload,
    require_native_original_kind,
)
from dpone.ports.semantic_refresh_artifact_store import (
    ArtifactCreateConflict,
    ArtifactObjectRef,
    ArtifactStoreUnavailable,
)
from dpone.ports.versioned_artifact_store import BoundedVersionedArtifactStore, VersionedArtifactIoBudget


class NativeOriginalStoreError(ArtifactStoreUnavailable):
    """The retained provider coordinates or original bytes could not be proved."""


class NativeOriginalStore:
    """Publish and read original bytes under authenticated composition inputs.

    The authority reference must identify the exact canonical authority document.
    Matching a digest is not authentication: the application supplies that trust.
    Each instance retains one attempt budget; no method renews its deadline.
    """

    def __init__(
        self,
        *,
        provider: BoundedVersionedArtifactStore,
        authority: NativeOriginalStorageAuthority,
        authority_ref: OriginalRef,
        budget: VersionedArtifactIoBudget,
    ) -> None:
        if type(authority_ref) is not OriginalRef or type(budget) is not VersionedArtifactIoBudget:
            raise TypeError("native store requires exact authority reference and I/O budget types")
        authority_bytes = encode_native_original_storage_authority(authority)
        reference = OriginalRef(authority_ref.locator, authority_ref.sha256)
        if reference.sha256 != _digest(authority_bytes):
            raise ValueError("storage authority reference differs from canonical authority bytes")
        self._authority = decode_native_original_storage_authority(authority_bytes)
        self._authority_ref = reference
        self._budget = VersionedArtifactIoBudget(budget.max_bytes, budget.chunk_bytes, budget.deadline_monotonic)
        self._provider = provider

    def publish(self, *, kind: NativeOriginalKind, subject: NativeOriginalSubject, payload: bytes) -> ArtifactObjectRef:
        """Create once, then prove metadata and bytes; reconcile lost ACK read-only."""
        require_native_original_kind(kind)
        budget = self._read_budget(self._budget.max_bytes)
        _canonical_payload(payload, budget.max_bytes)
        subject_bytes = encode_native_original_subject(subject)
        digest = _digest(payload)
        key = self._key(subject_bytes, kind, digest)
        candidate: ArtifactObjectRef | None
        try:
            candidate = self._provider.create(
                key=key,
                content=payload,
                sha256=digest,
                encryption_scope=self._authority.writer_scope,
                retention_until=self._authority.retention_until,
                budget=budget,
            )
        except (ArtifactCreateConflict, ArtifactStoreUnavailable):
            # An ambiguous acknowledgement permits observation, not another PUT.
            candidate = self._provider.head(key=key, budget=budget)
        reference = _provider_reference(candidate)
        if reference.sha256 != digest or reference.size_bytes != len(payload):
            raise NativeOriginalStoreError("published original content coordinates differ")
        observed = self.read(
            reference,
            expected_kind=kind,
            expected_subject=decode_native_original_subject(subject_bytes),
            max_bytes=budget.max_bytes,
        )
        if observed != payload:
            raise NativeOriginalStoreError("published original readback differs from requested bytes")
        return reference

    def read(
        self,
        exactref: ArtifactObjectRef,
        *,
        expected_kind: NativeOriginalKind,
        expected_subject: NativeOriginalSubject,
        max_bytes: int,
    ) -> bytes:
        """Prove all six reference coordinates and exact bytes without a mutation."""
        reference = _copy_reference(exactref)
        budget = self._read_budget(max_bytes)
        subject_bytes = encode_native_original_subject(expected_subject)
        expected_key = self._key(subject_bytes, expected_kind, reference.sha256)
        if (
            reference.key != expected_key
            or reference.size_bytes > budget.max_bytes
            or reference.encryption_scope != self._authority.writer_scope
            or reference.retention_until != self._authority.retention_until
        ):
            raise NativeOriginalStoreError("original reference differs from subject, kind, policy or byte budget")
        observed = _provider_reference(self._provider.head(key=expected_key, budget=budget))
        if observed != reference:
            raise NativeOriginalStoreError("independent original metadata differs from the complete reference")
        payload = self._provider.read_version(key=reference.key, version=reference.version, budget=budget)
        if type(payload) is not bytes or len(payload) > budget.max_bytes:
            raise NativeOriginalStoreError("original readback violates the bounded byte contract")
        if len(payload) != reference.size_bytes or _digest(payload) != reference.sha256:
            raise NativeOriginalStoreError("original readback size or digest differs")
        try:
            _canonical_payload(payload, budget.max_bytes)
        except (TypeError, ValueError) as exc:
            raise NativeOriginalStoreError("original readback is not a canonical native document") from exc
        return payload

    def _key(self, subject_bytes: bytes, kind: NativeOriginalKind, digest: str) -> str:
        require_native_original_kind(kind)
        key = f"{self._authority.artifact_prefix}/native-originals/v1/{sha256(subject_bytes).hexdigest()}/{kind}/{digest[7:]}"
        # Provider keys retain their own grammar, not OriginalRef locator rules.
        encode_native_delivery_json({"key": key})
        return key

    def _read_budget(self, max_bytes: int) -> VersionedArtifactIoBudget:
        if type(max_bytes) is not int or max_bytes <= 0:
            raise ValueError("original max_bytes must be an exact positive integer")
        maximum = min(max_bytes, self._budget.max_bytes, self._authority.max_artifact_bytes, MAX_NATIVE_JSON_BYTES)
        return VersionedArtifactIoBudget(
            maximum, min(self._budget.chunk_bytes, maximum), self._budget.deadline_monotonic
        )


def _digest(payload: bytes) -> str:
    return "sha256:" + sha256(payload).hexdigest()


def _canonical_payload(payload: bytes, maximum: int) -> None:
    if type(payload) is not bytes or len(payload) > maximum:
        raise ValueError("original payload must be exact bytes within the admitted byte budget")
    if encode_native_delivery_json(decode_native_delivery_json(payload)) != payload:
        raise ValueError("original payload must contain canonical native JSON")


def _copy_reference(value: ArtifactObjectRef) -> ArtifactObjectRef:
    return ArtifactObjectRef(**cast(dict, native_original_object_payload(value)))


def _provider_reference(value: ArtifactObjectRef | None) -> ArtifactObjectRef:
    try:
        return _copy_reference(cast(ArtifactObjectRef, value))
    except (TypeError, ValueError) as exc:
        raise NativeOriginalStoreError("provider did not return a valid original version reference") from exc
