"""Credential-free semantic-refresh artifact adapters for composition and tests."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from hashlib import sha256 as hashlib_sha256
from typing import TYPE_CHECKING

from dpone.ports.semantic_refresh_artifact_store import (
    ArtifactCreateConflict,
    ArtifactObjectRef,
    ArtifactStoreBinding,
    ArtifactStoreUnavailable,
    CreateOnlyVersionedArtifactStore,
)

if TYPE_CHECKING:
    from dpone.contracts.semantic_refresh_seal_authorization import (
        SemanticRefreshSealAuthorizationReceipt,
    )


@dataclass(frozen=True)
class CallbackCreateOnlyArtifactStore:
    """Adapt provider callbacks to the create-only versioned store port."""

    create_callback: Callable[[Mapping[str, object]], Mapping[str, object]]
    head_callback: Callable[[str], Mapping[str, object] | None]
    read_version_callback: Callable[[str, str], bytes]

    def create(
        self,
        *,
        key: str,
        content: bytes,
        sha256: str,
        encryption_scope: str,
        retention_until: str,
    ) -> ArtifactObjectRef:
        raw = self.create_callback(
            {
                "key": key,
                "content": content,
                "sha256": sha256,
                "encryption_scope": encryption_scope,
                "retention_until": retention_until,
            }
        )
        return _object_ref(raw)

    def head(self, *, key: str) -> ArtifactObjectRef | None:
        raw = self.head_callback(key)
        return _object_ref(raw) if raw is not None else None

    def read_version(self, *, key: str, version: str) -> bytes:
        return bytes(self.read_version_callback(key, version))


@dataclass(frozen=True)
class CallbackSemanticRefreshArtifactAuthority:
    """Delegate journal/source authorization to a composition-root callback."""

    authorize_callback: Callable[[Mapping[str, object]], bool]

    def assert_authorized(
        self,
        *,
        inventory: Mapping[str, object],
    ) -> None:
        authorized = self.authorize_callback(inventory)
        if authorized is not True:
            raise ValueError("durable journal/source authority rejected artifact sealing")


class InMemoryCreateOnlyArtifactStore:
    """Deterministic create-only store with exact version reads.

    This adapter is suitable for local planning/tests. Production object-store
    SDK adapters can implement the same narrow port without entering runtime
    policy.
    """

    def __init__(self, *, fail_after_create_on_call: int | None = None) -> None:
        self._objects: dict[str, tuple[ArtifactObjectRef, bytes]] = {}
        self._create_events: list[str] = []
        self._create_calls = 0
        self._fail_after_create_on_call = fail_after_create_on_call

    @property
    def create_events(self) -> tuple[str, ...]:
        return tuple(self._create_events)

    @property
    def version_count(self) -> int:
        return len(self._objects)

    def create(
        self,
        *,
        key: str,
        content: bytes,
        sha256: str,
        encryption_scope: str,
        retention_until: str,
    ) -> ArtifactObjectRef:
        self._create_calls += 1
        self._create_events.append(key)
        if key in self._objects:
            raise ArtifactCreateConflict(f"create-only artifact already exists: {key}")
        actual = "sha256:" + hashlib_sha256(content).hexdigest()
        if actual != sha256:
            raise ValueError("artifact content digest does not match create request")
        ref = ArtifactObjectRef(
            key=key,
            version="v1",
            size_bytes=len(content),
            sha256=sha256,
            encryption_scope=encryption_scope,
            retention_until=retention_until,
        )
        self._objects[key] = (ref, bytes(content))
        if self._fail_after_create_on_call == self._create_calls:
            self._fail_after_create_on_call = None
            raise ArtifactStoreUnavailable(f"create acknowledgement unavailable: {key}")
        return ref

    def head(self, *, key: str) -> ArtifactObjectRef | None:
        stored = self._objects.get(key)
        return stored[0] if stored is not None else None

    def read_version(self, *, key: str, version: str) -> bytes:
        stored = self._objects.get(key)
        if stored is None or stored[0].version != version:
            raise FileNotFoundError(f"artifact version is missing: {key}@{version}")
        return stored[1]


@dataclass(frozen=True, slots=True)
class StaticCreateOnlyArtifactStoreResolver:
    """Bind one credential-free store to exact authority for focused tests."""

    binding: ArtifactStoreBinding
    store: CreateOnlyVersionedArtifactStore

    def resolve(self, binding: ArtifactStoreBinding) -> CreateOnlyVersionedArtifactStore:
        if binding != self.binding:
            raise ValueError("artifact store binding differs from protected authority")
        return self.store


@dataclass(frozen=True)
class StaticSemanticRefreshArtifactAuthority:
    """Bind a composition-root authorization to exact durable identities."""

    authorized_inventory: Mapping[str, object]

    def assert_authorized(
        self,
        *,
        inventory: Mapping[str, object],
    ) -> None:
        if dict(inventory) != dict(self.authorized_inventory):
            raise ValueError("durable artifact inventory authority does not authorize sealing")


@dataclass(frozen=True)
class StaticSemanticRefreshSealAuthorization:
    """Exact test/local seal receipt lookup; never production evidence."""

    receipts: tuple[SemanticRefreshSealAuthorizationReceipt, ...]

    def load(
        self,
        *,
        workflow_execution_binding_sha256: str,
        operation_id: str,
    ) -> SemanticRefreshSealAuthorizationReceipt:
        matches = tuple(
            receipt
            for receipt in self.receipts
            if receipt.workflow_execution_binding_sha256 == workflow_execution_binding_sha256
            and receipt.operation_id == operation_id
        )
        if len(matches) != 1:
            raise ValueError("seal authorization receipt is absent or ambiguous")
        return matches[0]


__all__ = [
    "CallbackCreateOnlyArtifactStore",
    "CallbackSemanticRefreshArtifactAuthority",
    "InMemoryCreateOnlyArtifactStore",
    "StaticCreateOnlyArtifactStoreResolver",
    "StaticSemanticRefreshArtifactAuthority",
    "StaticSemanticRefreshSealAuthorization",
]


def _object_ref(raw: Mapping[str, object]) -> ArtifactObjectRef:
    if set(raw) != {"key", "version", "size_bytes", "sha256", "encryption_scope", "retention_until"}:
        raise ArtifactStoreUnavailable("artifact provider returned non-closed version metadata")
    key = raw.get("key")
    version = raw.get("version")
    size_bytes = raw.get("size_bytes")
    digest = raw.get("sha256")
    encryption_scope = raw.get("encryption_scope")
    retention_until = raw.get("retention_until")
    if (
        not isinstance(key, str)
        or not isinstance(version, str)
        or isinstance(size_bytes, bool)
        or not isinstance(size_bytes, int)
        or not isinstance(digest, str)
        or not isinstance(encryption_scope, str)
        or not isinstance(retention_until, str)
    ):
        raise ArtifactStoreUnavailable("artifact provider returned invalid version metadata")
    return ArtifactObjectRef(
        key=key,
        version=version,
        size_bytes=size_bytes,
        sha256=digest,
        encryption_scope=encryption_scope,
        retention_until=retention_until,
    )
