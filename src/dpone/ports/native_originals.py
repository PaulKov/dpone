"""Injected native original capabilities; no implicit backend or authority."""

from __future__ import annotations

from typing import Protocol

from dpone.contracts.native_identity import OriginalRef
from dpone.contracts.native_originals import NativeOriginalBinding, NativeOriginalKind, NativeOriginalSubject
from dpone.ports.semantic_refresh_artifact_store import ArtifactObjectRef


class NativeOriginalWriterPort(Protocol):
    """Publish once or reconcile the same immutable object, never invent a version."""

    def publish(self, *, kind: NativeOriginalKind, subject: NativeOriginalSubject, payload: bytes) -> ArtifactObjectRef:
        """Return independently verified provider identity or report uncertainty."""
        ...


class NativeOriginalReaderPort(Protocol):
    """Read authenticated original identity with an enforced byte bound."""

    def read(
        self,
        exactref: ArtifactObjectRef,
        *,
        expected_kind: NativeOriginalKind,
        expected_subject: NativeOriginalSubject,
        max_bytes: int,
    ) -> bytes:
        """Enforce max_bytes during reads, verify kind/subject and exact version.

        Concrete providers also need finite connection/read/retry policy. An
        unbounded legacy read followed by a size check does not implement this port.
        """
        ...


class NativeOriginalBindingPort(Protocol):
    """Create-only bindings in the authenticated shared control authority."""

    def bind(self, binding: NativeOriginalBinding) -> OriginalRef:
        """Bind the whole tuple or independently reconcile its exact existing value.

        Conflicting tuples cannot overwrite a locator. Unresolved acknowledgements
        raise rather than granting permission for another write or dispatch.
        """
        ...

    def resolve(
        self, reference: OriginalRef, *, expected_subject: NativeOriginalSubject, expected_kind: NativeOriginalKind
    ) -> NativeOriginalBinding:
        """Independently read and authenticate the complete retained binding."""
        ...
