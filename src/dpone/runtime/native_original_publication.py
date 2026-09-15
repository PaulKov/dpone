"""Publish-bound-readback orchestration without backend construction or retries."""

from __future__ import annotations

from hashlib import sha256
from typing import Protocol, get_args

from dpone.contracts.native_delivery_json import decode_native_delivery_json, encode_native_delivery_json
from dpone.contracts.native_identity import OriginalRef
from dpone.contracts.native_originals import (
    NativeOriginalBinding,
    NativeOriginalKind,
    NativeOriginalSubject,
    decode_native_original_binding,
    decode_native_original_subject,
    encode_native_original_binding,
    encode_native_original_subject,
)
from dpone.ports.native_originals import NativeOriginalBindingPort, NativeOriginalReaderPort, NativeOriginalWriterPort


class NativeOriginalPublicationError(RuntimeError):
    """Independent binding or byte verification did not establish success."""


class BoundNativeOriginalPublisher(Protocol):
    """Application-bound authority/providers, retaining explicit document identity."""

    def __call__(self, *, kind: NativeOriginalKind, locator: str, payload: bytes, max_bytes: int) -> OriginalRef: ...


def publish_bound_native_original(
    *,
    writer: NativeOriginalWriterPort,
    reader: NativeOriginalReaderPort,
    bindings: NativeOriginalBindingPort,
    subject: NativeOriginalSubject,
    kind: NativeOriginalKind,
    storage_authority: OriginalRef,
    locator: str,
    payload: bytes,
    max_bytes: int,
) -> OriginalRef:
    """Return a reference only after independent complete-binding and byte proof.

    Validation precedes all provider calls. Backend exceptions propagate; this
    helper never retries writes, deletes orphan bytes or fabricates a version.
    Concrete writers/binders must reconcile lost acknowledgements against the same
    immutable identity before returning, otherwise leave the outcome unresolved.
    No original binding or readback grants source/target dispatch authority.
    """
    if type(max_bytes) is not int or max_bytes <= 0:
        raise ValueError("max_bytes must be an exact positive integer")
    if type(payload) is not bytes or len(payload) > max_bytes:
        raise ValueError("original payload must be bytes within max_bytes")
    if type(kind) is not str or kind not in get_args(NativeOriginalKind):
        raise ValueError("unsupported native original kind")
    if encode_native_delivery_json(decode_native_delivery_json(payload)) != payload:
        raise ValueError("original payload must be canonical JSON bytes")
    if type(storage_authority) is not OriginalRef:
        raise TypeError("storage authority must be an OriginalRef")
    authority = OriginalRef(storage_authority.locator, storage_authority.sha256)
    subject_bytes = encode_native_original_subject(subject)
    expected_ref = OriginalRef(locator, "sha256:" + sha256(payload).hexdigest())

    object_ref = writer.publish(kind=kind, subject=decode_native_original_subject(subject_bytes), payload=payload)
    expected = NativeOriginalBinding(
        subject=decode_native_original_subject(subject_bytes),
        kind=kind,
        storage_authority=authority,
        object_ref=object_ref,
        payload_sha256=expected_ref.sha256,
        locator=expected_ref.locator,
    )
    # Snapshot identity before passing values to another capability. Even an
    # accidentally mutated frozen object cannot replace the expected coordinates.
    expected_bytes = encode_native_original_binding(expected)
    reference = bindings.bind(expected)
    if type(reference) is not OriginalRef:
        raise NativeOriginalPublicationError("binding returned an invalid original reference")
    reference.__post_init__()
    if reference != expected_ref:
        raise NativeOriginalPublicationError("binding reference differs from requested original")
    resolved = bindings.resolve(
        reference, expected_subject=decode_native_original_subject(subject_bytes), expected_kind=kind
    )
    if encode_native_original_binding(resolved) != expected_bytes:
        raise NativeOriginalPublicationError("independent binding differs from complete published identity")
    verified = decode_native_original_binding(expected_bytes)
    observed = reader.read(
        verified.object_ref, expected_subject=verified.subject, expected_kind=kind, max_bytes=max_bytes
    )
    if type(observed) is not bytes or len(observed) > max_bytes or observed != payload:
        raise NativeOriginalPublicationError("exact-version readback differs from original bytes")
    return expected_ref
