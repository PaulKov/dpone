"""Canonical observation-bundle manifest parsing and domain-separated digesting."""

from __future__ import annotations

import hashlib
import struct
from collections.abc import Mapping, Sequence
from dataclasses import dataclass

_DOMAIN = "dpone.ci-shadow-reconciliation-observation-bundle.v1"
_MAX_ENTRIES = 4_096
_MAX_PATH_BYTES = 1_024


class ObservationBundleError(ValueError):
    """Bundle closure is malformed, non-canonical, or unsafe to trust."""


@dataclass(frozen=True)
class ObservationBundleEntry:
    """One regular immutable file in the shared acquisition implementation closure."""

    path: str
    mode: str
    blob_sha256: str


def observation_bundle_digest(entries: Sequence[ObservationBundleEntry], *, manifest_sha256: str) -> str:
    """Return the policy-specified binary digest over closure and manifest bytes."""

    if not entries or len(entries) > _MAX_ENTRIES:
        raise ObservationBundleError("observation bundle entry count is outside the approved bound")
    encoded: list[bytes] = []
    previous: bytes | None = None
    for entry in entries:
        path = _confined_path_bytes(entry.path)
        if previous is not None and path <= previous:
            raise ObservationBundleError("observation bundle paths must be strictly UTF-8 ascending")
        previous = path
        digest = _digest_bytes(entry.blob_sha256)
        if entry.mode not in {"100644", "100755"}:
            raise ObservationBundleError("observation bundle mode is invalid")
        entry_bytes = struct.pack(">I", len(path)) + path + entry.mode.encode("ascii") + digest
        encoded.append(struct.pack(">I", len(entry_bytes)) + entry_bytes)
    manifest = struct.pack(">I", len(encoded)) + b"".join(encoded)
    manifest_digest = _digest_bytes(manifest_sha256)
    return (
        "sha256:"
        + hashlib.sha256(
            _DOMAIN.encode("utf-8") + b"\x00" + struct.pack(">Q", len(manifest)) + manifest + manifest_digest
        ).hexdigest()
    )


def entries_from_manifest(payload: Mapping[str, object]) -> tuple[ObservationBundleEntry, ...]:
    """Decode the closed YAML/JSON manifest shape without accepting unknown fields."""

    if set(payload) != {"schema", "domain", "entries"}:
        raise ObservationBundleError("observation bundle manifest has unknown fields")
    if payload.get("schema") != _DOMAIN or payload.get("domain") != _DOMAIN:
        raise ObservationBundleError("observation bundle manifest identity is invalid")
    raw_entries = payload.get("entries")
    if not isinstance(raw_entries, list):
        raise ObservationBundleError("observation bundle entries are invalid")
    entries: list[ObservationBundleEntry] = []
    for raw in raw_entries:
        if not isinstance(raw, Mapping) or set(raw) != {"path", "mode", "blob_sha256"}:
            raise ObservationBundleError("observation bundle entry has unknown fields")
        path = raw.get("path")
        mode = raw.get("mode")
        digest = raw.get("blob_sha256")
        if not isinstance(path, str) or not isinstance(mode, str) or not isinstance(digest, str):
            raise ObservationBundleError("observation bundle entry is invalid")
        entries.append(ObservationBundleEntry(path, mode, digest))
    _validate_entries(entries)
    return tuple(entries)


def evidence_manifest(entries: Sequence[ObservationBundleEntry], *, manifest_sha256: str) -> dict[str, object]:
    """Return the closed public artifact projection for already validated entries."""

    observation_bundle_digest(entries, manifest_sha256=manifest_sha256)
    return {
        "schema": _DOMAIN,
        "domain": _DOMAIN,
        "manifest_sha256": manifest_sha256,
        "entries": [{"path": entry.path, "mode": entry.mode, "blob_sha256": entry.blob_sha256} for entry in entries],
    }


def _validate_entries(entries: Sequence[ObservationBundleEntry]) -> None:
    """Validate ordered entries without requiring a self-referential manifest hash."""

    observation_bundle_digest(entries, manifest_sha256="sha256:" + "0" * 64)


def _confined_path_bytes(path: str) -> bytes:
    if not path or path.startswith("/") or "\x00" in path or ".." in path.split("/"):
        raise ObservationBundleError("observation bundle path is unsafe")
    try:
        encoded = path.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise ObservationBundleError("observation bundle path is not UTF-8") from exc
    if len(encoded) > _MAX_PATH_BYTES:
        raise ObservationBundleError("observation bundle path exceeds the approved bound")
    return encoded


def _digest_bytes(value: str) -> bytes:
    if not value.startswith("sha256:") or len(value) != 71:
        raise ObservationBundleError("observation bundle digest is invalid")
    try:
        decoded = bytes.fromhex(value[7:])
    except ValueError as exc:
        raise ObservationBundleError("observation bundle digest is invalid") from exc
    if len(decoded) != 32:
        raise ObservationBundleError("observation bundle digest is invalid")
    return decoded


__all__ = [
    "ObservationBundleEntry",
    "ObservationBundleError",
    "entries_from_manifest",
    "evidence_manifest",
    "observation_bundle_digest",
]
