"""Bounded payload contracts for immutable runtime init-fetch plans."""

from __future__ import annotations

import base64
import binascii
import hashlib
import os
import re
import stat
import tempfile
from builtins import bytes as Bytes
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from dpone.runtime.init_fetch_contract import InitFetchError, cache_relative_path

MAX_RUNTIME_AUTHORITY_PAYLOAD_BYTES = 4 * 1024
MAX_RUNTIME_AUTHORITY_PAYLOAD_BASE64_CHARS = ((MAX_RUNTIME_AUTHORITY_PAYLOAD_BYTES + 2) // 3) * 4
_DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
_TOKEN_RE = re.compile(r"^[A-Za-z0-9_.:-]{1,256}$")
MAX_SELECTED_RUNTIME_PAYLOADS = 16


@dataclass(frozen=True, slots=True)
class RuntimePayloadDescriptor:
    """Exact external runtime payload fetched from the pinned release."""

    id: str
    kind: str
    artifact_ref: str
    sha256: str
    bytes: int
    media_type: str

    def __post_init__(self) -> None:
        if not isinstance(self.id, str) or _TOKEN_RE.fullmatch(self.id) is None:
            raise ValueError("runtime_payload.id must be a bounded logical token")
        if self.kind not in {"dbt_project_bundle", "dbt_manifest", "dbt_selection_lock"}:
            raise ValueError("runtime_payload.kind is unsupported")
        cache_relative_path(self.artifact_ref)
        if not isinstance(self.sha256, str) or _DIGEST_RE.fullmatch(self.sha256) is None:
            raise ValueError("runtime_payload.sha256 must be a canonical sha256 digest")
        if isinstance(self.bytes, bool) or not isinstance(self.bytes, int) or self.bytes <= 0:
            raise ValueError("runtime_payload.bytes must be a positive integer")
        if not isinstance(self.media_type, str) or not self.media_type or len(self.media_type) > 200:
            raise ValueError("runtime_payload.media_type must be bounded text")

    def to_dict(self) -> dict[str, str | int]:
        return {
            "id": self.id,
            "kind": self.kind,
            "artifact_ref": self.artifact_ref,
            "sha256": self.sha256,
            "bytes": self.bytes,
            "media_type": self.media_type,
        }


@dataclass(frozen=True, slots=True)
class ImmutableRuntimeAuthorityPayload:
    """One exact safe-to-persist payload bound to decoded bytes and digest."""

    mode: str
    encoding: str
    payload_b64: str
    bytes: int
    sha256: str

    def __post_init__(self) -> None:
        self.decode()

    @classmethod
    def from_mapping(cls, value: object) -> ImmutableRuntimeAuthorityPayload:
        if not isinstance(value, Mapping) or frozenset(str(key) for key in value) != {
            "mode",
            "encoding",
            "payload_b64",
            "bytes",
            "sha256",
        }:
            raise _value_error("must contain exactly the closed immutable fields")
        return cls(
            mode=value["mode"],
            encoding=value["encoding"],
            payload_b64=value["payload_b64"],
            bytes=value["bytes"],
            sha256=value["sha256"],
        )

    @classmethod
    def from_bytes(cls, payload: Bytes, *, expected_sha256: str) -> ImmutableRuntimeAuthorityPayload:
        if not isinstance(payload, Bytes):
            raise _value_error("must be exact bytes")
        actual = _digest(payload)
        if (
            not isinstance(expected_sha256, str)
            or _DIGEST_RE.fullmatch(expected_sha256) is None
            or actual != expected_sha256
        ):
            raise _value_error("digest does not match exact bytes")
        return cls(
            mode="immutable_payload",
            encoding="base64",
            payload_b64=base64.b64encode(payload).decode("ascii"),
            bytes=len(payload),
            sha256=actual,
        )

    def decode(self) -> Bytes:
        if self.mode != "immutable_payload" or self.encoding != "base64":
            raise _value_error("mode or encoding is invalid")
        if (
            isinstance(self.bytes, bool)
            or not isinstance(self.bytes, int)
            or not 1 <= self.bytes <= MAX_RUNTIME_AUTHORITY_PAYLOAD_BYTES
        ):
            raise _value_error("decoded byte count is outside the limit")
        if (
            not isinstance(self.payload_b64, str)
            or not self.payload_b64
            or len(self.payload_b64) > MAX_RUNTIME_AUTHORITY_PAYLOAD_BASE64_CHARS
        ):
            raise _value_error("base64 text is outside the limit")
        if not isinstance(self.sha256, str) or _DIGEST_RE.fullmatch(self.sha256) is None:
            raise _value_error("digest is not canonical")
        try:
            payload = base64.b64decode(self.payload_b64, validate=True)
        except (ValueError, binascii.Error) as exc:
            raise _value_error("base64 text is invalid") from exc
        if base64.b64encode(payload).decode("ascii") != self.payload_b64:
            raise _value_error("base64 text is not canonical")
        if len(payload) != self.bytes:
            raise _value_error("decoded byte count does not match")
        if _digest(payload) != self.sha256:
            raise _value_error("digest does not match decoded bytes")
        return payload

    def to_dict(self) -> dict[str, str | int]:
        return {
            "mode": self.mode,
            "encoding": self.encoding,
            "payload_b64": self.payload_b64,
            "bytes": self.bytes,
            "sha256": self.sha256,
        }


def materialize_runtime_authority_payload(
    payload: ImmutableRuntimeAuthorityPayload,
    target: Path,
) -> None:
    """Atomically write and re-verify one payload without following links."""

    raw = _decode_runtime(payload)
    try:
        parent = target.parent
        metadata = parent.lstat()
        if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
            raise OSError("unsafe parent")
        descriptor, temporary_name = tempfile.mkstemp(prefix=".authority-", dir=parent)
        temporary = Path(temporary_name)
        try:
            with os.fdopen(descriptor, "wb", closefd=True) as stream:
                stream.write(raw)
                stream.flush()
                os.fsync(stream.fileno())
            os.chmod(temporary, 0o400, follow_symlinks=False)
            _verify_file(payload, temporary)
            os.replace(temporary, target)
        finally:
            temporary.unlink(missing_ok=True)
        _verify_file(payload, target)
    except InitFetchError:
        raise
    except OSError as exc:
        raise _runtime_error("runtime authority payload could not be materialized") from exc


def verify_materialized_runtime_authority_payload(
    payload: ImmutableRuntimeAuthorityPayload,
    target: Path,
) -> None:
    """Reopen and verify the materialized file before base-process authority."""

    _decode_runtime(payload)
    try:
        _verify_file(payload, target)
    except InitFetchError:
        raise
    except OSError as exc:
        raise _runtime_error("runtime authority payload file is unavailable") from exc


def _verify_file(payload: ImmutableRuntimeAuthorityPayload, path: Path) -> None:
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags)
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_size != payload.bytes:
            raise _runtime_error("runtime authority payload file metadata does not match")
        raw = os.read(descriptor, MAX_RUNTIME_AUTHORITY_PAYLOAD_BYTES + 1)
        if len(raw) != payload.bytes or _digest(raw) != payload.sha256:
            raise _runtime_error("runtime authority payload file integrity does not match")
    finally:
        os.close(descriptor)


def _decode_runtime(payload: ImmutableRuntimeAuthorityPayload) -> bytes:
    try:
        return payload.decode()
    except (TypeError, ValueError) as exc:
        raise _runtime_error("runtime authority payload contract is invalid") from exc


def _digest(payload: bytes) -> str:
    return "sha256:" + hashlib.sha256(payload).hexdigest()


def _value_error(message: str) -> ValueError:
    return ValueError(f"runtime authority payload {message}")


def _runtime_error(message: str) -> InitFetchError:
    return InitFetchError("DPONE_RUNTIME_AUTHORITY_PAYLOAD_INVALID", message)


__all__ = [
    "ImmutableRuntimeAuthorityPayload",
    "MAX_SELECTED_RUNTIME_PAYLOADS",
    "MAX_RUNTIME_AUTHORITY_PAYLOAD_BASE64_CHARS",
    "MAX_RUNTIME_AUTHORITY_PAYLOAD_BYTES",
    "RuntimePayloadDescriptor",
    "materialize_runtime_authority_payload",
    "verify_materialized_runtime_authority_payload",
]
