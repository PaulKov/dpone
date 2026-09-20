"""Parse-safe immutable runtime-authority source contracts for provider v5."""

from __future__ import annotations

import base64
import binascii
import hashlib
import re
from builtins import bytes as Bytes
from collections.abc import Mapping
from dataclasses import dataclass

MAX_RUNTIME_AUTHORITY_PAYLOAD_BYTES = 4 * 1024
MAX_RUNTIME_AUTHORITY_PAYLOAD_BASE64_CHARS = ((MAX_RUNTIME_AUTHORITY_PAYLOAD_BYTES + 2) // 3) * 4
_DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")


@dataclass(frozen=True, slots=True)
class RuntimeAuthoritySource:
    """Value-free coordinates for one deployment-owned Kubernetes Secret key."""

    mode: str
    secret_name: str
    secret_key: str

    def to_dict(self) -> dict[str, str]:
        return {
            "mode": self.mode,
            "secret_name": self.secret_name,
            "secret_key": self.secret_key,
        }


@dataclass(frozen=True, slots=True)
class ImmutableRuntimeAuthoritySource:
    """One safe-to-persist canonical payload bound to exact decoded bytes."""

    mode: str
    encoding: str
    payload_b64: str
    bytes: int
    sha256: str

    @classmethod
    def from_mapping(cls, value: object) -> ImmutableRuntimeAuthoritySource:
        if not isinstance(value, Mapping) or frozenset(str(key) for key in value) != {
            "mode",
            "encoding",
            "payload_b64",
            "bytes",
            "sha256",
        }:
            raise ValueError("runtime authority payload fields are invalid")
        source = cls(
            mode=value["mode"],
            encoding=value["encoding"],
            payload_b64=value["payload_b64"],
            bytes=value["bytes"],
            sha256=value["sha256"],
        )
        source.decode()
        return source

    def decode(self) -> Bytes:
        if self.mode != "immutable_payload" or self.encoding != "base64":
            raise ValueError("runtime authority payload mode or encoding is invalid")
        if (
            isinstance(self.bytes, bool)
            or not isinstance(self.bytes, int)
            or not 1 <= self.bytes <= MAX_RUNTIME_AUTHORITY_PAYLOAD_BYTES
        ):
            raise ValueError("runtime authority payload byte count is invalid")
        if (
            not isinstance(self.payload_b64, str)
            or not self.payload_b64
            or len(self.payload_b64) > MAX_RUNTIME_AUTHORITY_PAYLOAD_BASE64_CHARS
        ):
            raise ValueError("runtime authority payload base64 length is invalid")
        if not isinstance(self.sha256, str) or _DIGEST_RE.fullmatch(self.sha256) is None:
            raise ValueError("runtime authority payload digest is invalid")
        try:
            payload = base64.b64decode(self.payload_b64, validate=True)
        except (ValueError, binascii.Error) as exc:
            raise ValueError("runtime authority payload base64 is invalid") from exc
        if base64.b64encode(payload).decode("ascii") != self.payload_b64:
            raise ValueError("runtime authority payload base64 is not canonical")
        if len(payload) != self.bytes or "sha256:" + hashlib.sha256(payload).hexdigest() != self.sha256:
            raise ValueError("runtime authority payload integrity does not match")
        return payload

    def to_dict(self) -> dict[str, str | int]:
        return {
            "mode": self.mode,
            "encoding": self.encoding,
            "payload_b64": self.payload_b64,
            "bytes": self.bytes,
            "sha256": self.sha256,
        }


__all__ = [
    "ImmutableRuntimeAuthoritySource",
    "MAX_RUNTIME_AUTHORITY_PAYLOAD_BASE64_CHARS",
    "MAX_RUNTIME_AUTHORITY_PAYLOAD_BYTES",
    "RuntimeAuthoritySource",
]
