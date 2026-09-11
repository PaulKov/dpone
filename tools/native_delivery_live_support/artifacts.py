"""Canonical, immutable evidence objects and an atomic final run envelope."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
import uuid
from pathlib import Path
from typing import Any


def canonical_json(value: Any) -> bytes:
    """Serialize v1 identity bytes without non-finite numbers or ASCII escaping."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()


def digest(value: Any) -> str:
    """Hash canonical JSON, not a Python representation."""
    return hashlib.sha256(canonical_json(value)).hexdigest()


def _atomic(path: Path, payload: bytes, *, overwrite: bool) -> None:
    if path.is_symlink():
        raise ValueError("output_symlink")
    fd, temporary = tempfile.mkstemp(prefix=".delivery-", dir=path.parent)
    temp = Path(temporary)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        if overwrite:
            os.replace(temp, path)
        else:
            # An exclusive hard link closes the exists()/rename race.
            os.link(temp, path)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        temp.unlink(missing_ok=True)


class ArtifactStore:
    """Retain unique run objects; replacing an envelope never rewrites old objects."""

    def __init__(self, output: Path, *, overwrite: bool = False):
        self.output = output.absolute()
        self.overwrite = overwrite
        self.directory = self.output.parent / f"delivery-{uuid.uuid4().hex}"

    def preflight(self) -> None:
        """Refuse unusable destinations before any service is invoked."""
        if not self.output.parent.is_dir() or self.output.is_symlink():
            raise ValueError("invalid_output_destination")
        if self.output.exists() and not self.overwrite:
            raise FileExistsError("output_exists")
        if self.output.exists() and not self.output.is_file():
            raise ValueError("invalid_output_destination")

    def write(self, label: str, value: dict[str, Any]) -> dict[str, str]:
        """Write one immutable object and return a relative, byte-bound reference."""
        if not label or any(c not in "abcdefghijklmnopqrstuvwxyz0123456789-_" for c in label):
            raise ValueError("invalid_artifact_label")
        payload = canonical_json(value)
        self.directory.mkdir(mode=0o700, exist_ok=True)
        path = self.directory / f"{label}.json"
        _atomic(path, payload, overwrite=False)
        return {
            "path": path.relative_to(self.output.parent).as_posix(),
            "sha256": hashlib.sha256(payload).hexdigest(),
            "status": value["status"],
        }

    def publish(self, envelope: dict[str, Any]) -> None:
        """Publish last; invalid serialization cannot damage an existing envelope."""
        payload = canonical_json(envelope)
        self.preflight()
        _atomic(self.output, payload, overwrite=self.overwrite)


def read_artifact(root: Path, reference: dict[str, Any]) -> dict[str, Any]:
    """Verify retained bytes and reject traversal, symlinks and non-JSON objects."""
    relative = Path(reference["path"])
    if relative.is_absolute() or ".." in relative.parts or not relative.parts:
        raise ValueError("artifact_path_escape")
    path = root
    for part in relative.parts:
        path /= part
        if path.is_symlink():
            raise ValueError("artifact_symlink")
    if not path.resolve().is_relative_to(root.resolve()):
        raise ValueError("artifact_path_escape")
    payload = path.read_bytes()
    if hashlib.sha256(payload).hexdigest() != reference["sha256"]:
        raise ValueError("artifact_changed")
    value = json.loads(payload, parse_constant=lambda _: (_ for _ in ()).throw(ValueError("nonfinite_json")))
    if not isinstance(value, dict) or value.get("status") != reference["status"]:
        raise ValueError("artifact_status_mismatch")
    return value
