"""Feature-local boundary over descriptor-safe evidence file I/O."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from dpone.readiness.route_attestation_files import (
    RouteAttestationFileError,
    read_bounded_file,
    read_strict_json_mapping,
)


class SelfServiceEvidenceFileError(ValueError):
    """Preserve safe error classification without leaking readiness internals."""

    def __init__(self, code: str, message: str) -> None:
        self.code = code
        super().__init__(message)


def read_json(path: Path, *, max_bytes: int, label: str) -> tuple[dict[str, Any], bytes]:
    try:
        return read_strict_json_mapping(path, max_bytes=max_bytes, label=label)
    except RouteAttestationFileError as exc:
        raise SelfServiceEvidenceFileError(exc.code, str(exc)) from exc


def read_bytes(path: Path, *, max_bytes: int, label: str) -> bytes:
    try:
        return read_bounded_file(path, max_bytes=max_bytes, label=label)
    except RouteAttestationFileError as exc:
        raise SelfServiceEvidenceFileError(exc.code, str(exc)) from exc


__all__ = ["SelfServiceEvidenceFileError", "read_bytes", "read_json"]
