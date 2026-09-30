"""Bounded privacy scanner for shareable MSSQL native certification artifacts."""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

_MAX_ARTIFACT_BYTES = 8 << 20
_PRIVATE_KEYS = frozenset(
    {
        "connection_string",
        "database",
        "endpoint",
        "file_path",
        "host",
        "password",
        "port",
        "query",
        "schema",
        "stage",
        "table",
        "username",
    }
)


def _scan_document(value: Any, secret_needles: tuple[str, ...]) -> None:
    if isinstance(value, Mapping):
        if any(str(key).casefold() in _PRIVATE_KEYS for key in value):
            raise ValueError("mssql_native.evidence_privacy.private_key")
        for item in value.values():
            _scan_document(item, secret_needles)
    elif isinstance(value, list):
        for item in value:
            _scan_document(item, secret_needles)
    elif isinstance(value, str) and any(
        value == needle or (len(needle) >= 8 and needle in value) for needle in secret_needles
    ):
        raise ValueError("mssql_native.evidence_privacy.private_value")


def scan_mssql_native_shareable_artifacts(paths: Iterable[Path], *, secret_needles: Iterable[str]) -> None:
    """Reject private coordinates or caller-provided secret values in JSON artifacts."""
    needles = tuple(value for value in secret_needles if isinstance(value, str) and value)
    for path in paths:
        if path.is_symlink() or not path.is_file():
            raise ValueError("mssql_native.evidence_privacy.invalid_artifact")
        payload = path.read_bytes()
        if len(payload) > _MAX_ARTIFACT_BYTES:
            raise ValueError("mssql_native.evidence_privacy.invalid_artifact")
        try:
            document = json.loads(payload)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise ValueError("mssql_native.evidence_privacy.invalid_artifact") from error
        _scan_document(document, needles)


__all__ = ["scan_mssql_native_shareable_artifacts"]
