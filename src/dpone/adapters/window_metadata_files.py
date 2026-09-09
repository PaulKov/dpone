"""Crash-safe, local metadata files owned by bounded-window target adapters."""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Any

from dpone.contracts.bounded_window import WindowContractError


class FileWindowMetadataStore:
    """Fsync-backed metadata on an access-controlled local filesystem."""

    def save(self, path: Path, value: dict[str, Any]) -> None:
        """Atomically replace a receipt, fsyncing both file and directory."""
        path.parent.mkdir(parents=True, exist_ok=True)
        descriptor, temporary = tempfile.mkstemp(dir=path.parent)
        try:
            with os.fdopen(descriptor, "w") as stream:
                json.dump(value, stream, sort_keys=True)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, path)
            descriptor = os.open(path.parent, os.O_RDONLY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)

    def load(self, path: Path) -> dict[str, Any] | None:
        """Read only complete v1 metadata; corruption fails closed."""
        if not path.exists():
            return None
        try:
            value = json.loads(path.read_text())
            if not isinstance(value, dict) or type(value.get("version")) is not int or value["version"] != 1:
                raise ValueError("unsupported metadata version")
            return value
        except (ValueError, OSError) as error:
            raise WindowContractError("Invalid window receipt metadata") from error

    def remove(self, path: Path) -> None:
        """Settle deletion before acknowledging publication-marker cleanup."""
        try:
            path.unlink()
        except FileNotFoundError:
            pass
        try:
            descriptor = os.open(path.parent, os.O_RDONLY)
        except FileNotFoundError:
            return
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
