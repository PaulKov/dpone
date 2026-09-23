"""Ports for durable, create-only evidence persistence."""

from __future__ import annotations

from typing import Protocol


class CreateOnlyEvidenceWriterV1(Protocol):
    """Persist one exact evidence payload without replacement semantics."""

    def write(self, relative_name: str, payload: bytes) -> None:
        """Durably create or verify an identical immutable evidence target."""


__all__ = ["CreateOnlyEvidenceWriterV1", "ExactEvidenceReaderV1"]


class ExactEvidenceReaderV1(Protocol):
    """Read one bounded immutable receipt; no creation or repair capability."""

    def read(self, relative_name: str, byte_count: int, payload_sha256: str) -> bytes:
        """Return exact acknowledged bytes or reject without modifying storage."""
