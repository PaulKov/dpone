"""Ports for durable, create-only evidence persistence."""

from __future__ import annotations

from typing import Protocol


class CreateOnlyEvidenceWriterV1(Protocol):
    """Persist one exact evidence payload without replacement semantics."""

    def write(self, relative_name: str, payload: bytes) -> None:
        """Durably create or verify an identical immutable evidence target."""


__all__ = ["CreateOnlyEvidenceWriterV1"]
