"""Small shared helpers for transactional semantic-refresh publication adapters."""

from __future__ import annotations

import uuid
from collections.abc import Mapping
from typing import Protocol


class _Connection(Protocol):
    def rollback(self) -> None: ...


def publication_uuid_text(value: object) -> str | None:
    """Normalize SQL Server uniqueidentifier values to canonical lowercase text."""

    return None if value is None else str(uuid.UUID(str(value)))


def publication_acknowledgement(
    request: Mapping[str, object],
    journal_state: str,
) -> dict[str, object]:
    """Return the closed exact durable state acknowledgement."""

    return {
        **request,
        "atomic": True,
        "committed": True,
        "journal_state": journal_state,
    }


def rollback_quietly(connection: _Connection | None) -> None:
    """Best-effort rollback while preserving the authoritative original failure."""

    if connection is not None:
        try:
            connection.rollback()
        except Exception:
            pass


__all__ = [
    "publication_acknowledgement",
    "publication_uuid_text",
    "rollback_quietly",
]
