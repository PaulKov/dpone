"""Vendor-neutral DB-API lifecycle primitives for workspace activation."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, Protocol


class WorkspaceActivationCursor(Protocol):
    def execute(self, sql: str, *parameters: object) -> WorkspaceActivationCursor: ...

    def fetchone(self) -> tuple[Any, ...] | None: ...

    def fetchall(self) -> Sequence[tuple[Any, ...]]: ...

    def close(self) -> None: ...


class WorkspaceActivationConnection(Protocol):
    autocommit: bool

    def cursor(self) -> WorkspaceActivationCursor: ...

    def commit(self) -> None: ...

    def rollback(self) -> None: ...

    def close(self) -> None: ...


def row(cursor: WorkspaceActivationCursor) -> tuple[Any, ...] | None:
    value = cursor.fetchone()
    return None if value is None else tuple(value)


def rollback(connection: WorkspaceActivationConnection | None) -> None:
    if connection is not None:
        try:
            connection.rollback()
        except Exception:
            pass


def close(value: object | None) -> None:
    if value is not None:
        try:
            value.close()  # type: ignore[attr-defined]
        except Exception:
            pass


__all__ = [
    "WorkspaceActivationConnection",
    "WorkspaceActivationCursor",
    "close",
    "rollback",
    "row",
]
