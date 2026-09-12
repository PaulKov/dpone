"""Pinned-session cleanup for verified PostgreSQL relation snapshots."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class PostgresSnapshotCleanupResultV1:
    """Closed, redaction-safe cleanup result."""

    succeeded: bool
    connection_quarantined: bool
    close_failed: bool = False


def cleanup_pinned_snapshot(connector: Any, physical_connection: Any) -> PostgresSnapshotCleanupResultV1:
    """Rollback the pinned session and quarantine the connector on uncertainty."""

    already_quarantined = bool(getattr(connector, "quarantined", False) or getattr(connector, "_quarantined", False))
    try:
        if already_quarantined:
            physical_connection.rollback()
        elif getattr(connector, "_connection", None) is physical_connection:
            connector.rollback()
        else:
            physical_connection.rollback()
        physical_connection.autocommit = getattr(connector, "autocommit", True)
    except (asyncio.CancelledError, KeyboardInterrupt, SystemExit, GeneratorExit) as cancellation:
        try:
            _fence_pinned_and_quarantine_connector(connector, physical_connection)
        except BaseException:
            pass
        cancellation.__cause__ = cancellation.__context__ = None
        raise
    except BaseException:
        quarantined = False
        close_failed = False
        try:
            quarantined, close_failed = _fence_pinned_and_quarantine_connector(connector, physical_connection)
        except (asyncio.CancelledError, KeyboardInterrupt, SystemExit, GeneratorExit) as cancellation:
            cancellation.__cause__ = cancellation.__context__ = None
            raise
        except BaseException:
            pass
        return PostgresSnapshotCleanupResultV1(
            False,
            quarantined,
            close_failed,
        )
    return PostgresSnapshotCleanupResultV1(True, already_quarantined)


def _fence_pinned_and_quarantine_connector(connector: Any, physical_connection: Any) -> tuple[bool, bool]:
    """Fence the captured session before quarantining any replacement."""

    exact = getattr(connector, "quarantine_if_current", None)
    if callable(exact):
        if exact(physical_connection):
            return True, False
        close_failed = False
        try:
            physical_connection.close()
        except (asyncio.CancelledError, KeyboardInterrupt, SystemExit, GeneratorExit) as cancellation:
            try:
                _quarantine_connector(connector)
            except BaseException:
                pass
            cancellation.__cause__ = cancellation.__context__ = None
            raise
        except BaseException:
            close_failed = True
        return _quarantine_connector(connector), close_failed
    return _quarantine_connector(connector), False


def _quarantine_connector(connector: Any) -> bool:
    """Apply the connector-owned fail-closed fence and report observation."""

    quarantine = getattr(connector, "quarantine", None)
    if not callable(quarantine):
        return False
    quarantine()
    return bool(getattr(connector, "quarantined", False) or getattr(connector, "_quarantined", False))


__all__ = ["PostgresSnapshotCleanupResultV1", "cleanup_pinned_snapshot"]
