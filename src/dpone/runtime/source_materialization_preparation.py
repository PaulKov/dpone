"""Protect source snapshots until preparation hands them to an artifact owner."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import Any

from dpone.runtime.process_io import add_cleanup_failure_note
from dpone.runtime.source_materialization_audit import publish_source_materialization_cleanup


@contextmanager
def guard_source_materialization_preparation(
    *,
    cleanup: Callable[[], Any],
    provider: str | None,
    cleanup_policy: str,
) -> Iterator[None]:
    """Release eager snapshots if preparation fails before ownership handoff.

    Cover CREATE as well: SQL Server can leave an empty SELECT-INTO table after
    insertion fails. Preserve the primary failure and retention policies; a
    failed DROP is audited and attached as a redacted secondary exception note.
    """

    try:
        yield
    except BaseException as error:
        if cleanup_policy == "eager":
            try:
                result = cleanup()
            except Exception as cleanup_error:
                add_cleanup_failure_note(error, context="source materialization cleanup", cleanup_error=cleanup_error)
                result = {
                    "status": "failed",
                    "reason": "source_materialization_cleanup_failed",
                    "details": {"error_type": type(cleanup_error).__name__},
                }
            try:
                publish_source_materialization_cleanup(
                    provider=provider,
                    cleanup_policy=cleanup_policy,
                    result=result,
                )
            except Exception as audit_error:
                add_cleanup_failure_note(error, context="source cleanup audit", cleanup_error=audit_error)
        raise


__all__ = ["guard_source_materialization_preparation"]
