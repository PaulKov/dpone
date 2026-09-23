"""Failure-preserving cleanup for a SqlClient PREPARED attempt context."""

from __future__ import annotations

from dpone.services.mssql_tds_attempt import TdsAttempt


def cleanup_prepared_attempt(
    handle: object | None,
    attempt: TdsAttempt | None,
    *,
    deadline: float,
) -> None:
    """Close both owned resources and preserve every cleanup failure."""
    failures: list[BaseException] = []
    if handle is not None:
        close = getattr(handle, "close", None)
        if callable(close):
            try:
                close(deadline=deadline)
            except BaseException as error:
                failures.append(error)
    if attempt is not None:
        try:
            attempt.close(deadline=deadline)
        except BaseException as error:
            failures.append(error)
    if len(failures) == 1:
        raise failures[0]
    if failures:
        raise BaseExceptionGroup("mssql_native.sqlclient_prepared_attempt_cleanup_failed", failures)


__all__ = ("cleanup_prepared_attempt",)
