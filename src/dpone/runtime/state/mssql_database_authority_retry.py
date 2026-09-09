"""Bounded retry policy for fresh MSSQL staging-authority sessions."""

from __future__ import annotations

import logging
import math
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, TypeVar

from dpone.runtime.state.mssql_database_authority_errors import (
    MssqlDatabaseAuthorityVerificationError,
    MssqlStagingVerificationStage,
    detached_verification_error,
    extract_sanitized_sqlstate,
    sanitize_staging_verification_error,
)

MAX_MSSQL_STAGING_SESSION_RETRIES = 2
MSSQL_STAGING_SESSION_RETRY_BACKOFF_SECONDS = (5.0, 10.0)
MSSQL_STAGING_SESSION_RETRY_DEADLINE_SECONDS = 45.0
MSSQL_STAGING_SESSION_QUERY_TIMEOUT_SECONDS = 5
MSSQL_TRANSIENT_SESSION_SQLSTATES = frozenset(
    {
        "08001",
        "08003",
        "08007",
        "08S01",
        "HYT00",
        "HYT01",
    }
)

_ResultT = TypeVar("_ResultT")
_LOG = logging.getLogger(__name__)


_RETRYABLE_STAGES = frozenset(
    {
        MssqlStagingVerificationStage.STAGING_CONNECTOR,
        MssqlStagingVerificationStage.MASTER_CONNECTOR,
        MssqlStagingVerificationStage.MASTER_IDENTITY,
        MssqlStagingVerificationStage.STAGING_IDENTITY,
        MssqlStagingVerificationStage.STAGING_CURRENT_DATABASE,
        MssqlStagingVerificationStage.STAGING_PIN,
    }
)


@dataclass(frozen=True, slots=True)
class MssqlStagingSessionRetryPolicy:
    """Admit only known transient failures within one fixed wall-time budget."""

    max_retries: int = MAX_MSSQL_STAGING_SESSION_RETRIES
    backoff_seconds: tuple[float, ...] = MSSQL_STAGING_SESSION_RETRY_BACKOFF_SECONDS
    deadline_seconds: float = MSSQL_STAGING_SESSION_RETRY_DEADLINE_SECONDS

    def __post_init__(self) -> None:
        if (
            isinstance(self.max_retries, bool)
            or not isinstance(self.max_retries, int)
            or not 0 <= self.max_retries <= MAX_MSSQL_STAGING_SESSION_RETRIES
        ):
            raise ValueError(f"max_retries must be an integer between 0 and {MAX_MSSQL_STAGING_SESSION_RETRIES}")
        if len(self.backoff_seconds) < self.max_retries:
            raise ValueError("backoff_seconds must cover every configured retry")
        if any(
            isinstance(delay, bool) or not isinstance(delay, (int, float)) or not math.isfinite(delay) or delay <= 0
            for delay in self.backoff_seconds
        ):
            raise ValueError("backoff_seconds must contain only positive numbers")
        if (
            isinstance(self.deadline_seconds, bool)
            or not isinstance(self.deadline_seconds, (int, float))
            or not math.isfinite(self.deadline_seconds)
            or self.deadline_seconds <= 0
        ):
            raise ValueError("deadline_seconds must be positive")

    def is_retryable(self, error: MssqlDatabaseAuthorityVerificationError) -> bool:
        """Accept only a known transient SQLSTATE from a fresh-session stage."""

        try:
            stage = MssqlStagingVerificationStage(error.stage or "")
        except ValueError:
            return False
        return stage in _RETRYABLE_STAGES and error.sqlstate in MSSQL_TRANSIENT_SESSION_SQLSTATES

    def delay_seconds(self, *, retry_number: int) -> float:
        """Return the fixed backoff for one configured retry."""

        if isinstance(retry_number, bool) or not 1 <= retry_number <= self.max_retries:
            raise ValueError("retry_number must identify one configured retry")
        return float(self.backoff_seconds[retry_number - 1])


@dataclass(frozen=True, slots=True)
class MssqlStagingSessionDeadline:
    """Absolute monotonic budget shared by every blocking admission boundary."""

    expires_at: float
    monotonic: Callable[[], float]

    def remaining_seconds(self) -> float:
        """Return non-negative wall-clock budget remaining at this instant."""

        return max(0.0, self.expires_at - self.monotonic())

    def timeout_seconds(self, *, maximum: int) -> int:
        """Return an integer timeout that cannot extend beyond this deadline."""

        if isinstance(maximum, bool) or not isinstance(maximum, int) or maximum < 1:
            raise ValueError("maximum timeout must be a positive integer")
        timeout = min(maximum, math.floor(self.remaining_seconds()))
        if timeout < 1:
            raise self.exhausted_error()
        return timeout

    def ensure_active(self) -> None:
        """Reject a blocking step that consumed the complete admission budget."""

        if self.monotonic() >= self.expires_at:
            raise self.exhausted_error()

    def can_sleep(self, delay: float) -> bool:
        """Return whether a complete backoff still leaves time for another attempt."""

        return self.monotonic() + delay < self.expires_at

    @staticmethod
    def exhausted_error() -> MssqlDatabaseAuthorityVerificationError:
        return MssqlDatabaseAuthorityVerificationError(
            "mssql_transaction.staging_database_session_unavailable",
            stage=MssqlStagingVerificationStage.RETRY_DEADLINE,
            retry_exhausted=True,
            deadline_exhausted=True,
        )


class MssqlStagingSessionRetryRunner:
    """Run complete, independently cleaned staging-session attempts."""

    def __init__(
        self,
        *,
        policy: MssqlStagingSessionRetryPolicy | None = None,
        sleeper: Callable[[float], None] | None = None,
        monotonic: Callable[[], float] | None = None,
        logger: Any | None = None,
    ) -> None:
        self.policy = policy if policy is not None else MssqlStagingSessionRetryPolicy()
        self._sleeper = sleeper if sleeper is not None else time.sleep
        self._monotonic = monotonic if monotonic is not None else time.monotonic
        self._logger = logger if logger is not None else _LOG

    def run(self, operation: Callable[[MssqlStagingSessionDeadline], _ResultT]) -> _ResultT:
        """Return the first in-budget success or one sanitized stable error."""

        deadline = MssqlStagingSessionDeadline(
            expires_at=self._monotonic() + self.policy.deadline_seconds,
            monotonic=self._monotonic,
        )
        attempt = 0
        while True:
            attempt += 1
            caught: MssqlDatabaseAuthorityVerificationError | None = None
            try:
                result = operation(deadline)
            except MssqlDatabaseAuthorityVerificationError as error:
                caught = detached_verification_error(error)

            if caught is not None:
                if caught.deadline_exhausted:
                    raise _with_retry_evidence(
                        caught,
                        attempts=attempt,
                        retry_exhausted=True,
                        deadline_exhausted=True,
                    ) from None
                retryable = self.policy.is_retryable(caught)
                if not retryable:
                    raise _with_retry_evidence(caught, attempts=attempt) from None
                if attempt > self.policy.max_retries:
                    raise _with_retry_evidence(
                        caught,
                        attempts=attempt,
                        retry_exhausted=True,
                        deadline_exhausted=deadline.remaining_seconds() <= 0,
                    ) from None
                retry_number = attempt
                delay = self.policy.delay_seconds(retry_number=retry_number)
                if not deadline.can_sleep(delay):
                    raise _with_retry_evidence(
                        caught,
                        attempts=attempt,
                        retry_exhausted=True,
                        deadline_exhausted=True,
                    ) from None
                self._log_retry(caught, retry_number=retry_number, delay=delay)
                self._sleeper(delay)
                if deadline.remaining_seconds() <= 0:
                    raise _with_retry_evidence(
                        caught,
                        attempts=attempt,
                        retry_exhausted=True,
                        deadline_exhausted=True,
                    ) from None
                continue

            if deadline.remaining_seconds() > 0:
                return result
            cleanup_error = _close_result_strict_error(result)
            if cleanup_error is not None:
                raise _with_retry_evidence(
                    cleanup_error,
                    attempts=attempt,
                    retry_exhausted=True,
                    deadline_exhausted=True,
                ) from None
            raise MssqlDatabaseAuthorityVerificationError(
                "mssql_transaction.staging_database_session_unavailable",
                stage=MssqlStagingVerificationStage.RETRY_DEADLINE,
                attempts=attempt,
                retry_exhausted=True,
                deadline_exhausted=True,
            ) from None

    def _log_retry(
        self,
        error: MssqlDatabaseAuthorityVerificationError,
        *,
        retry_number: int,
        delay: float,
    ) -> None:
        try:
            self._logger.warning(
                "MSSQL staging authority transient retry: stage=%s sqlstate=%s retry=%s max_retries=%s "
                "backoff_seconds=%s",
                error.stage,
                error.sqlstate,
                retry_number,
                self.policy.max_retries,
                delay,
            )
        except Exception:
            return


def _with_retry_evidence(
    error: MssqlDatabaseAuthorityVerificationError,
    *,
    attempts: int,
    retry_exhausted: bool | None = None,
    deadline_exhausted: bool | None = None,
) -> MssqlDatabaseAuthorityVerificationError:
    return detached_verification_error(
        error,
        attempts=attempts,
        retry_exhausted=retry_exhausted,
        deadline_exhausted=deadline_exhausted,
    )


def _close_result_strict_error(result: object) -> MssqlDatabaseAuthorityVerificationError | None:
    close = getattr(result, "close_strict", None)
    if not callable(close):
        close = getattr(result, "close", None)
    if not callable(close):
        return None
    caught: MssqlDatabaseAuthorityVerificationError | None = None
    try:
        close()
    except MssqlDatabaseAuthorityVerificationError as error:
        caught = detached_verification_error(error)
    except Exception as error:
        caught = sanitize_staging_verification_error(
            error,
            stage=MssqlStagingVerificationStage.STAGING_CLEANUP,
            default_code="mssql_transaction.staging_database_session_cleanup_failed",
        )
    return caught


__all__ = [
    "MAX_MSSQL_STAGING_SESSION_RETRIES",
    "MSSQL_STAGING_SESSION_QUERY_TIMEOUT_SECONDS",
    "MSSQL_STAGING_SESSION_RETRY_BACKOFF_SECONDS",
    "MSSQL_STAGING_SESSION_RETRY_DEADLINE_SECONDS",
    "MSSQL_TRANSIENT_SESSION_SQLSTATES",
    "MssqlStagingSessionDeadline",
    "MssqlStagingSessionRetryPolicy",
    "MssqlStagingSessionRetryRunner",
    "MssqlStagingVerificationStage",
    "extract_sanitized_sqlstate",
    "sanitize_staging_verification_error",
]
