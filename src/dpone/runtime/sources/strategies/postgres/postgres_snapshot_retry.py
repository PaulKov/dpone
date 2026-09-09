"""Bounded retry policy for transient PostgreSQL snapshot conflicts."""

from __future__ import annotations

import random
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, TypeVar

POSTGRES_SERIALIZATION_FAILURE_SQLSTATE = "40001"
MAX_POSTGRES_SNAPSHOT_RETRIES = 2
_ResultT = TypeVar("_ResultT")
_SecondaryFailureRecorder = Callable[[BaseException, str, BaseException], None]


@dataclass(frozen=True, slots=True)
class PostgresSnapshotRetryPolicy:
    """Classify one failed snapshot attempt and calculate bounded backoff."""

    max_retries: int = MAX_POSTGRES_SNAPSHOT_RETRIES
    initial_delay_seconds: float = 2.0
    max_delay_seconds: float = 30.0
    jitter_ratio: float = 0.25

    def __post_init__(self) -> None:
        if (
            isinstance(self.max_retries, bool)
            or not isinstance(self.max_retries, int)
            or not 0 <= self.max_retries <= MAX_POSTGRES_SNAPSHOT_RETRIES
        ):
            raise ValueError(f"max_retries must be an integer between 0 and {MAX_POSTGRES_SNAPSHOT_RETRIES}")
        if self.initial_delay_seconds <= 0:
            raise ValueError("initial_delay_seconds must be positive")
        if self.max_delay_seconds < self.initial_delay_seconds:
            raise ValueError("max_delay_seconds must not be less than initial_delay_seconds")
        if not 0 <= self.jitter_ratio <= 1:
            raise ValueError("jitter_ratio must be between 0 and 1")

    def is_retryable(self, error: BaseException) -> bool:
        """Accept only SQLSTATE 40001 from the causal exception chain."""

        current: BaseException | None = error
        seen: set[int] = set()
        while current is not None and id(current) not in seen:
            seen.add(id(current))
            if str(getattr(current, "sqlstate", "") or "").strip() == POSTGRES_SERIALIZATION_FAILURE_SQLSTATE:
                return True
            current = current.__cause__ if current.__cause__ is not None else current.__context__
        return False

    def delay_seconds(self, *, retry_number: int, random_unit: float) -> float:
        """Return exponential backoff plus bounded proportional jitter."""

        if isinstance(retry_number, bool) or not 1 <= retry_number <= self.max_retries:
            raise ValueError("retry_number must identify one configured retry")
        if isinstance(random_unit, bool) or not 0 <= random_unit <= 1:
            raise ValueError("random_unit must be between 0 and 1")
        exponential = min(
            self.max_delay_seconds,
            self.initial_delay_seconds * (2 ** (retry_number - 1)),
        )
        return min(
            self.max_delay_seconds,
            exponential * (1 + self.jitter_ratio * random_unit),
        )


class PostgresSnapshotRetryRunner:
    """Run one source-only snapshot operation under the closed retry policy."""

    def __init__(
        self,
        *,
        policy: PostgresSnapshotRetryPolicy | None = None,
        sleeper: Callable[[float], None] | None = None,
        random_unit: Callable[[], float] | None = None,
        secondary_failure_recorder: _SecondaryFailureRecorder,
    ) -> None:
        self._policy = policy if policy is not None else PostgresSnapshotRetryPolicy()
        self._sleeper = sleeper if sleeper is not None else time.sleep
        self._random_unit = random_unit if random_unit is not None else random.random
        self._secondary_failure_recorder = secondary_failure_recorder

    def run(
        self,
        operation: Callable[[], _ResultT],
        *,
        connector: Any,
        logger: Any,
    ) -> _ResultT:
        """Return the first success or re-raise the authoritative failure."""

        retry_number = 0
        while True:
            try:
                return operation()
            except Exception as primary:
                if retry_number >= self._policy.max_retries or not self._policy.is_retryable(primary):
                    raise
                if not self._reset_connection(connector, primary):
                    raise
                retry_number += 1
                delay = self._policy.delay_seconds(
                    retry_number=retry_number,
                    random_unit=self._random_unit(),
                )
                self._log_retry(
                    logger,
                    primary,
                    retry_number=retry_number,
                    delay=delay,
                )
                self._sleeper(delay)

    def _reset_connection(self, connector: Any, primary: BaseException) -> bool:
        close = getattr(connector, "close", None)
        if not callable(close):
            add_note = getattr(primary, "add_note", None)
            if callable(add_note):
                add_note("postgres_snapshot.retry_connection_reset_unavailable")
            return False
        try:
            close()
        except Exception as secondary:
            self._secondary_failure_recorder(
                primary,
                "postgres_snapshot.retry_connection_reset_failed",
                secondary,
            )
            return False
        return True

    def _log_retry(
        self,
        logger: Any,
        primary: BaseException,
        *,
        retry_number: int,
        delay: float,
    ) -> None:
        try:
            logger.log_etl_progress(
                "POSTGRES_SNAPSHOT_RETRY",
                {
                    "SQLSTATE": POSTGRES_SERIALIZATION_FAILURE_SQLSTATE,
                    "Retry": retry_number,
                    "Max Retries": self._policy.max_retries,
                    "Backoff Seconds": delay,
                },
            )
        except Exception as secondary:
            self._secondary_failure_recorder(
                primary,
                "postgres_snapshot.retry_log_failed",
                secondary,
            )


__all__ = [
    "MAX_POSTGRES_SNAPSHOT_RETRIES",
    "POSTGRES_SERIALIZATION_FAILURE_SQLSTATE",
    "PostgresSnapshotRetryPolicy",
    "PostgresSnapshotRetryRunner",
]
