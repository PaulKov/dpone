"""Bounded, deterministic wait policy for exact-head PR receipt prerequisites."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Literal

ObservationState = Literal["PENDING", "READY", "TERMINAL_FAILURE"]
OutcomeState = Literal["READY", "TIMEOUT", "STALE_HEAD", "TERMINAL_FAILURE"]


@dataclass(frozen=True)
class Observation:
    """One classified prerequisite observation without network authority."""

    state: ObservationState
    reason: str | None = None

    @classmethod
    def pending(cls, reason: str) -> Observation:
        return cls("PENDING", reason)

    @classmethod
    def ready(cls) -> Observation:
        return cls("READY")

    @classmethod
    def terminal(cls, reason: str) -> Observation:
        return cls("TERMINAL_FAILURE", reason)


@dataclass(frozen=True)
class WaitOutcome:
    """Deterministic bounded waiting outcome, never itself a receipt PASS."""

    state: OutcomeState
    attempts: int
    elapsed_seconds: float
    reason: str | None = None


def wait_for_exact_head(
    *,
    expected_head: str,
    fetch_head: Callable[[], str],
    observe: Callable[[], Observation],
    now: Callable[[], float],
    sleep: Callable[[float], None],
    timeout_seconds: float,
    initial_backoff_seconds: float,
    max_backoff_seconds: float,
) -> WaitOutcome:
    """Poll retryable prerequisites with exponential backoff and head checks."""

    if timeout_seconds <= 0 or initial_backoff_seconds <= 0 or max_backoff_seconds < initial_backoff_seconds:
        raise ValueError("wait bounds must be positive and ordered")
    started_at, attempts, backoff = now(), 0, initial_backoff_seconds
    while True:
        elapsed = now() - started_at
        if elapsed >= timeout_seconds:
            return WaitOutcome("TIMEOUT", attempts, elapsed, "prerequisite deadline elapsed")
        if fetch_head() != expected_head:
            return WaitOutcome("STALE_HEAD", attempts, elapsed, "pull-request head changed")
        attempts += 1
        observation = observe()
        elapsed = now() - started_at
        if observation.state == "READY":
            return WaitOutcome("READY", attempts, elapsed)
        if observation.state == "TERMINAL_FAILURE":
            return WaitOutcome("TERMINAL_FAILURE", attempts, elapsed, observation.reason)
        remaining = timeout_seconds - elapsed
        if remaining <= 0:
            return WaitOutcome("TIMEOUT", attempts, elapsed, "prerequisite deadline elapsed")
        sleep(min(backoff, remaining))
        backoff = min(backoff * 2, max_backoff_seconds)
