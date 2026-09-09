"""Two-observation, always-unverified capacity calibration decision service."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Protocol

from dpone.contracts.ci_shadow_reconciliation import (
    CAPACITY_DECISION,
    AcquiredObservation,
    CapacityUsage,
    ReconciliationPolicy,
    capacity_code,
)


class RequestBudget(Protocol):
    """Narrow completed-observation budget view required by calibration."""

    @property
    def limits_crossed(self) -> tuple[str, ...]: ...

    def counters(self) -> dict[str, int | float]: ...


@dataclass(frozen=True)
class CapacityCalibration:
    """Diagnostic-only result; it deliberately contains no PASS representation."""

    decision: str
    code: str
    complete: bool
    observations_match: bool
    first_observation_digest: str
    second_observation_digest: str
    observation_started_at: datetime
    evidence_observed_through: datetime
    calibration_observed_at: datetime
    usage: CapacityUsage
    counters: dict[str, int | float]
    limits_crossed: tuple[str, ...]


def calibrate_capacity(
    acquire: Callable[[], AcquiredObservation],
    *,
    budget: RequestBudget,
    policy: ReconciliationPolicy,
    utc_clock: Callable[[], datetime],
) -> CapacityCalibration:
    """Perform exactly two independent acquisitions and no reads after the second.

    The acquisition callable owns all provider reads.  This service invokes it
    twice, then only evaluates captured bytes and the shared metered budget.
    """

    first = acquire()
    second = acquire()
    observed_at = _whole_utc_second(utc_clock())
    _require_observation_time(first, observed_at, policy)
    _require_observation_time(second, observed_at, policy)
    counters = budget.counters()
    usage = CapacityUsage(
        total_http_requests=_integer_counter(counters, "total_http_requests"),
        total_response_body_bytes=_integer_counter(counters, "total_response_body_bytes"),
        execution_wall_seconds=_numeric_counter(counters, "execution_wall_seconds"),
    )
    complete = first.complete and second.complete and not budget.limits_crossed
    observations_match = first.canonical_bytes == second.canonical_bytes
    return CapacityCalibration(
        decision=CAPACITY_DECISION,
        code=capacity_code(usage, complete=complete, observations_match=observations_match, policy=policy),
        complete=complete,
        observations_match=observations_match,
        first_observation_digest=first.digest,
        second_observation_digest=second.digest,
        observation_started_at=first.observation_started_at,
        evidence_observed_through=second.evidence_observed_through,
        calibration_observed_at=observed_at,
        usage=usage,
        counters=counters,
        limits_crossed=budget.limits_crossed,
    )


def blocked_capacity_calibration(
    *, budget: RequestBudget, observation_started_at: datetime, utc_clock: Callable[[], datetime]
) -> CapacityCalibration:
    """Render a closed diagnostic result after a recoverable acquisition failure.

    The result never invents a snapshot identity: both digests are a fixed
    explicit absent-value sentinel, ``complete`` and equality are false, and
    the accumulated counters remain available for safe recovery decisions.
    """

    observed_at = _whole_utc_second(utc_clock())
    started = _whole_utc_second(observation_started_at)
    if observed_at < started:
        raise ValueError("blocked capacity observation clock is not monotonic")
    counters = budget.counters()
    usage = CapacityUsage(
        total_http_requests=_integer_counter(counters, "total_http_requests"),
        total_response_body_bytes=_integer_counter(counters, "total_response_body_bytes"),
        execution_wall_seconds=_numeric_counter(counters, "execution_wall_seconds"),
    )
    absent = "sha256:" + "0" * 64
    return CapacityCalibration(
        decision=CAPACITY_DECISION,
        code="RECONCILIATION_PR4C_IMPLEMENTATION_BLOCKED",
        complete=False,
        observations_match=False,
        first_observation_digest=absent,
        second_observation_digest=absent,
        observation_started_at=started,
        evidence_observed_through=observed_at,
        calibration_observed_at=observed_at,
        usage=usage,
        counters=counters,
        limits_crossed=budget.limits_crossed,
    )


def _whole_utc_second(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError("calibration clock must be UTC-aware")
    return value.replace(microsecond=0)


def _require_observation_time(
    observation: AcquiredObservation, calibration: datetime, policy: ReconciliationPolicy
) -> None:
    started = _whole_utc_second(observation.observation_started_at)
    observed = _whole_utc_second(observation.evidence_observed_through)
    if not started <= observed <= calibration:
        raise ValueError("observation timestamps are not monotonic")
    if calibration - started > timedelta(seconds=policy.hard_max_wall_seconds):
        raise ValueError("observation is older than the parent wall-time budget")


def _integer_counter(counters: dict[str, int | float], name: str) -> int:
    value = counters.get(name)
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ValueError(f"capacity counter {name} is invalid")
    return value


def _numeric_counter(counters: dict[str, int | float], name: str) -> float:
    value = counters.get(name)
    if not isinstance(value, (int, float)) or isinstance(value, bool) or value < 0:
        raise ValueError(f"capacity counter {name} is invalid")
    return float(value)


__all__ = ["AcquiredObservation", "CapacityCalibration", "blocked_capacity_calibration", "calibrate_capacity"]
