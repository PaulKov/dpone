from __future__ import annotations

from datetime import UTC, datetime

import pytest

from dpone.contracts.ci_shadow_reconciliation import RECONCILIATION_CAPACITY_CALIBRATION_ONLY, ReconciliationPolicyV1
from dpone.services.ci.shadow_capacity import AcquiredObservation, blocked_capacity_calibration, calibrate_capacity
from dpone.services.ci.shadow_reconciliation_budget import RequestBudget


def test_capacity_service_performs_exactly_two_reads_then_returns_unverified_measurement() -> None:
    calls = 0

    def acquire() -> AcquiredObservation:
        nonlocal calls
        calls += 1
        return AcquiredObservation(
            b"same snapshot", True, datetime(2026, 8, 27, 3, 10, tzinfo=UTC), datetime(2026, 8, 27, 3, 15, tzinfo=UTC)
        )

    policy = ReconciliationPolicyV1.fixed()
    result = calibrate_capacity(
        acquire,
        budget=RequestBudget(policy=policy, monotonic_clock=lambda: 0.0),
        policy=policy,
        utc_clock=lambda: datetime(2026, 8, 27, 3, 15, tzinfo=UTC),
    )

    assert calls == 2
    assert result.decision == "UNVERIFIED"
    assert result.code == RECONCILIATION_CAPACITY_CALIBRATION_ONLY
    assert result.observations_match is True


def test_capacity_service_blocks_unequal_snapshots_without_a_third_read() -> None:
    records = iter([b"first", b"second"])
    policy = ReconciliationPolicyV1.fixed()

    result = calibrate_capacity(
        lambda: AcquiredObservation(
            next(records), True, datetime(2026, 8, 27, 3, 10, tzinfo=UTC), datetime(2026, 8, 27, 3, 15, tzinfo=UTC)
        ),
        budget=RequestBudget(policy=policy, monotonic_clock=lambda: 0.0),
        policy=policy,
        utc_clock=lambda: datetime(2026, 8, 27, 3, 15, tzinfo=UTC),
    )

    assert result.complete is True
    assert result.observations_match is False
    assert result.code == "RECONCILIATION_PR4C_IMPLEMENTATION_BLOCKED"


def test_capacity_service_rejects_stale_observation() -> None:
    policy = ReconciliationPolicyV1.fixed()
    stale = datetime(2026, 8, 27, 3, 0, tzinfo=UTC)

    with pytest.raises(ValueError, match="older"):
        calibrate_capacity(
            lambda: AcquiredObservation(b"same", True, stale, stale),
            budget=RequestBudget(policy=policy, monotonic_clock=lambda: 0.0),
            policy=policy,
            utc_clock=lambda: datetime(2026, 8, 27, 3, 15, 1, tzinfo=UTC),
        )


def test_blocked_capacity_calibration_retains_only_saturated_counters_and_no_snapshot_identity() -> None:
    policy = ReconciliationPolicyV1.fixed()
    result = blocked_capacity_calibration(
        budget=RequestBudget(policy=policy, monotonic_clock=lambda: 0.0),
        observation_started_at=datetime(2026, 8, 27, 3, 10, tzinfo=UTC),
        utc_clock=lambda: datetime(2026, 8, 27, 3, 15, tzinfo=UTC),
    )

    assert result.complete is False
    assert result.observations_match is False
    assert result.code == "RECONCILIATION_PR4C_IMPLEMENTATION_BLOCKED"
    assert result.first_observation_digest == "sha256:" + "0" * 64
