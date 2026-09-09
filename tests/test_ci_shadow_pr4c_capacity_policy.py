from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from dpone.contracts.ci_shadow_reconciliation import (
    RECONCILIATION_CAPACITY_CALIBRATION_ONLY,
    RECONCILIATION_PR4C_IMPLEMENTATION_BLOCKED,
    CapacityUsage,
    ReconciliationPolicyV1,
    ReconciliationPolicyV2,
    capacity_code,
)


def test_v1_policy_preserves_its_historical_contract() -> None:
    policy = ReconciliationPolicyV1.fixed()

    interval = policy.interval_for(datetime(2026, 8, 27, 3, 15, 0, 999_999, tzinfo=UTC))

    assert interval.scan_from == datetime(2026, 8, 13, 2, 45, tzinfo=UTC)
    assert interval.safe_scan_through == datetime(2026, 8, 27, 2, 45, tzinfo=UTC)
    assert interval.observation_started_at == datetime(2026, 8, 27, 3, 15, tzinfo=UTC)
    assert interval.safe_scan_through - interval.scan_from == timedelta(days=14)
    assert policy.producer_workflow_id == 343_714_753
    assert policy.auditor_workflow_id == 343_909_056
    assert policy.hard_max_http_requests == 800
    assert policy.approval_thresholds.total_http_requests == 400
    assert policy.policy_schema == "dpone.ci-shadow-reconciliation-policy.v1"
    assert policy.capacity_schema == "dpone.ci-shadow-reconciliation-capacity.v1"
    assert policy.sha256.startswith("sha256:")
    assert policy.canonical_bytes == ReconciliationPolicyV1.fixed().canonical_bytes


def test_v2_policy_is_fixed_and_has_its_own_canonical_evidence_contract() -> None:
    policy = ReconciliationPolicyV2.fixed()

    assert policy.hard_max_http_requests == 3_000
    assert policy.approval_thresholds.total_http_requests == 1_500
    assert policy.policy_schema == "dpone.ci-shadow-reconciliation-policy.v2"
    assert policy.capacity_schema == "dpone.ci-shadow-reconciliation-capacity.v2"
    assert policy.sha256 != ReconciliationPolicyV1.fixed().sha256


def test_policy_rejects_non_utc_and_mutable_parent_limits() -> None:
    with pytest.raises(ValueError, match="UTC-aware"):
        ReconciliationPolicyV1.fixed().interval_for(datetime(2026, 8, 27, 3, 15))

    with pytest.raises(ValueError, match="hard maxima"):
        ReconciliationPolicyV1(
            history_window_days=14,
            grace_minutes=30,
            hard_max_http_requests=801,
            hard_max_response_bytes=134_217_728,
            hard_max_wall_seconds=900,
        )


@pytest.mark.parametrize(
    ("usage", "complete", "equal", "expected"),
    [
        (CapacityUsage(1_500, 67_108_864, 450), True, True, RECONCILIATION_CAPACITY_CALIBRATION_ONLY),
        (CapacityUsage(1_501, 67_108_864, 450), True, True, RECONCILIATION_PR4C_IMPLEMENTATION_BLOCKED),
        (CapacityUsage(1_500, 67_108_865, 450), True, True, RECONCILIATION_PR4C_IMPLEMENTATION_BLOCKED),
        (CapacityUsage(1_500, 67_108_864, 450.000_001), True, True, RECONCILIATION_PR4C_IMPLEMENTATION_BLOCKED),
        (CapacityUsage(1, 1, 1), False, True, RECONCILIATION_PR4C_IMPLEMENTATION_BLOCKED),
        (CapacityUsage(1, 1, 1), True, False, RECONCILIATION_PR4C_IMPLEMENTATION_BLOCKED),
    ],
)
def test_capacity_code_is_always_unverified_and_qualifies_only_at_half_maxima(
    usage: CapacityUsage, complete: bool, equal: bool, expected: str
) -> None:
    assert (
        capacity_code(usage, complete=complete, observations_match=equal, policy=ReconciliationPolicyV2.fixed())
        == expected
    )
