"""Immutable policy and pure decisions for CI-shadow reconciliation evidence."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import ClassVar, Final, TypeAlias, TypeVar

RECONCILIATION_CAPACITY_CALIBRATION_ONLY: Final = "RECONCILIATION_CAPACITY_CALIBRATION_ONLY"
RECONCILIATION_PR4C_IMPLEMENTATION_BLOCKED: Final = "RECONCILIATION_PR4C_IMPLEMENTATION_BLOCKED"
CAPACITY_DECISION: Final = "UNVERIFIED"

_PolicyT = TypeVar("_PolicyT", bound="_FixedReconciliationPolicy")

_HARD_MAX_RESPONSE_BYTES: Final = 134_217_728
_HARD_MAX_WALL_SECONDS: Final = 900
_HISTORY_WINDOW_DAYS: Final = 14
_GRACE_MINUTES: Final = 30
_PRODUCER_WORKFLOW_ID: Final = 343_714_753
_AUDITOR_WORKFLOW_ID: Final = 343_909_056


@dataclass(frozen=True)
class ObservationInterval:
    """One exact closed UTC-second provider-observable observation interval."""

    scan_from: datetime
    safe_scan_through: datetime
    observation_started_at: datetime


@dataclass(frozen=True)
class AcquiredObservation:
    """One complete canonical observation, excluding mutable request counters."""

    canonical_bytes: bytes
    complete: bool
    observation_started_at: datetime
    evidence_observed_through: datetime

    @property
    def digest(self) -> str:
        """Return the canonical snapshot identity used only for equality checks."""

        return "sha256:" + hashlib.sha256(self.canonical_bytes).hexdigest()


@dataclass(frozen=True)
class CapacityUsage:
    """The three parent-owned counters relevant to calibration eligibility."""

    total_http_requests: int
    total_response_body_bytes: int
    execution_wall_seconds: float


@dataclass(frozen=True)
class _FixedReconciliationPolicy:
    """Shared implementation for immutable, versioned reconciliation policies."""

    history_window_days: int
    grace_minutes: int
    hard_max_http_requests: int
    hard_max_response_bytes: int
    hard_max_wall_seconds: int

    _hard_max_http_requests: ClassVar[int]
    _hard_max_response_bytes: ClassVar[int]
    _hard_max_wall_seconds: ClassVar[int]
    _policy_schema: ClassVar[str]
    _capacity_schema: ClassVar[str]

    def __post_init__(self) -> None:
        expected = self._expected_values
        actual = (
            self.history_window_days,
            self.grace_minutes,
            self.hard_max_http_requests,
            self.hard_max_response_bytes,
            self.hard_max_wall_seconds,
        )
        if actual != expected:
            raise ValueError(f"{type(self).__name__} hard maxima and interval are fixed by its approved contract")

    @classmethod
    def fixed(cls: type[_PolicyT]) -> _PolicyT:
        """Return the only supported policy without reading a runtime YAML file."""

        return cls(
            history_window_days=_HISTORY_WINDOW_DAYS,
            grace_minutes=_GRACE_MINUTES,
            hard_max_http_requests=cls._hard_max_http_requests,
            hard_max_response_bytes=cls._hard_max_response_bytes,
            hard_max_wall_seconds=cls._hard_max_wall_seconds,
        )

    @property
    def approval_thresholds(self) -> CapacityUsage:
        """Return the required two-times safety-factor thresholds."""

        return CapacityUsage(
            total_http_requests=self.hard_max_http_requests // 2,
            total_response_body_bytes=self.hard_max_response_bytes // 2,
            execution_wall_seconds=self.hard_max_wall_seconds / 2,
        )

    @property
    def producer_workflow_id(self) -> int:
        """Return the repository-scoped immutable Actions workflow identity."""

        return _PRODUCER_WORKFLOW_ID

    @property
    def auditor_workflow_id(self) -> int:
        """Return the repository-scoped immutable shadow-auditor identity."""

        return _AUDITOR_WORKFLOW_ID

    @property
    def canonical_bytes(self) -> bytes:
        """Return versioned canonical bytes that bind calibration to policy."""

        value = {
            "grace_minutes": self.grace_minutes,
            "hard_max_http_requests": self.hard_max_http_requests,
            "hard_max_response_bytes": self.hard_max_response_bytes,
            "hard_max_wall_seconds": self.hard_max_wall_seconds,
            "history_window_days": self.history_window_days,
            "producer_workflow_id": self.producer_workflow_id,
            "auditor_workflow_id": self.auditor_workflow_id,
            "schema": self.policy_schema,
        }
        return json.dumps(value, ensure_ascii=True, separators=(",", ":"), sort_keys=True).encode("ascii")

    @property
    def sha256(self) -> str:
        """Return the domain-independent digest published in capacity evidence."""

        return "sha256:" + hashlib.sha256(self.canonical_bytes).hexdigest()

    def interval_for(self, observed_at: datetime) -> ObservationInterval:
        """Derive the approved interval from one trusted UTC clock observation."""

        if observed_at.tzinfo is None or observed_at.utcoffset() != timedelta(0):
            raise ValueError("observation time must be UTC-aware")
        # ``datetime.UTC`` is unavailable in the repository's mypy target despite
        # being present at supported runtime versions.
        observed_second = observed_at.astimezone(timezone.utc).replace(microsecond=0)  # noqa: UP017
        safe_scan_through = observed_second - timedelta(minutes=self.grace_minutes)
        return ObservationInterval(
            scan_from=safe_scan_through - timedelta(days=self.history_window_days),
            safe_scan_through=safe_scan_through,
            observation_started_at=observed_second,
        )

    @property
    def _expected_values(self) -> tuple[int, int, int, int, int]:
        return (
            _HISTORY_WINDOW_DAYS,
            _GRACE_MINUTES,
            self._hard_max_http_requests,
            self._hard_max_response_bytes,
            self._hard_max_wall_seconds,
        )

    @property
    def policy_schema(self) -> str:
        """Return the immutable policy schema bound into evidence digests."""

        return self._policy_schema

    @property
    def capacity_schema(self) -> str:
        """Return the receipt schema emitted by this policy version."""

        return self._capacity_schema


class ReconciliationPolicyV1(_FixedReconciliationPolicy):
    """Historical PR4C policy; its values and canonical bytes are immutable."""

    _hard_max_http_requests: ClassVar[int] = 800
    _hard_max_response_bytes: ClassVar[int] = _HARD_MAX_RESPONSE_BYTES
    _hard_max_wall_seconds: ClassVar[int] = _HARD_MAX_WALL_SECONDS
    _policy_schema: ClassVar[str] = "dpone.ci-shadow-reconciliation-policy.v1"
    _capacity_schema: ClassVar[str] = "dpone.ci-shadow-reconciliation-capacity.v1"


class ReconciliationPolicyV2(_FixedReconciliationPolicy):
    """Successor PR4C policy with the separately approved request budget."""

    _hard_max_http_requests: ClassVar[int] = 3_000
    _hard_max_response_bytes: ClassVar[int] = _HARD_MAX_RESPONSE_BYTES
    _hard_max_wall_seconds: ClassVar[int] = _HARD_MAX_WALL_SECONDS
    _policy_schema: ClassVar[str] = "dpone.ci-shadow-reconciliation-policy.v2"
    _capacity_schema: ClassVar[str] = "dpone.ci-shadow-reconciliation-capacity.v2"


ReconciliationPolicy: TypeAlias = ReconciliationPolicyV1 | ReconciliationPolicyV2


def capacity_code(
    usage: CapacityUsage,
    *,
    complete: bool,
    observations_match: bool,
    policy: ReconciliationPolicy | None = None,
) -> str:
    """Return the only two capacity codes; neither result has PASS authority."""

    thresholds = (policy or ReconciliationPolicyV1.fixed()).approval_thresholds
    if (
        complete
        and observations_match
        and usage.total_http_requests <= thresholds.total_http_requests
        and usage.total_response_body_bytes <= thresholds.total_response_body_bytes
        and usage.execution_wall_seconds <= thresholds.execution_wall_seconds
    ):
        return RECONCILIATION_CAPACITY_CALIBRATION_ONLY
    return RECONCILIATION_PR4C_IMPLEMENTATION_BLOCKED


__all__ = [
    "CAPACITY_DECISION",
    "RECONCILIATION_CAPACITY_CALIBRATION_ONLY",
    "RECONCILIATION_PR4C_IMPLEMENTATION_BLOCKED",
    "AcquiredObservation",
    "CapacityUsage",
    "ObservationInterval",
    "ReconciliationPolicy",
    "ReconciliationPolicyV1",
    "ReconciliationPolicyV2",
    "capacity_code",
]
