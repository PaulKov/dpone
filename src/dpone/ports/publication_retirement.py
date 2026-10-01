"""Trusted observation and SQL-history capabilities for guarded retirement."""

from __future__ import annotations

from contextlib import AbstractContextManager
from typing import TYPE_CHECKING, Literal, Protocol

from dpone.contracts.publication_retirement import plan_retirement as plan_retirement
from dpone.contracts.publication_retirement import require_retirement_history as require_retirement_history
from dpone.contracts.publication_retirement import retirement_record as retirement_record
from dpone.contracts.publication_retirement_codec import decode_retirement_plan as decode_retirement_plan

if TYPE_CHECKING:
    from dpone.contracts.publication_retirement import PublicationRetirementObservation, PublicationRetirementPlan

RetirementInspection = Literal["absent", "exact", "conflict", "unknown"]
RetirementWrite = Literal["acknowledged", "conflict", "unknown"]


class PublicationRetirementAttempts(Protocol):
    """Durably claim once before SQL; never reclaim after crash/unknown ACK.

    Composition must bind all operator invocations to the same persistent
    journal. A process-local set is only a test double, not an admitted adapter.
    """

    def claim(self, operation_key: str) -> bool: ...


class PublicationRetirementAttemptJournal(PublicationRetirementAttempts, Protocol):
    """Pre-admitted persistent attempts, bound into an operator plan.

    Local identity alone cannot certify persistence or exclusion. Deployment
    must admit this capability once for every runner; paths cannot be chosen
    by an operator to reclaim an unknown attempt.
    """

    def require_ready(self) -> str:
        """Recheck readiness and return the canonical directory identity digest."""
        ...


class HeldRetirementObservation(Protocol):
    """Authenticate observations while deployment owns exclusion of all writers.

    No implementation may manufacture completeness from an empty current DDL
    queue, a paused scheduler, or user-supplied JSON. Capture original-operation
    history from admitted sources and fail if retention/coverage is uncertain.
    """

    def observe(self) -> PublicationRetirementObservation: ...
    def require_held(self) -> None: ...


class PublicationRetirementObserver(Protocol):
    """Hold an already admitted deployment freeze; never silently unfreeze it.

    Exiting the local context releases observation resources only. Deployment
    owns exclusion through readback and installation of the single new binding.
    """

    def hold(self) -> AbstractContextManager[HeldRetirementObservation]: ...


class PublicationRetirementStore(Protocol):
    """Exact immutable retired-slot/event operations, not native create/CAS.

    ``inspect`` validates full payload, origin, plan bindings and event history.
    A hash match alone is not exact readback. ``retire_if_absent`` executes one
    transaction with no retry; output before commit ACK is never acknowledged.
    Neither method mutates a ClickHouse target/candidate or issues a permit.
    """

    def inspect(self, plan: PublicationRetirementPlan) -> RetirementInspection: ...
    def retire_if_absent(self, plan: PublicationRetirementPlan) -> RetirementWrite: ...
