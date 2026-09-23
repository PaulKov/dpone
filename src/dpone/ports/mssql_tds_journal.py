"""Observation and deadline-bounded authority for durable TDS attempt state."""

from typing import Protocol

from dpone.contracts.mssql_tds_api import TdsAttemptIdentity, TdsAttemptPhase, TdsAttemptSnapshot, TdsLifecycleEvent
from dpone.contracts.mssql_tds_worker import ParentAuthority as ParentAuthority
from dpone.contracts.mssql_tds_worker import ParentRetirementRequired as ParentRetirementRequired
from dpone.contracts.mssql_tds_worker import Retired as Retired

__all__ = (
    "ParentRetirementRequired",
    "ParentAuthority",
    "Retired",
    "TdsAttemptObserver",
    "TdsAttemptWriter",
    "TdsJournalGateway",
)


class TdsAttemptObserver(Protocol):
    """A parent may inspect attempt state without inheriting mutation authority."""

    def read(self, identity: TdsAttemptIdentity) -> TdsAttemptSnapshot | None:
        """Return validated immutable state, rejecting changed invocation bindings."""


class TdsAttemptWriter(Protocol):
    """One supervisor thread owns its last acknowledged CAS revision.

    A failed/unknown save permanently invalidates this handle. SQL exclusion,
    process containment, and permission to retire targets remain separate
    capabilities; a journal check does not close an external check/effect race.
    """

    @property
    def snapshot(self) -> TdsAttemptSnapshot:
        """The immutable last acknowledged state; reading grants no authority."""

    def assert_authority(self) -> None:
        """Check current lease/revision before new effects; fail closed on uncertainty."""

    def advance(self, event: TdsLifecycleEvent, *, expected_phase: TdsAttemptPhase) -> TdsAttemptSnapshot:
        """Durably advance one typed event before issuing its dependent effect."""


class TdsJournalGateway(Protocol):
    """Deadline-bounded access to an actor-owned journal writer.

    A timeout permanently poisons the gateway, without cancelling an in-flight
    backend write. Late commits require reconciliation under a newer fence.
    The process owner never performs backend I/O through this port.
    """

    @property
    def snapshot(self) -> TdsAttemptSnapshot:
        """Return only the immutable last acknowledged local snapshot."""

    def assert_authority(self, *, deadline: float) -> None:
        """Await authority acknowledgement within an absolute monotonic deadline."""

    def advance(
        self, event: TdsLifecycleEvent, *, expected_phase: TdsAttemptPhase, deadline: float
    ) -> TdsAttemptSnapshot:
        """Await one durable transition; an uncertain response forbids reuse."""

    def close(self, *, deadline: float) -> None:
        """Bound teardown waiting; a live actor continues consuming run capacity."""
