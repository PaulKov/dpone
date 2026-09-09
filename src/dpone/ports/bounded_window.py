"""Narrow contracts for bounded-window I/O and its durable recovery journal."""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from typing import Protocol

from dpone.contracts.bounded_window import (
    ChunkReceipt,
    PublicationStatus,
    WindowChunk,
    WindowLease,
    WindowPlan,
    WindowRecord,
    WindowResult,
)


class WindowSource(Protocol):
    """Read an immutable version with isolated worker resources."""

    def validate(self, plan: WindowPlan) -> None:
        """Reject unavailable/restarted snapshots before any target writes."""

    def read(self, plan: WindowPlan, chunk: WindowChunk) -> Iterator[tuple[object, ...]]:
        """Read exactly [start,end); close cursor in generator finally on cancel."""


class WindowTarget(Protocol):
    """Thread-safe isolated staging and one atomic publication capability.

    All mutation methods enforce fencing, including writers outside dpone.
    A local journal lease alone cannot prove exclusion of external writers.
    """

    def validate(self, plan: WindowPlan) -> None:
        """Preflight topology, atomicity, resources, schema and writer exclusion."""

    def stage(
        self,
        plan: WindowPlan,
        chunk: WindowChunk,
        attempt_id: str,
        rows: Iterator[tuple[object, ...]],
        lease: WindowLease,
    ) -> ChunkReceipt:
        """Bound buffers, stage once and independently verify typed row parity."""

    def inspect_attempt(
        self, plan: WindowPlan, chunk: WindowChunk, attempt_id: str, lease: WindowLease
    ) -> ChunkReceipt | None:
        """Return only durable verified parity; None means not verified."""

    def discard_attempt(self, plan: WindowPlan, chunk: WindowChunk, attempt_id: str, lease: WindowLease) -> None:
        """Idempotently fence/join old writer and confirm isolated staging removal."""

    def prepare(self, plan: WindowPlan, receipts: Sequence[ChunkReceipt], lease: WindowLease) -> str:
        """Build/verify idempotent generation, retaining outside-window and NULL rows."""

    def publish(self, plan: WindowPlan, generation: str, lease: WindowLease) -> None:
        """Perform one atomic publication; never automatically retry internally."""

    def inspect_publication(self, plan: WindowPlan, generation: str) -> PublicationStatus:
        """Prove observed generation identity; ambiguity returns unknown."""


class WindowStore(Protocol):
    """Durable CAS journal and monotonically fenced target lease."""

    def acquire(self, target_id: str, owner: str, ttl: float) -> WindowLease:
        """Acquire only absent/expired lease; even same owner cannot overlap."""

    def assert_lease(self, lease: WindowLease) -> None:
        """Reject expired, released, or stale epochs."""

    def renew(self, lease: WindowLease, ttl: float) -> None:
        """Extend an unexpired owned epoch; never renew a released epoch.

        Implementations must bound I/O duration to at most 30 seconds. Renewal
        and release serialize atomically; a late renewal cannot resurrect a lease.
        """

    def release(self, lease: WindowLease) -> None:
        """Release only this owner/epoch; retain monotonic fencing history."""

    def load(self, key: str) -> WindowRecord | None:
        """Read the last committed record."""

    def save(self, key: str, expected: int | None, payload: str, lease: WindowLease) -> WindowRecord:
        """Atomically assert unexpired lease and compare revision before update."""


class WindowProgressJournal(Protocol):
    """Fenced progress projection; storage implementation is composition-owned."""

    def read(self, name: str) -> dict[str, object] | None:
        """Read a validated version-one progress record."""

    def write(self, name: str, data: dict[str, object]) -> None:
        """Persist a validated phase with CAS and current writer authority."""


class WindowExecutor(Protocol):
    """Application capability used by interval composition without a concrete runner."""

    def execute_leased(self, plan: WindowPlan, lease: WindowLease) -> WindowResult:
        """Complete or reconcile the immutable plan under caller-owned fencing."""
