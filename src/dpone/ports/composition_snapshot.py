"""Protected capabilities required by whole-snapshot publication.

Implementations are injected by the composition root. A DTO, a caller digest,
method presence or a mock is never evidence that these obligations are met.
"""

from typing import Protocol

from dpone.contracts.composition_activation import CompositionActivationOccurrence
from dpone.contracts.composition_attempt import CompositionAttemptIdentity
from dpone.contracts.composition_snapshot import (
    SnapshotCatalogObservation,
    SnapshotPublicationIntent,
    SnapshotPublicationRecord,
    SnapshotPublisherClosure,
)


class SnapshotPublicationAuthority(Protocol):
    """Reopen protected originals, exact owner/epochs and immutable issued users.

    There is deliberately no issue/re-enable operation here. Ordinary bindings
    cannot supply writer credentials or assert the protected service identity.
    """

    def load_prepared(self, attempt: CompositionAttemptIdentity, generation_ref: str) -> SnapshotPublicationIntent:
        """Verify original full source snapshot, generation, budgets and closed ingest.

        Reopen source/generation evidence and compare complete bytes, including
        typed content parity. Prove ingest reconnect closure and quiescence for
        its exact journaled user UUID before inspecting the sealed generation.
        The reference is SnapshotGeneration.record_sha256, never a caller plan.
        """

    def require_current(self, intent: SnapshotPublicationIntent, *, recovery: bool) -> CompositionActivationOccurrence:
        """Verify complete parent/plan/physical enrollment, scope and retained owner.

        Writes require ACTIVE and a RUNNING admitted attempt. Recovery accepts
        ACTIVE/RETIRING with the original attempt and every exact retained epoch,
        including COMMIT_UNKNOWN attempts. Neither path transfers ownership.
        Recheck actual issued-user UUIDs, gate state, endpoint/version, exclusive
        writer policy and source/generation evidence. A stale process cannot
        authorize itself by copying an occurrence or receipt.
        """

    def close_publisher(self, intent: SnapshotPublicationIntent) -> SnapshotPublisherClosure:
        """Irreversibly close the exact publisher and prove server quiescence.

        Serialize closure with authentication/dispatch; drain accepted, delayed
        and queued work, including pre-process-registration HTTP/DDL requests.
        Independently reopen full protected issuance/proof records. A disabled
        name, timeout, process exit or one empty sample is insufficient. Closing
        is monotonic and idempotent; unavailable proof raises and retains scope.
        """


class SnapshotPublicationStore(Protocol):
    """Protected immutable intent and exact state CAS; no expiring reservations.

    Enforce unique (exact attempt, target write subject) and exchange_query_id,
    rejecting refingerprinted intents for the same attempt/slot. Preserve full
    canonical bytes and immutable transition history. Worker principals cannot
    modify it. Each mutation rechecks exact parent/owner/epochs in its protected
    transaction. SQL/ClickHouse effects are not a distributed transaction.
    """

    def prepare(self, intent: SnapshotPublicationIntent) -> SnapshotPublicationRecord:
        """Acknowledge exact PREPARED insertion or identical existing PREPARED only."""

    def read(self, intent_sha256: str) -> SnapshotPublicationRecord | None:
        """Independently reopen complete canonical intent/state on a fresh connection."""

    def claim_exchange(self, expected: SnapshotPublicationRecord) -> SnapshotPublicationRecord | None:
        """Atomic exact PREPARED→EXCHANGE_INTENT CAS; acknowledge the winner once.

        Compare complete expected bytes and revision, ACTIVE/RUNNING parent
        authority and ready publisher gate. None denotes a losing CAS. Any
        commit-ack uncertainty raises, even if later readback sees a claim.
        Failure does not assert that storage persisted anything: retain parent,
        attempt and gate ownership pending exact reconciliation. Never return a
        prior winner's claim as a fresh success, and never blindly replay DML.
        """

    def resolve(
        self,
        expected: SnapshotPublicationRecord,
        *,
        state: str,
        closure: SnapshotPublisherClosure | None,
        observation: SnapshotCatalogObservation | None,
    ) -> SnapshotPublicationRecord:
        """CAS exact expected bytes to its validated transition with full evidence.

        Verify closure from protected originals for resolved outcomes; closure
        prevents later claim/dispatch. PREPARED may resolve NOT_PUBLISHED after
        explicit closure, but never PUBLISHED. Unknown outcomes remain blocking;
        a publication record neither commits checkpoints nor releases authority.
        """


class ClickHouseSnapshotCatalog(Protocol):
    """Authenticated fresh physical observation with complete catalog visibility."""

    def inspect(self, intent: SnapshotPublicationIntent) -> SnapshotCatalogObservation:
        """Observe both names together, exact protected service/database and B data.

        Recheck both schemas/designs and complete side effects: incoming/outgoing
        materialized views across databases, TTL, projections, computed/default
        columns, row policies, mutations, codecs/indices/constraints, remote or
        replicated topology and ambient writer grants. Include all retained
        bytes. B content is independently reconciled by the pinned typed
        algorithm. Missing rows/NULL byte observations never become zero. No
        cached or partial-visibility observation may satisfy this capability.
        """


class ClickHouseSnapshotExecutor(Protocol):
    """One fixed EXCHANGE through the protected issued publisher, no retry."""

    def exchange_once(self, intent: SnapshotPublicationIntent) -> None:
        """Render one exact database/target/generation pair and exchange_query_id.

        Reject arbitrary SQL/settings, ON CLUSTER, multi-pair statements,
        failover, sessions, hidden transport retries and replacement queries.
        Use fresh protected HTTP; enforce issuer/closure serialization at the
        actual dispatch boundary. No credential is returned, logged or stored
        in these records. Any ambiguous acknowledgement requires reconciliation.
        """
