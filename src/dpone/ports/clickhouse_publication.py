"""Protected backend obligations for method-aware publication.

No bundled production implementation is implied by this interface. Every method
must reopen protected originals and reject stale authority; DTOs are not proof.
"""

from contextlib import AbstractContextManager
from typing import Protocol

from dpone.contracts.clickhouse_publication import (
    PublicationIntent,
    PublicationObservation,
    PublicationRecord,
    PublicationState,
)


class GuardedPublicationBackend(Protocol):
    """Durable journal, complete evidence and one-shot execution capability.

    Deployment composition supplies authenticated catalog/content readers and a
    durable CAS store behind this narrow interface. It must exclude ALL writers,
    including SQL clients, mutations and alternative framework routes. Target
    ownership survives process death and unknown outcomes. No time-based lease
    expiry may enable a successor while an old publisher can still write.
    """

    def hold(self, operation_id: str) -> AbstractContextManager[None]:
        """Resolve stable tenant/target identity and assert retained owner/epoch.

        Serialize with all publishers; leaving the context releases only the
        execution mutex, NEVER unresolved durable target ownership. Recovery
        uses original authority without source reads or new credentials.
        """

    def read(self, operation_id: str) -> PublicationRecord | None:
        """Fresh protected read; reject invalid/version-mismatched full records."""

    def observe(self, operation_id: str) -> PublicationObservation:
        """Fresh complete physical design, partition inventory and typed content.

        Bind to the original endpoint and target, pin the evidence algorithm and
        server version. Require complete visibility and no unsupported side
        effects (including dependencies, TTL, policies and concurrent mutations).
        Candidate ingestion is permanently closed before any observation used
        for planning. Identity and evidence cannot be supplied by the caller.
        """

    def prepare(self, intent: PublicationIntent) -> PublicationRecord:
        """Durably create immutable PREPARED or return exact existing PREPARED.

        Enforce operation, query ID and physical-target uniqueness transactionally.
        Retain original full bytes/epoch and history; reject divergent reuse.
        Ambiguous writes raise, never synthesize success from readback.
        """

    def claim(self, record: PublicationRecord) -> PublicationRecord | None:
        """Acknowledged PREPARED-to-CLAIMED CAS grants this invocation one dispatch.

        None is a losing CAS. An ambiguous result raises. A previous claim may
        never be returned as a fresh grant. Serialize claim with monotonic closure.
        """

    def execute_once(self, intent: PublicationIntent) -> None:
        """Dispatch the exact frozen method once, without hidden transport retry.

        Recheck owner/epoch at dispatch. Render quoted identifiers from the frozen
        subject; replace uses the canonical partition ID (or verified tuple()).
        Never accept arbitrary SQL/settings, ON CLUSTER or multiple statements.
        Noop must not dispatch. A transport exception does not imply failure.
        """

    def close_and_drain(self, intent: PublicationIntent) -> None:
        """Irreversibly close original publisher and prove complete quiescence.

        Drain delayed, queued and pre-registration requests, not merely currently
        visible queries. Serialize against claim/dispatch; close is idempotent.
        Raise if proof is missing. Once closed, this intent can never dispatch.
        """

    def resolve(
        self, record: PublicationRecord, state: PublicationState, observed: PublicationObservation
    ) -> PublicationRecord:
        """CAS exact record to resolution with immutable closure/evidence history.

        Reopen and verify closure and evidence independently. Unknown retains
        ownership. No checkpoint, cleanup or authority release is implied.
        """
