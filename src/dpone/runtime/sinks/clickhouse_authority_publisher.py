"""Grant-consuming publisher and source-free conservative transport closure.

This service is not the complete guarded backend: observations, writer sealing,
outcome resolution, checkpointing and owner release belong to later composition.
"""

from __future__ import annotations

import hashlib

from dpone.contracts.clickhouse_authority import DispatchGrant, OperationBinding, TransportState
from dpone.contracts.clickhouse_native_publication import NativePublicationCompletion, NativePublicationRequest
from dpone.contracts.clickhouse_publication import JournalEntry, PublicationState, choose_publication
from dpone.ports.clickhouse_publication_transport import (
    NativePublicationTransport,
    PublicationAuthority,
    PublicationExclusion,
)
from dpone.runtime.sinks.clickhouse_guarded_publication import PublicationUnknown


class AuthorityPublicationPublisher:
    """Serialize dispatch and closure on the same protected local authority."""

    def __init__(
        self, authority: PublicationAuthority, exclusion: PublicationExclusion, transport: NativePublicationTransport
    ) -> None:
        self._authority, self._exclusion, self._transport = authority, exclusion, transport

    def _original(self, operation_id: str) -> tuple[OperationBinding, JournalEntry]:
        binding = self._authority.binding(operation_id)
        entry = self._authority.read(operation_id)
        if entry is None:
            raise PublicationUnknown("Original prepared publication is unavailable")
        intent, subject = entry.record.intent, binding.subject
        if (
            binding.operation_id != operation_id
            or intent.operation_id != operation_id
            or intent.before.subject != (subject.server_id, subject.database, subject.target, binding.candidate)
            or choose_publication(operation_id, intent.before) != intent
        ):
            raise PublicationUnknown("Original publication identity or selection differs")
        return binding, entry

    def execute_once(self, grant: DispatchGrant) -> None:
        """Consume the original grant once; ambiguous ACK never grants a retry."""
        try:
            with self._exclusion.hold(grant.operation_id) as session:
                binding, entry = self._original(grant.operation_id)
                if (
                    binding.epoch != grant.epoch
                    or entry.record.state != PublicationState.CLAIMED
                    or not entry.record.claim_granted
                ):
                    raise PublicationUnknown("No current acknowledged publication claim")
                request = NativePublicationRequest(binding, entry.record.intent)
                session.assert_current()
                if request.statement is None:
                    self._authority.close_without_send(grant.operation_id)
                    return
                self._authority.begin_send(grant)
                # The synchronous call is unreachable after a lost send-entry ACK.
                # In particular, reopening MAY_HAVE_SENT cannot authorize it.
                completion = self._transport.execute(request)
                session.assert_current()
                self._validate_completion(request, completion)
                self._authority.record_terminal(grant, completion.digest)
        except Exception:
            raise PublicationUnknown(
                "Publication is unresolved; inspect original authority, never replay SQL"
            ) from None

    @staticmethod
    def _validate_completion(request: NativePublicationRequest, completion: NativePublicationCompletion) -> None:
        if (
            not isinstance(completion, NativePublicationCompletion)
            or completion.operation_id != request.intent.operation_id
            or completion.query_id != request.intent.query_id
            or completion.server_id != request.binding.subject.server_id
            or completion.statement_digest != hashlib.sha256((request.statement or "").encode("utf-8")).hexdigest()
            or completion.driver_version != "0.2.10"
            or completion.server_version != (24, 8, 14)
        ):
            raise PublicationUnknown("Native completion does not bind the original request")

    def close_and_drain(self, operation_id: str) -> None:
        """Close only provably unsent or already durably terminal original work."""
        try:
            with self._exclusion.hold(operation_id) as session:
                self._original(operation_id)
                state = self._authority.transport_state(operation_id)
                session.assert_current()
                if state == TransportState.NOT_STARTED:
                    self._authority.close_without_send(operation_id)
                elif state not in (TransportState.CLOSED_WITHOUT_SEND, TransportState.CLOSED_TERMINAL):
                    raise PublicationUnknown("Possibly sent publication has no durable terminal completion")
        except Exception:
            raise PublicationUnknown("Transport closure is unproven; retain original owner and resources") from None
