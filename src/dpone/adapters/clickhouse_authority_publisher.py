"""Concrete authority/native transport bridge with conservative closure.

This service is not the complete guarded backend: observations, writer sealing,
outcome resolution, checkpointing and owner release belong to later composition.
"""

from __future__ import annotations

import hashlib
from typing import Protocol

from dpone.contracts import clickhouse_authority as authority_contract
from dpone.contracts import clickhouse_publication as publication
from dpone.contracts.clickhouse_publication import PublicationUnknown
from dpone.ports import clickhouse_publication_transport as native
from dpone.ports.clickhouse_publication_exclusion import PublicationExclusion


class PublicationAuthority(Protocol):
    """Consumer-owned persistence needs; no native or execution-lock policy."""

    def binding(self, operation_id: str) -> authority_contract.OperationBinding: ...
    def read(self, operation_id: str) -> publication.JournalEntry | None: ...
    def transport_state(self, operation_id: str) -> authority_contract.TransportState: ...
    def begin_send(self, grant: authority_contract.DispatchGrant) -> None: ...
    def close_without_send(self, operation_id: str) -> None: ...
    def record_terminal(self, grant: authority_contract.DispatchGrant, completion_digest: str) -> None: ...


class AuthorityPublicationPublisher:
    """Serialize dispatch and closure on the same protected local authority."""

    def __init__(
        self,
        authority: PublicationAuthority,
        exclusion: PublicationExclusion,
        transport: native.NativePublicationTransport,
    ) -> None:
        self._authority, self._exclusion, self._transport = authority, exclusion, transport

    def _original(self, operation_id: str) -> tuple[authority_contract.OperationBinding, publication.JournalEntry]:
        binding = self._authority.binding(operation_id)
        entry = self._authority.read(operation_id)
        if entry is None:
            raise PublicationUnknown("Original prepared publication is unavailable")
        intent, subject = entry.record.intent, binding.subject
        if (
            binding.operation_id != operation_id
            or intent.operation_id != operation_id
            or intent.before.subject != (subject.server_id, subject.database, subject.target, binding.candidate)
            or publication.choose_publication(operation_id, intent.before) != intent
        ):
            raise PublicationUnknown("Original publication identity or selection differs")
        return binding, entry

    def execute_once(self, grant: authority_contract.DispatchGrant) -> None:
        """Consume the original grant once; ambiguous ACK never grants a retry."""
        try:
            with self._exclusion.hold(grant.operation_id) as session:
                binding, entry = self._original(grant.operation_id)
                if (
                    binding.epoch != grant.epoch
                    or entry.record.state != publication.PublicationState.CLAIMED
                    or not entry.record.claim_granted
                ):
                    raise PublicationUnknown("No current acknowledged publication claim")
                intent = entry.record.intent
                request = native.NativePublicationRequest(binding, intent.method, intent.query_id, intent.partition_id)
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
    def _validate_completion(
        request: native.NativePublicationRequest, completion: native.NativePublicationCompletion
    ) -> None:
        if (
            not isinstance(completion, native.NativePublicationCompletion)
            or completion.operation_id != request.binding.operation_id
            or completion.query_id != request.query_id
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
                if state == authority_contract.TransportState.NOT_STARTED:
                    self._authority.close_without_send(operation_id)
                elif state not in (
                    authority_contract.TransportState.CLOSED_WITHOUT_SEND,
                    authority_contract.TransportState.CLOSED_TERMINAL,
                ):
                    raise PublicationUnknown("Possibly sent publication has no durable terminal completion")
        except Exception:
            raise PublicationUnknown("Transport closure is unproven; retain original owner and resources") from None
