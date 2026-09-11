"""Protected at-most-once ClickHouse dispatch and non-expiring closure barrier.

Use inside the trusted gateway: worker messages never supply completion or
closure evidence directly. The gateway is responsible for persisting the real
transport observation before ACK. Unacknowledged claims are never reset/refunded.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Protocol
from uuid import UUID

from dpone.adapters.composition_clickhouse_dispatch_queries import (
    DispatchBinding,
    DispatchQueries,
    document_sha256,
    require,
    terminal_document,
)
from dpone.adapters.composition_clickhouse_transport import ClickHouseDispatchObservation
from dpone.adapters.composition_mssql_attempts import ConnectionFactory, composition_control_transaction
from dpone.adapters.composition_mssql_schema import require_control_schema
from dpone.contracts.composition_clickhouse_dispatch import ClickHouseDispatch
from dpone.contracts.composition_persistence import (
    CompositionAttemptIdentity,
    CompositionAttemptProof,
    CompositionProofAuthority,
)
from dpone.contracts.composition_snapshot import SnapshotTarget
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object
from dpone.ports.composition_sql import CompositionSqlContext


class ClickHouseDispatchAuthority(Protocol):
    """Trusted independently reopening gate/budget and closure producers.

    Callbacks execute within the store's global SQL transaction. They must not
    commit, roll back, switch cursor/session or replace that transaction. Gate
    state/budget changes use the supplied cursor and participate in its commit.
    """

    def require_dispatch(self, context: CompositionSqlContext, dispatch: ClickHouseDispatch) -> None:
        """Recheck protected ready gate, enrolled endpoint/schema/UUID, scope and budgets."""

    def observe_closed(
        self,
        context: CompositionSqlContext,
        *,
        attempt: CompositionAttemptIdentity,
        target: SnapshotTarget,
        gate_id: str,
    ) -> tuple[CompositionAttemptProof, CompositionAttemptProof]:
        """Reopen durable CLOSED_GATES/QUIESCENCE originals for this exact issued UUID.

        Independently verify principal revocation and all admitted/queued work,
        including server-side sessions and transactions. Return the same durable
        originals on replay. HTTP completion alone is not proof of either fact.
        """


class MssqlClickHouseDispatchStore:
    """Atomic claims and complete-response barriers over externally installed SQL."""

    def __init__(
        self,
        connection_factory: ConnectionFactory,
        *,
        expected_service_id: str,
        attempt: CompositionAttemptIdentity,
        target: SnapshotTarget,
        gate_id: str,
        authority: ClickHouseDispatchAuthority,
        control_schema: str = "dpone_control",
    ) -> None:
        try:
            valid = str(UUID(expected_service_id)) == expected_service_id
        except (ValueError, TypeError, AttributeError):
            valid = False
        require(valid, "control_service")
        self._factory, self._service = connection_factory, expected_service_id
        self._schema = require_control_schema(control_schema)
        self._binding = DispatchBinding(attempt, target, gate_id)
        self._authority = authority

    @contextmanager
    def _transaction(self) -> Iterator[DispatchQueries]:
        with composition_control_transaction(self._factory, self._schema, self._service) as ledger:
            queries = DispatchQueries(ledger, self._binding)
            yield queries
            queries.check()

    def claim_once(self, dispatch: ClickHouseDispatch) -> None:
        """ACK only this invocation's new durable claim; never return a replay permit.

        A driver/commit uncertainty always raises, including when the insert
        later exists. No catch/readback path can turn a lost ACK into permission.
        """
        self._binding.require_dispatch(dispatch)
        with self._transaction() as q:
            original = q.require_scope(recovery=False)
            q.require_open()
            q.require_unclaimed(dispatch)
            q.check()
            self._authority.require_dispatch(q.ledger, dispatch)
            q.check()
            require(q.require_scope(recovery=False) == original, "authority_changed_subject")
            q.require_open()
            q.require_unclaimed(dispatch)
            q.append(
                "ch_dispatches",
                "dispatch_sha256,claim_key,operation_key,gate_id,query_id,dispatch_document",
                (
                    dispatch.dispatch_sha256,
                    dispatch.claim_key,
                    dispatch.attempt.attempt_sha256,
                    self._binding.gate_id,
                    dispatch.query_id,
                    dispatch.to_bytes(),
                ),
            )
            q.require_claim(dispatch)
        # This fresh audit is reachable only after this invocation committed a
        # new claim successfully. A lost-ACK exception never reaches readback.
        with self._transaction() as q:
            require(q.require_scope(recovery=False) == original, "claim_changed_subject")
            q.require_claim(dispatch)

    def record_completed(self, dispatch: ClickHouseDispatch, observation: ClickHouseDispatchObservation) -> None:
        """Persist the real complete transport response, including during CLOSING.

        An identical completed original is readable/idempotent. A different body,
        framing, byte count or identity cannot overwrite the retained observation.
        Acknowledging this method requires an independent committed readback.
        """
        self._binding.require_dispatch(dispatch)
        document = terminal_document(dispatch, observation)
        expected = document_sha256(document), document
        with self._transaction() as q:
            q.require_scope(recovery=True)
            q.require_claim(dispatch)
            previous = q.terminal(dispatch)
            if previous is None:
                q.append(
                    "ch_dispatch_terminals",
                    "dispatch_sha256,terminal_sha256,terminal_document",
                    (dispatch.dispatch_sha256, *expected),
                )
            else:
                require(previous == expected, "terminal_replay")
            require(q.terminal(dispatch) == expected, "terminal_readback")
        with self._transaction() as q:
            q.require_scope(recovery=True)
            q.require_claim(dispatch)
            require(q.terminal(dispatch) == expected, "terminal_readback")

    def begin_closure(self) -> None:
        """Durably stop all later claims before waiting for any in-flight response."""
        document = self._binding.closing_document()
        expected = document_sha256(document), document
        with self._transaction() as q:
            q.require_scope(recovery=True)
            previous = q.phase("CLOSING")
            if previous is None:
                require(q.phase("CLOSED") is None, "closure_order")
                q.append(
                    "ch_dispatch_closures",
                    "gate_id,phase,evidence_sha256,evidence_document",
                    (self._binding.gate_id, "CLOSING", *expected),
                )
            else:
                require(previous == expected, "closing_replay")
            q.require_closing()
        with self._transaction() as q:
            q.require_scope(recovery=True)
            q.require_closing()

    def require_drained(self) -> str:
        """Seal CLOSED only after all exact original responses and protected proofs.

        Paused pre-send, partial-response and lost-ACK dispatches remain pending.
        This performs no retries, deletion, expiration or ownership release.
        Returns the hash of the durable closure original, not an execution permit.
        """
        with self._transaction() as q:
            original = q.require_scope(recovery=True)
            q.require_closing()
            links = q.drained_links()
            q.check()
            proofs = self._authority.observe_closed(
                q.ledger,
                attempt=self._binding.attempt,
                target=self._binding.target,
                gate_id=self._binding.gate_id,
            )
            q.check()
            require(q.require_scope(recovery=True) == original, "closure_changed_subject")
            require(q.drained_links() == links, "closure_changed_dispatches")
            document = self._closed_document(links, proofs)
            expected = document_sha256(document), document
            previous = q.phase("CLOSED")
            if previous is None:
                q.append(
                    "ch_dispatch_closures",
                    "gate_id,phase,evidence_sha256,evidence_document",
                    (self._binding.gate_id, "CLOSED", *expected),
                )
            else:
                require(previous == expected, "closed_replay")
            require(q.phase("CLOSED") == expected, "closed_readback")
        with self._transaction() as q:
            q.require_scope(recovery=True)
            q.require_closing()
            require(q.drained_links() == links and q.phase("CLOSED") == expected, "closed_readback")
        return expected[0]

    def _closed_document(
        self, links: tuple[tuple[str, str], ...], proofs: tuple[CompositionAttemptProof, CompositionAttemptProof]
    ) -> bytes:
        require(type(proofs) is tuple and len(proofs) == 2, "closure_proof_shape")
        issued = (CompositionProofAuthority("clickhouse", self._binding.target.service_id, self._binding.principal_id),)
        for proof, kind in zip(proofs, ("CLOSED_GATES", "QUIESCENCE"), strict=True):
            require(type(proof) is CompositionAttemptProof, "closure_proof_shape")
            proof.require_attempt(self._binding.attempt)
            require(proof.kind == kind and proof.authorities == issued, "closure_proof_subject")
        body = strict_json_object(self._binding.closing_document())
        body.update(phase="CLOSED", dispatch_terminals=links, proofs=tuple(proof.to_dict() for proof in proofs))
        document = canonical_json_bytes(body)
        require(len(document) <= 8388608, "closure_budget")
        return document
