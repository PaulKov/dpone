"""Per-attempt ClickHouse issuance and durable sole-dispatcher closure.

Only the trusted supervisor receives credentials. Unknown create/enable or SQL
ACKs never reissue them. Incomplete ENABLING remains blocking: an old delayed
administrator request could otherwise reopen a disabled principal. Recovery
requires separately approved supervisor teardown/reconciliation (ADR 0063).
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from secrets import token_urlsafe
from typing import Protocol

from dpone.adapters.composition_clickhouse_dispatch_queries import (
    DispatchBinding,
    DispatchQueries,
    document_sha256,
    require,
)
from dpone.adapters.composition_clickhouse_dispatch_store import MssqlClickHouseDispatchStore
from dpone.adapters.composition_clickhouse_gate_queries import (
    ClickHouseGateBinding,
    ClickHouseGateQueries,
    ClickHouseLocalSupervisorObservation,
    ClickHouseSupervisorObservation,
    SupervisorObservation,
)
from dpone.adapters.composition_clickhouse_principal import (
    ClickHousePrincipalAdmin,
    IssuedClickHouseCredentials,
    require_uuid,
)
from dpone.adapters.composition_mssql_attempts import ConnectionFactory, composition_control_transaction
from dpone.adapters.composition_mssql_schema import require_control_schema
from dpone.adapters.composition_mssql_store_queries import CompositionMssqlLedger
from dpone.contracts.composition_clickhouse_dispatch import ClickHouseDispatch, ExchangeSnapshotDispatch
from dpone.contracts.composition_persistence import (
    CompositionAttemptIdentity,
    CompositionAttemptProof,
    CompositionProofAuthority,
    composition_attempt_epoch_subject,
)
from dpone.contracts.composition_snapshot import SnapshotTarget
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object
from dpone.ports.composition_sql import CompositionSqlContext


class ClickHouseSupervisorObserver(Protocol):
    """Root implementation inspects actual retained Linux/network enrollment."""

    def observe(
        self, context: CompositionSqlContext, *, attempt: CompositionAttemptIdentity, target: SnapshotTarget
    ) -> SupervisorObservation:
        """Freshly prove stable boot and sole dispatcher network path; retain original."""


class ClickHouseDispatchPolicy(Protocol):
    """Protected scope, enrollment and budget producer, sharing the SQL transaction."""

    def require_dispatch(self, context: CompositionSqlContext, dispatch: ClickHouseDispatch) -> None:
        """Independently reopen exact generation/schema and reserve dispatch budgets."""


class MssqlClickHouseGate:
    """One purpose per target; ingest and publisher use different principal UUIDs.

    Install gate and dispatch DDL externally before construction. Inject bounded
    admin I/O, the real supervisor observer and protected dispatch budget policy.
    No default adapter, local boolean or process-prefix check supplies authority.
    """

    def __init__(
        self,
        connection_factory: ConnectionFactory,
        *,
        expected_service_id: str,
        target: SnapshotTarget,
        purpose: str,
        principal_admin: ClickHousePrincipalAdmin,
        supervisor: ClickHouseSupervisorObserver,
        dispatch_policy: ClickHouseDispatchPolicy,
        control_schema: str = "dpone_control",
    ) -> None:
        require_uuid(expected_service_id)
        target.__post_init__()
        require(purpose in {"ingest", "publisher"}, "gate_purpose")
        self._factory, self._service, self._target, self._purpose = (
            connection_factory,
            expected_service_id,
            target,
            purpose,
        )
        self._schema = require_control_schema(control_schema)
        self._admin, self._supervisor, self._policy = principal_admin, supervisor, dispatch_policy

    def _binding(self, attempt: CompositionAttemptIdentity) -> ClickHouseGateBinding:
        return ClickHouseGateBinding(attempt, self._target, self._purpose)

    @contextmanager
    def _transaction(self, attempt: CompositionAttemptIdentity) -> Iterator[ClickHouseGateQueries]:
        with composition_control_transaction(self._factory, self._schema, self._service) as ledger:
            q = ClickHouseGateQueries(ledger, self._binding(attempt))
            yield q
            q.check()

    def _observe(self, q: ClickHouseGateQueries, *, recovery: bool) -> SupervisorObservation:
        original = q.scope(recovery=recovery)
        self._admin.require_target(self._target)
        q.check()
        observation = self._supervisor.observe(q.ledger, attempt=q.binding.attempt, target=self._target)
        q.check()
        require(
            type(observation) in (ClickHouseSupervisorObservation, ClickHouseLocalSupervisorObservation),
            "supervisor_shape",
        )
        observation.__post_init__()
        q.binding.document(observation)
        require(q.scope(recovery=recovery) == original, "supervisor_changed_scope")
        return observation

    def _checked(self, q: ClickHouseGateQueries, *, recovery: bool) -> SupervisorObservation:
        observation = self._observe(q, recovery=recovery)
        q.require_subject(observation)
        return observation

    @staticmethod
    def _phase_document(q: ClickHouseGateQueries, phase: str, user_id: str) -> bytes:
        return canonical_json_bytes({"gate_key": q.binding.key, "phase": phase, "gate_id": user_id})

    def issue_once(self, attempt: CompositionAttemptIdentity) -> IssuedClickHouseCredentials:
        """Durable intent -> disabled user -> UUID/issued row -> ENABLING -> READY.

        Every exceptional boundary leaves originals for reconciliation. There is
        no read-existing-as-permit path and no password reset or fallback user.
        """
        binding = self._binding(attempt)
        with self._transaction(attempt) as q:
            observation = self._observe(q, recovery=False)
            require(q.original("ch_gates") is None, "gate_issuance_replay")
            document = binding.document(observation)
            q.append(
                "ch_gates",
                "gate_key,operation_key,login_name,evidence_sha256,evidence_document",
                (binding.key, attempt.attempt_sha256, binding.username, document_sha256(document), document),
            )
            q.require_subject(observation)
        with self._transaction(attempt) as q:
            self._checked(q, recovery=False)
            require(q.original("ch_gate_bindings") is None, "gate_binding_replay")
        password = token_urlsafe(48)
        user_id = self._admin.create_disabled(binding.username, password)
        with self._transaction(attempt) as q:
            self._checked(q, recovery=False)
            require(q.original("ch_gate_bindings") is None, "gate_binding_replay")
            document = canonical_json_bytes({"gate_key": binding.key, "gate_id": user_id})
            q.append(
                "ch_gate_bindings",
                "gate_key,gate_id,evidence_sha256,evidence_document",
                (binding.key, user_id, document_sha256(document), document),
            )
            q.append(
                "issued_authorities",
                "operation_key,connector,service_id,principal_id",
                (attempt.attempt_sha256, "clickhouse", self._target.service_id, "clickhouse-user:" + user_id),
            )
            require(q.binding_id() == user_id, "gate_binding_readback")
        self._admin.grant_disabled(binding.username, user_id, self._target, self._purpose)
        with self._transaction(attempt) as q:
            observation = self._checked(q, recovery=False)
            require(
                q.binding_id() == user_id and q.event("ENABLING") is None and q.event("CLOSING") is None,
                "gate_enable_replay",
            )
            q.put_event("ENABLING", self._phase_document(q, "ENABLING", user_id))
        if type(observation) is ClickHouseLocalSupervisorObservation:
            self._admin.enable_local(binding.username, user_id)
        else:
            assert isinstance(observation, ClickHouseSupervisorObservation)
            self._admin.enable(binding.username, user_id, observation.supervisor_ip)
        with self._transaction(attempt) as q:
            observation = self._checked(q, recovery=False)
            require(q.binding_id() == user_id and q.event("CLOSING") is None, "gate_enable_closed")
            require(q.event("ENABLING") == self._phase_document(q, "ENABLING", user_id), "gate_enable_original")
            self._observe_enabled(binding.username, user_id, observation)
            self._checked(q, recovery=False)
            q.put_event("READY", self._phase_document(q, "READY", user_id))
        with self._transaction(attempt) as q:
            self._checked(q, recovery=False)
            require(
                q.binding_id() == user_id
                and q.event("READY") == self._phase_document(q, "READY", user_id)
                and q.event("CLOSING") is None,
                "gate_ready_readback",
            )
        return IssuedClickHouseCredentials(binding.username, user_id, password)

    def journal(self, attempt: CompositionAttemptIdentity, user_id: str) -> MssqlClickHouseDispatchStore:
        """The gateway binds the UUID returned by issuance; no new permit is granted."""
        return MssqlClickHouseDispatchStore(
            self._factory,
            expected_service_id=self._service,
            attempt=attempt,
            target=self._target,
            gate_id=user_id,
            authority=self,
            control_schema=self._schema,
        )

    def require_dispatch(self, context: CompositionSqlContext, dispatch: ClickHouseDispatch) -> None:
        """Journal callback: fresh gate, isolated supervisor, exact rights and budgets."""
        require(isinstance(context, CompositionMssqlLedger), "gate_context_type")
        assert isinstance(context, CompositionMssqlLedger)
        q = ClickHouseGateQueries(context, self._binding(dispatch.attempt))
        observation = self._checked(q, recovery=False)
        user_id = q.binding_id()
        DispatchBinding(dispatch.attempt, self._target, user_id).require_dispatch(dispatch)
        require(
            isinstance(dispatch, ExchangeSnapshotDispatch) == (self._purpose == "publisher"), "gate_dispatch_purpose"
        )
        require(
            q.event("READY") == self._phase_document(q, "READY", user_id)
            and q.event("ENABLING") == self._phase_document(q, "ENABLING", user_id)
            and q.event("CLOSING") is None
            and q.event("CLOSED") is None,
            "gate_not_ready",
        )
        self._observe_enabled(q.binding.username, user_id, observation)
        q.check()
        self._policy.require_dispatch(context, dispatch)
        q.check()
        self._checked(q, recovery=False)
        require(q.binding_id() == user_id and q.event("CLOSING") is None, "gate_changed_during_dispatch")

    def _observe_enabled(self, name: str, user_id: str, observation: SupervisorObservation) -> None:
        if type(observation) is ClickHouseLocalSupervisorObservation:
            self._admin.observe_local(name, user_id, self._target, self._purpose)
        else:
            assert isinstance(observation, ClickHouseSupervisorObservation)
            self._admin.observe(
                name, user_id, self._target, self._purpose, host=observation.supervisor_ip, revoked=False
            )

    def close(self, attempt: CompositionAttemptIdentity) -> CompositionAttemptProof:
        """CLOSING before drain; revoke only after every admitted response completed."""
        with self._transaction(attempt) as q:
            self._checked(q, recovery=True)
            user_id = q.binding_id()
            q.put_event("CLOSING", self._phase_document(q, "CLOSING", user_id))
        journal = self.journal(attempt, user_id)
        journal.begin_closure()
        with self._transaction(attempt) as q:
            self._checked(q, recovery=True)
            require(
                q.event("READY") == self._phase_document(q, "READY", user_id)
                and q.event("ENABLING") == self._phase_document(q, "ENABLING", user_id),
                "gate_enable_unresolved",
            )
            dq = DispatchQueries(q.ledger, DispatchBinding(attempt, self._target, user_id))
            dq.require_scope(recovery=True)
            dq.require_closing()
            dq.drained_links()
        self._admin.revoke(q.binding.username, user_id)
        with self._transaction(attempt) as q:
            observation = self._checked(q, recovery=True)
            dq = DispatchQueries(q.ledger, DispatchBinding(attempt, self._target, user_id))
            dq.require_scope(recovery=True)
            dq.require_closing()
            links = dq.drained_links()
            principal = self._admin.observe(
                q.binding.username, user_id, self._target, self._purpose, host=None, revoked=True
            )
            self._admin.require_quiescence(q.binding.username)
            self._checked(q, recovery=True)
            require(dq.drained_links() == links, "gate_drain_changed")
            evidence: dict[str, object] = {
                "gate_key": q.binding.key,
                "gate_id": user_id,
                "phase": "CLOSED",
                "supervisor": observation.to_dict(),
                "principal": principal,
                "dispatch_terminals": links,
                "quiescence": "complete-dispatch-barrier-and-empty-server-work",
            }
            create = q.event("CLOSED") is None
            q.put_event("CLOSED", canonical_json_bytes(evidence))
            for proof in self._proofs(attempt, user_id, evidence):
                q.persist_proof(proof, create=create)
        journal.require_drained()
        with self._transaction(attempt) as q:
            return self.observe_closed(q.ledger, attempt=attempt, target=self._target, gate_id=user_id)[0]

    def prove_quiescence(self, attempt: CompositionAttemptIdentity) -> CompositionAttemptProof:
        """Fresh exact durable closure read; process disappearance cannot substitute."""
        with self._transaction(attempt) as q:
            self._checked(q, recovery=True)
            user_id = q.binding_id()
        self.journal(attempt, user_id).require_drained()
        with self._transaction(attempt) as q:
            return self.observe_closed(q.ledger, attempt=attempt, target=self._target, gate_id=user_id)[1]

    def observe_closed(
        self,
        context: CompositionSqlContext,
        *,
        attempt: CompositionAttemptIdentity,
        target: SnapshotTarget,
        gate_id: str,
    ) -> tuple[CompositionAttemptProof, CompositionAttemptProof]:
        """Reopen actual principal and network facts against retained proof originals."""
        require(isinstance(context, CompositionMssqlLedger) and target == self._target, "gate_closure_context")
        assert isinstance(context, CompositionMssqlLedger)
        q = ClickHouseGateQueries(context, self._binding(attempt))
        observation = self._checked(q, recovery=True)
        require(q.binding_id() == gate_id, "gate_closure_uuid")
        for phase in ("ENABLING", "READY", "CLOSING"):
            require(q.event(phase) == self._phase_document(q, phase, gate_id), "gate_closure_order")
        original = q.event("CLOSED")
        require(original is not None, "gate_not_closed")
        assert original is not None
        dq = DispatchQueries(q.ledger, DispatchBinding(attempt, target, gate_id))
        dq.require_scope(recovery=True)
        dq.require_closing()
        links = dq.drained_links()
        principal = self._admin.observe(q.binding.username, gate_id, target, self._purpose, host=None, revoked=True)
        self._admin.require_quiescence(q.binding.username)
        self._checked(q, recovery=True)
        expected = {
            "gate_key": q.binding.key,
            "gate_id": gate_id,
            "phase": "CLOSED",
            "supervisor": observation.to_dict(),
            "principal": principal,
            "dispatch_terminals": links,
            "quiescence": "complete-dispatch-barrier-and-empty-server-work",
        }
        require(original == canonical_json_bytes(expected) and links == dq.drained_links(), "gate_closed_original")
        proofs = self._proofs(attempt, gate_id, strict_json_object(original))
        for proof in proofs:
            q.persist_proof(proof)
        return proofs

    def _proofs(
        self, attempt: CompositionAttemptIdentity, user_id: str, evidence: dict[str, object]
    ) -> tuple[CompositionAttemptProof, CompositionAttemptProof]:
        authority = (CompositionProofAuthority("clickhouse", self._target.service_id, "clickhouse-user:" + user_id),)
        digest = document_sha256(canonical_json_bytes(evidence))
        return (
            CompositionAttemptProof(
                "CLOSED_GATES",
                attempt.attempt_sha256,
                attempt.activation_request_sha256,
                composition_attempt_epoch_subject(attempt),
                authority,
                digest,
            ),
            CompositionAttemptProof(
                "QUIESCENCE",
                attempt.attempt_sha256,
                attempt.activation_request_sha256,
                composition_attempt_epoch_subject(attempt),
                authority,
                digest,
            ),
        )
