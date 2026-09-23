"""Original parent/journal continuation, with no SQL or replaceable assertions.

Construction is internal to retained OBSERVE composition. Actor intervals use
this concrete capability instead of accepting an arbitrary authority callback.
"""

from copy import deepcopy
from dataclasses import replace
from hashlib import sha256
from uuid import UUID

from dpone.ports.mssql_tds_coordinator import (
    AdvanceCoordinator,
    AssertCoordinatorAuthority,
    TdsCoordinatorGateway,
)
from dpone.ports.mssql_tds_coordinator_evidence import TdsCoordinatorEvidenceGateway
from dpone.services.mssql_tds_attempt import TdsAttempt
from dpone.services.mssql_tds_writer_contracts import (
    CoordinatorCredentialIntent,
    CoordinatorProcessRegistered,
    CoordinatorSessionRegistered,
    SqlClientObserveRequest,
    TdsCoordinatorAuthority,
    TdsCoordinatorCommand,
    TdsCoordinatorEvent,
    TdsCoordinatorEvidenceObservation,
    TdsCoordinatorEvidenceReceipt,
    TdsCoordinatorEvidenceRecord,
    TdsCoordinatorIdentity,
    TdsCoordinatorSnapshot,
    TdsCoordinatorStartup,
    TdsDirectorySnapshot,
    WindowOutcomeUnknown,
    advance_coordinator_state,
    authority_digest,
    coordinator_identity_digest,
    encode_authority,
    validate_authority,
    validate_original_record,
)
from dpone.services.mssql_tds_writer_contracts import (
    TdsCoordinatorEvidenceKind as Kind,
)
from dpone.services.mssql_tds_writer_contracts import (
    encode_coordinator_registration as encode_registration,
)


class TdsObserveContinuation:
    """Compare original acknowledged snapshots; never adopt unrelated revisions."""

    def __init__(
        self,
        attempt: TdsAttempt,
        helper_id: UUID,
        identity: TdsCoordinatorIdentity,
        gateway: TdsCoordinatorGateway,
        *,
        deadline: float,
    ) -> None:
        self._attempt, self._helper, self._identity = attempt, helper_id, identity
        self._gateway, self.deadline = gateway, deadline
        self._expected = gateway.observation.snapshot
        self._asserting = self._failed = False
        self._evidence: TdsCoordinatorEvidenceGateway | None = None
        self._evidence_attempts: dict = {}
        self._evidence_acks: tuple = ()
        self._evidence_snapshots: tuple = ()
        self._registered_startup: TdsCoordinatorStartup | None = None
        parent, _ = attempt._assert_observe(helper_id, deadline=deadline)
        if (
            type(self._expected) is not TdsCoordinatorSnapshot
            or self._expected.state.identity != identity
            or self._expected.state.ownership != parent.state.ownership
            or self._expected.state.execution_owner != parent.state.ownership
        ):
            raise ValueError("mssql_native.sqlclient_observe_binding")
        self.assert_current()

    def assert_current(self) -> None:
        """Check both original attempt actors and this exact coordinator ACK."""
        if self._failed or self._asserting:
            self._failed = True
            raise WindowOutcomeUnknown("mssql_native.sqlclient_observe_owner_unknown")
        self._asserting = True
        try:
            self._attempt._assert_observe(self._helper, deadline=self.deadline)
            observed = self._gateway.execute(AssertCoordinatorAuthority(), deadline=self.deadline)
            if (
                type(observed) is not TdsCoordinatorSnapshot
                or observed != self._expected
                or self._gateway.observation.snapshot != observed
                or self._failed
            ):
                raise ValueError("mssql_native.sqlclient_observe_ack_unknown")
            self._attempt._assert_observe(self._helper, deadline=self.deadline)
        except BaseException:
            self._failed = True
            raise
        finally:
            self._asserting = False

    def _assert_preparation_terminal(self, transition) -> None:
        """Check original coordinator ACK without invoking creation-only parent checks."""
        if (
            self._failed
            or self._asserting
            or self._attempt._preparation is not transition
            or transition.handle.continuation is not self
            or transition.expected is None
        ):
            raise WindowOutcomeUnknown("mssql_native.sqlclient_observe_owner_unknown")
        self._asserting = True
        try:
            observed = self._gateway.execute(AssertCoordinatorAuthority(), deadline=self.deadline)
            originals = (
                observed,
                self._expected,
                self._gateway.observation.snapshot,
                transition.coordinator,
                transition.coordinator_snapshot,
            )
            for original in originals:
                if type(original) is not TdsCoordinatorSnapshot:
                    raise ValueError("mssql_native.sqlclient_observe_ack_unknown")
                validate_original_record(original)
            if any(original != observed for original in originals):
                raise ValueError("mssql_native.sqlclient_observe_ack_unknown")
            if self._evidence is not None:
                registration = self._evidence.observation
                registrations = (registration, transition.registration, transition.registration_snapshot)
                for registration_original in registrations:
                    if type(registration_original) is not TdsCoordinatorEvidenceObservation:
                        raise ValueError("mssql_native.sqlclient_observe_ack_unknown")
                    validate_original_record(registration_original)
                if any(registration_original != registration for registration_original in registrations):
                    raise ValueError("mssql_native.sqlclient_observe_ack_unknown")
            elif transition.registration is not None or transition.registration_snapshot is not None:
                raise ValueError("mssql_native.sqlclient_observe_ack_unknown")
        except BaseException:
            self._failed = True
            raise
        finally:
            self._asserting = False

    def advance(self, event: TdsCoordinatorEvent) -> None:
        self.assert_current()
        try:
            previous = self._expected
            assert previous is not None
            state = advance_coordinator_state(previous.state, event, expected_phase=previous.state.phase)
            observed = self._gateway.execute(AdvanceCoordinator(event, previous.state.phase), deadline=self.deadline)
            if (
                type(observed) is not TdsCoordinatorSnapshot
                or observed.state != state
                or self._gateway.observation.snapshot != observed
                or (state != previous.state and observed.revision <= previous.revision)
                or (state == previous.state and observed != previous)
            ):
                raise ValueError("mssql_native.sqlclient_observe_ack_unknown")
            self._expected = replace(observed)
            self.assert_current()
        except BaseException:
            self._failed = True
            raise

    @staticmethod
    def reserved_identity(
        request: SqlClientObserveRequest, reserved: TdsDirectorySnapshot, implementation_sha256: str
    ) -> TdsCoordinatorIdentity:
        """Bind this OBSERVE to the acknowledged original last directory slot."""
        slot = reserved.state.slots[-1]
        if (
            reserved.state.parent != request.parent
            or slot.command is not TdsCoordinatorCommand.OBSERVE
            or slot.command_sha256 != request.command_sha256
            or slot.owner_fence != reserved.ownership.fence
        ):
            raise ValueError("mssql_native.sqlclient_observe_reservation_binding")
        return TdsCoordinatorIdentity(
            request.parent,
            slot.index,
            slot.operation_id,
            slot.command,
            slot.command_sha256,
            slot.owner_fence,
            implementation_sha256,
        )

    @property
    def operation_sha256(self) -> str:
        return coordinator_identity_digest(self._identity)

    def bind_evidence(self, evidence: TdsCoordinatorEvidenceGateway, admission: bytes) -> None:
        """Bind the original parent-local actor and persist nonsecret admission."""
        self.assert_current()
        if self._evidence is not None or evidence.observation != TdsCoordinatorEvidenceObservation(
            self.operation_sha256
        ):
            raise ValueError("mssql_native.sqlclient_observe_evidence_binding")
        self._evidence = evidence
        self._persist(Kind.ADMISSION, admission)

    def _persist(self, kind: Kind, payload: bytes) -> None:
        self.assert_current()
        if self._evidence is None:
            raise ValueError("mssql_native.sqlclient_observe_evidence_binding")
        try:
            if kind not in (Kind.ADMISSION, Kind.REGISTRATION, Kind.AUTHORITY) or kind in self._evidence_attempts:
                raise ValueError("mssql_native.sqlclient_observe_ack_unknown")
            record = TdsCoordinatorEvidenceRecord(self.operation_sha256, kind, payload)
            self._evidence_attempts[kind] = (record, None, None)
            receipt = self._evidence.write(record, deadline=self.deadline)
            self._evidence_attempts[kind] = (record, receipt, None)
            observed = self._evidence.observation
            self._evidence_attempts[kind] = (record, receipt, observed)
            if (
                type(receipt) is not TdsCoordinatorEvidenceReceipt
                or type(observed) is not TdsCoordinatorEvidenceObservation
            ):
                raise ValueError("mssql_native.sqlclient_observe_ack_unknown")
            validate_original_record((receipt, observed))
            if receipt != record.receipt or observed != TdsCoordinatorEvidenceObservation(
                self.operation_sha256, receipt
            ):
                raise ValueError("mssql_native.sqlclient_observe_ack_unknown")
            self.assert_current()
            captured = (record, receipt, observed)
            self._evidence_acks += (captured,)
            self._evidence_snapshots += (deepcopy(captured),)
        except BaseException:
            self._failed = True
            raise

    def retained_evidence(self) -> tuple:
        """Validate original finite ACKs locally, including after actor closure."""
        if self._failed or tuple(self._evidence_attempts) != (Kind.ADMISSION, Kind.REGISTRATION, Kind.AUTHORITY):
            raise WindowOutcomeUnknown("mssql_native.sqlclient_observe_ack_unknown")
        if len(self._evidence_acks) != 3 or self._evidence_acks != self._evidence_snapshots:
            raise WindowOutcomeUnknown("mssql_native.sqlclient_observe_ack_unknown")
        validate_original_record(self._evidence_snapshots)
        for record, receipt, observed in self._evidence_acks:
            validate_original_record((record, receipt, observed))
            attempted = self._evidence_attempts[record.kind]
            if (
                any(a is not b for a, b in zip(attempted, (record, receipt, observed), strict=True))
                or receipt != record.receipt
                or observed.receipt != receipt
            ):
                raise WindowOutcomeUnknown("mssql_native.sqlclient_observe_ack_unknown")
        return self._evidence_acks

    def register_process(self, startup: TdsCoordinatorStartup, admission: bytes) -> None:
        """Acknowledge actual startup evidence before ProcessRegistered."""
        if (
            self._registered_startup is not None
            or startup.implementation_sha256 != self._identity.implementation_sha256
        ):
            raise ValueError("mssql_native.sqlclient_observe_process_binding")
        registration = encode_registration(startup, sha256(admission).hexdigest())
        self._persist(Kind.REGISTRATION, registration)
        self.advance(CoordinatorProcessRegistered(startup.process, sha256(registration).hexdigest()))
        self._registered_startup = startup

    def acknowledge_credentials(self) -> None:
        """Only the parent calls after validating the nonsecret request ACK."""
        self.advance(CoordinatorCredentialIntent())

    def register_session(
        self, payload: bytes, request: SqlClientObserveRequest, nonce: bytes
    ) -> TdsCoordinatorAuthority:
        """Validate actual session, persist authority, then acknowledge registration."""
        self.assert_current()
        if self._registered_startup is None or self._expected is None:
            raise ValueError("mssql_native.sqlclient_observe_process_binding")
        authority = validate_authority(
            payload, request, self._identity, self._expected.state.execution_owner, self._registered_startup, nonce
        )
        self._persist(Kind.AUTHORITY, encode_authority(authority))
        self.advance(CoordinatorSessionRegistered(authority.session, authority_digest(authority)))
        return authority
