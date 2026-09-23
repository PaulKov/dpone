"""One-shot P9b settlement of the exact retained restricted-writer VERIFY."""

import math
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Never, cast

from dpone.services import mssql_tds_writer_admission as _writer_admission
from dpone.services.mssql_tds_restricted_writer_settlement_ports import (
    SettlementEvidence as SettlementEvidence,
)
from dpone.services.mssql_tds_restricted_writer_settlement_ports import (
    SettlementOperations as SettlementOperations,
)
from dpone.services.mssql_tds_restricted_writer_settlement_ports import (
    VerifyCoordinator as VerifyCoordinator,
)
from dpone.services.mssql_tds_restricted_writer_settlement_ports import (
    VerifySettlementOrigin as VerifySettlementOrigin,
)
from dpone.services.mssql_tds_restricted_writer_verify_coordinator import (
    AdvanceCoordinator,
    RestrictedWriterVerifyRetained,
)

RestrictedWriterVerified = _writer_admission.RestrictedWriterVerified
_create_restricted_writer_verified = _writer_admission._create_restricted_writer_verified

ERROR = "mssql_native.sqlclient_restricted_writer_remote_settlement_unknown"


class RestrictedWriterSettlementUnknown(RuntimeError):
    def __init__(self, owner: "_Owner") -> None:
        self.owner = owner
        super().__init__(ERROR)


_CLAIM_TOKEN = object()


class _SettlementClaim:
    """Private one-shot authority claimed before P9b allocates any resource."""

    __slots__ = ("_token", "retained", "operations", "_plan")

    def __init__(
        self, token: object, retained: RestrictedWriterVerifyRetained, operations: SettlementOperations
    ) -> None:
        if token is not _CLAIM_TOKEN or type(retained) is not RestrictedWriterVerifyRetained:
            raise ValueError(ERROR)
        self._token, self.retained, self.operations = token, retained, operations
        self._plan: object | None = None

    @property
    def plan(self) -> object | None:
        return self._plan

    def assert_owned(self, operations: SettlementOperations) -> None:
        if self._token is not _CLAIM_TOKEN or self.operations is not operations:
            raise ValueError(ERROR)
        self.retained._assert_p9b_claim(self)

    def bind_plan(self, plan: object, operations: SettlementOperations) -> None:
        self.assert_owned(operations)
        if self._plan is not None:
            raise ValueError(ERROR)
        self._plan = plan

    @property
    def coordinator(self) -> VerifyCoordinator:
        self.assert_owned(self.operations)
        return self.retained._p9b_coordinator(self)

    @property
    def coordinator_observation(self):
        self.assert_owned(self.operations)
        return self.retained._p9b_observation(self)

    def execute_coordinator(self, request: object, *, deadline: float):
        self.assert_owned(self.operations)
        return self.retained._p9b_execute(self, request, deadline=deadline)

    def close_coordinator(self, *, deadline: float) -> None:
        self.retained._p9b_close(deadline=deadline)


def _claim_restricted_writer(
    retained: RestrictedWriterVerifyRetained, operations: SettlementOperations
) -> _SettlementClaim:
    """Atomically consume the retained P9a authority before composition effects."""
    if type(retained) is not RestrictedWriterVerifyRetained:
        raise ValueError(ERROR)
    retained.assert_retained()
    claim = _SettlementClaim(_CLAIM_TOKEN, retained, operations)
    try:
        retained._claim_p9b(claim)
    except BaseException:
        raise ValueError(ERROR) from None
    return claim


@dataclass(slots=True, repr=False)
class _Owner:
    claim: _SettlementClaim
    retained: RestrictedWriterVerifyRetained
    origin: VerifySettlementOrigin
    coordinator: VerifyCoordinator
    evidence: SettlementEvidence
    operations: SettlementOperations
    phase: str = "READY"
    unknown: bool = False
    busy: bool = True
    _refs: tuple = field(default=(), repr=False)
    _current: object | None = field(default=None, repr=False)
    _completion: object | None = field(default=None, repr=False)
    _receipt: object | None = field(default=None, repr=False)
    _terminal: object | None = field(default=None, repr=False)

    def __post_init__(self) -> None:
        verify = self.retained._owner
        self._refs = (
            self.claim,
            self.retained,
            verify,
            verify._association,
            verify._request,
            verify._result,
            verify._reservation,
            verify._registration,
            tuple(verify._receipts),
            self.origin,
            self.coordinator,
            self.evidence,
            self.operations,
        )

    def fail(self) -> Never:
        self.unknown, self.phase = True, "UNKNOWN"
        raise RestrictedWriterSettlementUnknown(self) from None

    def guard(self, deadline: float, clock: Callable[[], float]) -> None:
        try:
            (
                claim,
                retained,
                verify,
                association,
                request,
                result,
                reservation,
                registration,
                receipts,
                origin,
                coord,
                evidence,
                operations,
            ) = self._refs
            if (
                self.unknown
                or not self.busy
                or self.claim is not claim
                or self.retained is not retained
                or retained._owner is not verify
                or verify._association is not association
                or verify._request is not request
                or verify._result is not result
                or verify._reservation is not reservation
                or verify._registration is not registration
                or tuple(verify._receipts) != receipts
                or any(value is not exact for value, exact in zip(verify._receipts, receipts, strict=True))
                or self.origin is not origin
                or self.coordinator is not coord
                or self.evidence is not evidence
                or self.operations is not operations
                or type(deadline) is not float
                or not math.isfinite(deadline)
                or clock() >= deadline
            ):
                self.fail()
            retained.assert_retained()
            claim.assert_owned(operations)
            origin.assert_verify_settlement(retained, deadline=deadline)
        except RestrictedWriterSettlementUnknown:
            raise
        except BaseException:
            self.fail()

    def effect(self, callback, deadline: float, clock: Callable[[], float]):
        self.guard(deadline, clock)
        try:
            value = callback()
            self.guard(deadline, clock)
            return value
        except RestrictedWriterSettlementUnknown:
            raise
        except BaseException:
            self.fail()

    def cleanup(self, callback: Callable[[], None]) -> None:
        try:
            callback()
        except BaseException:
            self.unknown, self.phase = True, "UNKNOWN"


def _advance(owner: _Owner, event, deadline: float, clock: Callable[[], float]) -> None:
    operations = owner.operations
    previous_value = owner._current
    if type(previous_value) is not operations.snapshot_type:
        owner.fail()
    previous = cast(Any, previous_value)
    expected = operations.expected_advance(previous.state, event)

    def execute():
        observed = owner.claim.execute_coordinator(AdvanceCoordinator(event, previous.state.phase), deadline=deadline)
        custody = owner.retained._exact_custody()
        custody.snapshot = observed
        return observed

    observed_value = owner.effect(execute, deadline, clock)
    if type(observed_value) is not operations.snapshot_type:
        owner.fail()
    observed = cast(Any, observed_value)
    if (
        observed.state != expected
        or observed.revision <= previous.revision
        or owner.claim.coordinator_observation.snapshot is not observed
    ):
        owner.fail()
    owner._current = observed


def settle_restricted_writer(
    claim: _SettlementClaim,
    origin: VerifySettlementOrigin,
    coordinator: VerifyCoordinator,
    evidence: SettlementEvidence,
    run_verifier: Callable[[], object],
    close_verifier: Callable[[float], None],
    *,
    deadline: float,
    cleanup_deadline: float,
    clock: Callable[[], float],
    operations: SettlementOperations,
    cleanup_custody: Callable[[], None] | None = None,
) -> object:
    """Consume P9a once; every admitted ambiguity is terminal UNKNOWN."""
    if type(claim) is not _SettlementClaim:
        raise ValueError(ERROR)
    try:
        claim.assert_owned(operations)
    except BaseException:
        raise ValueError(ERROR) from None
    retained = claim.retained
    if coordinator is not claim.coordinator:
        raise ValueError(ERROR)
    owner = _Owner(claim, retained, origin, coordinator, evidence, operations)
    directory = authority_sha = None
    try:
        if type(cleanup_deadline) is not float or not math.isfinite(cleanup_deadline) or clock() >= cleanup_deadline:
            owner.fail()
        owner.guard(deadline, clock)
        completion_value = owner.effect(run_verifier, deadline, clock)
        if type(completion_value) is not operations.completion_type:
            owner.fail()
        completion = cast(Any, completion_value)
        verify = retained._owner
        association = cast(Any, verify._association)
        reservation = cast(Any, verify._reservation)
        registration = cast(Any, verify._registration)
        local_exit_receipt = cast(Any, verify._receipts[-1])
        if (
            completion.request.plan is not claim.plan
            or completion.request.plan.grant_evidence is not association._verify_grant_ref
            or completion.request.plan.verify_request is not verify._request
            or completion.request.plan.verify_result is not verify._result
        ):
            owner.fail()
        operations.validate_result(completion.result, completion.request)
        owner._completion = completion
        authority_sha = operations.authority_digest(completion.result)
        attempt_sha = operations.attempt_digest(verify._request.parent)
        operation_sha = operations.operation_digest(association._verify_identity)
        record_value = operations.settlement_record(attempt_sha, operation_sha, completion)
        if type(record_value) is not operations.record_type:
            owner.fail()
        record = cast(Any, record_value)
        operations.validate_record(record)
        owner.phase = "REMOTE_EVIDENCE_ATTEMPTED"
        receipt = owner.effect(lambda: evidence.write(record, deadline=deadline), deadline, clock)
        if receipt != record.receipt or evidence.observation.receipt != receipt:
            owner.fail()
        owner._receipt = receipt
        owner.phase = "REMOTE_EVIDENCE"
        current_value = claim.coordinator_observation.snapshot
        if type(current_value) is not operations.snapshot_type:
            owner.fail()
        current = cast(Any, current_value)
        custody = retained._exact_custody()
        if (
            current is not custody.snapshot
            or current.state.phase.value != "session_registered"
            or current.state.identity != association._verify_identity
            or current.state.authentication_sha256 is None
            or current.state.process != registration.process
            or current.state.session != verify._result.opening.session
            or current.state.grant is not None
            or current.state.result is not None
        ):
            owner.fail()
        owner._current = current
        process = registration.process
        if type(process) is not operations.process_type:
            owner.fail()
        local = operations.local_event(
            operation_sha,
            process,
            current.state.authentication_sha256,
            local_exit_receipt.payload_sha256,
        )
        remote = operations.remote_event(
            operation_sha,
            verify._result.opening.session,
            authority_sha,
            receipt.payload_sha256,
        )
        _advance(owner, local, deadline, clock)
        _advance(owner, remote, deadline, clock)
        local_proof = operations.local_proof(
            attempt_sha,
            reservation.operation_id,
            process,
            local_exit_receipt.payload_sha256,
        )
        remote_proof = operations.remote_proof(
            attempt_sha,
            reservation.operation_id,
            authority_sha,
            receipt.payload_sha256,
        )
        owner.effect(lambda: origin.record_verify_local_containment(local_proof, deadline=deadline), deadline, clock)
        directory = owner.effect(
            lambda: origin.record_verify_remote_settlement(remote_proof, deadline=deadline), deadline, clock
        )
        if type(directory) is not operations.directory_type:
            owner.fail()
    except BaseException:
        owner.unknown, owner.phase = True, "UNKNOWN"
    finally:
        if cleanup_custody is None:
            owner.cleanup(lambda: close_verifier(cleanup_deadline))
            owner.cleanup(lambda: claim.close_coordinator(deadline=cleanup_deadline))
            owner.cleanup(lambda: evidence.close(deadline=cleanup_deadline))
        else:
            owner.cleanup(cleanup_custody)
    if (
        owner.unknown
        or directory is None
        or authority_sha is None
        or owner._completion is None
        or owner._receipt is None
    ):
        raise RestrictedWriterSettlementUnknown(owner) from None
    completed = cast(Any, owner._completion)
    try:
        terminal = _create_restricted_writer_verified(
            directory,
            owner._receipt,
            completed.receipts,
            authority_sha,
            owner=owner,
        )
    except BaseException:
        owner.unknown, owner.phase = True, "UNKNOWN"
        raise RestrictedWriterSettlementUnknown(owner) from None
    owner._terminal = terminal
    owner.busy, owner.phase = False, "SETTLED"
    return terminal
