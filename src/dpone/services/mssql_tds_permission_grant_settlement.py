"""Settle one exact locally released GRANT after isolated remote verification."""

import math
from collections.abc import Callable
from dataclasses import dataclass, field, replace
from typing import Never

from dpone.contracts.mssql_sqlclient_permission_grant_settlement import (
    CoordinatorLocalObserved,
    CoordinatorRemoteObserved,
    PermissionGrantDepartureCompletion,
    PermissionGrantRemoteSettlementEvidenceKind,
    PermissionGrantSettlementEvidenceRecord,
    SqlClientPermissionGrantDepartureResult,
    TdsCoordinatorLocalKind,
    TdsCoordinatorLocalObservation,
    TdsCoordinatorRemoteKind,
    TdsCoordinatorRemoteObservation,
    TdsCoordinatorSnapshot,
    TdsDirectorySnapshot,
    TdsLocalContainment,
    TdsRemoteSettlement,
    advance_coordinator_state,
    attempt_identity_digest,
    coordinator_identity_digest,
    encode_permission_grant_remote_settlement,
    process_identity_digest,
    session_authority_digest,
    validate_result,
)
from dpone.ports.mssql_tds_coordinator import AdvanceCoordinator
from dpone.services.mssql_tds_permission_grant_release import PermissionGrantLocallyReleased
from dpone.services.mssql_tds_permission_grant_terminal import PermissionGrantSettled as PermissionGrantSettled

ERROR = "mssql_native.sqlclient_permission_remote_settlement_unknown"


class PermissionGrantRemoteSettlementUnknown(ValueError):
    def __init__(self, owner: "_SettlementOwner") -> None:
        self.owner = owner
        super().__init__(ERROR)


@dataclass(slots=True, repr=False)
class _SettlementOwner:
    local: PermissionGrantLocallyReleased
    phase: str = "LOCAL_EXIT"
    _busy: bool = True
    _unknown: bool = False
    _refs: tuple = field(default=(), repr=False)
    _completion: PermissionGrantDepartureCompletion | None = field(default=None, repr=False)
    _coordinator_ref: object = field(default=None, repr=False)
    _evidence_ref: object = field(default=None, repr=False)
    _remote_snapshot: TdsCoordinatorSnapshot | None = field(default=None, repr=False)
    _receipt_refs: tuple = field(default=(), repr=False)
    _receipt_values: tuple = field(default=(), repr=False)
    _observation_ref: object | None = field(default=None, repr=False)
    _settled_ref: PermissionGrantSettled | None = field(default=None, repr=False)

    def __post_init__(self) -> None:
        held = self.local.held_owner
        self._coordinator_ref = held.coordinator
        self._evidence_ref = self.local.settlement_evidence
        self._refs = (
            self.local,
            held,
            held.association,
            held.coordinator,
            held.process,
            self.local.settlement_evidence,
            held.binding,
            held.authority,
            held.result,
            self.local.subject,
            self.local.local_exit,
            tuple(self.local.receipts),
        )

    def _fail(self) -> Never:
        self.poison()
        raise PermissionGrantRemoteSettlementUnknown(self) from None

    def poison(self) -> None:
        self._unknown, self.phase = True, "UNKNOWN"

    def integrity(self) -> None:
        try:
            (
                local,
                held,
                association,
                coordinator,
                process,
                evidence,
                binding,
                authority,
                result,
                subject,
                local_exit,
                receipts,
            ) = self._refs
            if (
                self.local is not local
                or local.held_owner is not held
                or local.association is not association
                or local.coordinator is not coordinator
                or local.process is not process
                or local.settlement_evidence is not evidence
                or held.binding is not binding
                or held.authority is not authority
                or held.result is not result
                or local.subject is not subject
                or local.local_exit is not local_exit
                or any(value is not reference for value, reference in zip(local.receipts[:3], receipts, strict=True))
                or getattr(association, "_local_release_owner", None) is not local
                or getattr(association, "_remote_settlement_owner", None) is not self
            ):
                raise ValueError
            if self._receipt_refs and (
                len(local.receipts) != 4
                or any(
                    value is not reference for value, reference in zip(local.receipts, self._receipt_refs, strict=True)
                )
                or tuple(replace(value) for value in local.receipts) != self._receipt_values
                or tuple(local._receipt_refs) != self._receipt_refs
                or local._observation_ref is not self._observation_ref
                or local._observation_ref != self._receipt_values[-1]
            ):
                raise ValueError
            if self._remote_snapshot is not None and (
                held._current is not self._remote_snapshot
                or held.coordinator.observation.snapshot is not self._remote_snapshot
            ):
                raise ValueError
        except BaseException:
            self.poison()

    def guard(self, deadline: float, clock: Callable[[], float]) -> None:
        try:
            (
                local,
                held,
                association,
                coordinator,
                process,
                evidence,
                binding,
                authority,
                result,
                subject,
                local_exit,
                receipts,
            ) = self._refs
            if self._unknown or not self._busy or self.local is not local:
                self._fail()
            if self.phase == "LOCAL_EXIT":
                local.assert_local_exit()
            if (
                local._unknown
                or local.held_owner is not held
                or local.association is not association
                or local.coordinator is not coordinator
                or local.process is not process
                or local.settlement_evidence is not evidence
                or held.binding is not binding
                or held.authority is not authority
                or held.result is not result
                or local.subject is not subject
                or local.local_exit is not local_exit
                or len(local.receipts) not in (3, 4)
                or any(value is not reference for value, reference in zip(local.receipts[:3], receipts, strict=True))
                or getattr(association, "_local_release_owner", None) is not local
                or getattr(association, "_remote_settlement_owner", None) is not self
                or type(deadline) is not float
                or not math.isfinite(deadline)
                or clock() >= deadline
            ):
                self._fail()
            self.integrity()
            if self._unknown:
                self._fail()
        except PermissionGrantRemoteSettlementUnknown:
            raise
        except BaseException:
            self._fail()

    def effect(self, callback, deadline: float, clock):
        self.guard(deadline, clock)
        try:
            value = callback()
            self.guard(deadline, clock)
            return value
        except BaseException:
            self._fail()

    def cleanup(self, callback: Callable[[], None]) -> None:
        try:
            callback()
        except BaseException:
            self.poison()
        self.integrity()


def _advance(owner: _SettlementOwner, event, deadline: float, clock) -> PermissionGrantLocallyReleased:
    owner.guard(deadline, clock)
    held = owner.local.held_owner
    previous = held._current
    if type(previous) is not TdsCoordinatorSnapshot:
        owner._fail()
    expected = advance_coordinator_state(previous.state, event, expected_phase=previous.state.phase)
    observed = owner.effect(
        lambda: held.coordinator.execute(AdvanceCoordinator(event, previous.state.phase), deadline=deadline),
        deadline,
        clock,
    )
    if (
        type(observed) is not TdsCoordinatorSnapshot
        or observed.state != expected
        or observed.revision <= previous.revision
        or held.coordinator.observation.snapshot is not observed
    ):
        owner._fail()
    held._current = observed
    return owner.local


def settle_permission_grant_remotely(
    local: PermissionGrantLocallyReleased,
    run_verifier: Callable[[], PermissionGrantDepartureCompletion],
    close_verifier: Callable[[float], None],
    *,
    deadline: float,
    cleanup_deadline: float,
    clock: Callable[[], float],
) -> PermissionGrantSettled:
    """Consume P8a once; ambiguity after any effect is sticky and never replayed."""
    if type(local) is not PermissionGrantLocallyReleased:
        raise ValueError(ERROR)
    owner = _SettlementOwner(local)
    association = local.association
    previous = getattr(association, "_remote_settlement_owner", None)
    if previous is not None:
        if type(previous) is _SettlementOwner:
            previous._fail()
        owner._fail()
    setattr(association, "_remote_settlement_owner", owner)
    final_directory = None
    authority_sha = None
    completion = None
    try:
        if type(cleanup_deadline) is not float or not math.isfinite(cleanup_deadline) or clock() >= cleanup_deadline:
            owner._fail()
        owner.guard(deadline, clock)
        completion = owner.effect(run_verifier, deadline, clock)
        if (
            type(completion) is not PermissionGrantDepartureCompletion
            or type(completion.result) is not SqlClientPermissionGrantDepartureResult
            or completion.request.plan.grant_evidence is not local.held_owner.result
        ):
            owner._fail()
        owner._completion = completion
        result = completion.result
        validate_result(result, completion.request)
        authority_sha = session_authority_digest(result.catalog_observer.authority).hex()
        evidence = local.settlement_evidence
        if evidence is None or local.subject is None or local.binding is None:
            owner._fail()
        payload = encode_permission_grant_remote_settlement(result, completion.receipts)
        record = PermissionGrantSettlementEvidenceRecord(
            local.subject,
            PermissionGrantRemoteSettlementEvidenceKind.REMOTE_SETTLEMENT,
            payload,
            binding=local.binding,
        )
        expected_receipt = record.receipt
        owner.phase = "REMOTE_EVIDENCE_ATTEMPTED"
        receipt = owner.effect(lambda: evidence.write(record, deadline=deadline), deadline, clock)
        observation = owner.effect(lambda: getattr(evidence, "observation"), deadline, clock)
        observed_receipt = owner.effect(lambda: getattr(observation, "receipt"), deadline, clock)
        if receipt != expected_receipt or observed_receipt != expected_receipt:
            owner._fail()
        local.receipts += (receipt,)
        local._receipt_refs += (receipt,)
        local._observation_ref = observed_receipt
        owner._receipt_refs = tuple(local.receipts)
        owner._receipt_values = tuple(replace(value) for value in local.receipts)
        owner._observation_ref = observed_receipt
        owner.phase = "REMOTE_EVIDENCE"
        held, binding = local.held_owner, local.binding
        current = held._current
        if current is None or current.state.authentication_sha256 is None:
            owner._fail()
        operation_sha = coordinator_identity_digest(binding.operation)
        local_observation = TdsCoordinatorLocalObservation(
            operation_sha,
            TdsCoordinatorLocalKind.CONTAINED,
            binding.startup.process,
            current.state.authentication_sha256,
            local.receipts[2].payload_sha256,
        )
        authority = held.authority
        if authority is None:
            owner._fail()
        remote_observation = TdsCoordinatorRemoteObservation(
            operation_sha,
            TdsCoordinatorRemoteKind.SETTLED,
            authority.session,
            authority_sha,
            receipt.payload_sha256,
        )
        _advance(owner, CoordinatorLocalObserved(local_observation), deadline, clock)
        _advance(owner, CoordinatorRemoteObserved(remote_observation), deadline, clock)
        owner._remote_snapshot = held._current
        local_proof = TdsLocalContainment(
            attempt_identity_digest(binding.operation.parent),
            binding.operation.operation_id,
            process_identity_digest(binding.startup.process),
            local.receipts[2].payload_sha256,
        )
        remote_proof = TdsRemoteSettlement(
            attempt_identity_digest(binding.operation.parent),
            binding.operation.operation_id,
            authority_sha,
            receipt.payload_sha256,
        )
        association.record_local_containment(local_proof)
        final_directory = association.record_remote_settlement(remote_proof)
        if type(final_directory) is not TdsDirectorySnapshot:
            owner._fail()
    except BaseException:
        owner.poison()
    finally:
        owner.cleanup(lambda: close_verifier(cleanup_deadline))
        owner.cleanup(lambda: getattr(owner._coordinator_ref, "close")(deadline=cleanup_deadline))
        owner.cleanup(lambda: getattr(owner._evidence_ref, "close")(deadline=cleanup_deadline))
    owner.integrity()
    if owner._unknown or final_directory is None or completion is None or authority_sha is None:
        raise PermissionGrantRemoteSettlementUnknown(owner) from None
    settled = PermissionGrantSettled(final_directory, owner._receipt_refs, completion.receipts, authority_sha)
    owner._settled_ref = settled
    setattr(association, "_settled_capability", settled)
    owner._busy, owner.phase = False, "SETTLED"
    return settled
