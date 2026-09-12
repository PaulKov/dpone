"""Shared transaction and proof machinery for MSSQL R1 V3 control effects.

The module deliberately owns session lifecycle but no SQL policy. Concrete
registration and authority-import backends receive the one caller-owned handle
and therefore cannot silently open or commit a second connection.
"""

from __future__ import annotations

from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Protocol
from uuid import UUID

from dpone.contracts.mssql_r1_v3_control_proof import (
    MssqlR1ControlOperationV1,
    MssqlR1ControlReceiptV1,
    MssqlR1V3ContractError,
    MssqlR1WriterHeadControlObservationV1,
    MssqlTargetPhysicalIdentityV1,
    build_control_effect_key,
    canonical_utc_text,
    require_digest,
    require_positive,
    require_uuid,
)

if TYPE_CHECKING:
    from dpone.contracts.mssql_r1_v3_control_proof import MssqlR1ControlFreshProofV1
    from dpone.ports.mssql_target_authority_v2 import MssqlTransactionSessionFactoryPort


class MssqlR1ControlError(RuntimeError):
    """Stable fail-closed error raised by a V3 control-plane provider."""


@dataclass(frozen=True, slots=True)
class MssqlR1ControlStateV1:
    """Typed target-local state used to construct and re-prove a control receipt."""

    registration_id: UUID
    active_registration_revision: int
    physical_authority_digest: bytes
    schema_contract_digest: bytes
    permission_contract_digest: bytes
    writer_head: MssqlR1WriterHeadControlObservationV1
    authority_set_digest: bytes | None = None
    override_payload_digest: bytes | None = None

    def __post_init__(self) -> None:
        require_uuid(self.registration_id, "registration_id")
        require_positive(self.active_registration_revision, "active_registration_revision")
        for name in (
            "physical_authority_digest",
            "schema_contract_digest",
            "permission_contract_digest",
        ):
            require_digest(getattr(self, name), name)
        if not isinstance(self.writer_head, MssqlR1WriterHeadControlObservationV1):
            raise MssqlR1V3ContractError("control state requires a typed writer head")
        for name in ("authority_set_digest", "override_payload_digest"):
            value = getattr(self, name)
            if value is not None:
                require_digest(value, name)
        if self.authority_set_digest is not None and self.override_payload_digest is not None:
            raise MssqlR1V3ContractError("control state cannot bind two import kinds")


@dataclass(frozen=True, slots=True)
class MssqlR1ExpectedControlEffectV1:
    """All immutable inputs needed to validate replay and build one receipt."""

    operation: MssqlR1ControlOperationV1
    physical_identity: MssqlTargetPhysicalIdentityV1
    target_binding_uuid: UUID
    registration_id: UUID
    input_payload_digest: bytes
    verification_receipt_digest: bytes
    expected_active_registration_revision: int | None
    expected_writer_head: MssqlR1WriterHeadControlObservationV1 | None
    required_observed_writer_head: MssqlR1WriterHeadControlObservationV1
    schema_contract_digest: bytes
    permission_contract_digest: bytes
    affected_authority_set_digest: bytes | None = None
    affected_override_payload_digest: bytes | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.operation, MssqlR1ControlOperationV1):
            raise MssqlR1V3ContractError("control operation is unsupported")
        if not isinstance(self.physical_identity, MssqlTargetPhysicalIdentityV1):
            raise MssqlR1V3ContractError("control effect requires physical identity")
        require_uuid(self.target_binding_uuid, "target_binding_uuid")
        require_uuid(self.registration_id, "registration_id")
        for name in (
            "input_payload_digest",
            "verification_receipt_digest",
            "schema_contract_digest",
            "permission_contract_digest",
        ):
            require_digest(getattr(self, name), name)
        if self.expected_active_registration_revision is not None:
            require_positive(self.expected_active_registration_revision, "expected_active_registration_revision")
        if self.expected_writer_head is not None and not isinstance(
            self.expected_writer_head, MssqlR1WriterHeadControlObservationV1
        ):
            raise MssqlR1V3ContractError("control effect predecessor writer head is invalid")
        if not isinstance(self.required_observed_writer_head, MssqlR1WriterHeadControlObservationV1):
            raise MssqlR1V3ContractError("control effect requires an exact observed writer head")
        for name in ("affected_authority_set_digest", "affected_override_payload_digest"):
            value = getattr(self, name)
            if value is not None:
                require_digest(value, name)

    @property
    def control_effect_key(self) -> bytes:
        return build_control_effect_key(
            self.operation,
            self.physical_identity.physical_object_coordinate_digest,
            self.input_payload_digest,
            self.expected_active_registration_revision,
        )


class _SessionWork(Protocol):
    def __call__(self, handle: object) -> MssqlR1ControlReceiptV1: ...


class _FreshProbe(Protocol):
    def __call__(self, handle: object) -> MssqlR1ControlFreshProofV1 | None: ...


class MssqlR1ControlTransactionRunner:
    """Execute one control effect and resolve ambiguous commit on a fresh session."""

    def __init__(self, session_factory: MssqlTransactionSessionFactoryPort) -> None:
        self._sessions = session_factory

    def run(self, work: _SessionWork, fresh_probe: _FreshProbe) -> MssqlR1ControlReceiptV1:
        session = self._sessions.open()
        handle: object | None = None
        active = False
        closed = False
        try:
            handle = session.begin()
            active = True
            session.assert_active(handle)
            receipt = work(handle)
            try:
                session.commit(handle)
                active = False
                return receipt
            except Exception as exc:
                active = False
                closed = True
                with suppress(Exception):
                    session.close()
                try:
                    proof = self.probe_fresh(fresh_probe)
                except Exception as probe_exc:
                    raise MssqlR1ControlError("postgres_mssql_r1.control_commit_outcome_unknown") from probe_exc
                if proof is None or proof.receipt != receipt:
                    raise MssqlR1ControlError("postgres_mssql_r1.control_commit_outcome_unknown") from exc
                return proof.receipt
        except Exception:
            if active and handle is not None:
                with suppress(Exception):
                    session.rollback(handle)
            raise
        finally:
            if not closed:
                with suppress(Exception):
                    session.close()

    def probe_fresh(self, probe: _FreshProbe) -> MssqlR1ControlFreshProofV1 | None:
        session = self._sessions.open()
        handle: object | None = None
        active = False
        try:
            handle = session.begin()
            active = True
            session.assert_active(handle)
            proof = probe(handle)
            session.commit(handle)
            active = False
            return proof
        except Exception:
            if active and handle is not None:
                with suppress(Exception):
                    session.rollback(handle)
            raise
        finally:
            with suppress(Exception):
                session.close()


def build_control_receipt(
    expected: MssqlR1ExpectedControlEffectV1,
    state: MssqlR1ControlStateV1,
    *,
    receipt_id: UUID,
    committed_at: datetime,
) -> MssqlR1ControlReceiptV1:
    """Build a receipt only after the complete candidate state was read back."""

    _validate_candidate_state(expected, state)
    previous = expected.expected_writer_head
    return MssqlR1ControlReceiptV1(
        control_receipt_id=receipt_id,
        control_effect_key=expected.control_effect_key,
        operation=expected.operation,
        physical_object_coordinate_digest=expected.physical_identity.physical_object_coordinate_digest,
        registered_physical_authority_digest=expected.physical_identity.registered_physical_authority_digest,
        target_binding_uuid=expected.target_binding_uuid,
        registration_id=expected.registration_id,
        input_payload_digest=expected.input_payload_digest,
        verification_receipt_digest=expected.verification_receipt_digest,
        expected_active_registration_revision=expected.expected_active_registration_revision,
        committed_active_registration_revision=state.active_registration_revision,
        expected_writer_head_revision=None if previous is None else previous.head_revision,
        observed_writer_head_revision=state.writer_head.head_revision,
        expected_writer_head_digest=None if previous is None else previous.head_digest,
        observed_writer_head_digest=state.writer_head.head_digest,
        schema_contract_digest=state.schema_contract_digest,
        permission_contract_digest=state.permission_contract_digest,
        affected_authority_set_digest=state.authority_set_digest,
        affected_override_payload_digest=state.override_payload_digest,
        committed_at=committed_at,
    )


def validate_control_proof(
    expected: MssqlR1ExpectedControlEffectV1,
    proof: MssqlR1ControlFreshProofV1 | None,
) -> MssqlR1ControlReceiptV1:
    """Reject partial, foreign, or conflicting control-receipt observations."""

    if proof is None:
        raise MssqlR1ControlError("postgres_mssql_r1.control_receipt_missing")
    receipt = proof.receipt
    expected_values = (
        expected.control_effect_key,
        expected.operation,
        expected.physical_identity.physical_object_coordinate_digest,
        expected.physical_identity.registered_physical_authority_digest,
        expected.target_binding_uuid,
        expected.registration_id,
        expected.input_payload_digest,
        expected.verification_receipt_digest,
        expected.expected_active_registration_revision,
        expected.schema_contract_digest,
        expected.permission_contract_digest,
        expected.affected_authority_set_digest,
        expected.affected_override_payload_digest,
    )
    observed_values = (
        receipt.control_effect_key,
        receipt.operation,
        receipt.physical_object_coordinate_digest,
        receipt.registered_physical_authority_digest,
        receipt.target_binding_uuid,
        receipt.registration_id,
        receipt.input_payload_digest,
        receipt.verification_receipt_digest,
        receipt.expected_active_registration_revision,
        receipt.schema_contract_digest,
        receipt.permission_contract_digest,
        receipt.affected_authority_set_digest,
        receipt.affected_override_payload_digest,
    )
    if observed_values != expected_values:
        raise MssqlR1ControlError("postgres_mssql_r1.control_receipt_conflict")
    previous = expected.expected_writer_head
    expected_head = (
        None if previous is None else previous.head_revision,
        None if previous is None else previous.head_digest,
    )
    if (receipt.expected_writer_head_revision, receipt.expected_writer_head_digest) != expected_head:
        raise MssqlR1ControlError("postgres_mssql_r1.control_writer_head_conflict")
    if (
        receipt.observed_writer_head_revision,
        receipt.observed_writer_head_digest,
    ) != (
        expected.required_observed_writer_head.head_revision,
        expected.required_observed_writer_head.head_digest,
    ):
        raise MssqlR1ControlError("postgres_mssql_r1.control_observed_writer_head_conflict")
    return receipt


def require_current_time(clock: Callable[[], datetime]) -> datetime:
    value = clock()
    canonical_utc_text(value, "control_clock")
    return value


def _validate_candidate_state(
    expected: MssqlR1ExpectedControlEffectV1,
    state: MssqlR1ControlStateV1,
) -> None:
    expected_revision = expected.expected_active_registration_revision
    if expected.operation is MssqlR1ControlOperationV1.PROVISION:
        committed_revision = 1
    elif expected.operation in {
        MssqlR1ControlOperationV1.REGISTRATION_ROTATE,
        MssqlR1ControlOperationV1.REGISTRATION_RETIRE,
    }:
        committed_revision = require_positive(expected_revision, "expected_active_registration_revision") + 1
    else:
        committed_revision = require_positive(expected_revision, "expected_active_registration_revision")
    expected_state = (
        expected.registration_id,
        committed_revision,
        expected.physical_identity.registered_physical_authority_digest,
        expected.schema_contract_digest,
        expected.permission_contract_digest,
        expected.affected_authority_set_digest,
        expected.affected_override_payload_digest,
    )
    observed_state = (
        state.registration_id,
        state.active_registration_revision,
        state.physical_authority_digest,
        state.schema_contract_digest,
        state.permission_contract_digest,
        state.authority_set_digest,
        state.override_payload_digest,
    )
    if observed_state != expected_state:
        raise MssqlR1ControlError("postgres_mssql_r1.control_state_conflict")
    if state.writer_head != expected.required_observed_writer_head:
        raise MssqlR1ControlError("postgres_mssql_r1.control_mutated_writer_head")


__all__ = [
    "MssqlR1ControlError",
    "MssqlR1ControlStateV1",
    "MssqlR1ControlTransactionRunner",
    "MssqlR1ExpectedControlEffectV1",
    "build_control_receipt",
    "require_current_time",
    "validate_control_proof",
]
