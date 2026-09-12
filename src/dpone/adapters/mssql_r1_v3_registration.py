"""Provisioner-only target registration and rotation for MSSQL R1 V3."""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from typing import TYPE_CHECKING, Protocol
from uuid import UUID, uuid4

from dpone.contracts.mssql_r1_v3_control_proof import (
    MssqlR1ControlOperationV1,
    MssqlR1WriterHeadControlObservationV1,
    require_digest,
)
from dpone.contracts.mssql_r1_v3_registration import (
    MssqlTargetRegistrationPayloadV1,
    RegistrationActionV1,
)

from .mssql_r1_v3_control import (
    MssqlR1ControlError,
    MssqlR1ControlStateV1,
    MssqlR1ControlTransactionRunner,
    MssqlR1ExpectedControlEffectV1,
    build_control_receipt,
    require_current_time,
    validate_control_proof,
)

if TYPE_CHECKING:
    from dpone.contracts.mssql_r1_v3_control import MssqlR1ControlReceiptV1
    from dpone.contracts.mssql_r1_v3_control_proof import MssqlR1ControlFreshProofV1
    from dpone.contracts.mssql_r1_v3_registration import (
        MssqlTargetRegistrationVerificationV1,
        SignedTargetRegistrationCommandV1,
    )
    from dpone.ports.mssql_r1_v3 import BlobSignatureVerifierV1Port
    from dpone.ports.mssql_target_authority_v2 import MssqlTransactionSessionFactoryPort


class MssqlR1RegistrationTransactionBackend(Protocol):
    """SQL-side registration operations performed on one supplied handle."""

    def lock_physical(self, handle: object, physical_coordinate_digest: bytes) -> None: ...

    def lock_binding(self, handle: object, target_binding_uuid: UUID) -> None: ...

    def install_and_attest_schema(self, handle: object) -> bytes: ...

    def find_control_proof(self, handle: object, control_effect_key: bytes) -> MssqlR1ControlFreshProofV1 | None: ...

    def assert_physical_target(self, handle: object, payload: MssqlTargetRegistrationPayloadV1) -> None: ...

    def read_registration_state(self, handle: object, target_binding_uuid: UUID) -> MssqlR1ControlStateV1 | None: ...

    def create_initial_registration(
        self,
        handle: object,
        payload: MssqlTargetRegistrationPayloadV1,
        verification: MssqlTargetRegistrationVerificationV1,
    ) -> None: ...

    def rotate_registration(
        self,
        handle: object,
        payload: MssqlTargetRegistrationPayloadV1,
        verification: MssqlTargetRegistrationVerificationV1,
    ) -> None: ...

    def attach_protected_properties(self, handle: object, payload: MssqlTargetRegistrationPayloadV1) -> None: ...

    def initialize_heads(self, handle: object, payload: MssqlTargetRegistrationPayloadV1) -> None: ...

    def apply_and_attest_permissions(self, handle: object) -> bytes: ...

    def read_committed_candidate(self, handle: object, target_binding_uuid: UUID) -> MssqlR1ControlStateV1: ...

    def read_server_time(self, handle: object) -> datetime: ...

    def append_control_receipt(self, handle: object, receipt: MssqlR1ControlReceiptV1) -> None: ...

    def prove_control_state(self, handle: object, control_effect_key: bytes) -> MssqlR1ControlFreshProofV1 | None: ...

    def probe_registration_fresh(self, handle: object, registration_id: UUID) -> MssqlR1ControlFreshProofV1 | None: ...


class MssqlTargetRegistrationProvisionerAdapter:
    """Verify a signed registration, then install or rotate it atomically."""

    def __init__(
        self,
        *,
        verifier: BlobSignatureVerifierV1Port,
        session_factory: MssqlTransactionSessionFactoryPort,
        backend: MssqlR1RegistrationTransactionBackend,
        schema_contract_digest: bytes,
        permission_contract_digest: bytes,
        initial_writer_head: MssqlR1WriterHeadControlObservationV1,
        clock: Callable[[], datetime],
        receipt_id_factory: Callable[[], UUID] = uuid4,
    ) -> None:
        self._verifier = verifier
        self._runner = MssqlR1ControlTransactionRunner(session_factory)
        self._backend = backend
        self._schema_digest = require_digest(schema_contract_digest, "schema_contract_digest")
        self._permission_digest = require_digest(permission_contract_digest, "permission_contract_digest")
        self._initial_writer_head = initial_writer_head
        self._clock = clock
        self._receipt_ids = receipt_id_factory

    def provision(self, command: SignedTargetRegistrationCommandV1) -> MssqlR1ControlReceiptV1:
        """Create or rotate one exact registration; exact repeats return its receipt."""

        verification = self._verifier.verify_registration(command)
        verification.validate_for_command(command)
        payload = MssqlTargetRegistrationPayloadV1.from_canonical_bytes(verification.payload_bytes)
        now = require_current_time(self._clock)
        if (
            not payload.issued_at <= verification.verified_at < payload.expires_at
            or not payload.issued_at <= now < payload.expires_at
        ):
            raise MssqlR1ControlError("postgres_mssql_r1.registration_not_current")

        expected_holder: list[MssqlR1ExpectedControlEffectV1] = []

        def work(handle: object) -> MssqlR1ControlReceiptV1:
            physical = payload.physical_identity
            self._backend.lock_physical(handle, physical.physical_object_coordinate_digest)
            self._backend.lock_binding(handle, payload.target_binding_uuid)
            self._backend.assert_physical_target(handle, payload)
            schema_digest = self._backend.install_and_attest_schema(handle)
            if schema_digest != self._schema_digest:
                raise MssqlR1ControlError("postgres_mssql_r1.schema_contract_conflict")
            replay_expected = self._replay_expected(payload, verification)
            existing = self._backend.find_control_proof(handle, replay_expected.control_effect_key)
            if existing is not None:
                expected = self._replay_expected(payload, verification, proof=existing)
                expected_holder.append(expected)
                return validate_control_proof(expected, existing)
            before = self._backend.read_registration_state(handle, payload.target_binding_uuid)
            expected = self._expected(payload, verification, before)
            expected_holder.append(expected)
            self._mutate(handle, payload, verification, before)
            permission_digest = self._backend.apply_and_attest_permissions(handle)
            if permission_digest != self._permission_digest:
                raise MssqlR1ControlError("postgres_mssql_r1.permission_contract_conflict")
            candidate = self._backend.read_committed_candidate(handle, payload.target_binding_uuid)
            committed_at = self._backend.read_server_time(handle)
            if not payload.issued_at <= committed_at < payload.expires_at:
                raise MssqlR1ControlError("postgres_mssql_r1.registration_expired_during_provision")
            receipt = build_control_receipt(
                expected,
                candidate,
                receipt_id=self._receipt_ids(),
                committed_at=committed_at,
            )
            self._backend.append_control_receipt(handle, receipt)
            validate_control_proof(expected, self._backend.prove_control_state(handle, expected.control_effect_key))
            return receipt

        def recover(handle: object) -> MssqlR1ControlFreshProofV1 | None:
            if not expected_holder:
                return None
            expected = expected_holder[-1]
            self._backend.lock_physical(handle, expected.physical_identity.physical_object_coordinate_digest)
            self._backend.lock_binding(handle, expected.target_binding_uuid)
            self._backend.assert_physical_target(handle, payload)
            proof = self._backend.prove_control_state(handle, expected.control_effect_key)
            if proof is not None:
                validate_control_proof(expected, proof)
            return proof

        return self._runner.run(work, recover)

    def probe_fresh(self, registration_id: UUID) -> MssqlR1ControlFreshProofV1 | None:
        """Read an exact registration/control proof on a new target-only session."""

        def probe(handle: object) -> MssqlR1ControlFreshProofV1 | None:
            proof = self._backend.probe_registration_fresh(handle, registration_id)
            if proof is not None and proof.receipt.registration_id != registration_id:
                raise MssqlR1ControlError("postgres_mssql_r1.control_receipt_conflict")
            return proof

        return self._runner.probe_fresh(probe)

    def _expected(
        self,
        payload: MssqlTargetRegistrationPayloadV1,
        verification: MssqlTargetRegistrationVerificationV1,
        before: MssqlR1ControlStateV1 | None,
    ) -> MssqlR1ExpectedControlEffectV1:
        operation = (
            MssqlR1ControlOperationV1.PROVISION
            if payload.registration_action is RegistrationActionV1.INITIAL
            else MssqlR1ControlOperationV1.REGISTRATION_ROTATE
        )
        if operation is MssqlR1ControlOperationV1.PROVISION:
            if before is not None:
                raise MssqlR1ControlError("postgres_mssql_r1.registration_already_exists")
            expected_writer = None
            required_writer = self._initial_writer_head
        else:
            if before is None:
                raise MssqlR1ControlError("postgres_mssql_r1.registration_predecessor_missing")
            expected_before = (
                payload.predecessor_registration_id,
                payload.expected_active_registration_revision,
                payload.physical_identity.registered_physical_authority_digest,
                self._schema_digest,
                self._permission_digest,
            )
            observed_before = (
                before.registration_id,
                before.active_registration_revision,
                before.physical_authority_digest,
                before.schema_contract_digest,
                before.permission_contract_digest,
            )
            if observed_before != expected_before:
                raise MssqlR1ControlError("postgres_mssql_r1.registration_predecessor_conflict")
            expected_writer = before.writer_head
            required_writer = before.writer_head
        return MssqlR1ExpectedControlEffectV1(
            operation=operation,
            physical_identity=payload.physical_identity,
            target_binding_uuid=payload.target_binding_uuid,
            registration_id=payload.registration_id,
            input_payload_digest=payload.payload_digest,
            verification_receipt_digest=verification.receipt_digest,
            expected_active_registration_revision=payload.expected_active_registration_revision,
            expected_writer_head=expected_writer,
            required_observed_writer_head=required_writer,
            schema_contract_digest=self._schema_digest,
            permission_contract_digest=self._permission_digest,
        )

    def _replay_expected(
        self,
        payload: MssqlTargetRegistrationPayloadV1,
        verification: MssqlTargetRegistrationVerificationV1,
        *,
        proof: MssqlR1ControlFreshProofV1 | None = None,
    ) -> MssqlR1ExpectedControlEffectV1:
        operation = (
            MssqlR1ControlOperationV1.PROVISION
            if payload.registration_action is RegistrationActionV1.INITIAL
            else MssqlR1ControlOperationV1.REGISTRATION_ROTATE
        )
        if operation is MssqlR1ControlOperationV1.PROVISION:
            previous = None
            observed = self._initial_writer_head
        elif proof is None:
            previous = self._initial_writer_head
            observed = self._initial_writer_head
        else:
            receipt = proof.receipt
            if receipt.expected_writer_head_revision is None or receipt.expected_writer_head_digest is None:
                raise MssqlR1ControlError("postgres_mssql_r1.control_writer_head_conflict")
            previous = MssqlR1WriterHeadControlObservationV1(
                receipt.expected_writer_head_revision,
                receipt.expected_writer_head_digest,
            )
            observed = previous
        return MssqlR1ExpectedControlEffectV1(
            operation=operation,
            physical_identity=payload.physical_identity,
            target_binding_uuid=payload.target_binding_uuid,
            registration_id=payload.registration_id,
            input_payload_digest=payload.payload_digest,
            verification_receipt_digest=verification.receipt_digest,
            expected_active_registration_revision=payload.expected_active_registration_revision,
            expected_writer_head=previous,
            required_observed_writer_head=observed,
            schema_contract_digest=self._schema_digest,
            permission_contract_digest=self._permission_digest,
        )

    def _mutate(
        self,
        handle: object,
        payload: MssqlTargetRegistrationPayloadV1,
        verification: MssqlTargetRegistrationVerificationV1,
        before: MssqlR1ControlStateV1 | None,
    ) -> None:
        if payload.registration_action is RegistrationActionV1.INITIAL:
            if before is not None:
                raise MssqlR1ControlError("postgres_mssql_r1.registration_already_exists")
            self._backend.create_initial_registration(handle, payload, verification)
            self._backend.attach_protected_properties(handle, payload)
            self._backend.initialize_heads(handle, payload)
            return
        self._backend.rotate_registration(handle, payload, verification)


__all__ = [
    "MssqlR1RegistrationTransactionBackend",
    "MssqlTargetRegistrationProvisionerAdapter",
]
