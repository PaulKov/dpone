"""Create-only signed generation and revoked-registration authority imports."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Protocol
from uuid import UUID, uuid4

from dpone.contracts.mssql_r1_v3_authority import (
    MssqlGenerationAuthoritySetIssuancePayloadV1,
    MssqlGenerationAuthoritySetV2,
    MssqlRevokedRegistrationCompletionOverridePayloadV1,
)
from dpone.contracts.mssql_r1_v3_control_proof import (
    MssqlR1ControlOperationV1,
    MssqlR1V3ContractError,
    MssqlTargetPhysicalIdentityV1,
    require_count,
    require_digest,
    require_uuid,
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
    from dpone.contracts.mssql_r1_v3_authority import (
        MssqlSignedPayloadVerificationV1,
        SignedGenerationAuthoritySetCommandV1,
        SignedRevokedOverrideCommandV1,
    )
    from dpone.contracts.mssql_r1_v3_control import MssqlR1ControlReceiptV1
    from dpone.contracts.mssql_r1_v3_control_proof import MssqlR1ControlFreshProofV1
    from dpone.ports.mssql_r1_v3 import BlobSignatureVerifierV1Port
    from dpone.ports.mssql_target_authority_v2 import MssqlTransactionSessionFactoryPort


@dataclass(frozen=True, slots=True)
class MssqlR1AuthorityImportTargetV1:
    """Environment-resolved target authority; never populated from an import bundle."""

    physical_identity: MssqlTargetPhysicalIdentityV1
    target_binding_uuid: UUID
    registration_id: UUID
    registration_payload_digest: bytes
    schema_contract_digest: bytes
    permission_contract_digest: bytes
    registration_revocation_revision: int = 0
    registration_verification_policy_digest: bytes | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.physical_identity, MssqlTargetPhysicalIdentityV1):
            raise MssqlR1V3ContractError("authority import requires physical target identity")
        require_uuid(self.target_binding_uuid, "target_binding_uuid")
        require_uuid(self.registration_id, "registration_id")
        for name in (
            "registration_payload_digest",
            "schema_contract_digest",
            "permission_contract_digest",
        ):
            require_digest(getattr(self, name), name)
        require_count(self.registration_revocation_revision, "registration_revocation_revision")
        if self.registration_verification_policy_digest is not None:
            require_digest(
                self.registration_verification_policy_digest,
                "registration_verification_policy_digest",
            )


class MssqlR1AuthorityImportTransactionBackend(Protocol):
    """Target-local import primitives sharing one provisioner transaction."""

    def lock_physical(self, handle: object, physical_coordinate_digest: bytes) -> None: ...

    def lock_binding(self, handle: object, target_binding_uuid: UUID) -> None: ...

    def lock_and_reprove_authority_set_effect(
        self,
        handle: object,
        target: MssqlR1AuthorityImportTargetV1,
        payload: MssqlGenerationAuthoritySetIssuancePayloadV1,
        authority_set: MssqlGenerationAuthoritySetV2,
    ) -> None: ...

    def lock_and_reprove_revoked_override_effect(
        self,
        handle: object,
        target: MssqlR1AuthorityImportTargetV1,
        payload: MssqlRevokedRegistrationCompletionOverridePayloadV1,
    ) -> None: ...

    def read_target_state(self, handle: object, target_binding_uuid: UUID) -> MssqlR1ControlStateV1: ...

    def assert_import_target(
        self,
        handle: object,
        target: MssqlR1AuthorityImportTargetV1,
        registration_revocation_revision: int,
    ) -> None: ...

    def find_control_proof(self, handle: object, control_effect_key: bytes) -> MssqlR1ControlFreshProofV1 | None: ...

    def create_authority_set(
        self,
        handle: object,
        payload: MssqlGenerationAuthoritySetIssuancePayloadV1,
        verification: MssqlSignedPayloadVerificationV1,
    ) -> None: ...

    def create_revoked_override(
        self,
        handle: object,
        payload: MssqlRevokedRegistrationCompletionOverridePayloadV1,
        verification: MssqlSignedPayloadVerificationV1,
    ) -> None: ...

    def read_imported_authority_set_digest(self, handle: object, issuance_id: UUID) -> bytes: ...

    def read_imported_override_digest(self, handle: object, override_id: UUID) -> bytes: ...

    def read_server_time(self, handle: object) -> datetime: ...

    def append_control_receipt(self, handle: object, receipt: MssqlR1ControlReceiptV1) -> None: ...

    def prove_control_state(self, handle: object, control_effect_key: bytes) -> MssqlR1ControlFreshProofV1 | None: ...


class MssqlGenerationAuthorityImporterAdapter:
    """Verify signed imports before I/O and commit each complete set atomically."""

    def __init__(
        self,
        *,
        verifier: BlobSignatureVerifierV1Port,
        session_factory: MssqlTransactionSessionFactoryPort,
        backend: MssqlR1AuthorityImportTransactionBackend,
        target: MssqlR1AuthorityImportTargetV1,
        clock: Callable[[], datetime],
        receipt_id_factory: Callable[[], UUID] = uuid4,
    ) -> None:
        self._verifier = verifier
        self._runner = MssqlR1ControlTransactionRunner(session_factory)
        self._backend = backend
        self._target = target
        self._clock = clock
        self._receipt_ids = receipt_id_factory

    def import_authority_set(self, command: SignedGenerationAuthoritySetCommandV1) -> MssqlR1ControlReceiptV1:
        verification = self._verifier.verify_authority_set(command)
        verification.validate_for_command(command)
        payload = MssqlGenerationAuthoritySetIssuancePayloadV1.from_canonical_bytes(verification.payload_bytes)
        now = require_current_time(self._clock)
        if (
            not payload.issued_at <= verification.verified_at < payload.expires_at
            or not payload.issued_at <= now < payload.expires_at
        ):
            raise MssqlR1ControlError("postgres_mssql_r1.authority_not_current")
        authority_set = MssqlGenerationAuthoritySetV2.from_canonical_bytes(payload.authority_set_bytes)
        self._validate_registration_binding(
            payload.target_binding_uuid,
            payload.registration_id,
            payload.registration_payload_digest,
            payload.registration_revocation_revision,
        )
        return self._import(
            operation=MssqlR1ControlOperationV1.AUTHORITY_IMPORT,
            payload_digest=payload.payload_digest,
            verification=verification,
            reprove=lambda handle: self._backend.lock_and_reprove_authority_set_effect(
                handle,
                self._target,
                payload,
                authority_set,
            ),
            affected_authority_set_digest=payload.authority_set_digest,
            affected_override_payload_digest=None,
            mutate=lambda handle: self._backend.create_authority_set(handle, payload, verification),
            observe=lambda handle: self._backend.read_imported_authority_set_digest(handle, payload.issuance_id),
            expires_at=payload.expires_at,
            not_before=payload.issued_at,
        )

    def import_revoked_override(self, command: SignedRevokedOverrideCommandV1) -> MssqlR1ControlReceiptV1:
        verification = self._verifier.verify_revoked_override(command)
        verification.validate_for_command(command)
        payload = MssqlRevokedRegistrationCompletionOverridePayloadV1.from_canonical_bytes(verification.payload_bytes)
        now = require_current_time(self._clock)
        if verification.verified_at >= payload.expires_at or now >= payload.expires_at:
            raise MssqlR1ControlError("postgres_mssql_r1.override_not_current")
        self._validate_registration_binding(
            payload.target_binding_uuid,
            payload.registration_id,
            payload.registration_payload_digest,
            self._target.registration_revocation_revision,
        )
        if (
            self._target.registration_verification_policy_digest is None
            or payload.expected_verification_policy_digest != self._target.registration_verification_policy_digest
        ):
            raise MssqlR1ControlError("postgres_mssql_r1.override_registration_policy_conflict")
        if self._target.registration_revocation_revision < 1:
            raise MssqlR1ControlError("postgres_mssql_r1.override_registration_not_revoked")
        return self._import(
            operation=MssqlR1ControlOperationV1.REVOKED_REGISTRATION_OVERRIDE_IMPORT,
            payload_digest=payload.payload_digest,
            verification=verification,
            reprove=lambda handle: self._backend.lock_and_reprove_revoked_override_effect(
                handle,
                self._target,
                payload,
            ),
            affected_authority_set_digest=None,
            affected_override_payload_digest=payload.payload_digest,
            mutate=lambda handle: self._backend.create_revoked_override(handle, payload, verification),
            observe=lambda handle: self._backend.read_imported_override_digest(handle, payload.override_id),
            expires_at=payload.expires_at,
            not_before=None,
        )

    def probe_authority_import_fresh(self, control_effect_key: bytes) -> MssqlR1ControlFreshProofV1 | None:
        require_digest(control_effect_key, "control_effect_key")

        def probe(handle: object) -> MssqlR1ControlFreshProofV1 | None:
            proof = self._backend.prove_control_state(handle, control_effect_key)
            if proof is not None and proof.receipt.control_effect_key != control_effect_key:
                raise MssqlR1ControlError("postgres_mssql_r1.control_receipt_conflict")
            return proof

        return self._runner.probe_fresh(probe)

    def probe_override_import_fresh(self, control_effect_key: bytes) -> MssqlR1ControlFreshProofV1 | None:
        return self.probe_authority_import_fresh(control_effect_key)

    def _import(
        self,
        *,
        operation: MssqlR1ControlOperationV1,
        payload_digest: bytes,
        verification: MssqlSignedPayloadVerificationV1,
        reprove: Callable[[object], None],
        affected_authority_set_digest: bytes | None,
        affected_override_payload_digest: bytes | None,
        mutate: Callable[[object], None],
        observe: Callable[[object], bytes],
        expires_at: datetime,
        not_before: datetime | None,
    ) -> MssqlR1ControlReceiptV1:
        expected_holder: list[MssqlR1ExpectedControlEffectV1] = []
        required_import_digest = affected_authority_set_digest or affected_override_payload_digest

        def work(handle: object) -> MssqlR1ControlReceiptV1:
            target = self._target
            self._backend.lock_physical(handle, target.physical_identity.physical_object_coordinate_digest)
            self._backend.lock_binding(handle, target.target_binding_uuid)
            reprove(handle)
            self._backend.assert_import_target(
                handle,
                target,
                target.registration_revocation_revision,
            )
            before = self._backend.read_target_state(handle, target.target_binding_uuid)
            self._validate_target_state(before)
            expected = MssqlR1ExpectedControlEffectV1(
                operation=operation,
                physical_identity=target.physical_identity,
                target_binding_uuid=target.target_binding_uuid,
                registration_id=target.registration_id,
                input_payload_digest=payload_digest,
                verification_receipt_digest=verification.receipt_digest,
                expected_active_registration_revision=before.active_registration_revision,
                expected_writer_head=before.writer_head,
                required_observed_writer_head=before.writer_head,
                schema_contract_digest=target.schema_contract_digest,
                permission_contract_digest=target.permission_contract_digest,
                affected_authority_set_digest=affected_authority_set_digest,
                affected_override_payload_digest=affected_override_payload_digest,
            )
            expected_holder.append(expected)
            existing = self._backend.find_control_proof(handle, expected.control_effect_key)
            if existing is not None:
                receipt = validate_control_proof(expected, existing)
                self._require_imported_digest(observe(handle), required_import_digest)
                return receipt
            mutate(handle)
            self._require_imported_digest(observe(handle), required_import_digest)
            candidate = MssqlR1ControlStateV1(
                registration_id=before.registration_id,
                active_registration_revision=before.active_registration_revision,
                physical_authority_digest=before.physical_authority_digest,
                schema_contract_digest=before.schema_contract_digest,
                permission_contract_digest=before.permission_contract_digest,
                writer_head=before.writer_head,
                authority_set_digest=affected_authority_set_digest,
                override_payload_digest=affected_override_payload_digest,
            )
            committed_at = self._backend.read_server_time(handle)
            if committed_at >= expires_at or (not_before is not None and committed_at < not_before):
                raise MssqlR1ControlError("postgres_mssql_r1.authority_expired_during_import")
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
            reprove(handle)
            self._backend.assert_import_target(
                handle,
                self._target,
                self._target.registration_revocation_revision,
            )
            self._validate_target_state(self._backend.read_target_state(handle, expected.target_binding_uuid))
            proof = self._backend.prove_control_state(handle, expected.control_effect_key)
            if proof is not None:
                validate_control_proof(expected, proof)
                self._require_imported_digest(observe(handle), required_import_digest)
            return proof

        return self._runner.run(work, recover)

    def _validate_registration_binding(
        self,
        binding: UUID,
        registration: UUID,
        payload_digest: bytes,
        revocation_revision: int,
    ) -> None:
        target = self._target
        if (
            binding,
            registration,
            payload_digest,
            revocation_revision,
        ) != (
            target.target_binding_uuid,
            target.registration_id,
            target.registration_payload_digest,
            target.registration_revocation_revision,
        ):
            raise MssqlR1ControlError("postgres_mssql_r1.authority_registration_conflict")

    @staticmethod
    def _require_imported_digest(observed_digest: bytes, required_digest: bytes | None) -> None:
        if required_digest is None or observed_digest != required_digest:
            raise MssqlR1ControlError("postgres_mssql_r1.authority_import_readback_conflict")

    def _validate_target_state(self, state: MssqlR1ControlStateV1) -> None:
        target = self._target
        if (
            state.registration_id,
            state.physical_authority_digest,
            state.schema_contract_digest,
            state.permission_contract_digest,
            state.authority_set_digest,
            state.override_payload_digest,
        ) != (
            target.registration_id,
            target.physical_identity.registered_physical_authority_digest,
            target.schema_contract_digest,
            target.permission_contract_digest,
            None,
            None,
        ):
            raise MssqlR1ControlError("postgres_mssql_r1.authority_target_state_conflict")


__all__ = [
    "MssqlGenerationAuthorityImporterAdapter",
    "MssqlR1AuthorityImportTargetV1",
    "MssqlR1AuthorityImportTransactionBackend",
]
