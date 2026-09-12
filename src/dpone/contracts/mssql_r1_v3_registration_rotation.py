"""Exact transition-only evidence for one R1 target-registration rotation."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass

from dpone.contracts.mssql_r1_v3_control import (
    MssqlR1ControlOperationV1,
    MssqlR1ControlReceiptV1,
    build_control_effect_key,
)
from dpone.contracts.mssql_r1_v3_identity import (
    MssqlR1V3ContractError,
    canonical_bytes,
    decode_canonical_bytes,
    expect_bytes,
)
from dpone.contracts.mssql_r1_v3_registration import RegistrationActionV1
from dpone.contracts.mssql_r1_v3_verified_target_authority import (
    MssqlR1ActiveRegistrationHeadObservationV1,
    MssqlR1RegistrationAdmissionEvidenceV1,
)
from dpone.contracts.postgres_mssql_type_target_enums import authority_decode, reject

_ROTATION = b"dpone-mssql-r1-registration-rotation-transition-evidence-v1\0"


@dataclass(frozen=True, slots=True, init=False)
class MssqlR1RegistrationRotationTransitionEvidenceV1:
    """Historical proof used only while installing an adjacent rotation."""

    predecessor_head: MssqlR1ActiveRegistrationHeadObservationV1
    successor_head: MssqlR1ActiveRegistrationHeadObservationV1
    rotation_control_receipt_payload: bytes
    rotation_control_receipt_digest: bytes

    def __init__(self, *args: object, **kwargs: object) -> None:
        del args, kwargs
        reject("rotation_transition_mismatch")

    @classmethod
    def _from_components(
        cls,
        predecessor_head: MssqlR1ActiveRegistrationHeadObservationV1,
        successor_head: MssqlR1ActiveRegistrationHeadObservationV1,
        receipt_payload: bytes,
        receipt_digest: bytes,
    ) -> MssqlR1RegistrationRotationTransitionEvidenceV1:
        value = object.__new__(cls)
        for field, item in zip(
            cls.__dataclass_fields__,
            (predecessor_head, successor_head, receipt_payload, receipt_digest),
            strict=True,
        ):
            object.__setattr__(value, field, item)
        value._validate_self_contained()
        return value

    @classmethod
    def create(
        cls,
        predecessor: MssqlR1RegistrationAdmissionEvidenceV1,
        successor: MssqlR1RegistrationAdmissionEvidenceV1,
        receipt: MssqlR1ControlReceiptV1,
        descriptor,
        security,
        schema_lock_digest: bytes,
    ) -> MssqlR1RegistrationRotationTransitionEvidenceV1:
        """Bind the receipt to both exact admitted registration authorities."""

        cls._require_contract_types(predecessor, successor, receipt)
        predecessor.validate_against_authorities(descriptor, security, schema_lock_digest)
        successor.validate_against_authorities(descriptor, security, schema_lock_digest)
        cls._validate_authorities(predecessor, successor, receipt)
        return cls._from_components(
            predecessor.active_head,
            successor.active_head,
            receipt.canonical_bytes,
            receipt.receipt_digest,
        )

    def _validate_self_contained(self) -> None:
        if (
            type(self.predecessor_head) is not MssqlR1ActiveRegistrationHeadObservationV1
            or type(self.successor_head) is not MssqlR1ActiveRegistrationHeadObservationV1
            or type(self.rotation_control_receipt_payload) is not bytes
            or type(self.rotation_control_receipt_digest) is not bytes
            or len(self.rotation_control_receipt_digest) != 32
        ):
            reject("rotation_transition_mismatch")
        if hashlib.sha256(self.rotation_control_receipt_payload).digest() != self.rotation_control_receipt_digest:
            reject("rotation_transition_mismatch")
        try:
            receipt = MssqlR1ControlReceiptV1.from_canonical_bytes(self.rotation_control_receipt_payload)
        except MssqlR1V3ContractError:
            reject("rotation_transition_mismatch")
        self._validate_heads_and_receipt(self.predecessor_head, self.successor_head, receipt)

    @staticmethod
    def _validate_heads_and_receipt(predecessor, successor, receipt) -> None:
        expected_effect = build_control_effect_key(
            MssqlR1ControlOperationV1.REGISTRATION_ROTATE,
            predecessor.physical_coordinate_digest,
            successor.active_registration_payload_digest,
            predecessor.active_registration_revision,
        )
        if (
            receipt.operation is not MssqlR1ControlOperationV1.REGISTRATION_ROTATE
            or predecessor.target_binding_uuid != successor.target_binding_uuid
            or predecessor.target_object_uuid != successor.target_object_uuid
            or predecessor.physical_coordinate_digest != successor.physical_coordinate_digest
            or predecessor.registered_physical_authority_digest != successor.registered_physical_authority_digest
            or predecessor.schema_contract_digest != successor.schema_contract_digest
            or predecessor.permission_contract_digest != successor.permission_contract_digest
            or predecessor.observation_contract_digest != successor.observation_contract_digest
            or predecessor.schema_lock_binding_digest != successor.schema_lock_binding_digest
            or successor.active_registration_revision != predecessor.active_registration_revision + 1
            or receipt.control_effect_key != expected_effect
            or receipt.physical_object_coordinate_digest != successor.physical_coordinate_digest
            or receipt.registered_physical_authority_digest != successor.registered_physical_authority_digest
            or receipt.target_binding_uuid != successor.target_binding_uuid
            or receipt.registration_id != successor.active_registration_id
            or receipt.input_payload_digest != successor.active_registration_payload_digest
            or receipt.verification_receipt_digest != successor.registration_verification_receipt_digest
            or receipt.expected_active_registration_revision != predecessor.active_registration_revision
            or receipt.committed_active_registration_revision != successor.active_registration_revision
            or receipt.schema_contract_digest != successor.schema_contract_digest
            or receipt.permission_contract_digest != successor.permission_contract_digest
            or receipt.control_receipt_id != successor.last_control_receipt_id
            or receipt.receipt_digest != successor.last_control_receipt_digest
            or not predecessor.observed_at <= receipt.committed_at <= successor.observed_at
        ):
            reject("rotation_transition_mismatch")

    @staticmethod
    def _require_contract_types(predecessor, successor, receipt) -> None:
        if (
            type(predecessor) is not MssqlR1RegistrationAdmissionEvidenceV1
            or type(successor) is not MssqlR1RegistrationAdmissionEvidenceV1
            or type(receipt) is not MssqlR1ControlReceiptV1
        ):
            reject("rotation_transition_mismatch")

    @staticmethod
    def _validate_authorities(predecessor, successor, receipt) -> None:
        MssqlR1RegistrationRotationTransitionEvidenceV1._require_contract_types(predecessor, successor, receipt)
        registration = successor.registration_payload
        if (
            registration.registration_action is not RegistrationActionV1.ROTATE
            or registration.predecessor_registration_id != predecessor.registration_payload.registration_id
            or registration.expected_active_registration_revision
            != predecessor.active_head.active_registration_revision
            or predecessor.stable_target.canonical_bytes != successor.stable_target.canonical_bytes
            or predecessor.target_catalog.canonical_bytes != successor.target_catalog.canonical_bytes
            or registration.catalog_contract_digest != successor.target_catalog.digest
        ):
            reject("rotation_transition_mismatch")
        MssqlR1RegistrationRotationTransitionEvidenceV1._validate_heads_and_receipt(
            predecessor.active_head,
            successor.active_head,
            receipt,
        )

    def validate_against_authorities(
        self,
        predecessor: MssqlR1RegistrationAdmissionEvidenceV1,
        successor: MssqlR1RegistrationAdmissionEvidenceV1,
        descriptor,
        security,
        schema_lock_digest: bytes,
    ) -> None:
        """Repeat authority binding after decoding self-contained evidence."""

        try:
            receipt = MssqlR1ControlReceiptV1.from_canonical_bytes(self.rotation_control_receipt_payload)
        except MssqlR1V3ContractError:
            reject("rotation_transition_mismatch")
        self._require_contract_types(predecessor, successor, receipt)
        predecessor.validate_against_authorities(descriptor, security, schema_lock_digest)
        successor.validate_against_authorities(descriptor, security, schema_lock_digest)
        self._validate_authorities(predecessor, successor, receipt)
        if self.predecessor_head != predecessor.active_head or self.successor_head != successor.active_head:
            reject("rotation_transition_mismatch")

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(
            _ROTATION,
            (
                self.predecessor_head.canonical_bytes,
                self.successor_head.canonical_bytes,
                self.rotation_control_receipt_payload,
                self.rotation_control_receipt_digest,
            ),
        )

    @classmethod
    @authority_decode
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1RegistrationRotationTransitionEvidenceV1:
        values = decode_canonical_bytes(payload, _ROTATION, field_count=4)
        return cls._from_components(
            MssqlR1ActiveRegistrationHeadObservationV1.from_canonical_bytes(
                expect_bytes(values[0], "predecessor head")
            ),
            MssqlR1ActiveRegistrationHeadObservationV1.from_canonical_bytes(expect_bytes(values[1], "successor head")),
            expect_bytes(values[2], "rotation receipt"),
            expect_bytes(values[3], "rotation receipt digest"),
        )


__all__ = ["MssqlR1RegistrationRotationTransitionEvidenceV1"]
