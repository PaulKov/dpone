"""Verified current-registration and rotation-stable target authority."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

from dpone.contracts.mssql_r1_v3_identity import (
    canonical_bytes,
    canonical_utc_text,
    decode_canonical_bytes,
    expect_bytes,
    expect_int,
    expect_text,
    expect_uuid,
    parse_canonical_utc_text,
    require_count,
    require_digest,
    require_positive,
    require_sql_int,
    require_uuid,
)
from dpone.contracts.mssql_r1_v3_registered_target_catalog import MssqlR1RegisteredTargetCatalogV1
from dpone.contracts.mssql_r1_v3_registration import (
    MssqlTargetRegistrationPayloadV1,
    MssqlTargetRegistrationVerificationV1,
)
from dpone.contracts.postgres_mssql_type_target_enums import (
    authority_decode,
    authority_validation,
    reject,
    require_identifier_v1,
    require_semantic_id_v1,
)

_STABLE = b"dpone-mssql-r1-rotation-stable-target-authority-v1\0"
_HEAD = b"dpone-mssql-r1-active-registration-head-observation-v1\0"
_ADMISSION = b"dpone-mssql-r1-registration-admission-evidence-v1\0"


@dataclass(frozen=True, slots=True)
class MssqlR1RotationStableTargetAuthorityV1:
    profile_id: str
    capability_tuple_digest: bytes
    resolved_profile_digest: bytes
    route_source_authority_sha256: bytes
    target_object_profile: str
    catalog_projection_version: str
    revocation_revision: int
    target_binding_uuid: UUID
    target_object_uuid: UUID
    recovery_domain_uuid: UUID
    recovery_domain_epoch: int
    server_instance_identity_sha256: bytes
    database_guid: UUID
    database_family_guid: UUID
    recovery_fork_guid: UUID
    database_name_digest: bytes
    schema_name_digest: bytes
    object_name_digest: bytes
    database_name: str
    schema_name: str
    object_name: str
    object_id: int
    physical_generation_uuid: UUID
    catalog_contract_digest: bytes
    target_contract_revision: int

    @authority_validation
    def __post_init__(self) -> None:
        require_semantic_id_v1(self.profile_id)
        if (
            type(self.target_object_profile) is not str
            or type(self.catalog_projection_version) is not str
            or self.target_object_profile != "ordinary_disk_rowstore_v1"
            or self.catalog_projection_version != "dpone-mssql-target-catalog-v1"
        ):
            reject("target_profile_unsupported", operator=True)
        for name in (
            "capability_tuple_digest",
            "resolved_profile_digest",
            "route_source_authority_sha256",
            "server_instance_identity_sha256",
            "database_name_digest",
            "schema_name_digest",
            "object_name_digest",
            "catalog_contract_digest",
        ):
            if type(getattr(self, name)) is not bytes:
                reject("invalid_facet")
            require_digest(getattr(self, name), name)
        for name in (
            "target_binding_uuid",
            "target_object_uuid",
            "recovery_domain_uuid",
            "database_guid",
            "database_family_guid",
            "recovery_fork_guid",
            "physical_generation_uuid",
        ):
            if type(getattr(self, name)) is not UUID:
                reject("invalid_facet")
            require_uuid(getattr(self, name), name)
        for name in ("database_name", "schema_name", "object_name"):
            if type(getattr(self, name)) is not str:
                reject("identifier_invalid")
            require_identifier_v1(getattr(self, name))
        if any(
            type(value) is not int
            for value in (self.object_id, self.recovery_domain_epoch, self.target_contract_revision)
        ):
            reject("invalid_facet")
        require_sql_int(self.object_id, "object ID")
        require_positive(self.recovery_domain_epoch, "recovery domain epoch")
        require_positive(self.target_contract_revision, "target contract revision")
        if type(self.revocation_revision) is not int:
            reject("invalid_facet")
        require_count(self.revocation_revision, "revocation revision")

    @classmethod
    def from_registration(
        cls, registration: MssqlTargetRegistrationPayloadV1
    ) -> MssqlR1RotationStableTargetAuthorityV1:
        if type(registration) is not MssqlTargetRegistrationPayloadV1:
            reject("registration_verification_mismatch", operator=True)
        omitted = {
            "registration_id",
            "registration_action",
            "predecessor_registration_id",
            "expected_active_registration_revision",
            "issued_at",
            "expires_at",
            "nonce",
        }
        return cls(*(getattr(registration, name) for name in registration.__dataclass_fields__ if name not in omitted))

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(_STABLE, tuple(getattr(self, name) for name in self.__dataclass_fields__))

    @property
    def digest(self) -> bytes:
        return hashlib.sha256(self.canonical_bytes).digest()

    @classmethod
    @authority_decode
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1RotationStableTargetAuthorityV1:
        values = list(decode_canonical_bytes(payload, _STABLE, field_count=25))
        for index in (0, 4, 5, 18, 19, 20):
            values[index] = expect_text(values[index], "stable text")
        for index in (1, 2, 3, 11, 15, 16, 17, 23):
            values[index] = require_digest(values[index], "stable digest")
        for index in (7, 8, 9, 12, 13, 14, 22):
            values[index] = expect_uuid(values[index], "stable UUID")
        for index in (6, 10, 21, 24):
            values[index] = expect_int(values[index], "stable integer")
        return cls(*values)  # type: ignore[arg-type]


@dataclass(frozen=True, slots=True)
class MssqlR1ActiveRegistrationHeadObservationV1:
    target_binding_uuid: UUID
    physical_coordinate_digest: bytes
    target_object_uuid: UUID
    active_registration_id: UUID
    active_registration_revision: int
    active_registration_payload_digest: bytes
    registration_verification_policy_digest: bytes
    registration_verification_receipt_digest: bytes
    revocation_revision: int
    registered_physical_authority_digest: bytes
    schema_contract_digest: bytes
    permission_contract_digest: bytes
    last_control_receipt_id: UUID
    last_control_receipt_digest: bytes
    projection_revision: int
    updated_at: datetime
    row_token: bytes
    observation_contract_digest: bytes
    schema_lock_binding_digest: bytes
    observed_at: datetime

    @authority_validation
    def __post_init__(self) -> None:
        for name in ("target_binding_uuid", "target_object_uuid", "active_registration_id", "last_control_receipt_id"):
            if type(getattr(self, name)) is not UUID:
                reject("invalid_facet")
            require_uuid(getattr(self, name), name)
        for name in (
            "physical_coordinate_digest",
            "active_registration_payload_digest",
            "registration_verification_policy_digest",
            "registration_verification_receipt_digest",
            "registered_physical_authority_digest",
            "schema_contract_digest",
            "permission_contract_digest",
            "last_control_receipt_digest",
            "observation_contract_digest",
            "schema_lock_binding_digest",
        ):
            if type(getattr(self, name)) is not bytes:
                reject("invalid_facet")
            require_digest(getattr(self, name), name)
        if any(
            type(value) is not int
            for value in (
                self.active_registration_revision,
                self.revocation_revision,
                self.projection_revision,
            )
        ):
            reject("invalid_facet")
        require_positive(self.active_registration_revision, "registration revision")
        require_positive(self.projection_revision, "projection revision")
        if type(self.revocation_revision) is not int:
            reject("invalid_facet")
        require_count(self.revocation_revision, "revocation revision")
        if type(self.row_token) is not bytes or len(self.row_token) != 8:
            reject("invalid_facet")
        if type(self.updated_at) is not datetime or type(self.observed_at) is not datetime:
            reject("invalid_facet")
        canonical_utc_text(self.updated_at, "updated at")
        canonical_utc_text(self.observed_at, "observed at")
        if self.updated_at > self.observed_at:
            reject("active_head_stale")

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(_HEAD, tuple(getattr(self, name) for name in self.__dataclass_fields__))

    @classmethod
    @authority_decode
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1ActiveRegistrationHeadObservationV1:
        values = list(decode_canonical_bytes(payload, _HEAD, field_count=20))
        for index in (0, 2, 3, 12):
            values[index] = expect_uuid(values[index], "head UUID")
        for index in (1, 5, 6, 7, 9, 10, 11, 13, 17, 18):
            values[index] = require_digest(values[index], "head digest")
        for index in (4, 8, 14):
            values[index] = expect_int(values[index], "head integer")
        values[15] = parse_canonical_utc_text(values[15], "updated at")
        values[16] = expect_bytes(values[16], "row token")
        values[19] = parse_canonical_utc_text(values[19], "observed at")
        return cls(*values)  # type: ignore[arg-type]


@dataclass(frozen=True, slots=True)
class MssqlR1RegistrationAdmissionEvidenceV1:
    registration_payload: MssqlTargetRegistrationPayloadV1
    registration_verification: MssqlTargetRegistrationVerificationV1
    active_head: MssqlR1ActiveRegistrationHeadObservationV1
    observed_at: datetime
    stable_target: MssqlR1RotationStableTargetAuthorityV1
    target_catalog: MssqlR1RegisteredTargetCatalogV1

    @classmethod
    @authority_validation
    def create(cls, registration, verification, head, catalog, descriptor, security, schema_lock_digest):
        if (
            type(registration) is not MssqlTargetRegistrationPayloadV1
            or type(verification) is not MssqlTargetRegistrationVerificationV1
            or type(head) is not MssqlR1ActiveRegistrationHeadObservationV1
            or type(catalog) is not MssqlR1RegisteredTargetCatalogV1
        ):
            reject("authority_splice")
        stable = MssqlR1RotationStableTargetAuthorityV1.from_registration(registration)
        value = cls(registration, verification, head, head.observed_at, stable, catalog)
        value.validate_against_authorities(descriptor, security, schema_lock_digest)
        return value

    @classmethod
    @authority_decode
    def create_from_persisted_bytes(
        cls,
        registration_payload: bytes,
        registration_verification: bytes,
        active_head: MssqlR1ActiveRegistrationHeadObservationV1,
        target_catalog: bytes,
        descriptor,
        security,
        schema_lock_digest: bytes,
    ) -> MssqlR1RegistrationAdmissionEvidenceV1:
        """Decode the exact persisted authority bytes before current admission."""

        registration = MssqlTargetRegistrationPayloadV1.from_canonical_bytes(registration_payload)
        verification = MssqlTargetRegistrationVerificationV1.from_canonical_bytes(registration_verification)
        catalog = MssqlR1RegisteredTargetCatalogV1.create(target_catalog)
        return cls.create(
            registration,
            verification,
            active_head,
            catalog,
            descriptor,
            security,
            schema_lock_digest,
        )

    @authority_validation
    def __post_init__(self) -> None:
        expected = (
            MssqlTargetRegistrationPayloadV1,
            MssqlTargetRegistrationVerificationV1,
            MssqlR1ActiveRegistrationHeadObservationV1,
            MssqlR1RotationStableTargetAuthorityV1,
            MssqlR1RegisteredTargetCatalogV1,
        )
        actual = (
            self.registration_payload,
            self.registration_verification,
            self.active_head,
            self.stable_target,
            self.target_catalog,
        )
        if any(type(value) is not contract for value, contract in zip(actual, expected, strict=True)):
            reject("authority_splice", operator=True)
        if type(self.observed_at) is not datetime:
            reject("invalid_facet")
        canonical_utc_text(self.observed_at, "observed at")

    def validate_against_authorities(self, descriptor, security, schema_lock_digest) -> None:
        from dpone.contracts.mssql_r1_v3_verified_target_validation import validate_registration_admission

        validate_registration_admission(
            self.registration_payload,
            self.registration_verification,
            self.active_head,
            self.observed_at,
            self.stable_target,
            self.target_catalog,
            descriptor,
            security,
            schema_lock_digest,
        )

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(
            _ADMISSION,
            (
                self.registration_payload.canonical_bytes,
                self.registration_verification.canonical_bytes,
                self.active_head.canonical_bytes,
                self.observed_at,
                self.stable_target.canonical_bytes,
                self.target_catalog.canonical_bytes,
            ),
        )

    @classmethod
    @authority_decode
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1RegistrationAdmissionEvidenceV1:
        values = decode_canonical_bytes(payload, _ADMISSION, field_count=6)
        return cls(
            MssqlTargetRegistrationPayloadV1.from_canonical_bytes(expect_bytes(values[0], "registration")),
            MssqlTargetRegistrationVerificationV1.from_canonical_bytes(expect_bytes(values[1], "verification")),
            MssqlR1ActiveRegistrationHeadObservationV1.from_canonical_bytes(expect_bytes(values[2], "head")),
            parse_canonical_utc_text(values[3], "observed at"),
            MssqlR1RotationStableTargetAuthorityV1.from_canonical_bytes(expect_bytes(values[4], "stable target")),
            MssqlR1RegisteredTargetCatalogV1.from_canonical_bytes(expect_bytes(values[5], "catalog")),
        )


__all__ = [
    "MssqlR1ActiveRegistrationHeadObservationV1",
    "MssqlR1RegistrationAdmissionEvidenceV1",
    "MssqlR1RotationStableTargetAuthorityV1",
]
