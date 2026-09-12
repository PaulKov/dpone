"""Signed target-registration contracts for PostgreSQL-to-MSSQL R1 V3."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

from dpone._compat import StrEnum
from dpone.contracts.mssql_r1_v3_identity import (
    MssqlR1V3ContractError,
    MssqlTargetPhysicalIdentityV1,
    canonical_bytes,
    canonical_digest,
    canonical_identifier_digest,
    canonical_utc_text,
    canonical_utf8_fields,
    decode_canonical_bytes,
    decode_canonical_utf8_fields,
    parse_canonical_utc_text,
    require_canonical_text,
    require_count,
    require_digest,
    require_identifier,
    require_positive,
    require_sql_int,
    require_uuid,
    validate_detached_command_binding,
)

_REGISTRATION_DOMAIN = b"dpone-mssql-target-registration-v1\0"
_VERIFICATION_POLICY_DOMAIN = b"dpone-mssql-target-registration-verification-policy-v1\0"
_VERIFICATION_RECEIPT_DOMAIN = b"dpone-mssql-target-registration-verification-v1\0"


class RegistrationActionV1(StrEnum):
    INITIAL = "initial"
    ROTATE = "rotate"


@dataclass(frozen=True, slots=True)
class MssqlTargetRegistrationPayloadV1:
    """Exact signed payload; signature and verification state are deliberately absent."""

    registration_id: UUID
    registration_action: RegistrationActionV1
    predecessor_registration_id: UUID | None
    expected_active_registration_revision: int | None
    issued_at: datetime
    expires_at: datetime
    nonce: bytes
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

    def __post_init__(self) -> None:
        for name in (
            "registration_id",
            "target_binding_uuid",
            "target_object_uuid",
            "recovery_domain_uuid",
            "database_guid",
            "database_family_guid",
            "recovery_fork_guid",
            "physical_generation_uuid",
        ):
            require_uuid(getattr(self, name), name)
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
            require_digest(getattr(self, name), name)
        if not isinstance(self.registration_action, RegistrationActionV1):
            raise MssqlR1V3ContractError("registration_action is unsupported")
        canonical_utc_text(self.issued_at, "issued_at")
        canonical_utc_text(self.expires_at, "expires_at")
        if self.expires_at <= self.issued_at:
            raise MssqlR1V3ContractError("registration expiry must be after issuance")
        if not isinstance(self.nonce, bytes) or len(self.nonce) != 16:
            raise MssqlR1V3ContractError("registration nonce must be exactly 128 bits")
        require_canonical_text(self.profile_id, "profile_id", maximum_bytes=256)
        if self.target_object_profile != "ordinary_disk_rowstore_v1":
            raise MssqlR1V3ContractError("target_object_profile is outside R1 V3")
        if self.catalog_projection_version != "dpone-mssql-target-catalog-v1":
            raise MssqlR1V3ContractError("catalog_projection_version is outside R1 V3")
        require_count(self.revocation_revision, "revocation_revision")
        require_positive(self.recovery_domain_epoch, "recovery_domain_epoch")
        require_sql_int(self.object_id, "object_id")
        require_positive(self.target_contract_revision, "target_contract_revision")
        for name in ("database_name", "schema_name", "object_name"):
            require_identifier(getattr(self, name), name)
            digest_field = f"{name}_digest"
            if getattr(self, digest_field) != canonical_identifier_digest(getattr(self, name)):
                raise MssqlR1V3ContractError(f"{digest_field} does not bind its canonical identifier")
        self._validate_action()

    def _validate_action(self) -> None:
        predecessor_complete = self.predecessor_registration_id is not None and (
            self.expected_active_registration_revision is not None
        )
        if self.registration_action is RegistrationActionV1.INITIAL:
            if self.predecessor_registration_id is not None or self.expected_active_registration_revision is not None:
                raise MssqlR1V3ContractError("initial registration cannot name a predecessor")
        elif not predecessor_complete:
            raise MssqlR1V3ContractError("rotation requires predecessor registration and revision")
        else:
            require_uuid(self.predecessor_registration_id, "predecessor_registration_id")
            require_positive(self.expected_active_registration_revision, "expected_active_registration_revision")

    @property
    def canonical_bytes(self) -> bytes:
        values = tuple(
            _registration_text(value)
            for value in (
                self.registration_id,
                self.registration_action.value,
                self.predecessor_registration_id,
                self.expected_active_registration_revision,
                self.issued_at,
                self.expires_at,
                self.nonce,
                self.profile_id,
                self.capability_tuple_digest,
                self.resolved_profile_digest,
                self.route_source_authority_sha256,
                self.target_object_profile,
                self.catalog_projection_version,
                self.revocation_revision,
                self.target_binding_uuid,
                self.target_object_uuid,
                self.recovery_domain_uuid,
                self.recovery_domain_epoch,
                self.server_instance_identity_sha256,
                self.database_guid,
                self.database_family_guid,
                self.recovery_fork_guid,
                self.database_name_digest,
                self.schema_name_digest,
                self.object_name_digest,
                self.database_name,
                self.schema_name,
                self.object_name,
                self.object_id,
                self.physical_generation_uuid,
                self.catalog_contract_digest,
                self.target_contract_revision,
            )
        )
        return canonical_utf8_fields(_REGISTRATION_DOMAIN, values)

    @property
    def payload_digest(self) -> bytes:
        return hashlib.sha256(self.canonical_bytes).digest()

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlTargetRegistrationPayloadV1:
        values: list[object] = list(decode_canonical_utf8_fields(payload, _REGISTRATION_DOMAIN, field_count=32))
        for index in (0, 2, 14, 15, 16, 19, 20, 21, 29):
            values[index] = None if index == 2 and values[index] == "" else _parse_uuid_text(values[index])
        try:
            values[1] = RegistrationActionV1(values[1])
        except ValueError as exc:
            raise MssqlR1V3ContractError("registration action is unsupported") from exc
        values[3] = None if values[3] == "" else _parse_uint_text(values[3])
        values[4] = parse_canonical_utc_text(values[4], "issued_at")
        values[5] = parse_canonical_utc_text(values[5], "expires_at")
        for index in (6, 8, 9, 10, 18, 22, 23, 24, 30):
            values[index] = _parse_hex_text(values[index])
        for index in (13, 17, 28, 31):
            values[index] = _parse_uint_text(values[index])
        decoded = cls(*values)  # type: ignore[arg-type]
        if decoded.canonical_bytes != payload:
            raise MssqlR1V3ContractError("registration payload has a noncanonical representation")
        return decoded

    @property
    def physical_identity(self) -> MssqlTargetPhysicalIdentityV1:
        return MssqlTargetPhysicalIdentityV1(
            target_object_uuid=self.target_object_uuid,
            server_instance_identity_sha256=self.server_instance_identity_sha256,
            database_guid=self.database_guid,
            database_family_guid=self.database_family_guid,
            recovery_fork_guid=self.recovery_fork_guid,
            recovery_domain_uuid=self.recovery_domain_uuid,
            recovery_domain_epoch=self.recovery_domain_epoch,
            database_name=self.database_name,
            database_name_digest=self.database_name_digest,
            schema_name=self.schema_name,
            schema_name_digest=self.schema_name_digest,
            object_name=self.object_name,
            object_name_digest=self.object_name_digest,
            object_id=self.object_id,
            physical_generation_uuid=self.physical_generation_uuid,
            catalog_contract_digest=self.catalog_contract_digest,
            target_contract_revision=self.target_contract_revision,
        )


@dataclass(frozen=True, slots=True)
class MssqlTargetRegistrationVerificationV1:
    """Immutable verifier output whose trust inputs cannot come from the signed bundle."""

    registration_payload_digest: bytes
    signature_bundle_digest: bytes
    signer_identity_digest: bytes
    trusted_root_digest: bytes
    cosign_policy_digest: bytes
    verifier_version: str
    verified_at: datetime
    certificate_identity_digest: bytes
    certificate_issuer_digest: bytes
    payload_bytes: bytes

    def __post_init__(self) -> None:
        for name in (
            "registration_payload_digest",
            "signature_bundle_digest",
            "signer_identity_digest",
            "trusted_root_digest",
            "cosign_policy_digest",
            "certificate_identity_digest",
            "certificate_issuer_digest",
        ):
            require_digest(getattr(self, name), name)
        require_canonical_text(self.verifier_version, "verifier_version", maximum_bytes=128)
        canonical_utc_text(self.verified_at, "verified_at")
        if not isinstance(self.payload_bytes, bytes) or not self.payload_bytes.startswith(_REGISTRATION_DOMAIN):
            raise MssqlR1V3ContractError("verification requires exact registration payload bytes")
        if hashlib.sha256(self.payload_bytes).digest() != self.registration_payload_digest:
            raise MssqlR1V3ContractError("registration payload digest does not match exact payload bytes")
        if (
            MssqlTargetRegistrationPayloadV1.from_canonical_bytes(self.payload_bytes).canonical_bytes
            != self.payload_bytes
        ):
            raise MssqlR1V3ContractError("verification contains a noncanonical registration payload")

    @property
    def verification_policy_digest(self) -> bytes:
        return canonical_digest(
            _VERIFICATION_POLICY_DOMAIN,
            (
                self.trusted_root_digest,
                self.cosign_policy_digest,
                self.verifier_version,
                self.certificate_identity_digest,
                self.certificate_issuer_digest,
            ),
        )

    @property
    def receipt_digest(self) -> bytes:
        return hashlib.sha256(self.canonical_bytes).digest()

    def validate_for_command(self, command: SignedTargetRegistrationCommandV1) -> None:
        """Reject verification evidence produced for different payload or bundle bytes."""

        if not isinstance(command, SignedTargetRegistrationCommandV1):
            raise MssqlR1V3ContractError("registration verification requires a registration command")
        validate_detached_command_binding(
            command.payload_bytes,
            command.sigstore_bundle,
            self.payload_bytes,
            self.registration_payload_digest,
            self.signature_bundle_digest,
        )

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(
            _VERIFICATION_RECEIPT_DOMAIN, tuple(getattr(self, name) for name in self.__dataclass_fields__)
        )

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlTargetRegistrationVerificationV1:
        values = list(decode_canonical_bytes(payload, _VERIFICATION_RECEIPT_DOMAIN, field_count=10))
        values[6] = parse_canonical_utc_text(values[6], "verified_at")
        return cls(*values)  # type: ignore[arg-type]


@dataclass(frozen=True, slots=True)
class SignedTargetRegistrationCommandV1:
    """Untrusted exact payload plus detached bundle passed to the injected verifier boundary."""

    payload_bytes: bytes
    sigstore_bundle: bytes

    def __post_init__(self) -> None:
        if not isinstance(self.payload_bytes, bytes) or not self.payload_bytes.startswith(_REGISTRATION_DOMAIN):
            raise MssqlR1V3ContractError("signed registration command has a wrong payload domain")
        if not isinstance(self.sigstore_bundle, bytes) or not self.sigstore_bundle:
            raise MssqlR1V3ContractError("signed registration command requires a detached bundle")
        MssqlTargetRegistrationPayloadV1.from_canonical_bytes(self.payload_bytes)

    @property
    def payload(self) -> MssqlTargetRegistrationPayloadV1:
        return MssqlTargetRegistrationPayloadV1.from_canonical_bytes(self.payload_bytes)


def _registration_text(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        raise MssqlR1V3ContractError("registration payload cannot encode boolean as integer")
    if isinstance(value, datetime):
        return canonical_utc_text(value, "registration_datetime")
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, bytes):
        return value.hex()
    if isinstance(value, int):
        return str(value)
    if isinstance(value, str):
        return value
    raise MssqlR1V3ContractError("registration payload field is not canonical UTF-8 text")


def _parse_uuid_text(value: object) -> UUID:
    if not isinstance(value, str):
        raise MssqlR1V3ContractError("registration UUID field must be UTF-8 text")
    try:
        return UUID(value)
    except ValueError as exc:
        raise MssqlR1V3ContractError("registration UUID field is invalid") from exc


def _parse_uint_text(value: object) -> int:
    if not isinstance(value, str) or (value != "0" and (not value or value[0] == "0" or not value.isascii())):
        raise MssqlR1V3ContractError("registration integer field is noncanonical")
    if not value.isdecimal():
        raise MssqlR1V3ContractError("registration integer field is invalid")
    return int(value)


def _parse_hex_text(value: object) -> bytes:
    if not isinstance(value, str) or len(value) % 2 or value.lower() != value:
        raise MssqlR1V3ContractError("registration binary field is noncanonical")
    try:
        return bytes.fromhex(value)
    except ValueError as exc:
        raise MssqlR1V3ContractError("registration binary field is invalid") from exc


__all__ = [
    "MssqlTargetRegistrationPayloadV1",
    "MssqlTargetRegistrationVerificationV1",
    "RegistrationActionV1",
    "SignedTargetRegistrationCommandV1",
]
