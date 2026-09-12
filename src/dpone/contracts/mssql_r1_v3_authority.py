"""Signed one-shot generation and revoked-registration authority contracts."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

from dpone._compat import StrEnum
from dpone.contracts.mssql_r1_v3_identity import (
    AUTHORITY_SET_CODEC_VERSION,
    MssqlR1V3ContractError,
    canonical_bytes,
    canonical_utc_text,
    decode_canonical_bytes,
    expect_enum,
    parse_canonical_utc_text,
    require_canonical_text,
    require_count,
    require_digest,
    require_positive,
    require_uuid,
    validate_detached_command_binding,
    validate_generation_authority_coordinates,
    validate_signed_command_bytes,
)

_AUTHORITY_REF_DOMAIN = b"dpone-r1-generation-authority-ref-v2\0"
_AUTHORITY_SET_DOMAIN = b"dpone-r1-generation-authority-set-v2\0"
_AUTHORITY_ISSUANCE_DOMAIN = b"dpone-r1-generation-authority-set-issuance-v1\0"
_OVERRIDE_DOMAIN = b"dpone-r1-revoked-registration-override-v1\0"
_VERIFICATION_DOMAIN = b"dpone-r1-signed-payload-verification-v1\0"


class MssqlGenerationAuthorityPurposeV2(StrEnum):
    INITIAL_CUTOVER = "initial_cutover"
    REBASELINE = "rebaseline"
    EMPTY_REFRESH = "empty_refresh"


class MssqlSignedPayloadKindV1(StrEnum):
    GENERATION_AUTHORITY_SET = "generation_authority_set"
    REVOKED_REGISTRATION_OVERRIDE = "revoked_registration_override"


@dataclass(frozen=True, slots=True)
class MssqlGenerationAuthorityRefV2:
    authority_id: UUID
    purpose: MssqlGenerationAuthorityPurposeV2
    effect_key: bytes
    source_snapshot_digest: bytes
    artifact_set_digest: bytes
    mutation_plan_digest: bytes
    expected_writer_generation: int | None
    candidate_writer_generation: int
    expected_head_revision: int | None
    candidate_head_revision: int
    recovery_identity_digest: bytes

    def __post_init__(self) -> None:
        require_uuid(self.authority_id, "authority_id")
        if not isinstance(self.purpose, MssqlGenerationAuthorityPurposeV2):
            raise MssqlR1V3ContractError("generation authority purpose is unsupported")
        for name in (
            "effect_key",
            "source_snapshot_digest",
            "artifact_set_digest",
            "mutation_plan_digest",
            "recovery_identity_digest",
        ):
            require_digest(getattr(self, name), name)
        validate_generation_authority_coordinates(
            self.purpose.value,
            self.expected_writer_generation,
            self.candidate_writer_generation,
            self.expected_head_revision,
            self.candidate_head_revision,
        )

    @property
    def canonical_values(self) -> tuple[object, ...]:
        return (
            self.authority_id,
            self.purpose,
            self.effect_key,
            self.source_snapshot_digest,
            self.artifact_set_digest,
            self.mutation_plan_digest,
            self.expected_writer_generation,
            self.candidate_writer_generation,
            self.expected_head_revision,
            self.candidate_head_revision,
            self.recovery_identity_digest,
        )

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(_AUTHORITY_REF_DOMAIN, self.canonical_values)

    @property
    def digest(self) -> bytes:
        return hashlib.sha256(self.canonical_bytes).digest()

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlGenerationAuthorityRefV2:
        values = decode_canonical_bytes(payload, _AUTHORITY_REF_DOMAIN, field_count=11)
        return cls(
            require_uuid(values[0], "authority_id"),
            expect_enum(MssqlGenerationAuthorityPurposeV2, values[1], "purpose"),
            require_digest(values[2], "effect_key"),
            require_digest(values[3], "source_snapshot_digest"),
            require_digest(values[4], "artifact_set_digest"),
            require_digest(values[5], "mutation_plan_digest"),
            _optional_positive(values[6], "expected_writer_generation"),
            require_positive(values[7], "candidate_writer_generation"),
            _optional_positive(values[8], "expected_head_revision"),
            require_positive(values[9], "candidate_head_revision"),
            require_digest(values[10], "recovery_identity_digest"),
        )


@dataclass(frozen=True, slots=True)
class MssqlGenerationAuthoritySetV2:
    refs: tuple[MssqlGenerationAuthorityRefV2, ...] = ()
    codec_version: str = AUTHORITY_SET_CODEC_VERSION

    def __post_init__(self) -> None:
        if self.codec_version != AUTHORITY_SET_CODEC_VERSION:
            raise MssqlR1V3ContractError("generation authority set codec is unsupported")
        if not isinstance(self.refs, tuple) or len(self.refs) > 2:
            raise MssqlR1V3ContractError("generation authority set must contain at most two refs")
        if not all(isinstance(item, MssqlGenerationAuthorityRefV2) for item in self.refs):
            raise MssqlR1V3ContractError("generation authority ref is invalid")
        allowed = {
            (),
            (MssqlGenerationAuthorityPurposeV2.INITIAL_CUTOVER,),
            (MssqlGenerationAuthorityPurposeV2.REBASELINE,),
            (MssqlGenerationAuthorityPurposeV2.EMPTY_REFRESH,),
            (MssqlGenerationAuthorityPurposeV2.INITIAL_CUTOVER, MssqlGenerationAuthorityPurposeV2.EMPTY_REFRESH),
            (MssqlGenerationAuthorityPurposeV2.REBASELINE, MssqlGenerationAuthorityPurposeV2.EMPTY_REFRESH),
        }
        if self.purposes not in allowed:
            raise MssqlR1V3ContractError("generation authority refs violate canonical order or are duplicate/extra")
        if self.refs:
            binding = self.refs[0].canonical_values[2:]
            if any(item.canonical_values[2:] != binding for item in self.refs[1:]):
                raise MssqlR1V3ContractError("generation authority refs must bind the same immutable effect")

    @property
    def purposes(self) -> tuple[MssqlGenerationAuthorityPurposeV2, ...]:
        return tuple(item.purpose for item in self.refs)

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(
            _AUTHORITY_SET_DOMAIN, (self.codec_version, tuple(item.canonical_bytes for item in self.refs))
        )

    @property
    def digest(self) -> bytes:
        return hashlib.sha256(self.canonical_bytes).digest()

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlGenerationAuthoritySetV2:
        codec, refs = decode_canonical_bytes(payload, _AUTHORITY_SET_DOMAIN, field_count=2)
        if not isinstance(refs, tuple) or not all(isinstance(item, bytes) for item in refs):
            raise MssqlR1V3ContractError("authority set refs must contain exact canonical bytes")
        return cls(tuple(MssqlGenerationAuthorityRefV2.from_canonical_bytes(item) for item in refs), str(codec))


@dataclass(frozen=True, slots=True)
class MssqlGenerationAuthoritySetIssuancePayloadV1:
    issuance_id: UUID
    target_binding_uuid: UUID
    registration_id: UUID
    registration_payload_digest: bytes
    registration_revocation_revision: int
    issued_at: datetime
    expires_at: datetime
    nonce: bytes
    authority_set_bytes: bytes
    authority_set_digest: bytes

    def __post_init__(self) -> None:
        for name in ("issuance_id", "target_binding_uuid", "registration_id"):
            require_uuid(getattr(self, name), name)
        require_digest(self.registration_payload_digest, "registration_payload_digest")
        require_count(self.registration_revocation_revision, "registration_revocation_revision")
        canonical_utc_text(self.issued_at, "issued_at")
        canonical_utc_text(self.expires_at, "expires_at")
        if self.expires_at <= self.issued_at:
            raise MssqlR1V3ContractError("authority issuance expiry must be after issuance")
        if not isinstance(self.nonce, bytes) or len(self.nonce) != 16:
            raise MssqlR1V3ContractError("authority issuance nonce must be 128 bits")
        authority_set = MssqlGenerationAuthoritySetV2.from_canonical_bytes(self.authority_set_bytes)
        if not authority_set.refs or authority_set.digest != require_digest(
            self.authority_set_digest, "authority_set_digest"
        ):
            raise MssqlR1V3ContractError("authority issuance does not bind one exact non-empty set")

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(
            _AUTHORITY_ISSUANCE_DOMAIN,
            (
                self.issuance_id,
                self.target_binding_uuid,
                self.registration_id,
                self.registration_payload_digest,
                self.registration_revocation_revision,
                self.issued_at,
                self.expires_at,
                self.nonce,
                self.authority_set_bytes,
                self.authority_set_digest,
            ),
        )

    @property
    def payload_digest(self) -> bytes:
        return hashlib.sha256(self.canonical_bytes).digest()

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlGenerationAuthoritySetIssuancePayloadV1:
        values = list(decode_canonical_bytes(payload, _AUTHORITY_ISSUANCE_DOMAIN, field_count=10))
        values[5] = parse_canonical_utc_text(values[5], "issued_at")
        values[6] = parse_canonical_utc_text(values[6], "expires_at")
        return cls(*values)  # type: ignore[arg-type]


@dataclass(frozen=True, slots=True)
class MssqlRevokedRegistrationCompletionOverridePayloadV1:
    override_id: UUID
    target_binding_uuid: UUID
    effect_key: bytes
    registration_id: UUID
    registration_payload_digest: bytes
    revocation_receipt_digest: bytes
    sealed_intent_digest: bytes
    artifact_set_digest: bytes
    mutation_plan_digest: bytes
    expected_operation_epoch: int
    expected_operation_projection_revision: int
    expected_verification_policy_digest: bytes
    expires_at: datetime

    def __post_init__(self) -> None:
        for name in ("override_id", "target_binding_uuid", "registration_id"):
            require_uuid(getattr(self, name), name)
        for name in (
            "effect_key",
            "registration_payload_digest",
            "revocation_receipt_digest",
            "sealed_intent_digest",
            "artifact_set_digest",
            "mutation_plan_digest",
            "expected_verification_policy_digest",
        ):
            require_digest(getattr(self, name), name)
        require_positive(self.expected_operation_epoch, "expected_operation_epoch")
        require_positive(self.expected_operation_projection_revision, "expected_operation_projection_revision")
        canonical_utc_text(self.expires_at, "expires_at")

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(_OVERRIDE_DOMAIN, tuple(getattr(self, name) for name in self.__dataclass_fields__))

    @property
    def payload_digest(self) -> bytes:
        return hashlib.sha256(self.canonical_bytes).digest()

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlRevokedRegistrationCompletionOverridePayloadV1:
        values = list(decode_canonical_bytes(payload, _OVERRIDE_DOMAIN, field_count=13))
        values[12] = parse_canonical_utc_text(values[12], "expires_at")
        return cls(*values)  # type: ignore[arg-type]


@dataclass(frozen=True, slots=True)
class MssqlSignedPayloadVerificationV1:
    payload_kind: MssqlSignedPayloadKindV1
    payload_digest: bytes
    signature_bundle_digest: bytes
    signer_identity_digest: bytes
    trusted_root_digest: bytes
    verification_policy_digest: bytes
    verifier_version: str
    verified_at: datetime
    payload_bytes: bytes

    def __post_init__(self) -> None:
        if not isinstance(self.payload_kind, MssqlSignedPayloadKindV1):
            raise MssqlR1V3ContractError("signed payload kind is unsupported")
        for name in (
            "payload_digest",
            "signature_bundle_digest",
            "signer_identity_digest",
            "trusted_root_digest",
            "verification_policy_digest",
        ):
            require_digest(getattr(self, name), name)
        require_canonical_text(self.verifier_version, "verifier_version", maximum_bytes=128)
        canonical_utc_text(self.verified_at, "verified_at")
        expected_domain = (
            _AUTHORITY_ISSUANCE_DOMAIN
            if self.payload_kind is MssqlSignedPayloadKindV1.GENERATION_AUTHORITY_SET
            else _OVERRIDE_DOMAIN
        )
        if not isinstance(self.payload_bytes, bytes) or not self.payload_bytes.startswith(expected_domain):
            raise MssqlR1V3ContractError("verification payload kind/domain mismatch")
        if hashlib.sha256(self.payload_bytes).digest() != self.payload_digest:
            raise MssqlR1V3ContractError("verification does not bind exact payload bytes")
        if self.payload_kind is MssqlSignedPayloadKindV1.GENERATION_AUTHORITY_SET:
            canonical_payload = MssqlGenerationAuthoritySetIssuancePayloadV1.from_canonical_bytes(
                self.payload_bytes
            ).canonical_bytes
        else:
            canonical_payload = MssqlRevokedRegistrationCompletionOverridePayloadV1.from_canonical_bytes(
                self.payload_bytes
            ).canonical_bytes
        if canonical_payload != self.payload_bytes:
            raise MssqlR1V3ContractError("verification payload is not canonical")

    @property
    def receipt_digest(self) -> bytes:
        return hashlib.sha256(self.canonical_bytes).digest()

    def validate_for_command(
        self,
        command: SignedGenerationAuthoritySetCommandV1 | SignedRevokedOverrideCommandV1,
    ) -> None:
        valid_kind = (
            self.payload_kind is MssqlSignedPayloadKindV1.GENERATION_AUTHORITY_SET
            and isinstance(command, SignedGenerationAuthoritySetCommandV1)
        ) or (
            self.payload_kind is MssqlSignedPayloadKindV1.REVOKED_REGISTRATION_OVERRIDE
            and isinstance(command, SignedRevokedOverrideCommandV1)
        )
        if not valid_kind:
            raise MssqlR1V3ContractError("verification command kind differs from signed payload kind")
        validate_detached_command_binding(
            command.payload_bytes,
            command.sigstore_bundle,
            self.payload_bytes,
            self.payload_digest,
            self.signature_bundle_digest,
        )

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(_VERIFICATION_DOMAIN, tuple(getattr(self, name) for name in self.__dataclass_fields__))

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlSignedPayloadVerificationV1:
        values = list(decode_canonical_bytes(payload, _VERIFICATION_DOMAIN, field_count=9))
        values[0] = expect_enum(MssqlSignedPayloadKindV1, values[0], "payload_kind")
        values[7] = parse_canonical_utc_text(values[7], "verified_at")
        return cls(*values)  # type: ignore[arg-type]


@dataclass(frozen=True, slots=True)
class SignedGenerationAuthoritySetCommandV1:
    payload_bytes: bytes
    sigstore_bundle: bytes

    def __post_init__(self) -> None:
        validate_signed_command_bytes(self.payload_bytes, self.sigstore_bundle, _AUTHORITY_ISSUANCE_DOMAIN)
        MssqlGenerationAuthoritySetIssuancePayloadV1.from_canonical_bytes(self.payload_bytes)

    @property
    def payload(self) -> MssqlGenerationAuthoritySetIssuancePayloadV1:
        return MssqlGenerationAuthoritySetIssuancePayloadV1.from_canonical_bytes(self.payload_bytes)


@dataclass(frozen=True, slots=True)
class SignedRevokedOverrideCommandV1:
    payload_bytes: bytes
    sigstore_bundle: bytes

    def __post_init__(self) -> None:
        validate_signed_command_bytes(self.payload_bytes, self.sigstore_bundle, _OVERRIDE_DOMAIN)
        MssqlRevokedRegistrationCompletionOverridePayloadV1.from_canonical_bytes(self.payload_bytes)

    @property
    def payload(self) -> MssqlRevokedRegistrationCompletionOverridePayloadV1:
        return MssqlRevokedRegistrationCompletionOverridePayloadV1.from_canonical_bytes(self.payload_bytes)


def _optional_positive(value: object, field: str) -> int | None:
    return None if value is None else require_positive(value, field)


__all__ = [
    "MssqlGenerationAuthorityPurposeV2",
    "MssqlGenerationAuthorityRefV2",
    "MssqlGenerationAuthoritySetIssuancePayloadV1",
    "MssqlGenerationAuthoritySetV2",
    "MssqlRevokedRegistrationCompletionOverridePayloadV1",
    "MssqlSignedPayloadKindV1",
    "MssqlSignedPayloadVerificationV1",
    "SignedGenerationAuthoritySetCommandV1",
    "SignedRevokedOverrideCommandV1",
]
