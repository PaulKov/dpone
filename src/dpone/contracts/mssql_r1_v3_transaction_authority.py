"""Transaction-bound generation-authority observations for MSSQL R1 V3."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import TYPE_CHECKING
from uuid import UUID

from dpone._compat import StrEnum
from dpone.contracts.mssql_r1_v3_authority import (
    MssqlGenerationAuthoritySetIssuancePayloadV1,
    MssqlGenerationAuthoritySetV2,
    MssqlSignedPayloadKindV1,
    MssqlSignedPayloadVerificationV1,
)
from dpone.contracts.mssql_r1_v3_identity import (
    MssqlR1V3ContractError,
    canonical_bytes,
    decode_canonical_bytes,
    expect_bytes,
    expect_enum,
    expect_tuple,
    require_count,
    require_digest,
    require_positive,
    require_uuid,
)
from dpone.contracts.mssql_r1_v3_registration import MssqlTargetRegistrationPayloadV1

if TYPE_CHECKING:
    from dpone.contracts.mssql_r1_v3_effect import MssqlR1EffectAttemptEnvelopeV3


_ADMITTED_DOMAIN = b"dpone-r1-admitted-generation-authority-set-v3\0"
_CONSUMED_DOMAIN = b"dpone-r1-consumed-generation-authority-set-v3\0"


class MssqlAuthorityObservationStateV3(StrEnum):
    ISSUED = "issued"
    CONSUMED = "consumed"


class R1ReplayResourceStateV3(StrEnum):
    SEALED = "sealed"
    ISSUED = "issued"
    CONSUMED = "consumed"


@dataclass(frozen=True, slots=True)
class MssqlGenerationAuthorityObservationValuesV3:
    transaction_id: UUID
    session_identity_digest: bytes
    effect_key: bytes
    sealed_request_digest: bytes
    issuance_id: UUID | None
    issuance_payload: bytes | None
    issuance_payload_digest: bytes | None
    verification_payload: bytes | None
    verification_receipt_digest: bytes | None
    registration_id: UUID | None
    registration_payload_digest: bytes | None
    registration_revocation_revision: int | None
    authority_set_payload: bytes
    authority_set_digest: bytes
    issuance_projection_revision: int | None
    ref_projection_revisions: tuple[int, ...]

    def validate(self) -> MssqlGenerationAuthoritySetV2:
        require_uuid(self.transaction_id, "transaction_id")
        require_digest(self.session_identity_digest, "session_identity_digest")
        require_digest(self.effect_key, "effect_key")
        require_digest(self.sealed_request_digest, "sealed_request_digest")
        authority_set = MssqlGenerationAuthoritySetV2.from_canonical_bytes(self.authority_set_payload)
        if authority_set.digest != require_digest(self.authority_set_digest, "authority_set_digest"):
            raise MssqlR1V3ContractError("authority-set digest differs from exact canonical bytes")
        optional = (
            self.issuance_id,
            self.issuance_payload,
            self.issuance_payload_digest,
            self.verification_payload,
            self.verification_receipt_digest,
            self.registration_id,
            self.registration_payload_digest,
            self.registration_revocation_revision,
            self.issuance_projection_revision,
        )
        if not authority_set.refs:
            if any(value is not None for value in optional) or self.ref_projection_revisions:
                raise MssqlR1V3ContractError("empty authority set must be an explicit no-issuance observation")
            return authority_set
        if any(value is None for value in optional):
            raise MssqlR1V3ContractError("non-empty authority observation has partial issuance identity")
        issuance = MssqlGenerationAuthoritySetIssuancePayloadV1.from_canonical_bytes(
            _bytes(self.issuance_payload, "issuance_payload")
        )
        verification = MssqlSignedPayloadVerificationV1.from_canonical_bytes(
            _bytes(self.verification_payload, "verification_payload")
        )
        if (
            issuance.issuance_id != require_uuid(self.issuance_id, "issuance_id")
            or issuance.payload_digest != require_digest(self.issuance_payload_digest, "issuance_payload_digest")
            or verification.receipt_digest
            != require_digest(self.verification_receipt_digest, "verification_receipt_digest")
            or verification.payload_kind is not MssqlSignedPayloadKindV1.GENERATION_AUTHORITY_SET
            or verification.payload_bytes != issuance.canonical_bytes
            or issuance.authority_set_bytes != self.authority_set_payload
            or issuance.authority_set_digest != self.authority_set_digest
            or issuance.registration_id != require_uuid(self.registration_id, "registration_id")
            or issuance.registration_payload_digest
            != require_digest(self.registration_payload_digest, "registration_payload_digest")
            or issuance.registration_revocation_revision
            != require_count(self.registration_revocation_revision, "registration_revocation_revision")
        ):
            raise MssqlR1V3ContractError("authority observation differs from signed issuance/verification")
        require_positive(self.issuance_projection_revision, "issuance_projection_revision")
        if len(self.ref_projection_revisions) != len(authority_set.refs):
            raise MssqlR1V3ContractError("authority projection revisions differ from ordered refs")
        for revision in self.ref_projection_revisions:
            require_positive(revision, "ref_projection_revision")
        return authority_set

    @property
    def canonical_values(self) -> tuple[object, ...]:
        return tuple(getattr(self, name) for name in self.__dataclass_fields__)


@dataclass(frozen=True, slots=True)
class MssqlAdmittedGenerationAuthoritySetV3:
    values: MssqlGenerationAuthorityObservationValuesV3
    state: MssqlAuthorityObservationStateV3 = MssqlAuthorityObservationStateV3.ISSUED

    def __post_init__(self) -> None:
        if not isinstance(self.values, MssqlGenerationAuthorityObservationValuesV3):
            raise MssqlR1V3ContractError("admitted authority observation is invalid")
        self.values.validate()
        if self.state is not MssqlAuthorityObservationStateV3.ISSUED:
            raise MssqlR1V3ContractError("admitted authority observation must be ISSUED")

    @property
    def authority_set(self) -> MssqlGenerationAuthoritySetV2:
        return self.values.validate()

    def validate_for_attempt(self, attempt: MssqlR1EffectAttemptEnvelopeV3) -> None:
        request = attempt.request
        registration = MssqlTargetRegistrationPayloadV1.from_canonical_bytes(request.registration_payload_bytes)
        if (
            self.values.effect_key,
            self.values.sealed_request_digest,
            self.authority_set,
        ) != (request.identity.effect_key, request.digest, request.authority_set):
            raise MssqlR1V3ContractError("admitted authority observation differs from sealed attempt")
        if self.authority_set.refs and (
            self.values.registration_id,
            self.values.registration_payload_digest,
            self.values.registration_revocation_revision,
            MssqlGenerationAuthoritySetIssuancePayloadV1.from_canonical_bytes(
                _bytes(self.values.issuance_payload, "issuance_payload")
            ).target_binding_uuid,
        ) != (
            request.registration_id,
            request.registration_payload_digest,
            registration.revocation_revision,
            request.identity.target_binding_uuid,
        ):
            raise MssqlR1V3ContractError("admitted issuance differs from sealed registration/target")
        if self.authority_set.refs:
            issuance = MssqlGenerationAuthoritySetIssuancePayloadV1.from_canonical_bytes(
                _bytes(self.values.issuance_payload, "issuance_payload")
            )
            if not issuance.issued_at <= request.admitted_at_server_time < issuance.expires_at:
                raise MssqlR1V3ContractError("generation-authority issuance is not valid at target admission time")

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(_ADMITTED_DOMAIN, (self.values.canonical_values, self.state))

    @property
    def digest(self) -> bytes:
        return hashlib.sha256(self.canonical_bytes).digest()

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlAdmittedGenerationAuthoritySetV3:
        raw, state = decode_canonical_bytes(payload, _ADMITTED_DOMAIN, field_count=2)
        return cls(_decode_values(raw), expect_enum(MssqlAuthorityObservationStateV3, state, "authority_state"))


@dataclass(frozen=True, slots=True)
class MssqlConsumedGenerationAuthoritySetV3:
    values: MssqlGenerationAuthorityObservationValuesV3
    consuming_receipt_id: UUID
    consuming_receipt_digest: bytes
    state: MssqlAuthorityObservationStateV3 = MssqlAuthorityObservationStateV3.CONSUMED

    def __post_init__(self) -> None:
        if not isinstance(self.values, MssqlGenerationAuthorityObservationValuesV3):
            raise MssqlR1V3ContractError("consumed authority observation is invalid")
        self.values.validate()
        require_uuid(self.consuming_receipt_id, "consuming_receipt_id")
        require_digest(self.consuming_receipt_digest, "consuming_receipt_digest")
        if self.state is not MssqlAuthorityObservationStateV3.CONSUMED:
            raise MssqlR1V3ContractError("consumed authority observation must be CONSUMED")

    @classmethod
    def from_admitted(
        cls,
        admitted: MssqlAdmittedGenerationAuthoritySetV3,
        receipt_id: UUID,
        receipt_digest: bytes,
    ) -> MssqlConsumedGenerationAuthoritySetV3:
        return cls(admitted.values, receipt_id, receipt_digest)

    @property
    def authority_set(self) -> MssqlGenerationAuthoritySetV2:
        return self.values.validate()

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(
            _CONSUMED_DOMAIN,
            (self.values.canonical_values, self.consuming_receipt_id, self.consuming_receipt_digest, self.state),
        )

    @property
    def digest(self) -> bytes:
        return hashlib.sha256(self.canonical_bytes).digest()

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlConsumedGenerationAuthoritySetV3:
        raw, receipt_id, receipt_digest, state = decode_canonical_bytes(payload, _CONSUMED_DOMAIN, field_count=4)
        return cls(
            _decode_values(raw),
            require_uuid(receipt_id, "consuming_receipt_id"),
            require_digest(receipt_digest, "consuming_receipt_digest"),
            expect_enum(MssqlAuthorityObservationStateV3, state, "authority_state"),
        )


def authority_observation_values(
    *,
    transaction_id: UUID,
    session_identity_digest: bytes,
    effect_key: bytes,
    sealed_request_digest: bytes,
    authority_set: MssqlGenerationAuthoritySetV2,
    issuance: MssqlGenerationAuthoritySetIssuancePayloadV1 | None = None,
    verification: MssqlSignedPayloadVerificationV1 | None = None,
    issuance_projection_revision: int | None = None,
    ref_projection_revisions: tuple[int, ...] = (),
) -> MssqlGenerationAuthorityObservationValuesV3:
    return MssqlGenerationAuthorityObservationValuesV3(
        transaction_id,
        session_identity_digest,
        effect_key,
        sealed_request_digest,
        None if issuance is None else issuance.issuance_id,
        None if issuance is None else issuance.canonical_bytes,
        None if issuance is None else issuance.payload_digest,
        None if verification is None else verification.canonical_bytes,
        None if verification is None else verification.receipt_digest,
        None if issuance is None else issuance.registration_id,
        None if issuance is None else issuance.registration_payload_digest,
        None if issuance is None else issuance.registration_revocation_revision,
        authority_set.canonical_bytes,
        authority_set.digest,
        issuance_projection_revision,
        ref_projection_revisions,
    )


def _decode_values(value: object) -> MssqlGenerationAuthorityObservationValuesV3:
    values = list(expect_tuple(value, "authority_observation_values", size=16))
    values[15] = tuple(values[15]) if isinstance(values[15], tuple) else values[15]
    return MssqlGenerationAuthorityObservationValuesV3(*values)  # type: ignore[arg-type]


def _bytes(value: bytes | None, field: str) -> bytes:
    if value is None:
        raise MssqlR1V3ContractError(f"{field} is required")
    return expect_bytes(value, field)


__all__ = [
    "MssqlAdmittedGenerationAuthoritySetV3",
    "MssqlAuthorityObservationStateV3",
    "MssqlConsumedGenerationAuthoritySetV3",
    "MssqlGenerationAuthorityObservationValuesV3",
    "R1ReplayResourceStateV3",
    "authority_observation_values",
]
