"""Canonical fresh-session observations used by MSSQL R1 V3 replay proofs."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING
from uuid import UUID

from dpone._compat import StrEnum
from dpone.contracts.mssql_r1_v3_authority import (
    MssqlGenerationAuthoritySetIssuancePayloadV1,
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
    require_digest,
    require_positive,
    require_uuid,
)
from dpone.contracts.mssql_r1_v3_receipt import MssqlR1EffectReceiptV3
from dpone.contracts.mssql_r1_v3_registration import MssqlTargetRegistrationPayloadV1
from dpone.contracts.mssql_r1_v3_stage_consumption import (
    ArtifactSetReplayObservationV3 as ArtifactSetReplayObservationV3,
)
from dpone.contracts.mssql_r1_v3_stage_consumption import (
    CheckpointReplayObservationV3 as CheckpointReplayObservationV3,
)
from dpone.contracts.mssql_r1_v3_transaction_authority import R1ReplayResourceStateV3

if TYPE_CHECKING:
    from dpone.contracts.mssql_r1_v3_effect import MssqlR1EffectRequestV3


_PROOF_REQUEST_DOMAIN = b"dpone-r1-effect-proof-request-v3\0"
_OBSERVATION_DOMAIN = b"dpone-r1-effect-replay-observation-v3\0"
_AUTHORITY_DOMAIN = _OBSERVATION_DOMAIN + b"authorities\0"


class R1ReplayOperationStateV3(StrEnum):
    SEALED = "sealed"
    COMMITTED = "committed"


class R1UnknownReplayReasonV3(StrEnum):
    AMBIGUOUS_RECEIPT = "ambiguous_receipt"
    INCOMPLETE_OBSERVATION = "incomplete_observation"
    CORRUPT_OBSERVATION = "corrupt_observation"


@dataclass(frozen=True, slots=True)
class EffectReceiptProofRequestV3:
    sealed_request_digest: bytes
    effect_key: bytes
    operation_epoch: int
    operation_projection_revision: int
    expected_override_id: UUID | None = None
    expected_override_digest: bytes | None = None

    def __post_init__(self) -> None:
        require_digest(self.sealed_request_digest, "sealed_request_digest")
        require_digest(self.effect_key, "effect_key")
        require_positive(self.operation_epoch, "operation_epoch")
        require_positive(self.operation_projection_revision, "operation_projection_revision")
        if (self.expected_override_id is None) != (self.expected_override_digest is None):
            raise MssqlR1V3ContractError("effect proof override identity must be all-or-none")
        if self.expected_override_id is not None:
            raise MssqlR1V3ContractError("revoked-registration override is not an active R1 capability")

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(
            _PROOF_REQUEST_DOMAIN,
            (
                self.sealed_request_digest,
                self.effect_key,
                self.operation_epoch,
                self.operation_projection_revision,
                self.expected_override_id,
                self.expected_override_digest,
            ),
        )

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> EffectReceiptProofRequestV3:
        return cls(*decode_canonical_bytes(payload, _PROOF_REQUEST_DOMAIN, field_count=6))  # type: ignore[arg-type]


@dataclass(frozen=True, slots=True)
class OperationReplayObservationV3:
    operation_key: bytes
    effect_key: bytes
    state: R1ReplayOperationStateV3
    operation_epoch: int
    operation_projection_revision: int
    sealed_request_digest: bytes
    committed_receipt_id: UUID | None = None
    committed_receipt_digest: bytes | None = None

    def __post_init__(self) -> None:
        require_digest(self.operation_key, "operation_key")
        require_digest(self.effect_key, "effect_key")
        require_digest(self.sealed_request_digest, "sealed_request_digest")
        require_positive(self.operation_epoch, "operation_epoch")
        require_positive(self.operation_projection_revision, "operation_projection_revision")
        if self.state is R1ReplayOperationStateV3.COMMITTED:
            require_uuid(self.committed_receipt_id, "committed_receipt_id")
            require_digest(self.committed_receipt_digest, "committed_receipt_digest")
        elif self.committed_receipt_id is not None or self.committed_receipt_digest is not None:
            raise MssqlR1V3ContractError("SEALED replay operation cannot name a committed receipt")

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(
            _OBSERVATION_DOMAIN + b"operation\0",
            (
                self.operation_key,
                self.effect_key,
                self.state,
                self.operation_epoch,
                self.operation_projection_revision,
                self.sealed_request_digest,
                self.committed_receipt_id,
                self.committed_receipt_digest,
            ),
        )

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> OperationReplayObservationV3:
        values = list(decode_canonical_bytes(payload, _OBSERVATION_DOMAIN + b"operation\0", field_count=8))
        values[2] = expect_enum(R1ReplayOperationStateV3, values[2], "operation_state")
        return cls(*values)  # type: ignore[arg-type]


@dataclass(frozen=True, slots=True)
class ReceiptDescendantLinkV3:
    receipt: MssqlR1EffectReceiptV3

    def __post_init__(self) -> None:
        if not isinstance(self.receipt, MssqlR1EffectReceiptV3):
            raise MssqlR1V3ContractError("descendant link requires an exact typed receipt")

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(_OBSERVATION_DOMAIN + b"descendant\0", (self.receipt.canonical_bytes,))

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> ReceiptDescendantLinkV3:
        (receipt,) = decode_canonical_bytes(payload, _OBSERVATION_DOMAIN + b"descendant\0", field_count=1)
        return cls(MssqlR1EffectReceiptV3.from_canonical_bytes(expect_bytes(receipt, "descendant_receipt")))


@dataclass(frozen=True, slots=True)
class WriterHeadReplayObservationV3:
    target_generation: int
    row_hash_generation: int
    head_revision: int
    last_receipt_id: UUID
    last_receipt_digest: bytes
    descendants: tuple[ReceiptDescendantLinkV3, ...] = ()

    def __post_init__(self) -> None:
        require_positive(self.target_generation, "target_generation")
        require_positive(self.row_hash_generation, "row_hash_generation")
        require_positive(self.head_revision, "head_revision")
        require_uuid(self.last_receipt_id, "last_receipt_id")
        require_digest(self.last_receipt_digest, "last_receipt_digest")
        if self.target_generation != self.row_hash_generation:
            raise MssqlR1V3ContractError("target and row-hash generations must be identical")
        if not isinstance(self.descendants, tuple) or not all(
            isinstance(item, ReceiptDescendantLinkV3) for item in self.descendants
        ):
            raise MssqlR1V3ContractError("writer-head descendant proof is invalid")

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(
            _OBSERVATION_DOMAIN + b"writer-head\0",
            (
                self.target_generation,
                self.row_hash_generation,
                self.head_revision,
                self.last_receipt_id,
                self.last_receipt_digest,
                tuple(item.canonical_bytes for item in self.descendants),
            ),
        )

    def proves_descendant_of(self, receipt: MssqlR1EffectReceiptV3) -> bool:
        current = receipt
        for item in self.descendants:
            header = item.receipt.header
            if (
                header.expected_writer_generation,
                header.expected_head_revision,
                header.predecessor_receipt_id,
                header.predecessor_receipt_digest,
            ) != (
                current.header.candidate_writer_generation,
                current.header.candidate_head_revision,
                current.header.receipt_id,
                current.digest,
            ):
                return False
            current = item.receipt
        return (
            self.target_generation,
            self.row_hash_generation,
            self.head_revision,
            self.last_receipt_id,
            self.last_receipt_digest,
        ) == (
            current.header.candidate_writer_generation,
            current.header.candidate_writer_generation,
            current.header.candidate_head_revision,
            current.header.receipt_id,
            current.digest,
        )

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> WriterHeadReplayObservationV3:
        values = list(decode_canonical_bytes(payload, _OBSERVATION_DOMAIN + b"writer-head\0", field_count=6))
        values[5] = tuple(
            ReceiptDescendantLinkV3.from_canonical_bytes(expect_bytes(item, "descendant"))
            for item in expect_tuple(values[5], "descendants")
        )
        return cls(*values)  # type: ignore[arg-type]


@dataclass(frozen=True, slots=True)
class AuthoritySetReplayObservationV3:
    issuance_id: UUID | None
    issuance_payload: bytes | None
    issuance_payload_digest: bytes | None
    verification_payload: bytes | None
    verification_receipt_digest: bytes | None
    issuance_state: R1ReplayResourceStateV3 | None
    issuance_consuming_receipt_id: UUID | None
    issuance_consuming_receipt_digest: bytes | None
    authority_ids: tuple[UUID, ...]
    authority_digests: tuple[bytes, ...]
    states: tuple[R1ReplayResourceStateV3, ...]
    consuming_receipt_ids: tuple[UUID | None, ...]
    consuming_receipt_digests: tuple[bytes | None, ...]

    def __post_init__(self) -> None:
        count = len(self.authority_ids)
        parallel = (self.authority_digests, self.states, self.consuming_receipt_ids, self.consuming_receipt_digests)
        if any(len(value) != count for value in parallel) or len(set(self.authority_ids)) != count:
            raise MssqlR1V3ContractError("authority replay observation is partial or duplicate")
        for authority_id in self.authority_ids:
            require_uuid(authority_id, "authority_id")
        for digest in self.authority_digests:
            require_digest(digest, "authority_digest")
        required_issuance = (
            self.issuance_id,
            self.issuance_payload,
            self.issuance_payload_digest,
            self.verification_payload,
            self.verification_receipt_digest,
            self.issuance_state,
        )
        consuming_issuance = (self.issuance_consuming_receipt_id, self.issuance_consuming_receipt_digest)
        if not count:
            if any(value is not None for value in required_issuance + consuming_issuance):
                raise MssqlR1V3ContractError("empty replay authority set must have no issuance")
            return
        if any(value is None for value in required_issuance):
            raise MssqlR1V3ContractError("non-empty replay authority set has partial issuance identity")
        issuance = MssqlGenerationAuthoritySetIssuancePayloadV1.from_canonical_bytes(
            _required_bytes(self.issuance_payload, "issuance_payload")
        )
        verification = MssqlSignedPayloadVerificationV1.from_canonical_bytes(
            _required_bytes(self.verification_payload, "verification_payload")
        )
        if (
            issuance.issuance_id != require_uuid(self.issuance_id, "issuance_id")
            or issuance.payload_digest != require_digest(self.issuance_payload_digest, "issuance_payload_digest")
            or verification.payload_kind is not MssqlSignedPayloadKindV1.GENERATION_AUTHORITY_SET
            or verification.payload_bytes != issuance.canonical_bytes
            or verification.receipt_digest
            != require_digest(self.verification_receipt_digest, "verification_receipt_digest")
        ):
            raise MssqlR1V3ContractError("replay issuance differs from exact verification payload")
        self._validate_consumption()

    def _validate_consumption(self) -> None:
        for state, receipt_id, receipt_digest in zip(
            self.states, self.consuming_receipt_ids, self.consuming_receipt_digests, strict=True
        ):
            if (receipt_id is None) != (receipt_digest is None):
                raise MssqlR1V3ContractError("authority replay receipt identity is partial")
            if state not in {R1ReplayResourceStateV3.ISSUED, R1ReplayResourceStateV3.CONSUMED}:
                raise MssqlR1V3ContractError("authority replay state is invalid")
            if (state is R1ReplayResourceStateV3.CONSUMED) != (receipt_id is not None and receipt_digest is not None):
                raise MssqlR1V3ContractError("authority replay lifecycle is inconsistent")
            if receipt_id is not None:
                require_uuid(receipt_id, "consuming_receipt_id")
            if receipt_digest is not None:
                require_digest(receipt_digest, "consuming_receipt_digest")
        consumed = self.issuance_state is R1ReplayResourceStateV3.CONSUMED
        if (self.issuance_consuming_receipt_id is None) != (self.issuance_consuming_receipt_digest is None):
            raise MssqlR1V3ContractError("replay issuance receipt identity is partial")
        if consumed != all(value is not None for value in self._issuance_consumption):
            raise MssqlR1V3ContractError("replay issuance lifecycle is inconsistent")
        if self.issuance_state not in {R1ReplayResourceStateV3.ISSUED, R1ReplayResourceStateV3.CONSUMED}:
            raise MssqlR1V3ContractError("replay issuance state is invalid")
        if self.issuance_consuming_receipt_id is not None:
            require_uuid(self.issuance_consuming_receipt_id, "issuance_consuming_receipt_id")
        if self.issuance_consuming_receipt_digest is not None:
            require_digest(self.issuance_consuming_receipt_digest, "issuance_consuming_receipt_digest")

    @property
    def _issuance_consumption(self) -> tuple[UUID | None, bytes | None]:
        return self.issuance_consuming_receipt_id, self.issuance_consuming_receipt_digest

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(_AUTHORITY_DOMAIN, tuple(getattr(self, name) for name in self.__dataclass_fields__))

    def validate_for_request(
        self,
        request: MssqlR1EffectRequestV3,
        state: R1ReplayResourceStateV3,
        receipt_id: UUID | None,
        receipt_digest: bytes | None,
    ) -> None:
        refs = request.authority_set.refs
        if (
            self.authority_ids != tuple(item.authority_id for item in refs)
            or self.authority_digests != tuple(item.digest for item in refs)
            or self.states != (state,) * len(refs)
            or self.consuming_receipt_ids != (receipt_id,) * len(refs)
            or self.consuming_receipt_digests != (receipt_digest,) * len(refs)
        ):
            raise MssqlR1V3ContractError("authority replay observation differs from sealed request")
        if not refs:
            return
        issuance = MssqlGenerationAuthoritySetIssuancePayloadV1.from_canonical_bytes(
            _required_bytes(self.issuance_payload, "issuance_payload")
        )
        registration = MssqlTargetRegistrationPayloadV1.from_canonical_bytes(request.registration_payload_bytes)
        if (
            issuance.target_binding_uuid,
            issuance.registration_id,
            issuance.registration_payload_digest,
            issuance.registration_revocation_revision,
            issuance.authority_set_bytes,
            issuance.authority_set_digest,
            self.issuance_state,
            *self._issuance_consumption,
        ) != (
            request.identity.target_binding_uuid,
            request.registration_id,
            request.registration_payload_digest,
            registration.revocation_revision,
            request.authority_set.canonical_bytes,
            request.authority_set.digest,
            state,
            receipt_id,
            receipt_digest,
        ):
            raise MssqlR1V3ContractError("replay issuance is not exact authority for sealed request")

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> AuthoritySetReplayObservationV3:
        values = list(decode_canonical_bytes(payload, _AUTHORITY_DOMAIN, field_count=13))
        values[5] = None if values[5] is None else expect_enum(R1ReplayResourceStateV3, values[5], "issuance_state")
        values[10] = tuple(
            expect_enum(R1ReplayResourceStateV3, item, "authority_state")
            for item in expect_tuple(values[10], "authority_states")
        )
        return cls(*values)  # type: ignore[arg-type]


def _required_bytes(value: bytes | None, field: str) -> bytes:
    if value is None:
        raise MssqlR1V3ContractError(f"{field} is required")
    return expect_bytes(value, field)


__all__ = [name for name in tuple(globals()) if name.endswith(("V1", "V3"))]
