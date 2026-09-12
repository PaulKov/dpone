"""Source-free admission contracts for MSSQL target authority V2.

The request contains only scheduler-stable coordinates and target-local
authority known before PostgreSQL business I/O. Source snapshot and staging
intent fields deliberately belong to the later sealing contract.
"""

from __future__ import annotations

from dataclasses import dataclass

from dpone._compat import StrEnum
from dpone.contracts.mssql_target_authority_v2_identity import (
    CONTRACT_VERSION,
    MssqlReceiptContractError,
    MssqlTargetHeadV2,
    MssqlTargetIdentityV2,
    SourceMode,
    WriterMode,
    build_effect_key,
    build_operation_key,
    require_digest,
    require_positive,
)
from dpone.contracts.mssql_target_authority_v2_receipts import (
    MssqlEffectReceiptV2,
    build_receipt,
)

_MAX_DESCENDANT_PROOF_RECEIPTS = 100_000
_MAX_LEASE_SECONDS = 3_600


class MssqlPreSourceAdmissionOutcomeV2(StrEnum):
    """Closed source-free decision returned by the target authority."""

    ADMITTED = "admitted"
    REPLAY_SUPPRESSED = "replay_suppressed"


@dataclass(frozen=True, slots=True)
class MssqlPreSourceAuthorityRequestV2:
    """Exact authority needed to replay or claim one stable target effect."""

    route_identity_sha256: bytes
    invocation_identity: str
    source_mode: SourceMode
    target_identity: MssqlTargetIdentityV2
    expected_head: MssqlTargetHeadV2 | None
    candidate_writer_generation: int
    candidate_head_revision: int
    source_authority_sha256: bytes
    type_policy_digest: bytes
    hash_policy_digest: bytes
    quality_policy_digest: bytes
    owner_id_digest: bytes
    lease_seconds: int
    max_descendant_proof_receipts: int = _MAX_DESCENDANT_PROOF_RECEIPTS
    v1_writer_retired: bool = True
    contract_version: str = CONTRACT_VERSION

    def __post_init__(self) -> None:
        for name in (
            "route_identity_sha256",
            "source_authority_sha256",
            "type_policy_digest",
            "hash_policy_digest",
            "quality_policy_digest",
            "owner_id_digest",
        ):
            require_digest(getattr(self, name), name)
        if not isinstance(self.source_mode, SourceMode):
            raise MssqlReceiptContractError("source mode is unsupported")
        if not isinstance(self.target_identity, MssqlTargetIdentityV2):
            raise MssqlReceiptContractError("target_identity must be MSSQL target authority V2")
        if (
            not self.contract_version
            or len(self.contract_version) > 64
            or self.contract_version != self.contract_version.strip()
        ):
            raise MssqlReceiptContractError("contract_version must be canonical non-empty text")
        # Validate scheduler-stable identity eagerly at the contract boundary.
        self.operation_key
        self.effect_key
        require_positive(self.candidate_writer_generation, "candidate_writer_generation")
        require_positive(self.candidate_head_revision, "candidate_head_revision")
        if isinstance(self.lease_seconds, bool) or not 1 <= self.lease_seconds <= _MAX_LEASE_SECONDS:
            raise MssqlReceiptContractError("lease_seconds must be in 1..3600")
        if (
            isinstance(self.max_descendant_proof_receipts, bool)
            or not 1 <= self.max_descendant_proof_receipts <= _MAX_DESCENDANT_PROOF_RECEIPTS
        ):
            raise MssqlReceiptContractError("descendant proof bound is invalid")
        if not isinstance(self.v1_writer_retired, bool):
            raise MssqlReceiptContractError("v1_writer_retired must be boolean")
        self._validate_transition()

    @property
    def operation_key(self) -> bytes:
        """Return the retry-stable key, excluding generations, epochs and owner."""

        return build_operation_key(
            route_identity_sha256=self.route_identity_sha256,
            invocation_identity=self.invocation_identity,
            source_mode=self.source_mode,
            target_binding_uuid=self.target_identity.target_binding_uuid,
            contract_version=self.contract_version,
        )

    @property
    def effect_key(self) -> bytes:
        return build_effect_key(
            operation_key=self.operation_key,
            source_mode=self.source_mode,
            target_binding_uuid=self.target_identity.target_binding_uuid,
            contract_version=self.contract_version,
        )

    @property
    def writer_mode(self) -> WriterMode:
        return WriterMode(self.source_mode.value)

    def matches_replay_receipt(self, receipt: MssqlEffectReceiptV2) -> bool:
        """Match only immutable authority available before source business I/O."""

        header = receipt.header
        try:
            valid_digest = receipt == build_receipt(header, receipt.body)
        except MssqlReceiptContractError:
            return False
        return valid_digest and (
            header.operation_key == self.operation_key
            and header.effect_key == self.effect_key
            and header.contract_version == self.contract_version
            and header.target_identity == self.target_identity
            and header.writer_mode is self.writer_mode
            and header.route_identity_sha256 == self.route_identity_sha256
            and header.source_authority_sha256 == self.source_authority_sha256
            and header.type_policy_digest == self.type_policy_digest
            and header.hash_policy_digest == self.hash_policy_digest
            and header.quality_policy_digest == self.quality_policy_digest
        )

    def _validate_transition(self) -> None:
        head = self.expected_head
        if head is None:
            if (self.candidate_writer_generation, self.candidate_head_revision) != (1, 1):
                raise MssqlReceiptContractError("initial target transition must be generation 1 revision 1")
            return
        if head.target_binding_uuid != self.target_identity.target_binding_uuid:
            raise MssqlReceiptContractError("expected head differs from target binding")
        if (
            head.recovery_domain_uuid != self.target_identity.recovery_domain_uuid
            or head.recovery_domain_epoch != self.target_identity.recovery_domain_epoch
        ):
            raise MssqlReceiptContractError("expected head differs from target recovery domain")
        same_generation = (
            self.writer_mode is head.writer_mode
            and self.candidate_writer_generation == head.writer_generation
            and self.candidate_head_revision == head.head_revision + 1
        )
        next_generation = (
            self.candidate_writer_generation == head.writer_generation + 1 and self.candidate_head_revision == 1
        )
        if not (same_generation or next_generation):
            message = (
                "writer mode transition requires an adjacent new generation"
                if self.writer_mode is not head.writer_mode
                else "target transition must be adjacent"
            )
            raise MssqlReceiptContractError(message)


@dataclass(frozen=True, slots=True)
class MssqlPreSourceAdmissionV2:
    """Committed target-only admission result; source I/O is always absent."""

    outcome: MssqlPreSourceAdmissionOutcomeV2
    operation_key: bytes
    effect_key: bytes
    operation_epoch: int
    replay_receipt: MssqlEffectReceiptV2 | None = None
    source_io_performed: bool = False

    def __post_init__(self) -> None:
        if not isinstance(self.outcome, MssqlPreSourceAdmissionOutcomeV2):
            raise MssqlReceiptContractError("pre-source admission outcome is unsupported")
        require_digest(self.operation_key, "operation_key")
        require_digest(self.effect_key, "effect_key")
        require_positive(self.operation_epoch, "operation_epoch")
        if not isinstance(self.source_io_performed, bool) or self.source_io_performed:
            raise MssqlReceiptContractError("pre-source admission cannot report source I/O")
        replay = self.outcome is MssqlPreSourceAdmissionOutcomeV2.REPLAY_SUPPRESSED
        if replay != (self.replay_receipt is not None):
            raise MssqlReceiptContractError("replay receipt is required only for replay suppression")
        if self.replay_receipt is not None:
            header = self.replay_receipt.header
            if header.operation_key != self.operation_key or header.effect_key != self.effect_key:
                raise MssqlReceiptContractError("replay receipt differs from admission identity")


__all__ = [
    "MssqlPreSourceAdmissionOutcomeV2",
    "MssqlPreSourceAdmissionV2",
    "MssqlPreSourceAuthorityRequestV2",
]
