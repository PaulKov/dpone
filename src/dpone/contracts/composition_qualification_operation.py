"""Exact internal qualification originals, without lifecycle or authority.

Construction, decoding and structural comparison do not authenticate grants,
observe a runner, reserve budgets, acquire domains, issue credentials or permit
terminalization. Protected journals must reopen real grant and plan originals,
verify actual invocation and enforce current owner/epoch state transactionally.
These records add no external nonproduction authority family.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, fields
from typing import Literal

from dpone.contracts.composition_activation import CompositionAdmissionError
from dpone.contracts.composition_ownership import CompositionOwnerReference, CompositionPhysicalClaim
from dpone.contracts.nonproduction_scope import (
    NonproductionAuthorityError,
    canonical_document,
    digest,
    document_sha256,
    exact_fields,
    ordered,
    parse_document,
    sequence,
    text,
    uuid_text,
)

_OWNER_SCHEMA = "dpone.composition-qualification-owner.v1"
_OPERATION_SCHEMA = "dpone.composition-qualification-operation.v1"


@dataclass(frozen=True, slots=True)
class CompositionQualificationOwner:
    """Immutable complete qualification scope bound to its actual run originals.

    The consumption digest comes from the original grant. Rebinding the same
    owner identity to changed originals must be rejected by protected storage;
    a newly hashed structural value cannot authorize that replacement.
    """

    qualification_run_id: str
    consumption_subject_sha256: str
    grant_sha256: str
    fixture_plan_sha256: str
    qualification_plan_sha256: str
    scope_sha256: str
    environment_id: str
    campaign_id: str
    claims: tuple[CompositionPhysicalClaim, ...]

    def __post_init__(self) -> None:
        try:
            uuid_text(self.qualification_run_id, version4=True)
            uuid_text(self.campaign_id, version4=True)
            uuid_text(self.environment_id)
            for value in (
                self.consumption_subject_sha256,
                self.grant_sha256,
                self.fixture_plan_sha256,
                self.qualification_plan_sha256,
                self.scope_sha256,
            ):
                digest(value)
            if type(self.claims) is not tuple or not 1 <= len(self.claims) <= 256:
                raise CompositionAdmissionError("qualification_owner_claims")
            if any(type(claim) is not CompositionPhysicalClaim for claim in self.claims):
                raise CompositionAdmissionError("qualification_owner_claims")
            ordered(tuple(claim.guard_id for claim in self.claims), maximum=256)
        except NonproductionAuthorityError:
            raise CompositionAdmissionError("qualification_owner") from None
        if {claim.connector for claim in self.claims} != {"mssql", "clickhouse", "postgres"}:
            raise CompositionAdmissionError("qualification_owner_partition")
        if sum(len(claim.read_subjects) + len(claim.write_subjects) for claim in self.claims) > 8192:
            raise CompositionAdmissionError("qualification_owner_budget")

    @property
    def owner_reference(self) -> CompositionOwnerReference:
        """Reference the actual qualification run, never a fabricated activation."""
        self.__post_init__()
        return CompositionOwnerReference("qualification", self.qualification_run_id)

    @property
    def owner_key(self) -> str:
        return self.owner_reference.owner_key

    @property
    def subject_sha256(self) -> str:
        """Hash the exact complete original owner, including all physical claims."""
        try:
            return document_sha256(self.to_dict())
        except NonproductionAuthorityError:
            raise CompositionAdmissionError("qualification_owner_document") from None

    def to_dict(self) -> dict[str, object]:
        """Return detached JSON-compatible values for the closed owner document."""
        self.__post_init__()
        return {"schema": _OWNER_SCHEMA, **asdict(self), "claims": [claim.to_dict() for claim in self.claims]}

    def to_bytes(self) -> bytes:
        """Produce finite bounded canonical UTF-8 original bytes."""
        try:
            return canonical_document(self.to_dict())
        except NonproductionAuthorityError:
            raise CompositionAdmissionError("qualification_owner_document") from None

    @classmethod
    def from_bytes(cls, raw: bytes, *, expected_sha256: str) -> CompositionQualificationOwner:
        """Reopen only canonical exact originals matching the required digest."""
        try:
            digest(expected_sha256)
            body = exact_fields(parse_document(raw, _OWNER_SCHEMA), {"schema", *(field.name for field in fields(cls))})
            body.pop("schema")
            value = cls(
                **dict(
                    body,
                    claims=tuple(
                        CompositionPhysicalClaim.from_dict(claim) for claim in sequence(body["claims"], maximum=256)
                    ),
                )
            )
            if value.subject_sha256 != expected_sha256:
                raise CompositionAdmissionError("qualification_owner_subject")
            return value
        except NonproductionAuthorityError:
            raise CompositionAdmissionError("qualification_owner_document") from None


@dataclass(frozen=True, slots=True)
class CompositionQualificationOperation:
    """One plan-selected work item and actual controlled runner invocation/try.

    Protected rows additionally enforce uniqueness of ``(owner_key, work_item_id,
    runner_invocation_id, try_number)``. Changing action, subjects or epochs must
    not evade replay protection by merely yielding a different operation hash.
    """

    owner_key: str
    owner_subject_sha256: str
    qualification_run_id: str
    grant_sha256: str
    fixture_plan_sha256: str
    qualification_plan_sha256: str
    work_item_id: str
    work_item_sha256: str
    action: Literal["fixture_seed", "route_qualification", "source_seal"]
    runner_invocation_id: str
    try_number: int
    guard_epochs: tuple[tuple[str, int], ...]

    def __post_init__(self) -> None:
        try:
            uuid_text(self.qualification_run_id, version4=True)
            uuid_text(self.runner_invocation_id, version4=True)
            text(self.work_item_id)
            for value in (
                self.owner_key,
                self.owner_subject_sha256,
                self.grant_sha256,
                self.fixture_plan_sha256,
                self.qualification_plan_sha256,
                self.work_item_sha256,
            ):
                digest(value)
            if type(self.action) is not str or self.action not in {
                "fixture_seed",
                "route_qualification",
                "source_seal",
            }:
                raise CompositionAdmissionError("qualification_operation_action")
            if self.owner_key != CompositionOwnerReference("qualification", self.qualification_run_id).owner_key:
                raise CompositionAdmissionError("qualification_operation_owner")
            _require_positive_bigint(self.try_number)
            if type(self.guard_epochs) is not tuple or not 1 <= len(self.guard_epochs) <= 256:
                raise CompositionAdmissionError("qualification_operation_epochs")
            for pair in self.guard_epochs:
                if type(pair) is not tuple or len(pair) != 2:
                    raise CompositionAdmissionError("qualification_operation_epochs")
                digest(pair[0])
                _require_positive_bigint(pair[1])
            ordered(tuple(guard for guard, _ in self.guard_epochs), maximum=256)
            canonical_document(asdict(self))
        except NonproductionAuthorityError:
            raise CompositionAdmissionError("qualification_operation") from None

    @property
    def operation_key(self) -> str:
        """Hash exact operation bytes, preserving slash, backslash and Unicode."""
        try:
            return document_sha256(self.to_dict())
        except NonproductionAuthorityError:
            raise CompositionAdmissionError("qualification_operation_document") from None

    @property
    def invocation_key(self) -> str:
        """Project exact replay coordinates without changing original operation bytes.

        Changed action, subjects or epochs cannot create a new invocation. The
        protected journal must enforce duplicate rejection and independently
        compare retained originals; this digest grants no admission or recovery.
        """
        self.__post_init__()
        try:
            return document_sha256(
                {
                    "schema": "dpone.composition-qualification-invocation-key.v1",
                    "owner_key": self.owner_key,
                    "work_item_id": self.work_item_id,
                    "runner_invocation_id": self.runner_invocation_id,
                    "try_number": self.try_number,
                }
            )
        except NonproductionAuthorityError:
            raise CompositionAdmissionError("qualification_operation_document") from None

    def require_owner(self, owner: CompositionQualificationOwner) -> None:
        """Compare original owner and guard membership; this grants no admission.

        Selected plan completeness, actual current epochs, owner state, current
        authentication and observed runner identity remain protected obligations.
        """
        self.__post_init__()
        if type(owner) is not CompositionQualificationOwner:
            raise CompositionAdmissionError("qualification_operation_owner")
        owner.__post_init__()
        if (
            self.owner_key,
            self.owner_subject_sha256,
            self.qualification_run_id,
            self.grant_sha256,
            self.fixture_plan_sha256,
            self.qualification_plan_sha256,
        ) != (
            owner.owner_key,
            owner.subject_sha256,
            owner.qualification_run_id,
            owner.grant_sha256,
            owner.fixture_plan_sha256,
            owner.qualification_plan_sha256,
        ):
            raise CompositionAdmissionError("qualification_operation_owner")
        if not {guard for guard, _ in self.guard_epochs} <= {claim.guard_id for claim in owner.claims}:
            raise CompositionAdmissionError("qualification_operation_owner_scope")

    def to_dict(self) -> dict[str, object]:
        """Return detached JSON values; epochs are arrays, never supplied objects."""
        self.__post_init__()
        return {"schema": _OPERATION_SCHEMA, **asdict(self), "guard_epochs": [list(pair) for pair in self.guard_epochs]}

    def to_bytes(self) -> bytes:
        """Produce the exact bounded original operation, without normalization."""
        try:
            return canonical_document(self.to_dict())
        except NonproductionAuthorityError:
            raise CompositionAdmissionError("qualification_operation_document") from None

    @classmethod
    def from_bytes(cls, raw: bytes, *, expected_sha256: str) -> CompositionQualificationOperation:
        """Require canonical closed original bytes and their externally pinned hash."""
        try:
            digest(expected_sha256)
            body = exact_fields(
                parse_document(raw, _OPERATION_SCHEMA), {"schema", *(field.name for field in fields(cls))}
            )
            body.pop("schema")
            value = cls(
                **dict(
                    body,
                    guard_epochs=tuple(
                        tuple(sequence(pair, maximum=2)) for pair in sequence(body["guard_epochs"], maximum=256)
                    ),
                )
            )
            if value.operation_key != expected_sha256:
                raise CompositionAdmissionError("qualification_operation_subject")
            return value
        except NonproductionAuthorityError:
            raise CompositionAdmissionError("qualification_operation_document") from None


def _require_positive_bigint(value: object) -> None:
    """Reject booleans and overflow before values reach protected BIGINT columns."""
    if type(value) is not int or not 1 <= value <= 2**63 - 1:
        raise CompositionAdmissionError("qualification_operation_positive_bigint")
