"""Closed qualification/execution claims and structural verification subjects.

No function here verifies a signature, consumes a grant, issues a credential or
authorizes source access. Trusted runtime must perform those protected steps,
including atomic campaign counters and the complete real-row qualification path.
Qualification can be reread for its exact compilation intent; execution cannot
move to another deployment/activation. Existing production readers are unchanged.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, fields
from datetime import datetime
from typing import Any

from dpone.contracts.nonproduction_authority import (
    GRANT_SCHEMA,
    NonproductionAuthorityPolicy,
    NonproductionLimits,
    NonproductionSignatureSubject,
    effective_limits,
    require_epoch,
    require_validity,
    utc_timestamp,
)
from dpone.contracts.nonproduction_scope import (
    NonproductionAuthorityError,
    NonproductionScope,
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
from dpone.contracts.strict_json import strict_json_object


@dataclass(frozen=True, slots=True)
class NonproductionWorkload:
    """One exact native/ordinary pack and its own lower execution ceilings."""

    workload_id: str
    constituent_id: str
    pack_sha256: str
    limits: NonproductionLimits

    def __post_init__(self) -> None:
        text(self.workload_id)
        if text(self.constituent_id) not in {"native", "standalone"}:
            raise NonproductionAuthorityError("constituent")
        digest(self.pack_sha256)
        if type(self.limits) is not NonproductionLimits:
            raise NonproductionAuthorityError("limits")
        self.limits.__post_init__()

    @classmethod
    def from_dict(cls, value: object) -> NonproductionWorkload:
        body = exact_fields(value, {"workload_id", "constituent_id", "pack_sha256", "limits"})
        return cls(**dict(body, limits=NonproductionLimits.from_dict(body["limits"])))


def _require_workloads(values: tuple[NonproductionWorkload, ...], maximum: int) -> None:
    if type(values) is not tuple or any(type(row) is not NonproductionWorkload for row in values):
        raise NonproductionAuthorityError("workloads")
    for row in values:
        row.__post_init__()
    ceiling = min((maximum, *(row.limits.max_workloads for row in values)))
    ordered(tuple(row.workload_id for row in values), maximum=ceiling)
    if {row.constituent_id for row in values} != {"native", "standalone"}:
        raise NonproductionAuthorityError("complete_parent")


@dataclass(frozen=True, slots=True)
class _NonproductionGrant:
    """Common immutable claims shared by exactly two explicit phase variants."""

    scope: NonproductionScope
    grant_id: str
    not_before: str
    expires_at: str
    revocation_epoch: int
    limits: NonproductionLimits
    phase: str

    def __post_init__(self) -> None:
        if type(self.scope) is not NonproductionScope or type(self.limits) is not NonproductionLimits:
            raise NonproductionAuthorityError("grant_shape")
        self.scope.__post_init__()
        self.limits.__post_init__()
        uuid_text(self.grant_id, version4=True)
        require_epoch(self.revocation_epoch)
        require_validity(self.not_before, self.expires_at, self.limits.max_validity_seconds)

    def to_dict(self) -> dict[str, Any]:
        self.__post_init__()
        return strict_json_object(canonical_document({"schema": GRANT_SCHEMA, **asdict(self)}))

    def to_bytes(self) -> bytes:
        return canonical_document(self.to_dict())

    @property
    def grant_sha256(self) -> str:
        return document_sha256(self.to_dict())

    @property
    def consumption_subject_sha256(self) -> str:
        """Stable one-time ledger key; refingerprinting claims cannot reset it.

        Runtime must atomically bind this key to exact grant bytes and the phase
        subjects. This pure value cannot establish whether it was consumed.
        """
        self.__post_init__()
        return document_sha256({"grant_id": self.grant_id, "phase": self.phase})

    def signature_subject(self, policy: NonproductionAuthorityPolicy) -> NonproductionSignatureSubject:
        """Expected exact signature subject, not a signature verification result."""
        policy.require_scope(self.scope)
        return NonproductionSignatureSubject(
            GRANT_SCHEMA, self.phase, self.grant_sha256, policy.policy_sha256, policy.signer
        )


@dataclass(frozen=True, slots=True)
class NonproductionQualificationGrant(_NonproductionGrant):
    """One-time qualification run and plans; never execution or activation claims."""

    qualification_run_id: str
    fixture_plan_sha256: str
    qualification_plan_sha256: str

    def __post_init__(self) -> None:
        _NonproductionGrant.__post_init__(self)
        if self.phase != "qualification":
            raise NonproductionAuthorityError("grant_phase")
        uuid_text(self.qualification_run_id, version4=True)
        digest(self.fixture_plan_sha256)
        digest(self.qualification_plan_sha256)

    @classmethod
    def from_bytes(cls, raw: bytes) -> NonproductionQualificationGrant:
        return cls(**_grant_values(raw, cls))

    def require_qualification_subject(
        self, *, qualification_run_id: str, fixture_plan_sha256: str, qualification_plan_sha256: str
    ) -> None:
        """Compare independently reconstructed run/plan subjects before seeding."""
        self.__post_init__()
        if (self.qualification_run_id, self.fixture_plan_sha256, self.qualification_plan_sha256) != (
            qualification_run_id,
            fixture_plan_sha256,
            qualification_plan_sha256,
        ):
            raise NonproductionAuthorityError("qualification_subject")


@dataclass(frozen=True, slots=True)
class NonproductionExecutionGrant(_NonproductionGrant):
    """Late grant for the known complete parent/deployment/activation and packs.

    It excludes the future scoped request hash, avoiding a circular identity.
    The scoped request must include this grant's digest without changing the
    existing meaning of runtime_context_sha256. Its complete validity window
    must fit every workload's lower validity ceiling.
    """

    qualified_set_sha256: str
    native_release_id: str
    parent_release_id: str
    deployment_id: str
    activation_id: str
    workloads: tuple[NonproductionWorkload, ...]

    def __post_init__(self) -> None:
        _NonproductionGrant.__post_init__(self)
        if self.phase != "execution":
            raise NonproductionAuthorityError("grant_phase")
        for value in (self.qualified_set_sha256, self.native_release_id, self.parent_release_id, self.deployment_id):
            digest(value)
        uuid_text(self.activation_id, version4=True)
        _require_workloads(self.workloads, self.limits.max_workloads)
        for workload in self.workloads:
            require_validity(self.not_before, self.expires_at, workload.limits.max_validity_seconds)

    @classmethod
    def from_bytes(cls, raw: bytes) -> NonproductionExecutionGrant:
        return cls(**_grant_values(raw, cls))

    def require_execution_subject(
        self,
        *,
        qualified_set_sha256: str,
        native_release_id: str,
        parent_release_id: str,
        deployment_id: str,
        activation_id: str,
        workloads: tuple[NonproductionWorkload, ...],
    ) -> None:
        """Compare the complete independently verified ancestor and pack closure."""
        self.__post_init__()
        _require_workloads(workloads, self.limits.max_workloads)
        if (
            self.qualified_set_sha256,
            self.native_release_id,
            self.parent_release_id,
            self.deployment_id,
            self.activation_id,
            self.workloads,
        ) != (qualified_set_sha256, native_release_id, parent_release_id, deployment_id, activation_id, workloads):
            raise NonproductionAuthorityError("execution_subject")

    def workload_limits(self, workload_id: str, *, policy: NonproductionAuthorityPolicy) -> NonproductionLimits:
        """Return exact effective ceilings, not authorization to run that workload."""
        self.__post_init__()
        policy.require_scope(self.scope)
        workload = next((row for row in self.workloads if row.workload_id == workload_id), None)
        if workload is None:
            raise NonproductionAuthorityError("workload_membership")
        return effective_limits(policy.limits, self.limits, workload.limits)


def _grant_values(
    raw: bytes, cls: type[NonproductionQualificationGrant] | type[NonproductionExecutionGrant]
) -> dict[str, Any]:
    body = dict(exact_fields(parse_document(raw, GRANT_SCHEMA), {"schema", *(field.name for field in fields(cls))}))
    del body["schema"]
    body["scope"] = NonproductionScope.from_dict(body["scope"])
    body["limits"] = NonproductionLimits.from_dict(body["limits"])
    if cls is NonproductionExecutionGrant:
        body["workloads"] = tuple(
            NonproductionWorkload.from_dict(row) for row in sequence(body["workloads"], maximum=64)
        )
    return body


def parse_nonproduction_grant(raw: bytes) -> NonproductionQualificationGrant | NonproductionExecutionGrant:
    """Dispatch only the two closed nonproduction phases; no production fallback."""
    phase = parse_document(raw, GRANT_SCHEMA).get("phase")
    if phase == "qualification":
        return NonproductionQualificationGrant.from_bytes(raw)
    if phase == "execution":
        return NonproductionExecutionGrant.from_bytes(raw)
    raise NonproductionAuthorityError("grant_phase")


def validate_grant_subject(
    grant: NonproductionQualificationGrant | NonproductionExecutionGrant,
    *,
    policy: NonproductionAuthorityPolicy,
    expected_policy_sha256: str,
    expected_scope: NonproductionScope,
    signature_subject: NonproductionSignatureSubject,
    now: datetime,
    current_revocation_epoch: int,
) -> None:
    """Check exact subjects *after* a trusted capability verified original bytes.

    All expected inputs must originate outside the claims being checked. This
    comparison is not cryptographic proof, protected enrollment, one-time grant
    consumption, production qualification or an activation permission result.
    """
    if type(grant) not in {NonproductionQualificationGrant, NonproductionExecutionGrant}:
        raise NonproductionAuthorityError("grant_phase")
    if type(policy) is not NonproductionAuthorityPolicy or type(signature_subject) is not NonproductionSignatureSubject:
        raise NonproductionAuthorityError("verification_subject")
    grant.__post_init__()
    signature_subject.__post_init__()
    digest(expected_policy_sha256)
    if policy.policy_sha256 != expected_policy_sha256:
        raise NonproductionAuthorityError("policy_pin")
    policy.require_scope(grant.scope)
    if type(expected_scope) is not NonproductionScope or grant.scope != expected_scope:
        raise NonproductionAuthorityError("scope_subject")
    policy.require_current(now=now, current_revocation_epoch=current_revocation_epoch)
    if grant.revocation_epoch != current_revocation_epoch or grant.grant_id in policy.revoked_grant_ids:
        raise NonproductionAuthorityError("revoked")
    require_validity(
        grant.not_before,
        grant.expires_at,
        min(policy.limits.max_validity_seconds, grant.limits.max_validity_seconds),
        now=now,
    )
    if utc_timestamp(grant.not_before) < utc_timestamp(policy.not_before) or utc_timestamp(
        grant.expires_at
    ) > utc_timestamp(policy.expires_at):
        raise NonproductionAuthorityError("policy_validity")
    if signature_subject != grant.signature_subject(policy):
        raise NonproductionAuthorityError("signature_subject")
    if isinstance(grant, NonproductionExecutionGrant):
        _require_workloads(grant.workloads, min(policy.limits.max_workloads, grant.limits.max_workloads))
