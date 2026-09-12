"""Retained MSSQL statements and externally proven renderer authority."""

from __future__ import annotations

import hashlib
import unicodedata
from dataclasses import dataclass

from dpone.contracts.mssql_r1_v3_identity import (
    MssqlR1V3ContractError,
    canonical_bytes,
    decode_canonical_bytes,
    expect_bytes,
    expect_enum,
    expect_tuple,
    require_canonical_text,
    require_digest,
)
from dpone.contracts.mssql_r1_v3_plan import (
    MutationPlanV1,
    R1BatchMutationPlanV1,
    R1MutationStepV1,
    R1XminMutationPlanV1,
)

RENDERER_ID = "dpone-mssql-r1-renderer"
RENDERER_VERSION = "1"
QUOTING_POLICY_VERSION = "dpone-mssql-bracket-quote-v1"
_STATEMENT_DOMAIN = b"dpone-r1-rendered-mssql-statement-v1\0"
_BUNDLE_DOMAIN = b"dpone-r1-rendered-mssql-mutation-bundle-v1\0"
_EXECUTION_DOMAIN = b"dpone-r1-rendered-mssql-execution-v1\0"
_RENDERER_AUTHORITY_DOMAIN = b"dpone-r1-mssql-renderer-authority-v1\0"
_ADMISSION_EVIDENCE_DOMAIN = b"dpone-r1-mssql-renderer-admission-evidence-v1\0"


@dataclass(frozen=True, slots=True)
class MssqlR1RendererAuthorityV1:
    """Exact renderer authority resolved independently from an environment profile."""

    resolved_profile_digest: bytes
    renderer_id: str
    renderer_version: str
    renderer_build_digest: bytes
    renderer_contract_digest: bytes
    quoting_policy_version: str
    verification_policy_digest: bytes

    def __post_init__(self) -> None:
        for name in (
            "resolved_profile_digest",
            "renderer_build_digest",
            "renderer_contract_digest",
            "verification_policy_digest",
        ):
            require_digest(getattr(self, name), name)
        require_canonical_text(self.renderer_id, "renderer_id", maximum_bytes=128)
        require_canonical_text(self.renderer_version, "renderer_version", maximum_bytes=64)
        require_canonical_text(self.quoting_policy_version, "quoting_policy_version", maximum_bytes=128)
        if (self.renderer_id, self.renderer_version, self.quoting_policy_version) != (
            RENDERER_ID,
            RENDERER_VERSION,
            QUOTING_POLICY_VERSION,
        ):
            raise MssqlR1V3ContractError("renderer authority provider identity is unsupported")

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(
            _RENDERER_AUTHORITY_DOMAIN,
            tuple(getattr(self, name) for name in self.__dataclass_fields__),
        )

    @property
    def digest(self) -> bytes:
        return hashlib.sha256(self.canonical_bytes).digest()

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1RendererAuthorityV1:
        return cls(*decode_canonical_bytes(payload, _RENDERER_AUTHORITY_DOMAIN, field_count=7))  # type: ignore[arg-type]


@dataclass(frozen=True, slots=True)
class MssqlR1RenderedStatementV1:
    step_kind: R1MutationStepV1
    statement_utf8_bytes: bytes
    statement_digest: bytes
    parameter_contract_digest: bytes
    result_contract_digest: bytes

    def __post_init__(self) -> None:
        if not isinstance(self.step_kind, R1MutationStepV1):
            raise MssqlR1V3ContractError("rendered statement step is unsupported")
        for name in ("statement_digest", "parameter_contract_digest", "result_contract_digest"):
            require_digest(getattr(self, name), name)
        if not isinstance(self.statement_utf8_bytes, bytes) or not self.statement_utf8_bytes:
            raise MssqlR1V3ContractError("rendered statement requires exact UTF-8 bytes")
        try:
            statement = self.statement_utf8_bytes.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise MssqlR1V3ContractError("rendered statement is not valid UTF-8") from exc
        if "\0" in statement or unicodedata.normalize("NFC", statement) != statement:
            raise MssqlR1V3ContractError("rendered statement is not canonical UTF-8")
        if hashlib.sha256(self.statement_utf8_bytes).digest() != self.statement_digest:
            raise MssqlR1V3ContractError("rendered statement digest differs from exact SQL bytes")

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(
            _STATEMENT_DOMAIN,
            tuple(getattr(self, name) for name in self.__dataclass_fields__),
        )

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1RenderedStatementV1:
        values = list(decode_canonical_bytes(payload, _STATEMENT_DOMAIN, field_count=5))
        values[0] = expect_enum(R1MutationStepV1, values[0], "step_kind")
        return cls(*values)  # type: ignore[arg-type]


@dataclass(frozen=True, slots=True)
class MssqlR1RendererAdmissionEvidenceV1:
    """Evidence emitted only by the injected verifier for exact rendered bytes."""

    resolved_profile_digest: bytes
    renderer_authority_digest: bytes
    semantic_plan_digest: bytes
    rendered_execution_digest: bytes
    verification_policy_digest: bytes
    verifier_id: str
    verifier_version: str
    attestation_bundle_digest: bytes

    def __post_init__(self) -> None:
        for name in (
            "resolved_profile_digest",
            "renderer_authority_digest",
            "semantic_plan_digest",
            "rendered_execution_digest",
            "verification_policy_digest",
            "attestation_bundle_digest",
        ):
            require_digest(getattr(self, name), name)
        require_canonical_text(self.verifier_id, "verifier_id", maximum_bytes=128)
        require_canonical_text(self.verifier_version, "verifier_version", maximum_bytes=64)

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(
            _ADMISSION_EVIDENCE_DOMAIN,
            tuple(getattr(self, name) for name in self.__dataclass_fields__),
        )

    @property
    def digest(self) -> bytes:
        return hashlib.sha256(self.canonical_bytes).digest()

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1RendererAdmissionEvidenceV1:
        return cls(*decode_canonical_bytes(payload, _ADMISSION_EVIDENCE_DOMAIN, field_count=8))  # type: ignore[arg-type]


@dataclass(frozen=True, slots=True)
class MssqlR1RenderedMutationBundleV1:
    semantic_plan_digest: bytes
    renderer_id: str
    renderer_version: str
    renderer_build_digest: bytes
    renderer_contract_digest: bytes
    quoting_policy_version: str
    statements: tuple[MssqlR1RenderedStatementV1, ...]
    renderer_authority_digest: bytes
    admission_evidence_digest: bytes

    def __post_init__(self) -> None:
        for name in (
            "semantic_plan_digest",
            "renderer_build_digest",
            "renderer_contract_digest",
            "renderer_authority_digest",
            "admission_evidence_digest",
        ):
            require_digest(getattr(self, name), name)
        require_canonical_text(self.renderer_id, "renderer_id", maximum_bytes=128)
        require_canonical_text(self.renderer_version, "renderer_version", maximum_bytes=64)
        require_canonical_text(self.quoting_policy_version, "quoting_policy_version", maximum_bytes=128)
        if (self.renderer_id, self.renderer_version, self.quoting_policy_version) != (
            RENDERER_ID,
            RENDERER_VERSION,
            QUOTING_POLICY_VERSION,
        ):
            raise MssqlR1V3ContractError("rendered bundle provider identity is unsupported")
        if not self.statements or not all(isinstance(item, MssqlR1RenderedStatementV1) for item in self.statements):
            raise MssqlR1V3ContractError("rendered bundle requires typed statements")
        steps = tuple(statement.step_kind for statement in self.statements)
        if len(steps) != len(set(steps)):
            raise MssqlR1V3ContractError("rendered bundle contains duplicate steps")

    @property
    def execution_payload_bytes(self) -> bytes:
        return canonical_bytes(
            _EXECUTION_DOMAIN,
            (
                self.semantic_plan_digest,
                self.renderer_id,
                self.renderer_version,
                self.renderer_build_digest,
                self.renderer_contract_digest,
                self.quoting_policy_version,
                tuple(statement.canonical_bytes for statement in self.statements),
                self.renderer_authority_digest,
            ),
        )

    @property
    def execution_payload_digest(self) -> bytes:
        return hashlib.sha256(self.execution_payload_bytes).digest()

    def validate_retained_for_plan(self, plan: MutationPlanV1) -> None:
        if not isinstance(plan, (R1BatchMutationPlanV1, R1XminMutationPlanV1)):
            raise MssqlR1V3ContractError("rendered bundle requires an R1 semantic plan")
        if (
            self.semantic_plan_digest != plan.digest
            or self.renderer_contract_digest != plan.template_set.renderer_contract_digest
            or tuple(statement.step_kind for statement in self.statements) != plan.template_set.ordered_steps
        ):
            raise MssqlR1V3ContractError("rendered bundle differs from sealed semantic plan")

    def validate_admission(
        self,
        plan: MutationPlanV1,
        authority: MssqlR1RendererAuthorityV1,
        evidence: MssqlR1RendererAdmissionEvidenceV1,
        resolved_profile_digest: bytes,
    ) -> None:
        self.validate_retained_for_plan(plan)
        expected_authority = (
            resolved_profile_digest,
            self.renderer_id,
            self.renderer_version,
            self.renderer_build_digest,
            self.renderer_contract_digest,
            self.quoting_policy_version,
        )
        observed_authority = (
            authority.resolved_profile_digest,
            authority.renderer_id,
            authority.renderer_version,
            authority.renderer_build_digest,
            authority.renderer_contract_digest,
            authority.quoting_policy_version,
        )
        expected_evidence = (
            resolved_profile_digest,
            authority.digest,
            plan.digest,
            self.execution_payload_digest,
            authority.verification_policy_digest,
        )
        observed_evidence = (
            evidence.resolved_profile_digest,
            evidence.renderer_authority_digest,
            evidence.semantic_plan_digest,
            evidence.rendered_execution_digest,
            evidence.verification_policy_digest,
        )
        if expected_authority != observed_authority or self.renderer_authority_digest != authority.digest:
            raise MssqlR1V3ContractError("rendered bundle differs from independently resolved renderer authority")
        if expected_evidence != observed_evidence or self.admission_evidence_digest != evidence.digest:
            raise MssqlR1V3ContractError("rendered bundle lacks exact independent admission evidence")

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(
            _BUNDLE_DOMAIN,
            (
                self.semantic_plan_digest,
                self.renderer_id,
                self.renderer_version,
                self.renderer_build_digest,
                self.renderer_contract_digest,
                self.quoting_policy_version,
                tuple(statement.canonical_bytes for statement in self.statements),
                self.renderer_authority_digest,
                self.admission_evidence_digest,
            ),
        )

    @property
    def digest(self) -> bytes:
        return hashlib.sha256(self.canonical_bytes).digest()

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1RenderedMutationBundleV1:
        values = list(decode_canonical_bytes(payload, _BUNDLE_DOMAIN, field_count=9))
        values[6] = tuple(
            MssqlR1RenderedStatementV1.from_canonical_bytes(expect_bytes(item, "rendered_statement"))
            for item in expect_tuple(values[6], "rendered statements")
        )
        return cls(*values)  # type: ignore[arg-type]


@dataclass(frozen=True, slots=True)
class MssqlR1VerifiedRenderedMutationV1:
    """Admission aggregate assembled only from injected registry and verifier results."""

    plan: MutationPlanV1
    bundle: MssqlR1RenderedMutationBundleV1
    authority: MssqlR1RendererAuthorityV1
    evidence: MssqlR1RendererAdmissionEvidenceV1
    resolved_profile_digest: bytes

    def __post_init__(self) -> None:
        require_digest(self.resolved_profile_digest, "resolved_profile_digest")
        self.bundle.validate_admission(self.plan, self.authority, self.evidence, self.resolved_profile_digest)


__all__ = [name for name in tuple(globals()) if name.startswith("MssqlR1") or name.endswith(("_ID", "_VERSION"))]
