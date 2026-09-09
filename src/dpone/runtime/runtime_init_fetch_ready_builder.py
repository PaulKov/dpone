"""Build one runtime ready record from verified publication inputs."""

from __future__ import annotations

from collections.abc import Mapping

from dpone.ports.airflow_deployment_attestation import (
    AirflowArtifactAttestationVerification,
    AirflowArtifactObservedSubject,
)
from dpone.runtime.init_fetch_contract import InitFetchError
from dpone.runtime.runtime_init_fetch_attestation_decision import attestation_decision_sha256
from dpone.runtime.runtime_init_fetch_plan import RuntimeInitFetchPlan
from dpone.runtime.runtime_init_fetch_ready_models import (
    ATTESTATION_REQUIREMENT_OPTIONAL,
    ATTESTATION_REQUIREMENT_REQUIRED,
    RUNTIME_FETCH_READY_SCHEMA,
    RUNTIME_FETCH_READY_SCHEMA_V2,
    ReadyArtifact,
    RuntimeFetchReady,
)


def build_runtime_fetch_ready(
    plan: RuntimeInitFetchPlan,
    *,
    plan_sha256: str,
    published: Mapping[str, ReadyArtifact],
    attestation_required: bool,
    attestation_status: str,
    attestation_verification: AirflowArtifactAttestationVerification | None,
    attestation_subject: AirflowArtifactObservedSubject | None,
    runtime_payload_sha256: str,
    verified_pack_fingerprint: str,
) -> RuntimeFetchReady:
    """Bind one exact plan, artifact set, and attestation decision."""

    if set(published) != {artifact.artifact_ref for artifact in plan.artifacts}:
        raise InitFetchError(
            "DPONE_RUNTIME_ARTIFACT_INTEGRITY_FAILED",
            "runtime ready publication does not contain the exact pinned artifact set",
        )
    trust_policy_sha256 = plan.trust_policy_ref["sha256"] if plan.trust_policy_ref is not None else None
    requirement = ATTESTATION_REQUIREMENT_REQUIRED if attestation_required else ATTESTATION_REQUIREMENT_OPTIONAL
    if attestation_status not in {"passed", "not_required"}:
        raise InitFetchError(
            "DPONE_RUNTIME_FETCH_READY_INVALID",
            "runtime ready attestation status is invalid",
        )
    if attestation_required and attestation_status != "passed":
        raise InitFetchError(
            "DPONE_RUNTIME_FETCH_READY_INVALID",
            "required runtime attestation did not pass",
        )
    if not attestation_required and attestation_verification is not None:
        raise InitFetchError(
            "DPONE_RUNTIME_FETCH_READY_INVALID",
            "optional runtime attestation cannot carry deployment evidence",
        )
    status = attestation_status
    attestation_id = None if attestation_verification is None else attestation_verification.attestation_id
    verification_sha256 = None if attestation_verification is None else attestation_verification.decision_sha256
    subject_kind = None if attestation_subject is None else "airflow_deployment"
    backend = None if attestation_subject is None else "cosign_public_key"
    observed_claims = () if attestation_subject is None else attestation_subject.observed_claims
    unobserved_claims = () if attestation_subject is None else attestation_subject.unobserved_claims
    return RuntimeFetchReady(
        plan_sha256=plan_sha256,
        release_id=plan.release_id,
        deployment_id=plan.deployment_id,
        runtime_image_digest=plan.runtime_image_digest,
        trust_tier=plan.trust_tier,
        artifact_registry_ref=plan.artifact_registry_ref,
        registry_config_sha256=plan.registry_config_ref["sha256"],
        trust_policy_sha256=trust_policy_sha256,
        workload_id=plan.workload_pack.id,
        pack_fingerprint=verified_pack_fingerprint,
        release=published[plan.release.artifact_ref],
        deployment=published[plan.deployment.artifact_ref],
        workload_pack=published[plan.workload_pack.artifact_ref],
        checksum_status="passed",
        effective_attestation_requirement=requirement,
        attestation_status=status,
        attestation_decision_sha256=attestation_decision_sha256(
            plan_sha256=plan_sha256,
            trust_policy_sha256=trust_policy_sha256,
            effective_requirement=requirement,
            attestation_status=status,
            artifact_attestation_subject_kind=subject_kind,
            artifact_attestation_backend=backend,
            artifact_attestation_id=attestation_id,
            artifact_attestation_verification_sha256=verification_sha256,
            artifact_attestation_observed_claims=observed_claims,
            artifact_attestation_unobserved_claims=unobserved_claims,
        ),
        runtime_payload_sha256=runtime_payload_sha256,
        artifact_attestation_subject_kind=subject_kind,
        artifact_attestation_backend=backend,
        artifact_attestation_id=attestation_id,
        artifact_attestation_verification_sha256=verification_sha256,
        artifact_attestation_observed_claims=observed_claims,
        artifact_attestation_unobserved_claims=unobserved_claims,
        schema=(RUNTIME_FETCH_READY_SCHEMA_V2 if attestation_verification is not None else RUNTIME_FETCH_READY_SCHEMA),
    )


__all__ = ["build_runtime_fetch_ready"]
