"""Select and execute one runtime artifact-attestation authority."""

from __future__ import annotations

from dataclasses import dataclass

from dpone.contracts.runtime_artifact_attestation import (
    RuntimeArtifactAttestationError,
)
from dpone.ports.airflow_deployment_attestation import (
    AirflowArtifactAttestationRegistryError,
    AirflowArtifactAttestationRejected,
    AirflowArtifactAttestationVerification,
    AirflowArtifactObservedSubject,
    AirflowDeploymentAttestationVerifier,
)
from dpone.ports.runtime_artifact_attestation import (
    RuntimeArtifactAttestationVerifier,
)
from dpone.runtime.init_fetch_contract import InitFetchError
from dpone.runtime.runtime_init_fetch_attestation import (
    StagedRuntimeArtifact,
    release_attestation_subject,
)
from dpone.runtime.runtime_init_fetch_plan import RuntimeInitFetchPlan


@dataclass(frozen=True, slots=True)
class RuntimeAttestationDecision:
    status: str
    deployment_verification: AirflowArtifactAttestationVerification | None = None
    deployment_subject: AirflowArtifactObservedSubject | None = None


def verify_runtime_attestation(
    *,
    plan: RuntimeInitFetchPlan,
    staged: tuple[StagedRuntimeArtifact, ...],
    required: bool,
    release_verifier: RuntimeArtifactAttestationVerifier | None,
    deployment_verifier: AirflowDeploymentAttestationVerifier | None,
    deployment_subject: AirflowArtifactObservedSubject | None = None,
) -> RuntimeAttestationDecision:
    """Verify through exactly one authority and normalize stable failures."""

    if not required:
        return RuntimeAttestationDecision(status="not_required")
    if release_verifier is not None and deployment_verifier is not None:
        raise InitFetchError(
            "DPONE_ARTIFACT_ATTESTATION_REQUIRED",
            "runtime init-fetch has multiple artifact attestation authorities",
        )
    if deployment_verifier is not None:
        if deployment_subject is None:
            raise InitFetchError(
                "DPONE_ARTIFACT_ATTESTATION_REQUIRED",
                "runtime deployment attestation subject is unavailable",
            )
        expected_policy_sha256 = None if plan.trust_policy_ref is None else plan.trust_policy_ref["sha256"]
        if deployment_verifier.policy_sha256 != expected_policy_sha256:
            raise InitFetchError(
                "DPONE_ARTIFACT_TRUST_POLICY_MISMATCH",
                "runtime deployment attestation policy does not match the pinned plan",
            )
        return _verify_deployment(deployment_subject, deployment_verifier)
    if release_verifier is None:
        raise InitFetchError(
            "DPONE_ARTIFACT_ATTESTATION_REQUIRED",
            "runtime init-fetch requires a configured artifact attestation verifier",
        )
    try:
        release_verifier.verify(subject=release_attestation_subject(plan, staged))
    except RuntimeArtifactAttestationError as exc:
        raise InitFetchError(exc.code, str(exc)) from exc
    except InitFetchError:
        raise
    except Exception as exc:  # noqa: BLE001 - verifier details are sensitive.
        raise InitFetchError(
            "DPONE_ARTIFACT_ATTESTATION_REQUIRED",
            "runtime artifact attestation could not be verified",
        ) from exc
    return RuntimeAttestationDecision(status="passed")


def _verify_deployment(
    subject: AirflowArtifactObservedSubject,
    verifier: AirflowDeploymentAttestationVerifier,
) -> RuntimeAttestationDecision:
    try:
        verification = verifier.verify(subject)
        if not verification.is_verified:
            raise InitFetchError(verification.code, verification.message)
    except AirflowArtifactAttestationRejected as exc:
        raise InitFetchError(
            exc.verification.code,
            exc.verification.message,
        ) from exc
    except AirflowArtifactAttestationRegistryError as exc:
        raise InitFetchError(
            exc.code,
            "runtime artifact attestation package could not be read",
        ) from exc
    except InitFetchError:
        raise
    except Exception as exc:  # noqa: BLE001 - verifier details are sensitive.
        raise InitFetchError(
            "DPONE_ARTIFACT_ATTESTATION_REQUIRED",
            "runtime artifact attestation could not be verified",
        ) from exc
    return RuntimeAttestationDecision(
        status="passed",
        deployment_verification=verification,
        deployment_subject=subject,
    )


__all__ = [
    "RuntimeAttestationDecision",
    "verify_runtime_attestation",
]
