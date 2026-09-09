"""CLI-facing preparation and publication of Airflow artifact attestations."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

from dpone.adapters.cosign_public_key_signature import CosignPublicKeyBlobSignatureVerifier
from dpone.adapters.deployment_cache_files import DeploymentCacheError
from dpone.contracts.airflow_artifact_attestation import (
    AirflowArtifactAttestation,
    AirflowArtifactExpectedSubject,
    sha256_bytes,
)
from dpone.ports.artifact_registry import ArtifactRegistryAuthority, ArtifactRegistryError
from dpone.readiness.airflow_artifact_delivery import ArtifactRegistryOptions
from dpone.readiness.airflow_artifact_trust_material import (
    AirflowArtifactTrustMaterialError,
    load_airflow_artifact_trust_material_from_files,
)
from dpone.readiness.airflow_attestation_files import (
    read_bounded_regular_file,
    write_immutable_file,
)
from dpone.readiness.airflow_self_service_models import SelfServiceResult, dpone_error, manual_fix
from dpone.runtime.airflow_artifact_delivery import AirflowArtifactDeliveryError
from dpone.runtime.airflow_deployment_attestation_subject import (
    read_airflow_artifact_control_roots,
)
from dpone.services.airflow_artifact_attestation import (
    AirflowArtifactAttestationBuilder,
    AirflowArtifactAttestationVerificationService,
)
from dpone.services.airflow_artifact_attestation_registry import (
    AirflowArtifactAttestationPackage,
    AirflowArtifactAttestationRegistryError,
    AirflowArtifactAttestationRegistryPublisher,
)

_MAX_INPUT_BYTES = 2 * 1024 * 1024


def prepare_artifact_attestation(
    *,
    cache_root: str,
    release_id: str,
    deployment_id: str,
    environment: str,
    artifact_registry_ref: str,
    registry_scope_id: str,
    publication_evidence_path: str,
    source_project: str,
    source_ref: str,
    source_git_sha: str,
    issued_at: str,
    output_path: str,
) -> SelfServiceResult:
    """Build canonical unsigned bytes for an external protected signer."""

    try:
        evidence = read_bounded_regular_file(
            Path(publication_evidence_path),
            maximum=_MAX_INPUT_BYTES,
            label="artifact attestation publication evidence",
        )
        release_bytes, deployment_bytes, index_bytes = read_airflow_artifact_control_roots(
            cache_root=Path(cache_root),
            release_id=release_id,
            deployment_id=deployment_id,
            environment=environment,
        )
        statement = AirflowArtifactAttestationBuilder().build(
            release_bytes=release_bytes,
            deployment_bytes=deployment_bytes,
            airflow_index_bytes=index_bytes,
            release_id=release_id,
            deployment_id=deployment_id,
            environment=environment,
            artifact_registry_ref=artifact_registry_ref,
            registry_scope_id=registry_scope_id,
            publication_evidence=evidence,
            source_project=source_project,
            source_ref=source_ref,
            source_git_sha=source_git_sha,
            issued_at=_issued_at(issued_at),
        )
        destination = Path(output_path)
        write_immutable_file(
            destination,
            statement.to_bytes(),
            label="artifact attestation output",
        )
        return SelfServiceResult(
            passed=True,
            details={
                "schema": "dpone.airflow-artifact-attestation-prepare.v1",
                "status": "prepared",
                "attestation_id": statement.attestation_id,
                "release_id": release_id,
                "deployment_id": deployment_id,
                "environment": environment,
                "statement_sha256": sha256_bytes(statement.to_bytes()),
                "statement_bytes": len(statement.to_bytes()),
                "output_path": destination.as_posix(),
                "errors": [],
                "warnings": [],
            },
            exit_code=0,
        )
    except Exception as exc:
        return _failure_from_exception(
            exc,
            "DPONE_ARTIFACT_ATTESTATION_PREPARE_FAILED",
            "artifact attestation statement could not be prepared",
            "inspect_exact_publish_evidence",
            stage="prepare",
        )


def publish_artifact_attestation(
    *,
    cache_root: str,
    publication_evidence_path: str,
    statement_path: str,
    sigstore_bundle_path: str,
    trust_policy_path: str,
    trust_key_root: str,
    registry_options: ArtifactRegistryOptions,
) -> SelfServiceResult:
    """Verify local and remote commitments, then publish the proof marker last."""

    try:
        if registry_options.connection_id and registry_options.connection_type is None:
            raise AirflowArtifactDeliveryError(
                "DPONE_ARTIFACT_DELIVERY_INPUT_INVALID",
                "connection_type is required when connection_id is configured",
            )
        statement_bytes = read_bounded_regular_file(
            Path(statement_path),
            maximum=256 * 1024,
            label="artifact attestation statement",
        )
        bundle_bytes = read_bounded_regular_file(
            Path(sigstore_bundle_path),
            maximum=_MAX_INPUT_BYTES,
            label="artifact attestation sigstore bundle",
        )
        evidence_bytes = read_bounded_regular_file(
            Path(publication_evidence_path),
            maximum=_MAX_INPUT_BYTES,
            label="artifact attestation publication evidence",
        )
        statement = AirflowArtifactAttestation.from_bytes(statement_bytes)
        subject = AirflowArtifactExpectedSubject(**statement.claims["subject"])
        _require_statement_matches_local(
            statement=statement,
            cache_root=Path(cache_root),
            evidence=evidence_bytes,
        )
        registry = registry_options.build()
        if (
            not isinstance(registry, ArtifactRegistryAuthority)
            or registry.authority_scope_id != subject.registry_scope_id
        ):
            raise ValueError("artifact registry authority differs from the signed subject")
        trust_material = load_airflow_artifact_trust_material_from_files(
            policy_path=Path(trust_policy_path),
            key_root=Path(trust_key_root),
        )
        verification = AirflowArtifactAttestationVerificationService(
            signature_verifier=CosignPublicKeyBlobSignatureVerifier()
        ).verify(
            attestation=statement_bytes,
            sigstore_bundle=bundle_bytes,
            policy=trust_material.policy,
            public_keys=trust_material.public_keys,
            expected=subject,
        )
        if not verification.is_verified:
            raise _AttestationVerificationRejected(verification.code)
        receipt = AirflowArtifactAttestationRegistryPublisher(registry).publish(
            environment=subject.environment,
            deployment_id=subject.deployment_id,
            package=AirflowArtifactAttestationPackage(statement_bytes, bundle_bytes),
            verification=verification,
        )
        return SelfServiceResult(
            passed=True,
            details={
                **receipt.to_dict(),
                "verification": verification.to_dict(),
            },
            exit_code=0,
        )
    except Exception as exc:
        return _failure_from_exception(
            exc,
            "DPONE_ARTIFACT_ATTESTATION_PUBLISH_FAILED",
            "artifact attestation could not be verified and published",
            "inspect_signing_policy_and_remote_evidence",
            stage="publish",
        )


def _require_statement_matches_local(
    *,
    statement: AirflowArtifactAttestation,
    cache_root: Path,
    evidence: bytes,
) -> None:
    claims = statement.claims
    source = claims["source"]
    subject = claims["subject"]
    issued_at = datetime.fromisoformat(str(claims["issued_at"]).replace("Z", "+00:00"))
    release_bytes, deployment_bytes, index_bytes = read_airflow_artifact_control_roots(
        cache_root=cache_root,
        release_id=str(subject["release_id"]),
        deployment_id=str(subject["deployment_id"]),
        environment=str(subject["environment"]),
    )
    rebuilt = AirflowArtifactAttestationBuilder().build(
        release_bytes=release_bytes,
        deployment_bytes=deployment_bytes,
        airflow_index_bytes=index_bytes,
        release_id=str(subject["release_id"]),
        deployment_id=str(subject["deployment_id"]),
        environment=str(subject["environment"]),
        artifact_registry_ref=str(subject["artifact_registry_ref"]),
        registry_scope_id=str(subject["registry_scope_id"]),
        publication_evidence=evidence,
        source_project=str(source["project"]),
        source_ref=str(source["ref"]),
        source_git_sha=str(source["git_sha"]),
        issued_at=issued_at,
    )
    if rebuilt.to_bytes() != statement.to_bytes():
        raise ValueError("signed statement differs from local exact publication evidence")


def _issued_at(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("artifact attestation issued_at must include a UTC offset")
    return parsed


class _AttestationVerificationRejected(RuntimeError):
    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


def _failure_from_exception(
    exc: BaseException,
    code: str,
    message: str,
    action: str,
    *,
    stage: str,
) -> SelfServiceResult:
    reported_code, exit_code = _failure_classification(exc, fallback=code, stage=stage)
    safe_message, safe_action = _failure_guidance(
        exc,
        code=reported_code,
        fallback_message=message,
        fallback_action=action,
    )
    return SelfServiceResult(
        passed=False,
        details={
            "schema": "dpone.airflow-artifact-attestation-error.v1",
            "status": "failed",
            "errors": [
                dpone_error(
                    reported_code,
                    safe_message,
                    stage="artifact_attestation",
                    fixes=[manual_fix(safe_action)],
                    docs_url="docs/airflow-artifact-attestation-operations.md",
                )
            ],
            "warnings": [],
        },
        exit_code=exit_code,
    )


def _failure_guidance(
    exc: BaseException,
    *,
    code: str,
    fallback_message: str,
    fallback_action: str,
) -> tuple[str, str]:
    if isinstance(exc, AirflowArtifactDeliveryError):
        if code == "DPONE_ARTIFACT_DELIVERY_INPUT_INVALID":
            return str(exc), "set_connection_type_env_airflow_or_vault"
        return fallback_message, fallback_action
    if isinstance(exc, ValueError) and "issued_at" in str(exc):
        return (
            "issued_at must be an RFC 3339 timestamp with a UTC offset",
            "use_ci_pipeline_created_at",
        )
    if isinstance(exc, ValueError) and "already contains different bytes" in str(exc):
        return (
            "output already contains different immutable bytes",
            "choose_new_output_or_restore_exact_bytes",
        )
    return fallback_message, fallback_action


def _failure_classification(
    exc: BaseException,
    *,
    fallback: str,
    stage: str,
) -> tuple[str, int]:
    if isinstance(exc, AirflowArtifactDeliveryError):
        if exc.code in {
            "DPONE_ARTIFACT_REGISTRY_SDK_UNAVAILABLE",
            "DPONE_ARTIFACT_REGISTRY_UNAVAILABLE",
        }:
            return exc.code, 3
        if exc.code == "DPONE_ARTIFACT_DELIVERY_INPUT_INVALID":
            return exc.code, 2
        return exc.code, 4
    if isinstance(exc, AirflowArtifactAttestationRegistryError):
        if exc.code == "DPONE_ARTIFACT_ATTESTATION_REGISTRY_UNAVAILABLE":
            return exc.code, 3
        return exc.code, 4
    if isinstance(exc, ArtifactRegistryError):
        return "DPONE_ARTIFACT_ATTESTATION_REGISTRY_UNAVAILABLE", 3
    if isinstance(exc, _AttestationVerificationRejected):
        exit_code = 3 if exc.code == "DPONE_ARTIFACT_ATTESTATION_VERIFIER_UNAVAILABLE" else 4
        return exc.code, exit_code
    if isinstance(exc, AirflowArtifactTrustMaterialError):
        exit_code = 4 if exc.code == "DPONE_ARTIFACT_ATTESTATION_KEY_MISMATCH" else 2
        return exc.code, exit_code
    if isinstance(
        exc,
        (
            DeploymentCacheError,
            OSError,
            ValueError,
        ),
    ):
        return fallback, 2
    if isinstance(exc, ImportError):
        return "DPONE_ARTIFACT_ATTESTATION_DEPENDENCY_UNAVAILABLE", 3
    return f"DPONE_INTERNAL_ARTIFACT_ATTESTATION_{stage.upper()}_FAILED", 5


__all__ = ["prepare_artifact_attestation", "publish_artifact_attestation"]
