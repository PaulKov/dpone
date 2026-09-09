"""Bounded verifier for production reference-deployment evidence."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.contracts.airflow_correlation import AirflowCorrelation


from collections.abc import Mapping
from datetime import datetime
from hashlib import sha256
from pathlib import Path
from typing import Any

from dpone.contracts.airflow_correlation import AirflowCorrelationError, parse_airflow_correlation
from dpone.contracts.airflow_deployment import (
    deployment_id,
    is_canonical_sha256_digest,
    release_id,
)
from dpone.contracts.airflow_run_identity import AirflowRunIdentity, AirflowRunIdentityError
from dpone.contracts.runtime_artifact_delivery import (
    is_pinned_artifact_registry_ref,
    missing_init_fetch_delivery_paths,
)
from dpone.ops.route_certification_matrix_evidence import RouteCertificationEvidenceReader
from dpone.ops.self_service_certification_files import SelfServiceEvidenceFileError, read_json
from dpone.ops.self_service_certification_models import (
    ReferenceDeploymentEvidence,
    ReferenceDeploymentProof,
    SelfServiceCertificationError,
)
from dpone.services.sample_route_certifications import certified_sampling_route_catalog

_MAX_FILE_BYTES = 4 * 1024 * 1024
_RELEASE_FILE = "release-set.json"
_DEPLOYMENT_FILE = "deployment-set.json"
_AIRFLOW_FILE = "airflow-evidence-bundle.json"


class ReferenceDeploymentEvidenceReader:
    """Cross-check existing immutable proof without replacing its authorities."""

    def __init__(self, *, route_reader: RouteCertificationEvidenceReader | None = None) -> None:
        self._route_reader = route_reader or RouteCertificationEvidenceReader()

    def read_many(
        self,
        directories: tuple[Path, ...],
        *,
        expected_commit: str,
        evaluated_at: datetime,
        max_age_hours: int,
    ) -> ReferenceDeploymentEvidence:
        proofs = tuple(
            self.read(
                directory,
                expected_commit=expected_commit,
                evaluated_at=evaluated_at,
                max_age_hours=max_age_hours,
            )
            for directory in _unique_directories(directories)
        )
        valid = tuple(proof for proof in proofs if proof.status == "PASS")
        deployments = {proof.deployment_id for proof in valid if proof.deployment_id is not None}
        signers = {proof.signer_identity for proof in valid if proof.signer_identity is not None}
        has_failures = any(proof.status == "FAIL" for proof in proofs)
        independent = len(valid) >= 2 and len(deployments) >= 2 and len(signers) >= 2
        status = "FAIL" if has_failures else ("PASS" if independent else "UNVERIFIED")
        blockers = []
        if not proofs:
            blockers.append("self_service.reference_deployments_missing")
        if proofs and not has_failures and len(valid) < 2:
            blockers.append("self_service.reference_deployments_insufficient")
        if len(valid) >= 2 and len(deployments) < 2:
            blockers.append("self_service.reference_deployments_not_independent")
        if len(valid) >= 2 and len(signers) < 2:
            blockers.append("self_service.reference_signers_not_independent")
        blockers.extend(blocker for proof in proofs for blocker in proof.blockers)
        return ReferenceDeploymentEvidence(
            status=status,
            supplied_count=len(proofs),
            valid_count=len(valid),
            distinct_deployment_count=len(deployments),
            distinct_signer_count=len(signers),
            proofs=proofs,
            blockers=tuple(dict.fromkeys(blockers)),
        )

    def read(
        self,
        directory: Path,
        *,
        expected_commit: str,
        evaluated_at: datetime,
        max_age_hours: int,
    ) -> ReferenceDeploymentProof:
        label = directory.name.strip()[:128] or "reference-deployment"
        if directory.is_symlink() or not directory.is_dir():
            raise SelfServiceCertificationError(
                "DPONE_SELF_SERVICE_REFERENCE_INPUT_UNSAFE",
                "Reference deployment must be an available regular local directory, not a symlink.",
            )
        blockers: list[str] = []
        release, release_bytes = _read(directory / _RELEASE_FILE, blockers, "release-set")
        deployment, deployment_bytes = _read(directory / _DEPLOYMENT_FILE, blockers, "deployment-set")
        airflow, airflow_bytes = _read(directory / _AIRFLOW_FILE, blockers, "Airflow evidence bundle")
        release_identity = _validate_release(release, expected_commit=expected_commit, blockers=blockers)
        deployment_identity = _validate_deployment(
            deployment,
            release_identity=release_identity,
            blockers=blockers,
        )
        route_result = self._route_reader.read(
            directory,
            candidates=certified_sampling_route_catalog(),
            expected_commit=expected_commit,
            evaluated_at=evaluated_at,
            max_age_hours=max_age_hours,
        )
        signer_identity = None
        if route_result.issue is not None:
            blockers.append(route_result.issue.code)
        elif route_result.proof is None:
            blockers.append("self_service.reference_route_proof_missing")
        else:
            route_proof = route_result.proof
            blockers.extend(route_proof.blockers)
            if not route_proof.production_certified:
                blockers.append("self_service.reference_route_not_production_certified")
            if route_proof.release_id != release_identity or route_proof.deployment_id != deployment_identity:
                blockers.append("self_service.reference_route_identity_mismatch")
            signer_identity = route_proof.signer_identity
        correlation_id = _validate_airflow(
            airflow,
            release_identity=release_identity,
            deployment_identity=deployment_identity,
            deployment=deployment,
            blockers=blockers,
        )
        return ReferenceDeploymentProof(
            evidence_set=label,
            status="FAIL" if blockers else "PASS",
            release_id=release_identity,
            deployment_id=deployment_identity,
            signer_identity=signer_identity,
            correlation_id=correlation_id,
            release_set_sha256=_sha256_bytes(release_bytes) if release_bytes else None,
            deployment_set_sha256=_sha256_bytes(deployment_bytes) if deployment_bytes else None,
            airflow_evidence_sha256=_sha256_bytes(airflow_bytes) if airflow_bytes else None,
            blockers=tuple(dict.fromkeys(blockers)),
        )


def _read(path: Path, blockers: list[str], label: str) -> tuple[dict[str, Any], bytes]:
    try:
        return read_json(path, max_bytes=_MAX_FILE_BYTES, label=label)
    except SelfServiceEvidenceFileError:
        blockers.append("self_service.reference_file_invalid")
        return {}, b""


def _validate_release(
    payload: Mapping[str, Any],
    *,
    expected_commit: str,
    blockers: list[str],
) -> str | None:
    identity = payload.get("release_id")
    provenance = payload.get("provenance")
    commit = provenance.get("source_commit") if isinstance(provenance, Mapping) else None
    artifacts = payload.get("artifacts")
    artifact_sections_valid = isinstance(artifacts, Mapping) and all(
        isinstance(artifacts.get(section), list) for section in ("dag_specs", "workload_packs", "canonical_schemas")
    )
    if payload.get("schema") != "dpone.release-set.v1" or not is_canonical_sha256_digest(identity):
        blockers.append("self_service.reference_release_invalid")
        return str(identity) if isinstance(identity, str) else None
    if not artifact_sections_valid:
        blockers.append("self_service.reference_release_invalid")
    if release_id(payload) != identity:
        blockers.append("self_service.reference_release_digest_mismatch")
    if commit != expected_commit:
        blockers.append("self_service.reference_release_commit_mismatch")
    return str(identity)


def _validate_deployment(
    payload: Mapping[str, Any],
    *,
    release_identity: str | None,
    blockers: list[str],
) -> str | None:
    identity = payload.get("deployment_id")
    required_digests = (
        payload.get("binding_set_ref"),
        payload.get("connection_registry_ref"),
        payload.get("credential_runtime_ref"),
        payload.get("runtime_image_digest"),
    )
    if (
        payload.get("schema") != "dpone.deployment-set.v1"
        or payload.get("deployment_type") != "environment"
        or payload.get("runnable") is not True
        or payload.get("environment") != "production"
        or payload.get("release_ref") != release_identity
        or not is_canonical_sha256_digest(identity)
        or not all(is_canonical_sha256_digest(value) for value in required_digests)
    ):
        blockers.append("self_service.reference_deployment_invalid")
        return str(identity) if isinstance(identity, str) else None
    if deployment_id(payload) != identity:
        blockers.append("self_service.reference_deployment_digest_mismatch")
    delivery = payload.get("runtime_artifact_delivery")
    identity_config = delivery.get("identity") if isinstance(delivery, Mapping) else None
    source = delivery.get("source") if isinstance(delivery, Mapping) else None
    verify = delivery.get("verify") if isinstance(delivery, Mapping) else None
    registry_ref = delivery.get("artifact_registry_ref") if isinstance(delivery, Mapping) else None
    source_ref = source.get("artifact_registry_ref") if isinstance(source, Mapping) else None
    if (
        not isinstance(delivery, Mapping)
        or delivery.get("mode") != "init_fetch"
        or missing_init_fetch_delivery_paths(delivery)
        or not is_pinned_artifact_registry_ref(registry_ref)
        or not isinstance(identity_config, Mapping)
        or identity_config.get("method") != "kubernetes_workload_identity"
        or not _text(identity_config.get("service_account"))
        or not isinstance(source, Mapping)
        or source_ref != registry_ref
        or not is_pinned_artifact_registry_ref(source_ref)
        or not isinstance(verify, Mapping)
        or verify.get("checksums") != "required"
        or verify.get("attestations") != "required_for_prod"
    ):
        blockers.append("self_service.reference_delivery_invalid")
    return str(identity)


def _validate_airflow(
    payload: Mapping[str, Any],
    *,
    release_identity: str | None,
    deployment_identity: str | None,
    deployment: Mapping[str, Any],
    blockers: list[str],
) -> str | None:
    if payload.get("kind") != "gitops.airflow_evidence_bundle" or payload.get("runner_policy") != "release":
        blockers.append("self_service.reference_airflow_policy_invalid")
    if payload.get("blockers") != []:
        blockers.append("self_service.reference_airflow_blocked")
    artifacts = payload.get("artifacts")
    required = (
        tuple(item for item in artifacts if isinstance(item, Mapping) and item.get("required") is True)
        if isinstance(artifacts, list)
        else ()
    )
    if (
        not isinstance(artifacts, list)
        or any(not isinstance(item, Mapping) for item in artifacts)
        or not required
        or any(item.get("exists") is not True or item.get("passed") is not True for item in required)
    ):
        blockers.append("self_service.reference_airflow_artifacts_invalid")
    try:
        identity = AirflowRunIdentity.from_mapping(payload.get("run_identity"))
    except AirflowRunIdentityError:
        blockers.append("self_service.reference_run_identity_invalid")
        return None
    try:
        correlation = parse_airflow_correlation(payload.get("correlation"))
    except AirflowCorrelationError:
        blockers.append("self_service.reference_correlation_invalid")
        return None
    if not correlation.complete:
        blockers.append("self_service.reference_correlation_incomplete")
    if identity.dag_spec is None or identity.airflow_bundle is None:
        blockers.append("self_service.reference_run_identity_incomplete")
    _cross_check_identity(
        identity,
        correlation=correlation,
        payload=payload,
        deployment=deployment,
        release_identity=release_identity,
        deployment_identity=deployment_identity,
        blockers=blockers,
    )
    return correlation.correlation_id


def _cross_check_identity(
    identity: AirflowRunIdentity,
    *,
    correlation: AirflowCorrelation,
    payload: Mapping[str, Any],
    deployment: Mapping[str, Any],
    release_identity: str | None,
    deployment_identity: str | None,
    blockers: list[str],
) -> None:
    expected = [
        identity.release_id == release_identity,
        identity.deployment_id == deployment_identity,
        identity.binding_set_ref == deployment.get("binding_set_ref"),
        identity.connection_registry_ref == deployment.get("connection_registry_ref"),
        identity.credential_runtime_ref == deployment.get("credential_runtime_ref"),
        identity.runtime_image_digest == deployment.get("runtime_image_digest"),
        correlation.artifacts.release_id == release_identity,
        correlation.artifacts.deployment_id == deployment_identity,
        correlation.artifacts.workload_id == identity.workload_pack.id,
        correlation.artifacts.workload_pack_sha256 == identity.workload_pack.sha256,
    ]
    pod = payload.get("pod")
    attempt = payload.get("attempt")
    delivery = deployment.get("runtime_artifact_delivery")
    delivery_identity = delivery.get("identity") if isinstance(delivery, Mapping) else None
    if not isinstance(pod, Mapping):
        expected.append(False)
    else:
        expected.extend(
            (
                pod.get("pod_name") == correlation.pod.name,
                _text(pod.get("pod_uid")) is not None,
                pod.get("namespace") == correlation.pod.namespace,
                pod.get("image_digest") == correlation.pod.image_digest == identity.runtime_image_digest,
                isinstance(delivery_identity, Mapping)
                and pod.get("service_account") == delivery_identity.get("service_account"),
            )
        )
    if not isinstance(attempt, Mapping) or dict(attempt) != correlation.airflow.to_dict():
        expected.append(False)
    if not all(expected):
        blockers.append("self_service.reference_cross_identity_mismatch")


def _unique_directories(values: tuple[Path, ...]) -> tuple[Path, ...]:
    result: list[Path] = []
    seen: set[str] = set()
    for value in values:
        key = str(value.absolute())
        if key not in seen:
            seen.add(key)
            result.append(value)
    return tuple(result)


def _text(value: object) -> str | None:
    return value.strip() if isinstance(value, str) and value.strip() else None


def _sha256_bytes(value: bytes) -> str:
    return "sha256:" + sha256(value).hexdigest()


__all__ = ["ReferenceDeploymentEvidenceReader"]
