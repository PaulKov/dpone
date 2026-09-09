from __future__ import annotations

import json
from pathlib import Path, PurePosixPath

import pytest

from dpone.adapters.cosign_public_key_signature import CosignPublicKeyBlobSignatureVerifier
from dpone.adapters.object_storage_artifact_registry import ObjectStorageArtifactRegistry
from dpone.contracts.airflow_artifact_attestation import (
    AirflowArtifactAttestation,
    AirflowArtifactAttestationVerification,
    AirflowArtifactExpectedSubject,
    sha256_bytes,
)
from dpone.contracts.airflow_deployment_trust_policy import POLICY_SCHEMA, AirflowDeploymentTrustPolicy
from dpone.contracts.blob_signature import BlobSignatureVerification, BlobVerificationCommandResult
from dpone.readiness.airflow_artifact_trust_material import (
    load_airflow_artifact_trust_material_from_files,
)
from dpone.readiness.airflow_deployment_attestation_verifier import (
    RegistryAirflowDeploymentAttestationVerifier,
)
from dpone.runtime.airflow_deployment_attestation_subject import (
    subject_from_cache_projection,
)
from dpone.runtime.deployment_cache_models import ValidatedDeploymentProjection
from dpone.services.airflow_artifact_attestation_consumer import (
    AirflowArtifactTrustMaterial,
)
from dpone.services.airflow_artifact_attestation_registry import (
    AirflowArtifactAttestationPackage,
    AirflowArtifactAttestationRegistryError,
    AirflowArtifactAttestationRegistryPublisher,
    AirflowArtifactAttestationRegistryReader,
)
from dpone.storage import LocalObjectStorageClient, ObjectStorageUri

SHA_A = "sha256:" + "a" * 64
SHA_B = "sha256:" + "b" * 64
SHA_C = "sha256:" + "c" * 64
SHA_D = "sha256:" + "d" * 64
SHA_E = "sha256:" + "e" * 64
SHA_F = "sha256:" + "f" * 64


class _CommandRunner:
    def __init__(self, results: list[BlobVerificationCommandResult]) -> None:
        self.results = list(results)
        self.calls: list[tuple[str, ...]] = []

    def run(self, args: tuple[str, ...], *, timeout_seconds: int) -> BlobVerificationCommandResult:
        assert timeout_seconds == 10
        self.calls.append(args)
        return self.results.pop(0)


class _SignatureVerifier:
    def verify_blob(self, **_: object) -> BlobSignatureVerification:
        return BlobSignatureVerification.verified("3.0.4")


def test_cosign_public_key_adapter_uses_fixed_safe_arguments() -> None:
    runner = _CommandRunner(
        [
            BlobVerificationCommandResult(0, '{"gitVersion":"v3.0.4"}', ""),
            BlobVerificationCommandResult(0, "", ""),
        ]
    )
    verifier = CosignPublicKeyBlobSignatureVerifier(runner=runner, executable="/usr/local/bin/cosign")

    result = verifier.verify_blob(
        blob=b"statement",
        sigstore_bundle=b"bundle",
        public_key=b"public-key",
        policy=_policy(),
    )

    assert result.status == "verified"
    assert runner.calls[0] == ("/usr/local/bin/cosign", "version", "--json")
    verify_args = runner.calls[1]
    assert verify_args[0:2] == ("/usr/local/bin/cosign", "verify-blob")
    assert "--bundle" in verify_args
    assert "--key" in verify_args
    assert "--private-infrastructure" in verify_args
    assert "--trusted-root" not in verify_args
    assert "--certificate-identity" not in verify_args


def test_attestation_registry_publishes_marker_last_and_round_trips(tmp_path: Path) -> None:
    registry = _registry(tmp_path)
    statement = _attestation()
    package = AirflowArtifactAttestationPackage(statement.to_bytes(), b"sigstore-bundle")
    receipt = AirflowArtifactAttestationVerification(
        decision="verified",
        code="DPONE_ARTIFACT_ATTESTATION_VERIFIED",
        message="verified",
        attestation_id=statement.attestation_id,
        policy_fingerprint=SHA_A,
        public_key_id="current",
        public_key_sha256=SHA_B,
        verifier_version="3.0.4",
        verified_at="2026-07-29T10:01:00Z",
    )

    published = AirflowArtifactAttestationRegistryPublisher(registry).publish(
        environment="prod",
        deployment_id=SHA_B,
        package=package,
        verification=receipt,
    )
    fetched = AirflowArtifactAttestationRegistryReader(registry).fetch(
        environment="prod",
        deployment_id=SHA_B,
    )

    assert published.created_objects == 3
    assert fetched == package
    assert published.attestation_id == statement.attestation_id


def test_attestation_registry_recovers_partial_equal_package_and_replays(
    tmp_path: Path,
) -> None:
    registry = _registry(tmp_path)
    statement = _attestation()
    package = AirflowArtifactAttestationPackage(statement.to_bytes(), b"sigstore-bundle")
    prefix = PurePosixPath(
        "attestations",
        "deployments",
        "prod",
        SHA_B.replace(":", "-", 1),
    )
    statement_path = tmp_path / "statement"
    bundle_path = tmp_path / "bundle"
    statement_path.write_bytes(package.attestation)
    bundle_path.write_bytes(package.sigstore_bundle)
    registry.create_file(prefix / "artifact-attestation.json", statement_path)
    registry.create_file(prefix / "artifact-attestation.sigstore.json", bundle_path)
    publisher = AirflowArtifactAttestationRegistryPublisher(registry)

    recovered = publisher.publish(
        environment="prod",
        deployment_id=SHA_B,
        package=package,
        verification=_verified_receipt(statement),
    )
    replayed = publisher.publish(
        environment="prod",
        deployment_id=SHA_B,
        package=package,
        verification=_verified_receipt(statement),
    )

    assert (recovered.created_objects, recovered.existing_equal_objects) == (1, 2)
    assert (replayed.created_objects, replayed.existing_equal_objects) == (0, 3)
    assert (
        AirflowArtifactAttestationRegistryReader(registry).fetch(
            environment="prod",
            deployment_id=SHA_B,
        )
        == package
    )


def test_attestation_registry_rejects_partial_package_without_marker(tmp_path: Path) -> None:
    registry = _registry(tmp_path)
    prefix = (
        tmp_path
        / "remote"
        / "example-data-bucket"
        / "immutable"
        / "attestations"
        / "deployments"
        / "prod"
        / SHA_B.replace(":", "-", 1)
    )
    prefix.mkdir(parents=True)
    (prefix / "artifact-attestation.json").write_bytes(_attestation().to_bytes())

    with pytest.raises(AirflowArtifactAttestationRegistryError) as exc:
        AirflowArtifactAttestationRegistryReader(registry).fetch(
            environment="prod",
            deployment_id=SHA_B,
        )

    assert exc.value.code == "DPONE_ARTIFACT_ATTESTATION_NOT_FOUND"


def test_cache_adapter_verifies_registry_package_against_exact_local_bytes(
    tmp_path: Path,
) -> None:
    registry = _registry(tmp_path)
    key = b"public-key"
    policy = _policy_model(registry_scope_id=registry.authority_scope_id)
    policy_bytes = _canonical(policy.to_dict())
    cache_root, projection, expected = _cache_projection(
        tmp_path,
        policy_sha256=sha256_bytes(policy_bytes),
        registry_scope_id=registry.authority_scope_id,
    )
    statement = AirflowArtifactAttestation.create(
        {
            "subject": expected.to_dict(),
            "source": {
                "project": "platform/example-workloads",
                "ref": "refs/heads/master",
                "git_sha": "1" * 40,
            },
            "publication": {
                "evidence_sha256": SHA_F,
                "verification_mode": "remote_readback_sha256",
            },
            "issued_at": "2026-07-29T10:00:00Z",
        }
    )
    AirflowArtifactAttestationRegistryPublisher(registry).publish(
        environment="prod",
        deployment_id=SHA_B,
        package=AirflowArtifactAttestationPackage(statement.to_bytes(), b"sigstore-bundle"),
        verification=_verified_receipt(statement),
    )
    verifier = RegistryAirflowDeploymentAttestationVerifier(
        registry=registry,
        trust_material=AirflowArtifactTrustMaterial(
            policy=policy,
            policy_sha256=sha256_bytes(policy_bytes),
            public_keys={"current": key},
        ),
        signature_verifier=_SignatureVerifier(),
    )

    verification = verifier.verify(
        subject_from_cache_projection(
            cache_root=cache_root,
            projection=projection,
            registry_scope_id=registry.authority_scope_id,
        )
    )

    assert verification.is_verified
    assert verification.attestation_id == statement.attestation_id
    assert verification.public_key_id == "current"


def test_mounted_trust_material_rejects_public_key_digest_mismatch(
    tmp_path: Path,
) -> None:
    policy = _policy_model(registry_scope_id=SHA_A)
    policy_path = tmp_path / "policy.json"
    key_path = tmp_path / "current.pub"
    policy_path.write_bytes(_canonical(policy.to_dict()))
    key_path.write_bytes(b"different-key")

    with pytest.raises(ValueError, match="public-key digest does not match policy"):
        load_airflow_artifact_trust_material_from_files(
            policy_path=policy_path,
            key_root=tmp_path,
        )


def test_mounted_trust_material_supports_two_key_rotation_window(
    tmp_path: Path,
) -> None:
    current = b"current-public-key"
    next_key = b"next-public-key"
    policy = AirflowDeploymentTrustPolicy.from_mapping(
        {
            **_policy_model(registry_scope_id=SHA_A).to_dict(),
            "trusted_public_keys": {
                "current": {
                    "file": "current.pub",
                    "sha256": sha256_bytes(current),
                },
                "next": {
                    "file": "next.pub",
                    "sha256": sha256_bytes(next_key),
                },
            },
        }
    )
    policy_path = tmp_path / "policy.json"
    policy_path.write_bytes(_canonical(policy.to_dict()))
    (tmp_path / "current.pub").write_bytes(current)
    (tmp_path / "next.pub").write_bytes(next_key)

    material = load_airflow_artifact_trust_material_from_files(
        policy_path=policy_path,
        key_root=tmp_path,
    )

    assert material.public_keys == {
        "current": current,
        "next": next_key,
    }


def _registry(tmp_path: Path) -> ObjectStorageArtifactRegistry:
    return ObjectStorageArtifactRegistry(
        client=LocalObjectStorageClient(tmp_path / "remote"),
        root=ObjectStorageUri.parse("s3://example-data-bucket/immutable"),
    )


def _attestation() -> AirflowArtifactAttestation:
    return AirflowArtifactAttestation.create(
        {
            "subject": AirflowArtifactExpectedSubject(
                release_id=SHA_A,
                deployment_id=SHA_B,
                environment="prod",
                artifact_registry_ref="dpone_prod",
                registry_scope_id=SHA_A,
                release_set_sha256=SHA_C,
                deployment_sha256=SHA_D,
                airflow_index_sha256=SHA_E,
                runtime_image_digest=SHA_F,
            ).to_dict(),
            "source": {
                "project": "platform/example-workloads",
                "ref": "refs/heads/master",
                "git_sha": "1" * 40,
            },
            "publication": {
                "evidence_sha256": SHA_F,
                "verification_mode": "remote_readback_sha256",
            },
            "issued_at": "2026-07-29T10:00:00Z",
        }
    )


def _policy():
    return _policy_model(registry_scope_id=SHA_A).cosign


def _policy_model(*, registry_scope_id: str) -> AirflowDeploymentTrustPolicy:
    return AirflowDeploymentTrustPolicy.from_mapping(
        {
            "schema": POLICY_SCHEMA,
            "trust_tier": "production",
            "attestations": "required_for_prod",
            "backend": "cosign_public_key_v1",
            "trusted_public_keys": {
                "current": {
                    "file": "current.pub",
                    "sha256": sha256_bytes(b"public-key"),
                }
            },
            "cosign": {
                "minimum_version": "3.0.4",
                "maximum_version_exclusive": "4.0.0",
                "timeout_seconds": 10,
            },
            "allowed_environments": ["prod"],
            "allowed_artifact_registry_refs": ["dpone_prod"],
            "allowed_registry_scope_ids": [registry_scope_id],
            "allowed_source_projects": ["platform/example-workloads"],
            "allowed_source_refs": ["refs/heads/master"],
            "revoked_attestation_ids": [],
            "revoked_public_key_ids": [],
        }
    )


def _cache_projection(
    tmp_path: Path,
    *,
    policy_sha256: str,
    registry_scope_id: str,
) -> tuple[Path, ValidatedDeploymentProjection, AirflowArtifactExpectedSubject]:
    cache_root = tmp_path / "cache"
    release_dir = cache_root / "releases" / SHA_A.replace(":", "-", 1)
    deployment_dir = cache_root / "deployments" / "prod" / SHA_B.replace(":", "-", 1)
    release_dir.mkdir(parents=True)
    deployment_dir.mkdir(parents=True)
    release_bytes = _canonical({"release_id": SHA_A})
    deployment = {
        "release_ref": SHA_A,
        "deployment_id": SHA_B,
        "environment": "prod",
        "runtime_image_digest": SHA_F,
        "runtime_artifact_delivery": {
            "artifact_registry_ref": "dpone_prod",
            "trust_policy_ref": {"sha256": policy_sha256},
        },
    }
    index = {"deployment_id": SHA_B}
    deployment_bytes = _canonical(deployment)
    index_bytes = _canonical(index)
    (release_dir / "release-set.json").write_bytes(release_bytes)
    (deployment_dir / "deployment.json").write_bytes(deployment_bytes)
    (deployment_dir / "airflow-index.json").write_bytes(index_bytes)
    return (
        cache_root,
        ValidatedDeploymentProjection(deployment=deployment, airflow_index=index),
        AirflowArtifactExpectedSubject(
            release_id=SHA_A,
            deployment_id=SHA_B,
            environment="prod",
            artifact_registry_ref="dpone_prod",
            registry_scope_id=registry_scope_id,
            release_set_sha256=sha256_bytes(release_bytes),
            deployment_sha256=sha256_bytes(deployment_bytes),
            airflow_index_sha256=sha256_bytes(index_bytes),
            runtime_image_digest=SHA_F,
        ),
    )


def _verified_receipt(
    statement: AirflowArtifactAttestation,
) -> AirflowArtifactAttestationVerification:
    return AirflowArtifactAttestationVerification(
        decision="verified",
        code="DPONE_ARTIFACT_ATTESTATION_VERIFIED",
        message="verified",
        attestation_id=statement.attestation_id,
        policy_fingerprint=SHA_A,
        public_key_id="current",
        public_key_sha256=SHA_B,
        verifier_version="3.0.4",
        verified_at="2026-07-29T10:01:00Z",
    )


def _canonical(value: object) -> bytes:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode()
