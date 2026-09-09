from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

import pytest

from dpone.contracts.airflow_artifact_attestation import (
    ATTESTATION_SCHEMA,
    AirflowArtifactAttestation,
    AirflowArtifactExpectedSubject,
    AirflowArtifactObservedSubject,
    sha256_bytes,
)
from dpone.contracts.airflow_deployment_trust_policy import POLICY_SCHEMA, AirflowDeploymentTrustPolicy
from dpone.contracts.blob_signature import BlobSignatureVerification
from dpone.gitops.schema_validation import GitOpsSchemaValidator
from dpone.services.airflow_artifact_attestation import (
    AirflowArtifactAttestationBuilder,
    AirflowArtifactAttestationVerificationService,
    AirflowArtifactSignatureVerifier,
)

SHA_A = "sha256:" + "a" * 64
SHA_B = "sha256:" + "b" * 64
SHA_C = "sha256:" + "c" * 64
SHA_D = "sha256:" + "d" * 64
SHA_E = "sha256:" + "e" * 64
SHA_F = "sha256:" + "f" * 64


class _SignatureVerifier:
    def __init__(self, *, verified_key: bytes | None) -> None:
        self.verified_key = verified_key
        self.calls: list[bytes] = []

    def verify_blob(
        self,
        *,
        blob: bytes,
        sigstore_bundle: bytes,
        public_key: bytes,
        policy: object,
    ) -> BlobSignatureVerification:
        del blob, sigstore_bundle, policy
        self.calls.append(public_key)
        if public_key == self.verified_key:
            return BlobSignatureVerification.verified("3.0.4")
        return BlobSignatureVerification.invalid("3.0.4")


class _UnavailableSignatureVerifier:
    def verify_blob(self, **_: object) -> BlobSignatureVerification:
        return BlobSignatureVerification.unverified("verifier_unavailable")


def test_builder_emits_canonical_attestation_bound_to_exact_publication(tmp_path: Path) -> None:
    fixture = _publication_fixture(tmp_path)

    attestation = AirflowArtifactAttestationBuilder().build(
        release_bytes=fixture["release_bytes"],
        deployment_bytes=fixture["deployment_bytes"],
        airflow_index_bytes=fixture["index_bytes"],
        release_id=SHA_A,
        deployment_id=SHA_B,
        environment="prod",
        artifact_registry_ref="dpone_prod",
        registry_scope_id=SHA_D,
        publication_evidence=fixture["evidence"],
        source_project="platform/example-workloads",
        source_ref="refs/heads/master",
        source_git_sha="1" * 40,
        issued_at=datetime(2026, 7, 29, 10, 0, tzinfo=UTC),
    )

    raw = attestation.to_bytes()
    parsed = AirflowArtifactAttestation.from_bytes(raw)

    assert parsed == attestation
    assert parsed.schema == ATTESTATION_SCHEMA
    assert parsed.claims["subject"] == {
        "release_id": SHA_A,
        "deployment_id": SHA_B,
        "environment": "prod",
        "artifact_registry_ref": "dpone_prod",
        "registry_scope_id": SHA_D,
        "release_set_sha256": sha256_bytes(fixture["release_bytes"]),
        "deployment_sha256": sha256_bytes(fixture["deployment_bytes"]),
        "airflow_index_sha256": sha256_bytes(fixture["index_bytes"]),
        "runtime_image_digest": SHA_C,
    }
    assert raw == json.dumps(parsed.to_dict(), ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode()


def test_attestation_parser_rejects_duplicate_keys_and_noncanonical_bytes() -> None:
    with pytest.raises(ValueError, match="duplicate"):
        AirflowArtifactAttestation.from_bytes(
            b'{"schema":"dpone.airflow-artifact-attestation.v1","schema":"duplicate"}'
        )

    fixture = _attestation()
    pretty = (json.dumps(fixture.to_dict(), indent=2, sort_keys=True) + "\n").encode()
    with pytest.raises(ValueError, match="canonical"):
        AirflowArtifactAttestation.from_bytes(pretty)


def test_verifier_accepts_one_trusted_key_and_records_exact_subject() -> None:
    attestation = _attestation()
    key_a = b"public-key-a"
    key_b = b"public-key-b"
    policy = _policy(
        keys=(
            ("old", key_a),
            ("current", key_b),
        )
    )
    signature = _SignatureVerifier(verified_key=key_b)

    receipt = AirflowArtifactAttestationVerificationService(
        signature_verifier=signature,
        clock=lambda: datetime(2026, 7, 29, 10, 1, tzinfo=UTC),
    ).verify(
        attestation=attestation.to_bytes(),
        sigstore_bundle=b"bundle",
        policy=policy,
        public_keys={"old": key_a, "current": key_b},
        expected=_expected(),
    )

    assert receipt.decision == "verified"
    assert receipt.code == "DPONE_ARTIFACT_ATTESTATION_VERIFIED"
    assert receipt.public_key_id == "current"
    assert receipt.public_key_sha256 == sha256_bytes(key_b)
    assert signature.calls == [key_a, key_b]


def test_runtime_observed_subject_does_not_claim_unmounted_airflow_index() -> None:
    attestation = _attestation()
    key = b"public-key"
    expected = _expected()
    observed = AirflowArtifactObservedSubject(
        **{
            **expected.to_dict(),
            "airflow_index_sha256": None,
        }
    )
    service = AirflowArtifactAttestationVerificationService(
        signature_verifier=_SignatureVerifier(verified_key=key),
        clock=lambda: datetime(2026, 7, 29, 10, 1, tzinfo=UTC),
    )

    verified = service.verify(
        attestation=attestation.to_bytes(),
        sigstore_bundle=b"bundle",
        policy=_policy(keys=(("current", key),)),
        public_keys={"current": key},
        expected=observed,
    )
    rejected = service.verify(
        attestation=attestation.to_bytes(),
        sigstore_bundle=b"bundle",
        policy=_policy(keys=(("current", key),)),
        public_keys={"current": key},
        expected=replace(observed, deployment_sha256=SHA_A),
    )

    assert verified.is_verified
    assert observed.unobserved_claims == ("airflow_index_sha256",)
    assert rejected.code == "DPONE_ARTIFACT_ATTESTATION_SUBJECT_MISMATCH"


@pytest.mark.parametrize(
    ("signature_verifier", "expected_code"),
    [
        (
            _SignatureVerifier(verified_key=None),
            "DPONE_ARTIFACT_ATTESTATION_SIGNATURE_INVALID",
        ),
        (
            _UnavailableSignatureVerifier(),
            "DPONE_ARTIFACT_ATTESTATION_VERIFIER_UNAVAILABLE",
        ),
    ],
)
def test_verifier_preserves_signature_failure_code(
    signature_verifier: AirflowArtifactSignatureVerifier,
    expected_code: str,
) -> None:
    key = b"public-key"

    receipt = AirflowArtifactAttestationVerificationService(
        signature_verifier=signature_verifier,
    ).verify(
        attestation=_attestation().to_bytes(),
        sigstore_bundle=b"bundle",
        policy=_policy(keys=(("current", key),)),
        public_keys={"current": key},
        expected=_expected(),
    )

    assert receipt.code == expected_code
    assert receipt.is_verified is False


def test_publication_receipt_schema_accepts_embedded_verification_evidence() -> None:
    verification = AirflowArtifactAttestationVerificationService(
        signature_verifier=_SignatureVerifier(verified_key=b"public-key"),
        clock=lambda: datetime(2026, 7, 29, 10, 1, tzinfo=UTC),
    ).verify(
        attestation=_attestation().to_bytes(),
        sigstore_bundle=b"bundle",
        policy=_policy(keys=(("current", b"public-key"),)),
        public_keys={"current": b"public-key"},
        expected=_expected(),
    )
    payload = {
        "schema": "dpone.airflow-artifact-attestation-publish.v1",
        "passed": True,
        "changes": [],
        "status": "published",
        "attestation_id": _attestation().attestation_id,
        "environment": "prod",
        "deployment_id": SHA_B,
        "created_objects": 3,
        "existing_equal_objects": 0,
        "verified_objects": 3,
        "verification": verification.to_dict(),
        "errors": [],
    }

    assert (
        GitOpsSchemaValidator().validate(
            payload,
            expected_kind="dpone.airflow-artifact-attestation-publish.v1",
        )
        == ()
    )


@pytest.mark.parametrize(
    ("mutate", "code"),
    [
        (
            lambda expected: AirflowArtifactExpectedSubject(**{**expected.to_dict(), "environment": "dev"}),
            "DPONE_ARTIFACT_ATTESTATION_SUBJECT_MISMATCH",
        ),
        (
            lambda expected: AirflowArtifactExpectedSubject(
                **{**expected.to_dict(), "artifact_registry_ref": "other_registry"}
            ),
            "DPONE_ARTIFACT_ATTESTATION_SUBJECT_MISMATCH",
        ),
    ],
)
def test_verifier_rejects_cross_context_replay(mutate, code: str) -> None:
    key = b"public-key"
    receipt = AirflowArtifactAttestationVerificationService(
        signature_verifier=_SignatureVerifier(verified_key=key)
    ).verify(
        attestation=_attestation().to_bytes(),
        sigstore_bundle=b"bundle",
        policy=_policy(keys=(("current", key),)),
        public_keys={"current": key},
        expected=mutate(_expected()),
    )

    assert receipt.decision == "invalid"
    assert receipt.code == code


def test_verifier_rejects_untrusted_source_revoked_attestation_and_key_mismatch() -> None:
    attestation = _attestation()
    key = b"public-key"

    wrong_source = _policy(keys=(("current", key),), allowed_source_projects=("other/project",))
    source_receipt = AirflowArtifactAttestationVerificationService(
        signature_verifier=_SignatureVerifier(verified_key=key)
    ).verify(
        attestation=attestation.to_bytes(),
        sigstore_bundle=b"bundle",
        policy=wrong_source,
        public_keys={"current": key},
        expected=_expected(),
    )
    assert source_receipt.code == "DPONE_ARTIFACT_ATTESTATION_SOURCE_NOT_ALLOWED"

    revoked = _policy(keys=(("current", key),), revoked_attestation_ids=(attestation.attestation_id,))
    revoked_receipt = AirflowArtifactAttestationVerificationService(
        signature_verifier=_SignatureVerifier(verified_key=key)
    ).verify(
        attestation=attestation.to_bytes(),
        sigstore_bundle=b"bundle",
        policy=revoked,
        public_keys={"current": key},
        expected=_expected(),
    )
    assert revoked_receipt.code == "DPONE_ARTIFACT_ATTESTATION_REVOKED"

    mismatch_receipt = AirflowArtifactAttestationVerificationService(
        signature_verifier=_SignatureVerifier(verified_key=key)
    ).verify(
        attestation=attestation.to_bytes(),
        sigstore_bundle=b"bundle",
        policy=_policy(keys=(("current", key),)),
        public_keys={"current": b"different-key"},
        expected=_expected(),
    )
    assert mismatch_receipt.code == "DPONE_ARTIFACT_ATTESTATION_KEY_MISMATCH"
    assert mismatch_receipt.decision == "unverified"


def _publication_fixture(tmp_path: Path) -> dict[str, object]:
    cache_root = tmp_path / "cache"
    release_dir = cache_root / "releases" / SHA_A.replace(":", "-", 1)
    deployment_dir = cache_root / "deployments" / "prod" / SHA_B.replace(":", "-", 1)
    release_dir.mkdir(parents=True)
    deployment_dir.mkdir(parents=True)
    release_bytes = b'{"release":"exact"}'
    deployment_bytes = json.dumps({"runtime_image_digest": SHA_C}, separators=(",", ":"), sort_keys=True).encode()
    index_bytes = b'{"index":"exact"}'
    (release_dir / "release-set.json").write_bytes(release_bytes)
    (deployment_dir / "deployment.json").write_bytes(deployment_bytes)
    (deployment_dir / "airflow-index.json").write_bytes(index_bytes)
    evidence_payload = {
        "schema": "dpone.airflow-artifact-publish.v2",
        "status": "published",
        "release_id": SHA_A,
        "deployment_id": SHA_B,
        "environment": "prod",
        "artifact_registry_ref": "dpone_prod",
        "publication_commitment": {
            "schema": "dpone.airflow-publication-commitment.v1",
            "verification_mode": "remote_readback_sha256",
            "registry_scope_id": SHA_D,
            "projection_verified": True,
            "release": {
                "object_key": "releases/x/release-set.json",
                "sha256": sha256_bytes(release_bytes),
                "bytes": len(release_bytes),
            },
            "deployment": {
                "object_key": "deployments/prod/x/deployment.json",
                "sha256": sha256_bytes(deployment_bytes),
                "bytes": len(deployment_bytes),
            },
            "airflow_index": {
                "object_key": "deployments/prod/x/airflow-index.json",
                "sha256": sha256_bytes(index_bytes),
                "bytes": len(index_bytes),
            },
        },
    }
    evidence = json.dumps(evidence_payload, separators=(",", ":"), sort_keys=True).encode()
    return {
        "cache_root": cache_root,
        "evidence": evidence,
        "release_bytes": release_bytes,
        "deployment_bytes": deployment_bytes,
        "index_bytes": index_bytes,
    }


def _attestation() -> AirflowArtifactAttestation:
    return AirflowArtifactAttestation.create(
        {
            "subject": _expected().to_dict(),
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


def _expected() -> AirflowArtifactExpectedSubject:
    return AirflowArtifactExpectedSubject(
        release_id=SHA_A,
        deployment_id=SHA_B,
        environment="prod",
        artifact_registry_ref="dpone_prod",
        registry_scope_id=SHA_A,
        release_set_sha256=SHA_C,
        deployment_sha256=SHA_D,
        airflow_index_sha256=SHA_E,
        runtime_image_digest=SHA_F,
    )


def _policy(
    *,
    keys: tuple[tuple[str, bytes], ...],
    allowed_source_projects: tuple[str, ...] = ("platform/example-workloads",),
    revoked_attestation_ids: tuple[str, ...] = (),
) -> AirflowDeploymentTrustPolicy:
    return AirflowDeploymentTrustPolicy.from_mapping(
        {
            "schema": POLICY_SCHEMA,
            "trust_tier": "production",
            "attestations": "required_for_prod",
            "backend": "cosign_public_key_v1",
            "trusted_public_keys": {
                key_id: {
                    "file": f"{key_id}.pub",
                    "sha256": sha256_bytes(value),
                }
                for key_id, value in keys
            },
            "cosign": {
                "minimum_version": "3.0.4",
                "maximum_version_exclusive": "4.0.0",
                "timeout_seconds": 10,
            },
            "allowed_environments": ["prod"],
            "allowed_artifact_registry_refs": ["dpone_prod"],
            "allowed_registry_scope_ids": [SHA_A],
            "allowed_source_projects": list(allowed_source_projects),
            "allowed_source_refs": ["refs/heads/master"],
            "revoked_attestation_ids": list(revoked_attestation_ids),
            "revoked_public_key_ids": [],
        }
    )
