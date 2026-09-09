from __future__ import annotations

import json
from datetime import datetime

from dpone._compat import UTC
from dpone.contracts.route_attestation import (
    RouteAttestationExpectedSubject,
    SignatureVerificationResult,
    sha256_bytes,
)
from dpone.services.route_attestation_builder import RouteAttestationBuilder
from dpone.services.route_attestation_verification import RouteAttestationVerificationService

_RELEASE_ID = "sha256:" + "a" * 64
_DEPLOYMENT_ID = "sha256:" + "b" * 64
_IMAGE_DIGEST = "sha256:" + "c" * 64
_TRUST_ROOT_DIGEST = sha256_bytes(b"trusted-root")


class _SignatureVerifier:
    def __init__(self, result: SignatureVerificationResult | None = None) -> None:
        self.result = result or SignatureVerificationResult.verified("3.0.4")
        self.calls = 0

    def verify_blob(self, **kwargs: object) -> SignatureVerificationResult:
        del kwargs
        self.calls += 1
        return self.result


def _certification() -> dict[str, object]:
    return {
        "schema_version": "dpone.route_certification_bundle.v1",
        "evidence_status": "PASS",
        "release": "0.72.6",
        "profile": "vendor_live",
        "route": {"source": "mssql", "sink": "clickhouse", "strategy": "incremental_merge"},
        "passed": True,
        "level": "certified",
        "score": 100.0,
        "blockers": [],
        "artifact_index": {},
    }


def _deployment() -> dict[str, object]:
    return {
        "schema": "dpone.deployment-set.v1",
        "deployment_id": _DEPLOYMENT_ID,
        "deployment_type": "environment",
        "runnable": True,
        "environment": "production",
        "release_ref": _RELEASE_ID,
        "runtime_image_digest": _IMAGE_DIGEST,
        "runtime_artifact_delivery": {"mode": "init_fetch"},
    }


def _raw(payload: dict[str, object]) -> bytes:
    return (json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n").encode()


def _inputs() -> tuple[bytes, bytes, dict[str, object], RouteAttestationExpectedSubject]:
    certification = _certification()
    artifact = RouteAttestationBuilder().build(
        route_id="mssql_clickhouse_incremental_merge_airflow_kpo",
        certification_bundle=certification,
        certification_bundle_bytes=_raw(certification),
        deployment=_deployment(),
        authorization_profile="safe_sample_production",
        issued_at="2026-07-15T10:00:00Z",
        not_before="2026-07-15T10:00:00Z",
        expires_at="2026-07-16T10:00:00Z",
    )
    policy = {
        "schema": "dpone.route-attestation-policy.v1",
        "backend": "cosign_keyless_v1",
        "certificate_identity": "https://github.com/PaulKov/dpone/.github/workflows/route-attestation.yml@refs/heads/master",
        "certificate_oidc_issuer": "https://token.actions.githubusercontent.com",
        "trusted_root": {"path": "/etc/dpone/sigstore/trusted_root.json", "sha256": _TRUST_ROOT_DIGEST},
        "cosign": {"minimum_version": "3.0.4", "maximum_version_exclusive": "4.0.0", "timeout_seconds": 30},
        "allowed_environments": ["production"],
        "allowed_authorization_profiles": ["safe_sample_production"],
        "allowed_certification_profiles": ["vendor_live"],
        "minimum_certification_level": "certified",
        "max_validity_seconds": 86400,
        "clock_skew_seconds": 0,
        "revoked_attestation_ids": [],
        "revoked_certificate_identities": [],
    }
    expected = RouteAttestationExpectedSubject(
        route_id="mssql_clickhouse_incremental_merge_airflow_kpo",
        release_id=_RELEASE_ID,
        deployment_id=_DEPLOYMENT_ID,
        environment="production",
        runtime_image_digest=_IMAGE_DIGEST,
        authorization_profile="safe_sample_production",
    )
    return artifact.to_bytes(), _raw(certification), policy, expected


def test_verification_produces_safe_tuple_bound_decision() -> None:
    attestation, certification, policy, expected = _inputs()
    verifier = _SignatureVerifier()
    result = RouteAttestationVerificationService(
        signature_verifier=verifier,
        clock=lambda: datetime(2026, 7, 15, 12, tzinfo=UTC),
    ).verify(
        attestation=attestation,
        sigstore_bundle=b"sigstore-bundle",
        certification_bundle=certification,
        policy=policy,
        trusted_root=b"trusted-root",
        expected=expected,
        trusted_root_sha256=sha256_bytes(b"trusted-root"),
    )

    assert result.decision == "verified"
    assert result.code == "DPONE_ROUTE_ATTESTATION_VERIFIED"
    assert result.route_id == expected.route_id
    assert result.release_id == _RELEASE_ID
    assert result.deployment_id == _DEPLOYMENT_ID
    assert result.signer["certificate_identity"] == policy["certificate_identity"]
    assert verifier.calls == 1


def test_verification_rejects_one_byte_certification_mutation() -> None:
    attestation, certification, policy, expected = _inputs()
    mutated = certification.replace(b'"score":100.0', b'"score":99.0')
    verifier = _SignatureVerifier()
    result = RouteAttestationVerificationService(signature_verifier=verifier).verify(
        attestation=attestation,
        sigstore_bundle=b"sigstore-bundle",
        certification_bundle=mutated,
        policy=policy,
        trusted_root=b"trusted-root",
        expected=expected,
        trusted_root_sha256=sha256_bytes(b"trusted-root"),
    )

    assert result.decision == "invalid"
    assert result.code == "DPONE_ROUTE_ATTESTATION_CERTIFICATION_DIGEST_MISMATCH"
    assert verifier.calls == 0


def test_verification_rejects_cross_deployment_replay() -> None:
    attestation, certification, policy, expected = _inputs()
    replayed = RouteAttestationExpectedSubject(
        route_id=expected.route_id,
        release_id=expected.release_id,
        deployment_id="sha256:" + "e" * 64,
        environment=expected.environment,
        runtime_image_digest=expected.runtime_image_digest,
        authorization_profile=expected.authorization_profile,
    )
    result = RouteAttestationVerificationService(signature_verifier=_SignatureVerifier()).verify(
        attestation=attestation,
        sigstore_bundle=b"sigstore-bundle",
        certification_bundle=certification,
        policy=policy,
        trusted_root=b"trusted-root",
        expected=replayed,
        trusted_root_sha256=sha256_bytes(b"trusted-root"),
    )

    assert result.decision == "invalid"
    assert result.code == "DPONE_ROUTE_ATTESTATION_SUBJECT_MISMATCH"


def test_verification_rejects_invalid_time_windows_and_revocation() -> None:
    attestation, certification, policy, expected = _inputs()
    not_yet_valid = RouteAttestationVerificationService(
        signature_verifier=_SignatureVerifier(),
        clock=lambda: datetime(2026, 7, 15, 9, tzinfo=UTC),
    ).verify(
        attestation=attestation,
        sigstore_bundle=b"sigstore-bundle",
        certification_bundle=certification,
        policy=policy,
        trusted_root=b"trusted-root",
        expected=expected,
        trusted_root_sha256=sha256_bytes(b"trusted-root"),
    )
    assert (not_yet_valid.decision, not_yet_valid.code) == (
        "invalid",
        "DPONE_ROUTE_ATTESTATION_NOT_YET_VALID",
    )

    expired = RouteAttestationVerificationService(
        signature_verifier=_SignatureVerifier(),
        clock=lambda: datetime(2026, 7, 17, tzinfo=UTC),
    ).verify(
        attestation=attestation,
        sigstore_bundle=b"sigstore-bundle",
        certification_bundle=certification,
        policy=policy,
        trusted_root=b"trusted-root",
        expected=expected,
        trusted_root_sha256=sha256_bytes(b"trusted-root"),
    )
    assert (expired.decision, expired.code) == ("invalid", "DPONE_ROUTE_ATTESTATION_EXPIRED")

    attestation_id = json.loads(attestation)["attestation_id"]
    revoked_policy = {**policy, "revoked_attestation_ids": [attestation_id]}
    revoked = RouteAttestationVerificationService(
        signature_verifier=_SignatureVerifier(),
        clock=lambda: datetime(2026, 7, 15, 12, tzinfo=UTC),
    ).verify(
        attestation=attestation,
        sigstore_bundle=b"sigstore-bundle",
        certification_bundle=certification,
        policy=revoked_policy,
        trusted_root=b"trusted-root",
        expected=expected,
        trusted_root_sha256=sha256_bytes(b"trusted-root"),
    )
    assert (revoked.decision, revoked.code) == ("invalid", "DPONE_ROUTE_ATTESTATION_REVOKED")

    signer_revoked_policy = {
        **policy,
        "revoked_certificate_identities": [policy["certificate_identity"]],
    }
    signer_revoked = RouteAttestationVerificationService(
        signature_verifier=_SignatureVerifier(),
        clock=lambda: datetime(2026, 7, 15, 12, tzinfo=UTC),
    ).verify(
        attestation=attestation,
        sigstore_bundle=b"sigstore-bundle",
        certification_bundle=certification,
        policy=signer_revoked_policy,
        trusted_root=b"trusted-root",
        expected=expected,
        trusted_root_sha256=sha256_bytes(b"trusted-root"),
    )
    assert (signer_revoked.decision, signer_revoked.code) == (
        "invalid",
        "DPONE_ROUTE_ATTESTATION_REVOKED",
    )


def test_verifier_unavailability_is_not_an_invalid_signature_or_success() -> None:
    attestation, certification, policy, expected = _inputs()
    unavailable = SignatureVerificationResult.unverified(
        "DPONE_ROUTE_ATTESTATION_VERIFIER_UNAVAILABLE",
        "Route attestation verifier is unavailable.",
    )
    result = RouteAttestationVerificationService(signature_verifier=_SignatureVerifier(unavailable)).verify(
        attestation=attestation,
        sigstore_bundle=b"sigstore-bundle",
        certification_bundle=certification,
        policy=policy,
        trusted_root=b"trusted-root",
        expected=expected,
        trusted_root_sha256=sha256_bytes(b"trusted-root"),
    )

    assert result.decision == "unverified"
    assert result.code == "DPONE_ROUTE_ATTESTATION_VERIFIER_UNAVAILABLE"


def test_policy_rejects_unknown_fields_and_unsafe_version_before_signature() -> None:
    attestation, certification, policy, expected = _inputs()
    verifier = _SignatureVerifier()
    invalid_policy = {
        **policy,
        "unexpected": "password=must-not-leak",
        "cosign": {**policy["cosign"], "minimum_version": "3.0.3"},
    }

    result = RouteAttestationVerificationService(signature_verifier=verifier).verify(
        attestation=attestation,
        sigstore_bundle=b"sigstore-bundle",
        certification_bundle=certification,
        policy=invalid_policy,
        trusted_root=b"trusted-root",
        expected=expected,
        trusted_root_sha256=sha256_bytes(b"trusted-root"),
    )

    assert (result.decision, result.code) == ("invalid", "DPONE_ROUTE_ATTESTATION_POLICY_INVALID")
    assert verifier.calls == 0
    assert "must-not-leak" not in repr(result.to_dict())


def test_attestation_rejects_unknown_claims_and_non_finite_json_before_signature() -> None:
    attestation, certification, policy, expected = _inputs()
    payload = json.loads(attestation)
    payload["claims"]["subject"]["unexpected"] = "value"
    verifier = _SignatureVerifier()

    extra_claim = RouteAttestationVerificationService(signature_verifier=verifier).verify(
        attestation=_raw(payload),
        sigstore_bundle=b"sigstore-bundle",
        certification_bundle=certification,
        policy=policy,
        trusted_root=b"trusted-root",
        expected=expected,
        trusted_root_sha256=sha256_bytes(b"trusted-root"),
    )
    non_finite = RouteAttestationVerificationService(signature_verifier=verifier).verify(
        attestation=b'{"schema":NaN}',
        sigstore_bundle=b"sigstore-bundle",
        certification_bundle=certification,
        policy=policy,
        trusted_root=b"trusted-root",
        expected=expected,
        trusted_root_sha256=sha256_bytes(b"trusted-root"),
    )

    assert (extra_claim.decision, extra_claim.code) == ("invalid", "DPONE_ROUTE_ATTESTATION_INPUT_INVALID")
    assert (non_finite.decision, non_finite.code) == ("invalid", "DPONE_ROUTE_ATTESTATION_INPUT_INVALID")
    assert verifier.calls == 0
