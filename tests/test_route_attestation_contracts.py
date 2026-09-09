from __future__ import annotations

from dpone.contracts.route_attestation import RouteAttestationVerification
from dpone.gitops.schema_contracts import get_gitops_schema_contract
from dpone.gitops.schema_validation import GitOpsSchemaValidator


def test_route_attestation_contracts_are_registered() -> None:
    expected = {
        "dpone.route-attestation.v1": "route-attestation",
        "dpone.route-attestation-policy.v1": "route-attestation-policy",
        "dpone.route-attestation-verification.v1": "route-attestation-verification",
    }
    assert {kind: get_gitops_schema_contract(kind).name for kind in expected} == expected


def test_verification_receipt_schema_allows_only_safe_known_metadata() -> None:
    receipt = RouteAttestationVerification(
        decision="verified",
        code="DPONE_ROUTE_ATTESTATION_VERIFIED",
        message="Route attestation is valid for this deployment.",
        attestation_id="sha256:" + "1" * 64,
        attestation_sha256="sha256:" + "2" * 64,
        certification_bundle_sha256="sha256:" + "3" * 64,
        policy_fingerprint="sha256:" + "4" * 64,
        route_id="mssql_clickhouse_incremental_merge_airflow_kpo",
        release_id="sha256:" + "5" * 64,
        deployment_id="sha256:" + "6" * 64,
        environment="production",
        authorization_profile="safe_sample_production",
        signer={
            "backend": "cosign_keyless_v1",
            "certificate_identity": "ci://dpone/route-attestation",
            "certificate_oidc_issuer": "https://token.actions.githubusercontent.com",
            "verifier_version": "3.0.4",
        },
        validity={"not_before": "2026-07-15T10:00:00Z", "expires_at": "2026-07-16T10:00:00Z"},
        verified_at="2026-07-15T12:00:00Z",
    ).to_dict()
    validator = GitOpsSchemaValidator()

    assert validator.validate(receipt, expected_kind="dpone.route-attestation-verification.v1") == ()
    receipt["signer"]["vault_token"] = "must-not-be-schema-valid"
    issues = validator.validate(receipt, expected_kind="dpone.route-attestation-verification.v1")

    assert issues
