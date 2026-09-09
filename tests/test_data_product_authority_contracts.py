from __future__ import annotations

import json
from pathlib import Path

from jsonschema import Draft202012Validator

from dpone.readiness.data_product_authority import (
    ApprovalQuorumVerifier,
    AuthorityCheckEvaluator,
    AuthorityGate,
    AuthorityRegistryBuilder,
)
from dpone.readiness.data_product_authority_signing import EvidenceSignatureVerifier, EvidenceSigner


def test_authority_registry_builds_disabled_and_blocks_invalid_config() -> None:
    disabled = AuthorityRegistryBuilder().build(manifest=_manifest(enabled=False))
    invalid = AuthorityRegistryBuilder().build(manifest=_manifest(duplicate_identity=True, unknown_role=True))

    assert disabled["schema_version"] == "dpone.data_product_authority_registry.v1"
    assert disabled["status"] == "disabled"
    assert disabled["product"]["id"] == "analytics.orders"
    assert disabled["identities"] == []
    assert invalid["status"] == "blocked"
    assert "data_product_authority.duplicate_identity:alice" in invalid["blockers"]
    assert "data_product_authority.unknown_role:alice:missing_role" in invalid["blockers"]


def test_authority_check_grants_unknown_actor_policy_and_sod() -> None:
    registry = AuthorityRegistryBuilder().build(manifest=_manifest())
    allowed = AuthorityCheckEvaluator().check(
        registry=registry,
        actor="data-governance",
        action="policy_waiver.approve",
        subject=_waiver_request(requested_by="alice"),
    )
    no_grant = AuthorityCheckEvaluator().check(
        registry=registry,
        actor="alice",
        action="policy_waiver.approve",
        subject=_waiver_request(requested_by="carol"),
    )
    self_approval = AuthorityCheckEvaluator().check(
        registry=registry,
        actor="bob",
        action="policy_waiver.approve",
        subject=_waiver_request(requested_by="bob"),
    )
    unknown_warning = AuthorityCheckEvaluator().check(
        registry=AuthorityRegistryBuilder().build(manifest=_manifest(unknown_actor="warn")),
        actor="unknown-user",
        action="policy_waiver.approve",
        subject=_waiver_request(requested_by="alice"),
    )

    assert allowed["schema_version"] == "dpone.data_product_authority_check.v1"
    assert allowed["status"] == "allowed"
    assert allowed["granted_roles"] == ["governance_approver"]
    assert no_grant["status"] == "blocked"
    assert "data_product_authority.action_not_granted:alice:policy_waiver.approve" in no_grant["blockers"]
    assert self_approval["status"] == "blocked"
    assert "data_product_authority.sod_self_approval:requested_by" in self_approval["blockers"]
    assert unknown_warning["status"] == "warning"
    assert "data_product_authority.unknown_actor:unknown-user" in unknown_warning["warnings"]


def test_approval_quorum_requires_distinct_actors_roles_and_request_match() -> None:
    registry = AuthorityRegistryBuilder().build(manifest=_manifest())
    request = _waiver_request(requested_by="alice")
    passing = ApprovalQuorumVerifier().verify(
        registry=registry,
        request=request,
        approvals=(
            _approval("bob", request_id=request["waiver_request_id"]),
            _approval("data-governance", request_id=request["waiver_request_id"]),
        ),
    )
    duplicate_and_mismatch = ApprovalQuorumVerifier().verify(
        registry=registry,
        request=request,
        approvals=(
            _approval("bob", request_id=request["waiver_request_id"]),
            _approval("bob", request_id=request["waiver_request_id"]),
            _approval("data-governance", request_id="sha256:" + "0" * 64),
        ),
    )

    assert passing["schema_version"] == "dpone.data_product_approval_quorum.v1"
    assert passing["status"] == "allowed"
    assert passing["approved_actors"] == ["bob", "data-governance"]
    assert passing["satisfied_roles"] == ["data_owner", "governance_approver"]
    assert duplicate_and_mismatch["status"] == "blocked"
    assert "data_product_authority.duplicate_approval_actor:bob" in duplicate_and_mismatch["blockers"]
    assert "data_product_authority.approval_request_mismatch:data-governance" in duplicate_and_mismatch["blockers"]


def test_hmac_signature_is_stable_secret_free_and_verifies(monkeypatch) -> None:
    registry = AuthorityRegistryBuilder().build(manifest=_manifest())
    artifact = _waiver()
    monkeypatch.setenv("DPONE_AUTHORITY_SIGNING_KEY", "super-secret-test-key")

    signer = EvidenceSigner()
    signature = signer.sign(registry=registry, artifact=artifact, actor="data-governance")
    repeated = signer.sign(registry=registry, artifact=artifact, actor="data-governance")
    verified = EvidenceSignatureVerifier().verify(registry=registry, artifact=artifact, signature=signature)
    tampered = EvidenceSignatureVerifier().verify(
        registry=registry,
        artifact={**artifact, "rule_id": "different_rule"},
        signature=signature,
    )
    wrong_signer = EvidenceSignatureVerifier().verify(
        registry=registry,
        artifact=artifact,
        signature={**signature, "actor": "alice"},
    )

    assert signature["schema_version"] == "dpone.data_product_evidence_signature.v1"
    assert signature["status"] == "signed"
    assert signature["evidence_signature_id"] == repeated["evidence_signature_id"]
    assert "super-secret-test-key" not in json.dumps(signature)
    assert verified["status"] == "verified"
    assert tampered["status"] == "blocked"
    assert "data_product_authority.signature_artifact_hash_mismatch" in tampered["blockers"]
    assert wrong_signer["status"] == "blocked"
    assert "data_product_authority.signature_actor_mismatch" in wrong_signer["blockers"]


def test_authority_gate_profiles_report_and_public_schemas(monkeypatch) -> None:
    registry = AuthorityRegistryBuilder().build(manifest=_manifest())
    request = _waiver_request(requested_by="alice")
    check = AuthorityCheckEvaluator().check(
        registry=registry,
        actor="data-governance",
        action="policy_waiver.approve",
        subject=request,
    )
    quorum = ApprovalQuorumVerifier().verify(
        registry=registry,
        request=request,
        approvals=(
            _approval("bob", request_id=request["waiver_request_id"]),
            _approval("data-governance", request_id=request["waiver_request_id"]),
        ),
    )
    monkeypatch.setenv("DPONE_AUTHORITY_SIGNING_KEY", "super-secret-test-key")
    signature = EvidenceSigner().sign(registry=registry, artifact=_waiver(), actor="data-governance")
    strict = AuthorityGate().evaluate(
        authority_check=check,
        approval_quorum=quorum,
        signatures=(signature,),
        profile="prod_strict",
    )
    advisory = AuthorityGate().evaluate(
        authority_check={**check, "status": "blocked", "blockers": ["blocked"]},
        approval_quorum={},
        signatures=(),
        profile="advisory",
    )
    report = AuthorityGate().report(gate=strict)

    assert strict["schema_version"] == "dpone.data_product_authority_gate.v1"
    assert strict["status"] == "allowed"
    assert advisory["status"] == "warning"
    assert advisory["blockers"] == []
    assert report["schema_version"] == "dpone.data_product_authority_report.v1"
    assert "# Data Product Authority Report" in report["markdown"]

    for name, payload in (
        ("data-product-authority-registry.schema.json", registry),
        ("data-product-authority-check.schema.json", check),
        ("data-product-approval-quorum.schema.json", quorum),
        ("data-product-evidence-signature.schema.json", signature),
        ("data-product-authority-gate.schema.json", strict),
        ("data-product-authority-report.schema.json", report),
    ):
        schema = json.loads((Path("docs/schemas/data-product") / name).read_text(encoding="utf-8"))
        Draft202012Validator(schema).validate(payload)


def _manifest(
    *,
    enabled: bool = True,
    duplicate_identity: bool = False,
    unknown_role: bool = False,
    unknown_actor: str = "block",
) -> dict:
    identities = [
        {
            "id": "alice",
            "type": "user",
            "owner": "data-platform",
            "groups": ["data-platform"],
            "roles": ["change_author"] + (["missing_role"] if unknown_role else []),
        },
        {
            "id": "bob",
            "type": "user",
            "owner": "data-platform",
            "groups": ["data-platform"],
            "roles": ["data_owner"],
        },
        {
            "id": "data-governance",
            "type": "group",
            "owner": "governance",
            "roles": ["governance_approver"],
        },
    ]
    if duplicate_identity:
        identities.append({**identities[0]})
    return {
        "sink": {
            "options": {
                "data_product": {
                    "id": "analytics.orders",
                    "owner": "data-platform",
                    "tier": "gold",
                    "criticality": "high",
                    "authority": {
                        "enabled": enabled,
                        "mode": "gate",
                        "profile": "prod_strict",
                        "unknown_actor": unknown_actor,
                        "identities": identities,
                        "roles": [
                            {
                                "id": "data_owner",
                                "grants": ["policy_waiver.approve", "release_closeout.approve"],
                            },
                            {
                                "id": "governance_approver",
                                "grants": ["policy_waiver.approve", "regulated_release.approve"],
                            },
                            {"id": "change_author", "grants": ["change.author"]},
                        ],
                        "approval": {
                            "quorum": {
                                "policy_waiver": {
                                    "min_approvals": 2,
                                    "required_roles": ["data_owner", "governance_approver"],
                                }
                            }
                        },
                        "separation_of_duties": {
                            "prevent_self_approval": True,
                            "disallow_same_actor_for": ["requested_by", "implemented_by", "approved_by"],
                        },
                        "signing": {
                            "enabled": True,
                            "algorithm": "hmac_sha256",
                            "key_env": "DPONE_AUTHORITY_SIGNING_KEY",
                            "required_for": ["dpone.data_product_waiver.v1"],
                        },
                    },
                }
            }
        }
    }


def _waiver_request(*, requested_by: str) -> dict:
    return {
        "schema_version": "dpone.data_product_waiver_request.v1",
        "status": "requested",
        "waiver_request_id": "sha256:" + "1" * 64,
        "policy_evaluation_id": "sha256:" + "2" * 64,
        "policy_pack_id": "sha256:" + "3" * 64,
        "product_id": "analytics.orders",
        "rule_id": "require_assertion_gate",
        "severity": "critical",
        "requested_by": requested_by,
        "implemented_by": "alice",
        "expires_at": "2026-07-12T00:00:00Z",
        "blockers": [],
        "warnings": [],
    }


def _approval(actor: str, *, request_id: str) -> dict:
    return {
        "actor": actor,
        "waiver_request_id": request_id,
        "approved_at": "2026-06-28T12:00:00Z",
        "expires_at": "2026-07-12T00:00:00Z",
        "ticket": f"GOV-{actor}",
    }


def _waiver() -> dict:
    return {
        "schema_version": "dpone.data_product_waiver.v1",
        "waiver_id": "sha256:" + "4" * 64,
        "status": "approved",
        "waiver_request_id": "sha256:" + "1" * 64,
        "policy_evaluation_id": "sha256:" + "2" * 64,
        "policy_pack_id": "sha256:" + "3" * 64,
        "product_id": "analytics.orders",
        "rule_id": "require_assertion_gate",
        "actor": "data-governance",
        "blockers": [],
        "warnings": [],
    }
