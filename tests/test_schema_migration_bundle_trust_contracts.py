from __future__ import annotations

import json

from dpone.readiness.migration_control import MigrationPack, MigrationTarget
from dpone.readiness.schema_migration_bundle import MigrationBundleBuilder, MigrationEvidenceArtifact
from dpone.readiness.schema_migration_bundle_policy import (
    MigrationBundlePolicyEvaluator,
    MigrationBundlePolicyOptions,
)
from dpone.readiness.schema_migration_trust import (
    MigrationTrustAttestor,
    MigrationTrustPolicy,
    MigrationTrustVerifier,
)


def test_attest_builds_signed_provenance_and_trust_verify_passes() -> None:
    bundle, artifact = _bundle()
    verification = {"status": "passed", "blockers": [], "warnings": []}

    provenance = MigrationTrustAttestor().attest(
        bundle=bundle,
        verification=verification,
        provenance_source="github_actions",
        repository="https://github.com/acme/data-platform",
        commit_sha="a" * 40,
        ref="refs/heads/main",
        run_id="12345",
        workflow=".github/workflows/schema-migration.yml",
        protected_ref=True,
        signing_key="secret",
        signing_key_id="ci-hmac",
        finished_at="2026-06-22T12:00:00Z",
    )

    assert provenance["schema_version"] == "dpone.schema_migration_provenance.v1"
    assert provenance["status"] == "attested"
    assert provenance["bundle_id"] == bundle["bundle_id"]
    assert provenance["pack_id"] == bundle["pack_id"]
    assert provenance["statement"]["_type"] == "https://in-toto.io/Statement/v1"
    assert provenance["statement"]["predicateType"] == "https://slsa.dev/provenance/v1"
    assert provenance["signature"]["algorithm"] == "HMAC-SHA256"

    policy = MigrationTrustPolicy.from_mapping(
        {
            "schema_version": "dpone.schema_migration_trust_policy.v1",
            "profile": "prod_trusted",
            "require_signed_provenance": True,
            "allowed_producers": ["dpone schema migration bundle attest"],
            "allowed_repositories": ["https://github.com/acme/data-platform"],
            "allowed_refs": ["refs/heads/main", "refs/tags/v*"],
            "require_protected_ref": True,
            "require_commit_sha": True,
            "require_ci_run": True,
            "signature": {"allowed_algorithms": ["HMAC-SHA256"], "allowed_key_ids": ["ci-hmac"]},
        }
    )

    trust = MigrationTrustVerifier().verify(
        bundle=bundle,
        verification=verification,
        provenance=provenance,
        policy=policy,
        signing_key="secret",
        external_attestations=(),
    )

    assert trust["schema_version"] == "dpone.schema_migration_trust_verification.v1"
    assert trust["status"] == "trusted"
    assert trust["bundle_id"] == bundle["bundle_id"]
    assert trust["pack_id"] == bundle["pack_id"]
    assert trust["provenance_id"] == provenance["provenance_id"]
    assert trust["blockers"] == []
    assert artifact.payload["pack_id"] == bundle["pack_id"]


def test_trust_verify_blocks_subject_mismatch_and_bad_signature_key() -> None:
    bundle, _ = _bundle()
    verification = {"status": "passed", "blockers": [], "warnings": []}
    provenance = MigrationTrustAttestor().attest(
        bundle=bundle,
        verification=verification,
        provenance_source="github_actions",
        repository="https://github.com/acme/data-platform",
        commit_sha="a" * 40,
        ref="refs/heads/main",
        run_id="12345",
        workflow=".github/workflows/schema-migration.yml",
        protected_ref=True,
        signing_key="secret",
        signing_key_id="ci-hmac",
        finished_at="2026-06-22T12:00:00Z",
    )
    provenance["subject"]["subject_digest"] = "sha256:" + "0" * 64

    trust = MigrationTrustVerifier().verify(
        bundle=bundle,
        verification=verification,
        provenance=provenance,
        policy=MigrationTrustPolicy.from_mapping(
            {
                "require_signed_provenance": True,
                "signature": {"allowed_algorithms": ["HMAC-SHA256"], "allowed_key_ids": ["ci-hmac"]},
            }
        ),
        signing_key="wrong",
        external_attestations=(),
    )

    assert trust["status"] == "blocked"
    assert "migration_trust.subject_mismatch" in trust["blockers"]
    assert "migration_trust.signature_mismatch" in trust["blockers"]


def test_external_attestation_required_and_subject_mismatch_blocks() -> None:
    bundle, _ = _bundle()
    verification = {"status": "passed", "blockers": [], "warnings": []}
    provenance = MigrationTrustAttestor().attest(
        bundle=bundle,
        verification=verification,
        provenance_source="gitlab_ci",
        repository="https://gitlab.com/acme/data-platform",
        commit_sha="b" * 40,
        ref="refs/tags/v1.2.3",
        run_id="pipeline-77",
        workflow=".gitlab-ci.yml",
        protected_ref=True,
        finished_at="2026-06-22T12:00:00Z",
    )

    trust = MigrationTrustVerifier().verify(
        bundle=bundle,
        verification=verification,
        provenance=provenance,
        policy=MigrationTrustPolicy.from_mapping(
            {
                "external_attestations": {
                    "required": True,
                    "allowed_kinds": ["github_artifact_attestation", "sigstore_bundle"],
                }
            }
        ),
        signing_key=None,
        external_attestations=(
            {
                "kind": "github_artifact_attestation",
                "status": "verified",
                "subject_digest": "sha256:" + "1" * 64,
            },
        ),
    )

    assert trust["status"] == "blocked"
    assert "migration_trust.external_subject_mismatch:github_artifact_attestation" in trust["blockers"]


def test_bundle_gate_can_require_trusted_provenance_opt_in() -> None:
    bundle, artifact = _bundle()
    policy = MigrationBundlePolicyOptions.resolve(
        profile="prod_strict",
        policy_payload={
            "required_artifacts": ["migration_pack"],
            "require_trusted_provenance": True,
            "fail_on_warnings": False,
        },
    )
    evaluator = MigrationBundlePolicyEvaluator()

    missing = evaluator.evaluate(
        bundle=bundle,
        verification={"status": "passed", "blockers": [], "warnings": []},
        policy=policy,
        artifact_payloads={"migration_pack": artifact.payload},
        trust_verification=None,
    )
    trusted = evaluator.evaluate(
        bundle=bundle,
        verification={"status": "passed", "blockers": [], "warnings": []},
        policy=policy,
        artifact_payloads={"migration_pack": artifact.payload},
        trust_verification={
            "schema_version": "dpone.schema_migration_trust_verification.v1",
            "status": "trusted",
            "bundle_id": bundle["bundle_id"],
            "pack_id": bundle["pack_id"],
            "blockers": [],
            "warnings": [],
        },
    )

    assert missing["status"] == "blocked"
    assert "migration_bundle_gate.trust_verification_required" in missing["blockers"]
    assert trusted["status"] == "allowed"


def _bundle() -> tuple[dict[str, object], MigrationEvidenceArtifact]:
    pack = MigrationPack.build(
        target=MigrationTarget(sink_type="clickhouse", table="analytics.orders"),
        desired={"sink_type": "clickhouse", "table": "analytics.orders", "order_by": ["id"]},
        actual={"sink_type": "clickhouse", "table": "analytics.orders", "order_by": ["id"]},
        changes=({"change_type": "table_setting", "path": "table_settings.index_granularity"},),
        ddl=("ALTER TABLE analytics.orders MODIFY SETTING index_granularity = 8192",),
        strategy="online_safe",
    )
    artifact = _artifact("migration_pack", "pack.json", pack.to_dict(command="plan"), required=True)
    bundle = MigrationBundleBuilder().build(artifacts=(artifact,), attest=True)
    return bundle, artifact


def _artifact(kind: str, path: str, payload: dict[str, object], *, required: bool) -> MigrationEvidenceArtifact:
    content = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
    return MigrationEvidenceArtifact.from_bytes(kind=kind, path=path, content=content, required=required)
