from __future__ import annotations

from pathlib import Path


def test_schema_migration_control_docs_cover_user_and_developer_contracts() -> None:
    user_docs = Path("docs/schema-migration-control.md").read_text(encoding="utf-8")
    developer_docs = Path("docs/developer-schema-migration-control.md").read_text(encoding="utf-8")
    mkdocs = Path("mkdocs.yml").read_text(encoding="utf-8")
    readme = Path("docs/README.md").read_text(encoding="utf-8")
    architecture = Path("docs/architecture.md").read_text(encoding="utf-8")
    cli_reference = Path("docs/cli-reference.md").read_text(encoding="utf-8")
    schema_dir = Path("docs/schemas/schema-migration")

    for command in (
        "dpone schema migration plan",
        "dpone schema migration baseline",
        "dpone schema migration apply",
        "dpone schema migration history",
        "dpone schema migration rollback",
        "dpone schema migration verify-env",
        "dpone schema migration certify",
        "dpone schema migration promote",
        "dpone schema migration rehearse plan",
        "dpone schema migration rehearse run",
        "dpone schema migration rehearse certify",
        "dpone schema migration rehearse report",
        "dpone schema migration rehearse fixture plan",
        "dpone schema migration rehearse fixture build",
        "dpone schema migration rehearse fixture profile",
        "dpone schema migration post-apply plan",
        "dpone schema migration post-apply run",
        "dpone schema migration post-apply certify",
        "dpone schema migration post-apply report",
        "dpone schema migration watch plan",
        "dpone schema migration watch run",
        "dpone schema migration watch status",
        "dpone schema migration watch certify",
        "dpone schema migration watch report",
        "dpone schema migration remediation plan",
        "dpone schema migration remediation apply",
        "dpone schema migration remediation certify",
        "dpone schema migration remediation report",
        "dpone schema migration backup plan",
        "dpone schema migration backup create",
        "dpone schema migration backup restore plan",
        "dpone schema migration backup restore run",
        "dpone schema migration backup certify",
        "dpone schema migration backup report",
        "dpone schema migration recovery point record",
        "dpone schema migration recovery point latest",
        "dpone schema migration recovery chain verify",
        "dpone schema migration recovery restore plan",
        "dpone schema migration recovery restore run",
        "dpone schema migration recovery restore certify",
        "dpone schema migration recovery retention plan",
        "dpone schema migration bundle build",
        "dpone schema migration bundle verify",
        "dpone schema migration bundle gate",
        "dpone schema migration bundle diff",
        "dpone schema migration bundle attest",
        "dpone schema migration bundle trust verify",
        "dpone schema migration registry record",
        "dpone schema migration registry history",
        "dpone schema migration registry latest",
        "dpone schema migration registry audit-report",
        "dpone schema migration review render",
    ):
        assert command in user_docs

    for term in (
        "MigrationControlFacade",
        "MigrationPack",
        "MigrationLedgerStore",
        "ArtifactMigrationLedgerStore",
        "ShadowMigrationPlanner",
        "TargetShadowMigrationDialect",
        "ClickHouseShadowMigrationDialect",
        "MigrationOperationExecutor",
        "ClickHouseMigrationOperationExecutor",
        "MigrationEnvironmentContract",
        "MigrationEnvironmentRef",
        "EnvironmentLedgerStore",
        "ArtifactEnvironmentLedgerStore",
        "MigrationEnvironmentVerifier",
        "MigrationPromotionPlanner",
        "MigrationPromotionGate",
        "MigrationPromotionFacade",
        "MigrationRehearsalPlan",
        "MigrationRehearsalPlanner",
        "MigrationRehearsalRunner",
        "MigrationRehearsalCertifier",
        "MigrationRehearsalFacade",
        "MigrationRehearsalPolicyProfile",
        "MigrationFixturePlanner",
        "SyntheticMigrationFixtureProvider",
        "ArtifactSampleFixtureProvider",
        "DataMaskingPolicy",
        "TargetFixtureSeeder",
        "MigrationDataProfileAnalyzer",
        "MigrationRehearsalDataFacade",
        "MigrationEvidenceBundle",
        "MigrationEvidenceArtifact",
        "MigrationBundleBuilder",
        "MigrationBundleVerifier",
        "MigrationBundlePolicyOptions",
        "MigrationBundlePolicyProfileRegistry",
        "MigrationBundlePolicyEvaluator",
        "MigrationBundleGateDecision",
        "MigrationBundleGateFacade",
        "MigrationBundleDiffOptions",
        "MigrationBundleDiffInputs",
        "MigrationBundleDiffChange",
        "MigrationBundleDiffClassifier",
        "MigrationBundleDiffBuilder",
        "MigrationBundleDiffFacade",
        "MigrationReviewRenderer",
        "MigrationBundleFacade",
        "MigrationTrustSubject",
        "MigrationProvenanceStatement",
        "MigrationProvenanceBuilder",
        "ArtifactSignatureProvider",
        "LocalHmacSignatureProvider",
        "ExternalAttestationEvidence",
        "MigrationTrustPolicy",
        "MigrationTrustVerifier",
        "MigrationTrustFacade",
        "ScmCiTemplateCatalog",
        "MigrationEvidenceRegistryRecord",
        "MigrationEvidenceArtifactRef",
        "MigrationEvidenceRecorder",
        "MigrationEvidenceRegistryStore",
        "LocalJsonEvidenceRegistryStore",
        "SqliteEvidenceRegistryStore",
        "MigrationEvidenceQuery",
        "MigrationEvidenceQueryService",
        "MigrationEvidenceAuditReporter",
        "MigrationEvidenceRegistryFacade",
        "PostApplyVerificationPlan",
        "PostApplyVerificationPlanner",
        "PostApplyVerificationRunner",
        "PostApplyCertifier",
        "PostApplyVerificationFacade",
        "PostApplyTargetInspector",
        "TargetCanaryExecutor",
        "ClickHousePostApplyVerifier",
        "RollbackWindowEvaluator",
        "MigrationWatchPlan",
        "MigrationWatchPlanner",
        "MigrationWatchRunner",
        "MigrationWatchSample",
        "TargetWatchProbe",
        "ClickHouseWatchProbe",
        "MigrationWatchRemediationAdvisor",
        "MigrationWatchCertifier",
        "MigrationWatchFacade",
        "MigrationRemediationPlan",
        "MigrationRemediationPlanner",
        "RollbackCapabilityClassifier",
        "TargetRemediationDialect",
        "ClickHouseRemediationDialect",
        "MigrationRemediationRunner",
        "MigrationRemediationCertifier",
        "MigrationRemediationFacade",
        "MigrationBackupOptions",
        "MigrationBackupRequirementClassifier",
        "MigrationBackupPlanner",
        "TargetBackupDialect",
        "ClickHouseBackupDialect",
        "MigrationBackupRunner",
        "MigrationRestorePlanner",
        "MigrationRestoreRunner",
        "MigrationBackupCertifier",
        "MigrationBackupFacade",
        "RecoveryPoint",
        "RecoveryPointRecorder",
        "RecoveryCatalogStore",
        "LocalJsonRecoveryCatalogStore",
        "SqliteRecoveryCatalogStore",
        "RecoveryPointSelector",
        "RecoveryChainVerifier",
        "RecoveryRetentionPlanner",
        "TargetRecoveryDialect",
        "ClickHouseRecoveryDialect",
        "dpone.schema_migration_evidence_registry_record.v1",
        "dpone.schema_migration_evidence_registry.v1",
        "dpone.schema_migration_evidence_registry_query.v1",
        "dpone.schema_migration_evidence_audit_report.v1",
        "evidence_registry.record_conflict",
        "registry vs ledger",
        "ColumnProjectionPlanner",
        "EXCHANGE TABLES",
        "--phase",
        "--execute",
        "--target-connection",
        "--environment",
        "--environment-contract",
        "--promotion",
        "dpone.schema_migration_environments.v1",
        "dpone.schema_migration_environment_certification.v1",
        "dpone.schema_migration_promotion.v1",
        "dpone.schema_migration_promotion_approval.v1",
        "dpone.schema_migration_rehearsal_plan.v1",
        "dpone.schema_migration_rehearsal_run.v1",
        "dpone.schema_migration_rehearsal_certificate.v1",
        "dpone.schema_migration_fixture_plan.v1",
        "dpone.schema_migration_fixture_build.v1",
        "dpone.schema_migration_data_profile.v1",
        "dpone.schema_migration_post_apply_plan.v1",
        "dpone.schema_migration_post_apply_run.v1",
        "dpone.schema_migration_post_apply_certificate.v1",
        "dpone.schema_migration_watch_plan.v1",
        "dpone.schema_migration_watch_run.v1",
        "dpone.schema_migration_watch_certificate.v1",
        "dpone.schema_migration_remediation_plan.v1",
        "dpone.schema_migration_remediation_run.v1",
        "dpone.schema_migration_remediation_certificate.v1",
        "dpone.schema_migration_backup_plan.v1",
        "dpone.schema_migration_backup_run.v1",
        "dpone.schema_migration_restore_plan.v1",
        "dpone.schema_migration_restore_run.v1",
        "dpone.schema_migration_backup_certificate.v1",
        "dpone.schema_migration_recovery_point.v1",
        "dpone.schema_migration_recovery_catalog.v1",
        "dpone.schema_migration_recovery_chain_verification.v1",
        "dpone.schema_migration_recovery_restore_plan.v1",
        "dpone.schema_migration_recovery_restore_run.v1",
        "dpone.schema_migration_recovery_restore_certificate.v1",
        "dpone.schema_migration_recovery_retention_plan.v1",
        "schema_migration_remediation.unsupported_target",
        "schema_migration_remediation.blocked_after_contract",
        "schema_migration_backup.restore_prod_blocked",
        "restore_from_backup",
        "restore_to_point",
        "recovery_point",
        "recovery_chain_verification",
        "recovery_point_recorded",
        "recovery_chain_verified",
        "latest usable restore point",
        "backup_certificate",
        "backup_certified",
        "restore_rehearsed",
        "BACKUP TABLE",
        "RESTORE TABLE",
        "migration_promotion.promotion_required",
        "schema_migration_rehearsal.unsupported_target",
        "schema_migration_rehearsal.prod_environment_blocked",
        "operation_status",
        "dpone.schema_migration_bundle.v1",
        "dpone.schema_migration_bundle_verification.v1",
        "dpone.schema_migration_bundle_policy.v1",
        "dpone.schema_migration_bundle_gate.v1",
        "dpone.schema_migration_bundle_diff.v1",
        "dpone.schema_migration_trust_policy.v1",
        "dpone.schema_migration_provenance.v1",
        "dpone.schema_migration_trust_verification.v1",
        "dpone.schema_migration_review.v1",
        "prod_strict",
        "pr_review",
        "stage_certified",
        "regulated",
        "bundle gate",
        "bundle diff",
        "evidence registry",
        "registry record",
        "rehearsal_certificate",
        "post_apply_certificate",
        "watch_certificate",
        "remediation_certificate",
        "controlled remediation",
        "rollback_certified",
        "remediated",
        "release watch",
        "query health",
        "system.query_log",
        "schema_migration_watch.unsupported_target",
        "schema_migration_watch.physical_drift",
        "schema_migration_watch.query_health_blocked",
        "rollback_required",
        "watched",
        "closed",
        "fixture_build",
        "quality_profile",
        "schema_migration_profile.duplicate_key",
        "schema_migration_rehearsal.row_count_drift",
        "pre-prod rehearsal proof",
        "post-apply verification",
        "canary queries",
        "rollback window",
        "verify vs gate vs diff vs review",
        "SLSA",
        "in-toto",
        "GitHub Artifact Attestations",
        "Sigstore",
        "Cosign",
        "HMAC-SHA256",
        "trusted provenance",
        "bundle_id",
        "bundle_digest",
        "SCM owns branch protection",
        "dpone owns pack consistency",
        "migration.validation_mismatch",
        "target-backed",
        "GitHub",
        "GitLab",
        "Bitbucket",
        "dlt",
        "Airbyte",
        "Fivetran",
        "Informatica",
        "Pentaho",
        "Liquibase",
        "Flyway",
    ):
        assert term in user_docs + developer_docs

    assert "schema-migration-control.md" in mkdocs
    assert "developer-schema-migration-control.md" in mkdocs
    assert "Schema migration control" in readme
    assert "Migration control plane" in architecture
    assert "dpone schema migration verify-env" in cli_reference
    assert "dpone schema migration certify" in cli_reference
    assert "dpone schema migration promote" in cli_reference
    assert "dpone schema migration rehearse plan" in cli_reference
    assert "dpone schema migration rehearse run" in cli_reference
    assert "dpone schema migration rehearse certify" in cli_reference
    assert "dpone schema migration rehearse report" in cli_reference
    assert "dpone schema migration rehearse fixture plan" in cli_reference
    assert "dpone schema migration rehearse fixture build" in cli_reference
    assert "dpone schema migration rehearse fixture profile" in cli_reference
    assert "dpone schema migration post-apply plan" in cli_reference
    assert "dpone schema migration post-apply run" in cli_reference
    assert "dpone schema migration post-apply certify" in cli_reference
    assert "dpone schema migration post-apply report" in cli_reference
    assert "dpone schema migration watch plan" in cli_reference
    assert "dpone schema migration watch run" in cli_reference
    assert "dpone schema migration watch status" in cli_reference
    assert "dpone schema migration watch certify" in cli_reference
    assert "dpone schema migration watch report" in cli_reference
    assert "dpone schema migration remediation plan" in cli_reference
    assert "dpone schema migration remediation apply" in cli_reference
    assert "dpone schema migration remediation certify" in cli_reference
    assert "dpone schema migration remediation report" in cli_reference
    assert "dpone schema migration backup plan" in cli_reference
    assert "dpone schema migration backup create" in cli_reference
    assert "dpone schema migration backup restore plan" in cli_reference
    assert "dpone schema migration backup restore run" in cli_reference
    assert "dpone schema migration backup certify" in cli_reference
    assert "dpone schema migration backup report" in cli_reference
    assert "dpone schema migration recovery point record" in cli_reference
    assert "dpone schema migration recovery point latest" in cli_reference
    assert "dpone schema migration recovery chain verify" in cli_reference
    assert "dpone schema migration recovery restore plan" in cli_reference
    assert "dpone schema migration recovery restore run" in cli_reference
    assert "dpone schema migration recovery restore certify" in cli_reference
    assert "dpone schema migration recovery retention plan" in cli_reference
    assert "dpone schema migration bundle build" in cli_reference
    assert "dpone schema migration bundle verify" in cli_reference
    assert "dpone schema migration bundle gate" in cli_reference
    assert "dpone schema migration bundle diff" in cli_reference
    assert "dpone schema migration bundle attest" in cli_reference
    assert "dpone schema migration bundle trust verify" in cli_reference
    assert "dpone schema migration registry record" in cli_reference
    assert "dpone schema migration registry history" in cli_reference
    assert "dpone schema migration registry latest" in cli_reference
    assert "dpone schema migration registry audit-report" in cli_reference
    assert "dpone schema migration review render" in cli_reference
    assert (schema_dir / "environments.schema.json").exists()
    assert (schema_dir / "environment-certification.schema.json").exists()
    assert (schema_dir / "promotion-approval.schema.json").exists()
    assert (schema_dir / "promotion.schema.json").exists()
    assert (schema_dir / "rehearsal-plan.schema.json").exists()
    assert (schema_dir / "rehearsal-run.schema.json").exists()
    assert (schema_dir / "rehearsal-certificate.schema.json").exists()
    assert (schema_dir / "fixture-plan.schema.json").exists()
    assert (schema_dir / "fixture-build.schema.json").exists()
    assert (schema_dir / "data-profile.schema.json").exists()
    assert (schema_dir / "post-apply-plan.schema.json").exists()
    assert (schema_dir / "post-apply-run.schema.json").exists()
    assert (schema_dir / "post-apply-certificate.schema.json").exists()
    assert (schema_dir / "watch-plan.schema.json").exists()
    assert (schema_dir / "watch-run.schema.json").exists()
    assert (schema_dir / "watch-certificate.schema.json").exists()
    assert (schema_dir / "remediation-plan.schema.json").exists()
    assert (schema_dir / "remediation-run.schema.json").exists()
    assert (schema_dir / "remediation-certificate.schema.json").exists()
    assert (schema_dir / "backup-plan.schema.json").exists()
    assert (schema_dir / "backup-run.schema.json").exists()
    assert (schema_dir / "restore-plan.schema.json").exists()
    assert (schema_dir / "restore-run.schema.json").exists()
    assert (schema_dir / "backup-certificate.schema.json").exists()
    assert (schema_dir / "recovery-point.schema.json").exists()
    assert (schema_dir / "recovery-catalog.schema.json").exists()
    assert (schema_dir / "recovery-chain-verification.schema.json").exists()
    assert (schema_dir / "recovery-restore-plan.schema.json").exists()
    assert (schema_dir / "recovery-restore-run.schema.json").exists()
    assert (schema_dir / "recovery-restore-certificate.schema.json").exists()
    assert (schema_dir / "recovery-retention-plan.schema.json").exists()
    assert (schema_dir / "bundle.schema.json").exists()
    assert (schema_dir / "bundle-verification.schema.json").exists()
    assert (schema_dir / "bundle-policy.schema.json").exists()
    assert (schema_dir / "bundle-gate.schema.json").exists()
    assert (schema_dir / "bundle-diff.schema.json").exists()
    assert (schema_dir / "schema-migration-trust-policy.schema.json").exists()
    assert (schema_dir / "schema-migration-provenance.schema.json").exists()
    assert (schema_dir / "schema-migration-trust-verification.schema.json").exists()
    assert (schema_dir / "evidence-registry-record.schema.json").exists()
    assert (schema_dir / "evidence-registry.schema.json").exists()
    assert (schema_dir / "evidence-registry-query.schema.json").exists()
    assert (schema_dir / "evidence-audit-report.schema.json").exists()
    assert (schema_dir / "review.schema.json").exists()
    for ci_template in (
        Path("docs/examples/ci/schema-migration/github-actions.yml"),
        Path("docs/examples/ci/schema-migration/gitlab-ci.yml"),
        Path("docs/examples/ci/schema-migration/bitbucket-pipelines.yml"),
    ):
        assert ci_template.exists()
        assert "dpone schema migration bundle gate" in ci_template.read_text(encoding="utf-8")
        assert "dpone schema migration bundle diff" in ci_template.read_text(encoding="utf-8")
        assert "dpone schema migration bundle attest" in ci_template.read_text(encoding="utf-8")
        assert "dpone schema migration bundle trust verify" in ci_template.read_text(encoding="utf-8")
        assert "dpone schema migration rehearse plan" in ci_template.read_text(encoding="utf-8")
        assert "dpone schema migration rehearse run" in ci_template.read_text(encoding="utf-8")
        assert "dpone schema migration rehearse certify" in ci_template.read_text(encoding="utf-8")
        assert "dpone schema migration rehearse fixture plan" in ci_template.read_text(encoding="utf-8")
        assert "dpone schema migration rehearse fixture build" in ci_template.read_text(encoding="utf-8")
        assert "dpone schema migration rehearse fixture profile" in ci_template.read_text(encoding="utf-8")
        assert "dpone schema migration post-apply plan" in ci_template.read_text(encoding="utf-8")
        assert "dpone schema migration post-apply run" in ci_template.read_text(encoding="utf-8")
        assert "dpone schema migration post-apply certify" in ci_template.read_text(encoding="utf-8")
        assert "dpone schema migration watch plan" in ci_template.read_text(encoding="utf-8")
        assert "dpone schema migration watch run" in ci_template.read_text(encoding="utf-8")
        assert "dpone schema migration watch certify" in ci_template.read_text(encoding="utf-8")
        assert "dpone schema migration remediation plan" in ci_template.read_text(encoding="utf-8")
        assert "dpone schema migration remediation apply" in ci_template.read_text(encoding="utf-8")
        assert "dpone schema migration remediation certify" in ci_template.read_text(encoding="utf-8")
        assert "dpone schema migration backup plan" in ci_template.read_text(encoding="utf-8")
        assert "dpone schema migration backup create" in ci_template.read_text(encoding="utf-8")
        assert "dpone schema migration backup certify" in ci_template.read_text(encoding="utf-8")
        assert "dpone schema migration recovery point record" in ci_template.read_text(encoding="utf-8")
        assert "dpone schema migration recovery chain verify" in ci_template.read_text(encoding="utf-8")
        assert "dpone schema migration registry record" in ci_template.read_text(encoding="utf-8")
