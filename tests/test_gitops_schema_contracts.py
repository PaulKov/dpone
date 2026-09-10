from __future__ import annotations

import json
from pathlib import Path

import jsonschema
import pytest
import yaml

from dpone.contracts.deployment_cache_retention_state import retention_operation_id
from dpone.gitops.schema_contracts import get_gitops_schema_contract, gitops_schema_contracts
from dpone.gitops.schema_validation import GitOpsSchemaValidator
from dpone.manifest.project_discovery import ProjectDiscoveryService
from dpone.readiness.airflow_self_service_composition import build_airflow_self_service_service
from dpone.services.workload_index_contract import workload_index_from_snapshot

ROOT = Path(__file__).resolve().parents[1]
SCHEMA_DIR = ROOT / "docs" / "schemas" / "gitops"


def _yaml_block_after(document: str, marker: str) -> dict[str, object]:
    marker_offset = document.index(marker)
    start = document.index("```yaml", marker_offset) + len("```yaml")
    end = document.index("```", start)
    payload = yaml.safe_load(document[start:end])
    assert isinstance(payload, dict)
    return payload


def _complete_init_fetch_delivery() -> dict[str, object]:
    return {
        "mode": "init_fetch",
        "artifact_registry_ref": "dpone-prod-artifacts",
        "identity": {"method": "kubernetes_workload_identity", "service_account": "dpone-runtime"},
        "source": {"artifact_registry_ref": "dpone-prod-artifacts"},
        "verify": {"checksums": "required", "attestations": "required_for_prod"},
    }


def test_runtime_pod_apply_schema_rejects_false_success() -> None:
    contract = get_gitops_schema_contract("dpone.airflow-runtime-pod-retention-apply.v1")
    digest = "sha256:" + "a" * 64
    payload = {
        "schema": contract.kind,
        "status": "ok",
        "operation_id": digest,
        "evidence_status": "complete",
        "evidence_durability": "process_ordered",
        "namespace": "airflow",
        "actor": "ci://retention",
        "actor_source": "operator_acknowledgement",
        "authorization_authority": "kubernetes_api_rbac",
        "credential_mode": "in-cluster",
        "deletion_evidence": "api_request_accepted_not_observed",
        "observed_at": "2026-08-03T00:00:00Z",
        "minimum_age_seconds": 300,
        "max_delete_count": 10,
        "age_basis": "creation_timestamp_fallback",
        "terminal_age_exact": False,
        "items": [
            {
                "sequence": 1,
                "pod_ref": digest,
                "precondition_ref": digest,
                "pod_name": "failed-pod",
                "phase": "Failed",
                "action": "failed",
                "reason": "delete_failed",
                "error_code": "DPONE_AIRFLOW_RUNTIME_POD_RETENTION_DELETE_FAILED",
            }
        ],
        "delete_accepted_pod_names": [],
        "skipped_pod_names": [],
        "failed_pod_names": ["failed-pod"],
    }

    with pytest.raises(jsonschema.ValidationError):
        jsonschema.Draft202012Validator(contract.schema).validate(payload)


def test_runtime_pod_apply_schema_rejects_batch_limit_as_success() -> None:
    digest = "sha256:" + "a" * 64
    payload = {
        "schema": "dpone.airflow-runtime-pod-retention-apply.v1",
        "status": "ok",
        "operation_id": digest,
        "evidence_status": "complete",
        "evidence_durability": "process_ordered",
        "namespace": "airflow",
        "actor": "ci://retention",
        "actor_source": "operator_acknowledgement",
        "authorization_authority": "kubernetes_api_rbac",
        "credential_mode": "in-cluster",
        "deletion_evidence": "api_request_accepted_not_observed",
        "observed_at": "2026-08-03T00:00:00Z",
        "minimum_age_seconds": 300,
        "max_delete_count": 10,
        "age_basis": "creation_timestamp_fallback",
        "terminal_age_exact": False,
        "items": [
            {
                "sequence": 1,
                "pod_ref": digest,
                "precondition_ref": digest,
                "pod_name": "deferred-pod",
                "phase": "Failed",
                "action": "skipped",
                "reason": "batch_limit",
            }
        ],
        "delete_accepted_pod_names": [],
        "skipped_pod_names": ["deferred-pod"],
        "failed_pod_names": [],
    }

    assert GitOpsSchemaValidator().validate(
        payload,
        expected_kind="dpone.airflow-runtime-pod-retention-apply.v1",
    )


def test_retention_recovery_schema_rejects_recovered_state_with_pending_work() -> None:
    contract = get_gitops_schema_contract("dpone.deployment-cache-retention-recovery.v1")
    digest = "sha256:" + "b" * 64
    payload = {
        "schema": contract.kind,
        "revision": digest,
        "status": "recovered",
        "restored_deployment_ids": [],
        "pending_deployment_ids": [digest],
        "quarantined_paths": [],
        "transactions": {},
    }

    with pytest.raises(jsonschema.ValidationError):
        jsonschema.Draft202012Validator(contract.schema).validate(payload)


def test_retention_apply_schema_rejects_deleting_protected_deployment() -> None:
    contract = get_gitops_schema_contract("dpone.deployment-cache-retention-apply.v3")
    digest = "sha256:" + "c" * 64
    payload = {
        "schema": contract.kind,
        "environment": "dev",
        "promoted_by": "ci://retention",
        "current_deployment_id": digest,
        "items": [
            {
                "deployment_id": digest,
                "action": "deleted",
                "reason": "current",
                "path": "/cache/current",
            }
        ],
        "deleted_deployment_ids": [digest],
        "skipped_deployment_ids": [],
        "reviewed_plan_sha256": digest,
        "activation_history_revision": digest,
        "operation_id": digest,
        "review_id": "00000000-0000-4000-8000-000000000001",
        "receipt_revision": digest,
        "transaction_status": "committed",
    }

    with pytest.raises(jsonschema.ValidationError):
        jsonschema.Draft202012Validator(contract.schema).validate(payload)


def test_retention_apply_semantics_reject_summary_drift_from_items() -> None:
    digest = "sha256:" + "d" * 64
    review_id = "00000000-0000-4000-8000-000000000001"
    payload = {
        "schema": "dpone.deployment-cache-retention-apply.v3",
        "environment": "dev",
        "promoted_by": "ci://retention",
        "current_deployment_id": None,
        "items": [
            {
                "deployment_id": digest,
                "action": "deleted",
                "reason": "unreferenced",
                "path": "/cache/generations/stale",
            }
        ],
        "deleted_deployment_ids": [],
        "skipped_deployment_ids": [],
        "reviewed_plan_sha256": digest,
        "activation_history_revision": digest,
        "operation_id": retention_operation_id(
            environment="dev",
            reviewed_plan_sha256=digest,
            review_id=review_id,
        ),
        "review_id": review_id,
        "receipt_revision": digest,
        "transaction_status": "committed",
    }

    issues = GitOpsSchemaValidator().validate(
        payload,
        expected_kind="dpone.deployment-cache-retention-apply.v3",
    )

    assert [issue.code for issue in issues] == ["schema_derived_projection_mismatch"]
    assert issues[0].path == "deleted_deployment_ids"


def test_retention_plan_semantics_reject_projection_and_protection_drift() -> None:
    current = "sha256:" + "a" * 64
    protected = "sha256:" + "b" * 64
    payload = {
        "schema": "dpone.deployment-cache-retention-plan.v1",
        "environment": "dev",
        "current_deployment_id": current,
        "protected_deployment_ids": [protected],
        "items": [
            {"deployment_id": current, "action": "delete", "reason": "unreferenced", "path": "/current"},
            {"deployment_id": protected, "action": "delete", "reason": "unreferenced", "path": "/protected"},
        ],
        "delete_candidates": [],
    }

    issues = GitOpsSchemaValidator().validate(payload, expected_kind=payload["schema"])

    assert {issue.code for issue in issues} == {
        "schema_derived_projection_mismatch",
        "schema_state_semantics_invalid",
    }
    assert {issue.path for issue in issues} == {"delete_candidates"}


def test_retention_apply_v1_semantics_reject_projection_and_current_deletion() -> None:
    current = "sha256:" + "a" * 64
    payload = {
        "schema": "dpone.deployment-cache-retention-apply.v1",
        "environment": "dev",
        "promoted_by": "ci://retention",
        "current_deployment_id": current,
        "items": [
            {"deployment_id": current, "action": "deleted", "reason": "current", "path": "/current"},
        ],
        "deleted_deployment_ids": [],
        "skipped_deployment_ids": [],
    }

    issues = GitOpsSchemaValidator().validate(payload, expected_kind=payload["schema"])

    assert {issue.code for issue in issues} == {
        "schema_derived_projection_mismatch",
        "schema_state_semantics_invalid",
    }
    assert {issue.path for issue in issues} == {"deleted_deployment_ids"}


def test_retention_apply_v3_semantics_reject_wrong_operation_identity() -> None:
    deployment_id = "sha256:" + "a" * 64
    reviewed_plan = "sha256:" + "b" * 64
    review_id = "00000000-0000-4000-8000-000000000001"
    payload = {
        "schema": "dpone.deployment-cache-retention-apply.v3",
        "environment": "dev",
        "promoted_by": "ci://retention",
        "current_deployment_id": None,
        "items": [
            {"deployment_id": deployment_id, "action": "deleted", "reason": "unreferenced", "path": "/stale"},
        ],
        "deleted_deployment_ids": [deployment_id],
        "skipped_deployment_ids": [],
        "reviewed_plan_sha256": reviewed_plan,
        "activation_history_revision": "sha256:" + "c" * 64,
        "operation_id": "sha256:" + "d" * 64,
        "review_id": review_id,
        "receipt_revision": "sha256:" + "e" * 64,
        "transaction_status": "committed",
    }

    issues = GitOpsSchemaValidator().validate(payload, expected_kind=payload["schema"])

    assert [issue.code for issue in issues] == ["schema_state_semantics_invalid"]
    assert issues[0].path == "operation_id"


def test_retention_apply_semantics_rejects_deleted_and_skipped_overlap() -> None:
    deployment_id = "sha256:" + "a" * 64
    payload = {
        "schema": "dpone.deployment-cache-retention-apply.v1",
        "environment": "dev",
        "promoted_by": "ci://retention",
        "current_deployment_id": None,
        "items": [
            {"deployment_id": deployment_id, "action": "deleted", "reason": "unreferenced", "path": "/old"},
            {
                "deployment_id": deployment_id,
                "action": "skipped",
                "reason": "retention_evidence",
                "path": "/old",
            },
        ],
        "deleted_deployment_ids": [deployment_id],
        "skipped_deployment_ids": [deployment_id],
    }

    issues = GitOpsSchemaValidator().validate(payload, expected_kind=payload["schema"])

    assert [issue.code for issue in issues] == [
        "schema_state_semantics_invalid",
        "schema_state_semantics_invalid",
    ]
    assert {issue.path for issue in issues} == {"items"}


def test_retention_plan_semantics_rejects_duplicate_deployment_identity() -> None:
    deployment_id = "sha256:" + "a" * 64
    item = {"deployment_id": deployment_id, "action": "delete", "reason": "unreferenced", "path": "/old"}
    payload = {
        "schema": "dpone.deployment-cache-retention-plan.v1",
        "environment": "dev",
        "current_deployment_id": None,
        "protected_deployment_ids": [],
        "items": [item, item],
        "delete_candidates": [deployment_id, deployment_id],
    }

    issues = GitOpsSchemaValidator().validate(payload, expected_kind=payload["schema"])

    assert {issue.code for issue in issues} == {"schema_state_semantics_invalid"}
    assert {issue.path for issue in issues} == {"items", "delete_candidates"}


@pytest.mark.parametrize(
    "kind",
    (
        "dpone.deployment-cache-retention-apply.v1",
        "dpone.deployment-cache-retention-apply.v2",
        "dpone.deployment-cache-retention-apply.v3",
    ),
)
def test_retention_apply_semantics_rejects_duplicate_authoritative_items(kind: str) -> None:
    deployment_id = "sha256:" + "a" * 64
    item = {"deployment_id": deployment_id, "action": "deleted", "reason": "unreferenced", "path": "/old"}
    payload: dict[str, object] = {
        "schema": kind,
        "environment": "dev",
        "promoted_by": "ci://retention",
        "current_deployment_id": None,
        "items": [item, item],
        "deleted_deployment_ids": [deployment_id],
        "skipped_deployment_ids": [],
    }
    if kind != "dpone.deployment-cache-retention-apply.v1":
        payload["reviewed_plan_sha256"] = "sha256:" + "b" * 64
        payload["activation_history_revision"] = "sha256:" + "c" * 64
    if kind == "dpone.deployment-cache-retention-apply.v3":
        review_id = "00000000-0000-4000-8000-000000000001"
        payload["operation_id"] = retention_operation_id(
            environment="dev",
            reviewed_plan_sha256=str(payload["reviewed_plan_sha256"]),
            review_id=review_id,
        )
        payload["review_id"] = review_id
        payload["receipt_revision"] = "sha256:" + "d" * 64
        payload["transaction_status"] = "committed"

    issues = GitOpsSchemaValidator().validate(payload, expected_kind=kind)

    assert "schema_state_semantics_invalid" in {issue.code for issue in issues}
    assert "items" in {issue.path for issue in issues}


def test_retention_apply_v3_semantics_converts_identity_bounds_to_issue() -> None:
    deployment_id = "sha256:" + "a" * 64
    review_id = "00000000-0000-4000-8000-000000000001"
    payload = {
        "schema": "dpone.deployment-cache-retention-apply.v3",
        "environment": "x" * 129,
        "promoted_by": "ci://retention",
        "current_deployment_id": None,
        "items": [{"deployment_id": deployment_id, "action": "deleted", "reason": "unreferenced", "path": "/old"}],
        "deleted_deployment_ids": [deployment_id],
        "skipped_deployment_ids": [],
        "reviewed_plan_sha256": "sha256:" + "b" * 64,
        "activation_history_revision": "sha256:" + "c" * 64,
        "operation_id": "sha256:" + "d" * 64,
        "review_id": review_id,
        "receipt_revision": "sha256:" + "e" * 64,
        "transaction_status": "committed",
    }

    issues = GitOpsSchemaValidator().validate(payload, expected_kind=payload["schema"])

    assert [issue.code for issue in issues] == ["schema_state_semantics_invalid"]
    assert issues[0].path == "operation_id"


def test_gitops_schema_contract_catalog_is_public_and_documented() -> None:
    contracts = {contract.name: contract for contract in gitops_schema_contracts()}

    assert set(contracts) == {
        "affected",
        "affected-workloads",
        "plan",
        "verify",
        "bundle",
        "attestation",
        "workloads",
        "release-set",
        "release-set-v2",
        "release-set-v3",
        "release-composition",
        "deployment-set",
        "deployment-set-v2",
        "deployment-set-v3",
        "airflow-deployment-index-v2",
        "airflow-deployment-index-v3",
        "mssql-asset-outlet-projection",
        "current-pointer",
        "binding-set",
        "connection-registry",
        "connection-registry-migration-plan",
        "credential-runtime",
        "catalog-bundle",
        "catalog-trust-policy",
        "catalog-bundle-verification",
        "extension-check-receipt",
        "extension-conformance-request",
        "extension-conformance",
        "authoring-migration",
        "route-attestation",
        "route-attestation-policy",
        "route-attestation-verification",
        "route-matrix-claim",
        "route-certification-matrix",
        "self-service-usability-study",
        "self-service-certification",
        "deployment-cache-retention-plan",
        "deployment-cache-retention-apply",
        "deployment-cache-retention-apply-v2",
        "deployment-cache-retention-apply-v3",
        "deployment-cache-activation-history-v2",
        "deployment-cache-retention-recovery",
        "deployment-cache-retention-recovery-v2",
        "deployment-cache-recovery-plan",
        "deployment-cache-recovery-apply",
        "airflow-cache-status-publication",
        "airflow-cache-status-publication-failure",
        "safe-sample-cli-argument-validation",
        "safe-sample-source-request",
        "safe-sample-data-copier-registry",
        "safe-sample-certified-copy-request",
        "safe-sample-execution-plan",
        "safe-sample-airflow-deployment-context",
        "safe-sample-runtime-readiness",
        "safe-sample-runtime-handoff",
        "safe-sample-runtime-execution",
        "safe-sample-runtime-evidence-write",
        "safe-sample-runtime-run",
        "selected-safe-sample-report",
        "safe-sample-data-copy",
        "mssql-clickhouse-safe-sample-copy-config",
        "mssql-safe-sample-read-plan",
        "clickhouse-safe-sample-insert-plan",
        "airflow-render",
        "airflow-doctor",
        "airflow-image-contract",
        "airflow-run-spec",
        "airflow-runtime-evidence",
        "airflow-runtime-profile",
        "airflow-xcom-summary",
        "airflow-correlation",
        "airflow-deployment-identity",
        "airflow-mapping-plan",
        "airflow-mapping-item",
        "airflow-run-identity",
        "airflow-rerun-plan",
        "airflow-runtime-init-fetch-plan",
        "airflow-runtime-init-fetch-plan-v2",
        "airflow-runtime-init-fetch-plan-v3",
        "airflow-runtime-pod-retention-plan",
        "airflow-runtime-pod-retention-apply",
        "airflow-runtime-pod-retention-event",
        "airflow-runtime-pod-retention-render",
        "runtime-fetch-ready",
        "runtime-fetch-ready-v2",
        "runtime-artifact-trust-policy-v2",
        "runtime-artifact-attestation-verification",
        "airflow-cluster-doctor",
        "airflow-k8s-manifests",
        "airflow-admission-check",
        "airflow-pack",
        "airflow-reconcile",
        "airflow-outcome-gate",
        "airflow-pod-contract",
        "airflow-pod-doctor",
        "airflow-k8s-smoke",
        "airflow-pod-launch-evidence",
        "airflow-evidence-bundle",
        "airflow-connection-bridge-plan",
        "airflow-connection-secret-gc-plan",
        "airflow-connection-secret-gc-apply",
        "airflow-artifact-publish",
        "airflow-artifact-publish-v2",
        "airflow-artifact-attestation",
        "airflow-artifact-attestation-verification",
        "airflow-artifact-attestation-prepare",
        "airflow-artifact-attestation-marker",
        "airflow-artifact-attestation-publish",
        "airflow-artifact-attestation-error",
        "airflow-deployment-trust-policy",
        "airflow-deployment-trust-policy-render",
        "airflow-cache-materialize",
        "airflow-loader-ack",
        "airflow-loader-ack-v2",
        "airflow-desired-state-authority",
        "airflow-desired-state-authority-v2",
        "airflow-desired-deployment",
        "airflow-desired-state-publish-intent",
        "airflow-desired-state-publish-preparation",
        "airflow-desired-state-publish",
        "airflow-desired-state-publish-v2",
        "airflow-desired-state-fetch",
        "airflow-desired-state-reconcile",
        "airflow-desired-state-checkpoint",
        "airflow-desired-state-recovery",
        "airflow-artifact-index",
        "airflow-preflight",
        "airflow-deployment-index",
        "airflow-explain",
        "airflow-authoring-migration-plan",
        "airflow-authoring-fix",
        "airflow-operator-diagnostics",
        "connection-check",
        "live-preflight",
        "selection-state",
        "selection-report",
        "test-manifest",
        "test-report",
        "test-suite-report",
        "error",
        "project",
        "domain-dag",
        "domain-ownership",
        "workload-index",
        "workload-change-impact",
        "workload-index-promotion",
    }
    for name, contract in contracts.items():
        schema = contract.schema
        assert schema["$schema"] == "https://json-schema.org/draft/2020-12/schema"
        assert schema["$id"].endswith(f"/{name}.schema.json")
        assert schema["title"].startswith("dpone GitOps")
        assert schema["type"] == "object"
        assert "required" in schema
        assert json.loads((SCHEMA_DIR / f"{name}.schema.json").read_text(encoding="utf-8")) == schema


def test_gitops_schema_contract_lookup_rejects_unknown_kind() -> None:
    assert get_gitops_schema_contract("bundle").kind == "gitops.bundle"
    assert get_gitops_schema_contract("workloads").kind == "gitops.workloads"
    assert get_gitops_schema_contract("gitops.affected_workloads").name == "affected-workloads"
    assert get_gitops_schema_contract("gitops.bundle").name == "bundle"
    assert get_gitops_schema_contract("airflow-doctor").kind == "gitops.airflow_doctor"
    assert get_gitops_schema_contract("gitops.airflow_image_contract").name == "airflow-image-contract"
    assert get_gitops_schema_contract("airflow-run-spec").kind == "gitops.airflow_run_spec"
    assert get_gitops_schema_contract("gitops.airflow_runtime_evidence").name == "airflow-runtime-evidence"
    assert get_gitops_schema_contract("gitops.airflow_runtime_profile").name == "airflow-runtime-profile"
    assert get_gitops_schema_contract("gitops.airflow_xcom_summary").name == "airflow-xcom-summary"
    assert get_gitops_schema_contract("dpone.airflow-run-identity.v1").name == "airflow-run-identity"
    assert get_gitops_schema_contract("dpone.airflow-correlation.v1").name == "airflow-correlation"
    assert get_gitops_schema_contract("dpone.airflow-rerun-plan.v1").name == "airflow-rerun-plan"
    assert get_gitops_schema_contract("dpone.airflow-runtime-init-fetch-plan.v1").name == (
        "airflow-runtime-init-fetch-plan"
    )
    assert get_gitops_schema_contract("dpone.airflow-runtime-init-fetch-plan.v2").name == (
        "airflow-runtime-init-fetch-plan-v2"
    )
    assert get_gitops_schema_contract("dpone.airflow-runtime-init-fetch-plan.v3").name == (
        "airflow-runtime-init-fetch-plan-v3"
    )
    assert get_gitops_schema_contract("dpone.runtime-fetch-ready.v1").name == "runtime-fetch-ready"
    assert get_gitops_schema_contract("dpone.runtime-artifact-trust-policy.v2").name == (
        "runtime-artifact-trust-policy-v2"
    )
    assert get_gitops_schema_contract("dpone.runtime-artifact-attestation-verification.v1").name == (
        "runtime-artifact-attestation-verification"
    )
    assert get_gitops_schema_contract("gitops.airflow_cluster_doctor").name == "airflow-cluster-doctor"
    assert get_gitops_schema_contract("gitops.airflow_k8s_manifests").name == "airflow-k8s-manifests"
    assert get_gitops_schema_contract("gitops.airflow_admission_check").name == "airflow-admission-check"
    assert get_gitops_schema_contract("gitops.airflow_pack").name == "airflow-pack"
    assert get_gitops_schema_contract("gitops.airflow_reconcile").name == "airflow-reconcile"
    assert get_gitops_schema_contract("airflow-outcome-gate").kind == "gitops.airflow_outcome_gate"
    assert get_gitops_schema_contract("airflow-pod-contract").kind == "gitops.airflow_pod_contract"
    assert get_gitops_schema_contract("gitops.airflow_pod_doctor").name == "airflow-pod-doctor"
    assert get_gitops_schema_contract("gitops.airflow_k8s_smoke").name == "airflow-k8s-smoke"
    assert get_gitops_schema_contract("gitops.airflow_pod_launch_evidence").name == "airflow-pod-launch-evidence"
    assert get_gitops_schema_contract("gitops.airflow_evidence_bundle").name == "airflow-evidence-bundle"
    assert get_gitops_schema_contract("gitops.airflow_connection_bridge_plan").name == (
        "airflow-connection-bridge-plan"
    )
    assert get_gitops_schema_contract("dpone.airflow-connection-secret-gc-plan.v1").name == (
        "airflow-connection-secret-gc-plan"
    )
    assert get_gitops_schema_contract("dpone.airflow-connection-secret-gc-apply.v1").name == (
        "airflow-connection-secret-gc-apply"
    )
    assert get_gitops_schema_contract("gitops.airflow_artifact_index").name == "airflow-artifact-index"
    assert get_gitops_schema_contract("dpone.airflow_loader_ack.v1").name == "airflow-loader-ack"
    assert get_gitops_schema_contract("dpone.airflow_loader_ack.v2").name == ("airflow-loader-ack-v2")
    assert get_gitops_schema_contract("dpone.airflow-desired-deployment.v1").name == ("airflow-desired-deployment")
    assert get_gitops_schema_contract("dpone.airflow-desired-state-publish.v1").name == (
        "airflow-desired-state-publish"
    )
    assert get_gitops_schema_contract("dpone.airflow-desired-state-publish-preparation.v1").name == (
        "airflow-desired-state-publish-preparation"
    )
    assert get_gitops_schema_contract("dpone.airflow-desired-state-publish.v2").name == (
        "airflow-desired-state-publish-v2"
    )
    assert get_gitops_schema_contract("dpone.airflow-desired-state-fetch.v1").name == ("airflow-desired-state-fetch")
    assert get_gitops_schema_contract("dpone.airflow-desired-state-reconcile.v1").name == (
        "airflow-desired-state-reconcile"
    )
    assert get_gitops_schema_contract("dpone.airflow-desired-state-checkpoint.v1").name == (
        "airflow-desired-state-checkpoint"
    )
    assert get_gitops_schema_contract("dpone.airflow-desired-state-recovery.v1").name == (
        "airflow-desired-state-recovery"
    )
    assert get_gitops_schema_contract("gitops.airflow_preflight").name == "airflow-preflight"
    assert get_gitops_schema_contract("dpone.airflow-deployment-index.v1").name == "airflow-deployment-index"
    assert get_gitops_schema_contract("dpone.airflow-explain.v1").name == "airflow-explain"
    assert get_gitops_schema_contract("dpone.airflow-authoring-migration-plan.v1").name == (
        "airflow-authoring-migration-plan"
    )
    assert get_gitops_schema_contract("dpone.airflow-authoring-fix.v1").name == "airflow-authoring-fix"
    assert get_gitops_schema_contract("dpone.authoring-migration.v1").name == "authoring-migration"
    assert get_gitops_schema_contract("dpone.airflow-operator-diagnostics.v1").name == "airflow-operator-diagnostics"
    assert get_gitops_schema_contract("dpone.connection-check.v1").name == "connection-check"
    assert get_gitops_schema_contract("dpone.live-preflight.v1").name == "live-preflight"
    assert get_gitops_schema_contract("dpone.error.v1").name == "error"
    assert get_gitops_schema_contract("dpone.test.v1").name == "test-manifest"
    assert get_gitops_schema_contract("dpone.test-report.v1").name == "test-report"
    assert get_gitops_schema_contract("dpone.test-suite-report.v1").name == "test-suite-report"
    assert get_gitops_schema_contract("dpone.release-set.v1").name == "release-set"
    assert get_gitops_schema_contract("dpone.release-set.v2").name == "release-set-v2"
    assert get_gitops_schema_contract("dpone.deployment-set.v1").name == "deployment-set"
    assert get_gitops_schema_contract("dpone.deployment-set.v2").name == "deployment-set-v2"
    assert get_gitops_schema_contract("dpone.airflow-deployment-index.v2").name == ("airflow-deployment-index-v2")
    assert get_gitops_schema_contract("dpone.deployment-set.v3").name == "deployment-set-v3"
    assert get_gitops_schema_contract("dpone.airflow-deployment-index.v3").name == ("airflow-deployment-index-v3")
    assert get_gitops_schema_contract("dpone.current-pointer.v1").name == "current-pointer"
    assert get_gitops_schema_contract("dpone.binding-set.v1").name == "binding-set"
    assert get_gitops_schema_contract("dpone.connection-registry.v1").name == "connection-registry"
    assert get_gitops_schema_contract("dpone.connection-registry-migration-plan.v1").name == (
        "connection-registry-migration-plan"
    )
    assert get_gitops_schema_contract("dpone.credential-runtime.v1").name == "credential-runtime"
    assert get_gitops_schema_contract("dpone.route-attestation.v1").name == "route-attestation"
    assert get_gitops_schema_contract("dpone.route-attestation-policy.v1").name == "route-attestation-policy"
    assert get_gitops_schema_contract("dpone.route-attestation-verification.v1").name == (
        "route-attestation-verification"
    )
    assert get_gitops_schema_contract("dpone.route-certification-matrix.v1").name == ("route-certification-matrix")
    assert get_gitops_schema_contract("dpone.self-service-usability-study.v1").name == ("self-service-usability-study")
    assert get_gitops_schema_contract("dpone.self-service-certification.v1").name == ("self-service-certification")
    assert get_gitops_schema_contract("dpone.deployment-cache-retention-plan.v1").name == (
        "deployment-cache-retention-plan"
    )
    assert get_gitops_schema_contract("dpone.deployment-cache-retention-apply.v1").name == (
        "deployment-cache-retention-apply"
    )
    assert get_gitops_schema_contract("dpone.deployment-cache-retention-apply.v2").name == (
        "deployment-cache-retention-apply-v2"
    )
    assert get_gitops_schema_contract("dpone.deployment-cache-activation-history.v2").name == (
        "deployment-cache-activation-history-v2"
    )
    assert get_gitops_schema_contract("dpone.deployment-cache-retention-recovery.v1").name == (
        "deployment-cache-retention-recovery"
    )
    assert get_gitops_schema_contract("dpone.deployment-cache-retention-recovery.v2").name == (
        "deployment-cache-retention-recovery-v2"
    )
    assert get_gitops_schema_contract("dpone.deployment-cache-recovery-plan.v1").name == (
        "deployment-cache-recovery-plan"
    )
    assert get_gitops_schema_contract("dpone.deployment-cache-recovery-apply.v1").name == (
        "deployment-cache-recovery-apply"
    )
    assert get_gitops_schema_contract("dpone.safe-sample-cli-argument-validation.v1").name == (
        "safe-sample-cli-argument-validation"
    )
    assert get_gitops_schema_contract("dpone.safe-sample-source-request.v1").name == "safe-sample-source-request"
    assert get_gitops_schema_contract("dpone.safe-sample-data-copier-registry.v1").name == (
        "safe-sample-data-copier-registry"
    )
    assert get_gitops_schema_contract("dpone.safe-sample-certified-copy-request.v1").name == (
        "safe-sample-certified-copy-request"
    )
    assert get_gitops_schema_contract("dpone.safe-sample-execution-plan.v1").name == "safe-sample-execution-plan"
    assert get_gitops_schema_contract("dpone.safe-sample-airflow-deployment-context.v1").name == (
        "safe-sample-airflow-deployment-context"
    )
    assert get_gitops_schema_contract("dpone.safe-sample-runtime-readiness.v1").name == (
        "safe-sample-runtime-readiness"
    )
    assert get_gitops_schema_contract("dpone.safe-sample-runtime-handoff.v1").name == ("safe-sample-runtime-handoff")
    assert get_gitops_schema_contract("dpone.safe-sample-runtime-execution.v1").name == (
        "safe-sample-runtime-execution"
    )
    assert get_gitops_schema_contract("dpone.safe-sample-runtime-evidence-write.v1").name == (
        "safe-sample-runtime-evidence-write"
    )
    assert get_gitops_schema_contract("dpone.safe-sample-runtime-run.v1").name == "safe-sample-runtime-run"
    assert get_gitops_schema_contract("dpone.selected-safe-sample-report.v1").name == ("selected-safe-sample-report")
    assert get_gitops_schema_contract("dpone.safe-sample-data-copy.v1").name == "safe-sample-data-copy"
    assert get_gitops_schema_contract("dpone.mssql-clickhouse-safe-sample-copy-config.v1").name == (
        "mssql-clickhouse-safe-sample-copy-config"
    )
    assert get_gitops_schema_contract("dpone.mssql-safe-sample-read-plan.v1").name == ("mssql-safe-sample-read-plan")
    assert get_gitops_schema_contract("dpone.clickhouse-safe-sample-insert-plan.v1").name == (
        "clickhouse-safe-sample-insert-plan"
    )
    assert get_gitops_schema_contract("unknown") is None


@pytest.mark.parametrize(
    "kind",
    [
        "dpone.deployment-set.v2",
        "dpone.airflow-deployment-index.v2",
    ],
)
def test_v2_reader_contract_preserves_nullable_airflow_bundle_ref(
    kind: str,
) -> None:
    schema = get_gitops_schema_contract(kind).schema

    assert schema["properties"]["airflow_bundle_ref"] == {
        "type": ["string", "null"],
    }


def test_airflow_desired_state_schemas_enforce_correlated_states() -> None:
    validator = GitOpsSchemaValidator()
    digest = "sha256:" + "a" * 64
    desired = {
        "schema": "dpone.airflow-desired-deployment.v1",
        "environment": "dev",
        "source": {
            "project": "platform/example-workloads",
            "ref": "master",
            "pipeline_id": "123",
            "job_id": "456",
            "occurrence_id": "123e4567-e89b-42d3-a456-426614174000",
            "git_sha": "1" * 40,
        },
        "promotion": {
            "registry_scope_id": digest,
            "release_id": digest,
            "deployment_id": digest,
            "airflow_index_sha256": digest,
            "runtime_image_digest": digest,
            "expected_dag_ids": ["DAG__platform__sale_plan_type1__smoke"],
            "publication_evidence_sha256": digest,
        },
        "previous": {"revision": None, "deployment_id": None},
        "promoted_at": "2026-07-28T12:00:00Z",
    }
    assert validator.validate(desired, expected_kind=str(desired["schema"])) == ()

    preparation = {
        "schema": "dpone.airflow-desired-state-publish-preparation.v1",
        "candidate": {
            "environment": "dev",
            "authority_sha256": digest,
            "project": "platform/example-workloads",
            "source_ref": "master",
            "pipeline_id": "123",
            "job_id": "456",
            "git_sha": "1" * 40,
            "registry_scope_id": digest,
            "release_id": digest,
            "deployment_id": digest,
            "airflow_index_sha256": digest,
            "runtime_image_digest": digest,
            "expected_dag_ids": ["DAG__platform__sale_plan_type1__smoke"],
            "publication_evidence_sha256": digest,
            "expected_revision": None,
        },
        "intent": {
            "schema": "dpone.airflow-desired-state-publish-intent.v1",
            "candidate_sha256": digest,
            "occurrence_id": "123e4567-e89b-42d3-a456-426614174000",
            "promoted_at": "2026-07-28T12:00:00Z",
        },
    }
    assert validator.validate(preparation, expected_kind=str(preparation["schema"])) == ()

    publish = {
        "schema": "dpone.airflow-desired-state-publish.v2",
        "passed": True,
        "status": "published",
        "outcome": "created",
        "environment": "dev",
        "occurrence_id": "123e4567-e89b-42d3-a456-426614174000",
        "preparation_job_id": "456",
        "publisher_job_id": "789",
        "release_id": digest,
        "deployment_id": digest,
        "desired_state_sha256": digest,
        "previous_revision": None,
        "committed_revision": '"etag"',
        "published_at": "2026-07-28T12:00:00Z",
        "state_may_have_changed": False,
    }
    assert validator.validate(publish, expected_kind=str(publish["schema"])) == ()

    desired["previous"] = {"revision": '"etag"', "deployment_id": None}
    assert [issue.code for issue in validator.validate(desired, expected_kind=str(desired["schema"]))] == [
        "schema_one_of_mismatch"
    ]

    unchanged_fetch = {
        "schema": "dpone.airflow-desired-state-fetch.v1",
        "passed": True,
        "status": "unchanged",
        "environment": "dev",
        "observed_revision": '"etag"',
        "desired_state_sha256": None,
        "deployment_id": None,
        "output_path": "/var/lib/dpone/desired-state.json",
    }
    assert (
        validator.validate(
            unchanged_fetch,
            expected_kind=str(unchanged_fetch["schema"]),
        )
        == ()
    )
    unchanged_fetch["deployment_id"] = digest
    assert [
        issue.code
        for issue in validator.validate(
            unchanged_fetch,
            expected_kind=str(unchanged_fetch["schema"]),
        )
    ] == ["schema_one_of_mismatch"]

    reconcile = {
        "schema": "dpone.airflow-desired-state-reconcile.v1",
        "passed": True,
        "status": "activated",
        "environment": "dev",
        "observed_revision": '"etag"',
        "desired_state_sha256": digest,
        "registry_scope_id": digest,
        "source_project": "platform/example-workloads",
        "source_ref": "master",
        "release_id": digest,
        "deployment_id": digest,
        "occurrence_id": "123e4567-e89b-42d3-a456-426614174000",
        "source_git_sha": "1" * 40,
        "airflow_index_sha256": digest,
        "runtime_image_digest": digest,
        "expected_dag_ids": ["DAG__platform__sale_plan_type1__smoke"],
        "activation_id": "123e4567-e89b-42d3-a456-426614174001",
        "previous_deployment_id": None,
        "materialized": True,
        "activated": True,
    }
    assert validator.validate(reconcile, expected_kind=str(reconcile["schema"])) == ()
    reconcile["activated"] = False
    assert [
        issue.code
        for issue in validator.validate(
            reconcile,
            expected_kind=str(reconcile["schema"]),
        )
    ] == ["schema_one_of_mismatch"]

    recovery = {
        "schema": "dpone.airflow-desired-state-recovery.v1",
        "observed_revision": '"etag"',
        "desired_state": {
            "schema": "dpone.airflow-desired-deployment.v1",
            "environment": "dev",
            "source": {
                "project": "platform/example-workloads",
                "ref": "master",
                "pipeline_id": "1",
                "job_id": "2",
                "occurrence_id": "123e4567-e89b-42d3-a456-426614174000",
                "git_sha": "1" * 40,
            },
            "promotion": {
                "registry_scope_id": digest,
                "release_id": digest,
                "deployment_id": digest,
                "airflow_index_sha256": digest,
                "runtime_image_digest": digest,
                "expected_dag_ids": ["DAG__platform__sale_plan_type1__smoke"],
                "publication_evidence_sha256": digest,
            },
            "previous": {"revision": None, "deployment_id": None},
            "promoted_at": "2026-07-28T10:00:00Z",
        },
    }
    assert validator.validate(recovery, expected_kind=str(recovery["schema"])) == ()


def test_release_set_contract_requires_one_safe_relative_artifact_locator() -> None:
    validator = GitOpsSchemaValidator()

    def release_with(artifact: dict[str, str]) -> dict[str, object]:
        return {
            "schema": "dpone.release-set.v1",
            "release_id": "sha256:" + "a" * 64,
            "artifacts": {
                "dag_specs": [artifact],
                "workload_packs": [],
                "canonical_schemas": [],
            },
        }

    valid = validator.validate(
        release_with(
            {
                "id": "orders_daily",
                "path": "dags/orders_daily.dag-spec.json",
                "sha256": "sha256:" + "b" * 64,
            }
        ),
        expected_kind="dpone.release-set.v1",
    )
    ambiguous = validator.validate(
        release_with(
            {
                "id": "orders_daily",
                "path": "dags/orders_daily.dag-spec.json",
                "artifact_ref": "dags/orders_daily.dag-spec.json",
                "sha256": "sha256:" + "b" * 64,
            }
        ),
        expected_kind="dpone.release-set.v1",
    )
    self_referential = validator.validate(
        release_with(
            {
                "id": "orders_daily",
                "artifact_ref": "cache://releases/sha256-a/dags/orders_daily.dag-spec.json",
                "sha256": "sha256:" + "b" * 64,
            }
        ),
        expected_kind="dpone.release-set.v1",
    )

    assert valid == ()
    assert [(issue.code, issue.path) for issue in ambiguous] == [("schema_one_of_mismatch", "artifacts.dag_specs[0]")]
    assert [(issue.code, issue.path) for issue in self_referential] == [
        ("schema_one_of_mismatch", "artifacts.dag_specs[0]")
    ]


def test_release_set_contract_validates_only_explicit_compact_pack_promotion() -> None:
    validator = GitOpsSchemaValidator()
    release = {
        "schema": "dpone.release-set.v1",
        "release_id": "sha256:" + "a" * 64,
        "artifacts": {
            "dag_specs": [],
            "workload_packs": [],
            "canonical_schemas": [],
        },
        "promotion": {
            "schema": "dpone.compact-pack-release-promotion.v1",
            "profile": "compact_v2_runtime_connection_context",
        },
    }

    assert validator.validate(release, expected_kind="dpone.release-set.v1") == ()

    release["promotion"]["profile"] = "unsafe"
    issues = validator.validate(release, expected_kind="dpone.release-set.v1")

    assert [(issue.code, issue.path) for issue in issues] == [("schema_const_mismatch", "promotion.profile")]

    release["promotion"] = {
        "schema": "acme.release-promotion.v1",
        "profile": "blue_green",
        "vendor_extension": True,
    }
    assert validator.validate(release, expected_kind="dpone.release-set.v1") == ()


@pytest.mark.parametrize(
    "promotion",
    (
        {"schema": "dpone.compact-pack-release-promotion.v1"},
        {"profile": "compact_v2_runtime_connection_context"},
        {
            "schema": "dpone.compact-pack-release-promotion.v1",
            "profile": "compact_v2_runtime_connection_context",
            "extra": True,
        },
        {
            "schema": "other",
            "profile": "compact_v2_runtime_connection_context",
        },
        {
            "schema": "dpone.compact-pack-release-promotion.v999",
            "profile": "unsafe",
        },
    ),
)
def test_release_set_contract_rejects_malformed_explicit_compact_promotion(
    promotion: dict[str, object],
) -> None:
    release = {
        "schema": "dpone.release-set.v1",
        "release_id": "sha256:" + "a" * 64,
        "artifacts": {
            "dag_specs": [],
            "workload_packs": [],
            "canonical_schemas": [],
        },
        "promotion": promotion,
    }

    assert GitOpsSchemaValidator().validate(release, expected_kind="dpone.release-set.v1")


@pytest.mark.parametrize(
    "promotion",
    (
        "legacy-marker",
        None,
        7,
        ["legacy-marker"],
        {
            "schema": "acme.release-promotion.v1",
            "profile": "blue_green",
            "vendor_extension": True,
        },
    ),
)
def test_release_set_validators_preserve_legacy_promotion_extensions(
    promotion: object,
) -> None:
    release = {
        "schema": "dpone.release-set.v1",
        "release_id": "sha256:" + "a" * 64,
        "artifacts": {
            "dag_specs": [],
            "workload_packs": [],
            "canonical_schemas": [],
        },
        "promotion": promotion,
    }

    assert GitOpsSchemaValidator().validate(release, expected_kind="dpone.release-set.v1") == ()
    jsonschema.validate(
        release,
        json.loads((SCHEMA_DIR / "release-set.schema.json").read_text(encoding="utf-8")),
    )


def test_release_set_v1_runtime_payload_compatibility_inventory_has_safe_ids() -> None:
    release = {
        "schema": "dpone.release-set.v1",
        "release_id": "sha256:" + "a" * 64,
        "artifacts": {
            "dag_specs": [],
            "workload_packs": [],
            "canonical_schemas": [],
            "runtime_payloads": [
                {
                    "id": "dbt_selection_daily_marts",
                    "kind": "dbt_selection_lock",
                    "path": "runtime/dbt/daily_marts.selection-lock.json",
                    "sha256": "sha256:" + "b" * 64,
                    "bytes": 1024,
                    "media_type": "application/vnd.dpone.dbt-selection-lock+json",
                }
            ],
        },
    }
    validator = GitOpsSchemaValidator()

    assert validator.validate(release, expected_kind="dpone.release-set.v1") == ()
    jsonschema.validate(
        release,
        json.loads((SCHEMA_DIR / "release-set.schema.json").read_text(encoding="utf-8")),
    )

    release["artifacts"]["runtime_payloads"][0]["id"] = "dbt_selection_team:prod"  # type: ignore[index]
    assert [
        (issue.code, issue.path) for issue in validator.validate(release, expected_kind="dpone.release-set.v1")
    ] == [("schema_pattern_mismatch", "artifacts.runtime_payloads[0].id")]


def test_release_set_v2_requires_bounded_runtime_payloads() -> None:
    validator = GitOpsSchemaValidator()
    payload = {
        "schema": "dpone.release-set.v2",
        "release_id": "sha256:" + "a" * 64,
        "producer": {
            "dpone_version": "0.73.20",
            "wire_contract": "dpone.dbt-airflow-self-service.v1",
        },
        "selection_authority": "dbt_cli",
        "selection_fingerprint": "sha256:" + "c" * 64,
        "artifacts": {
            "dag_specs": [],
            "workload_packs": [],
            "canonical_schemas": [],
            "runtime_payloads": [
                {
                    "id": "daily_marts_project",
                    "kind": "dbt_project_bundle",
                    "path": "runtime/dbt/daily_marts.project.tar.gz",
                    "sha256": "sha256:" + "b" * 64,
                    "bytes": 1024,
                    "media_type": "application/vnd.dpone.dbt-project-bundle+gzip",
                }
            ],
        },
        "provenance": {
            "source": "dpone dbt compile",
            "source_snapshot_sha256": "sha256:" + "d" * 64,
            "selection_fingerprints": ["sha256:" + "e" * 64],
            "route_certifications": [
                {
                    "variant_id": ("mssql:clickhouse:incremental_merge|native_bcp_to_clickhouse|widening|kpo"),
                    "route_id": "mssql:clickhouse:incremental_merge",
                    "transport": "native_bcp_to_clickhouse",
                    "schema_evolution": "widening",
                    "airflow_runtime_mode": "kpo",
                    "snapshot_id": "sha256:" + "f" * 64,
                    "support": "supported",
                    "certification_level": "production-certified",
                    "evidence_status": "PASS",
                    "evidence_refs": ["sha256:" + "1" * 64],
                    "evidence_reason_codes": [],
                }
            ],
        },
    }

    assert validator.validate(payload, expected_kind="dpone.release-set.v2") == ()
    logical_id = json.loads(json.dumps(payload))
    logical_id["artifacts"]["runtime_payloads"][0]["id"] = "daily:marts:project"
    assert validator.validate(logical_id, expected_kind="dpone.release-set.v2") == ()

    oversized = json.loads(json.dumps(payload))
    oversized["artifacts"]["runtime_payloads"][0]["bytes"] = 256 * 1024 * 1024 + 1
    issues = validator.validate(oversized, expected_kind="dpone.release-set.v2")
    assert [(issue.code, issue.path) for issue in issues] == [
        ("schema_maximum_violation", "artifacts.runtime_payloads[0].bytes")
    ]

    missing_authority = json.loads(json.dumps(payload))
    del missing_authority["selection_authority"]
    assert GitOpsSchemaValidator().validate(
        missing_authority,
        expected_kind="dpone.release-set.v2",
    )


def test_recovery_plan_schema_accepts_current_state_and_cache_integrity_issue() -> None:
    deployment_id = "sha256:" + "a" * 64
    issues = GitOpsSchemaValidator().validate(
        {
            "schema": "dpone.deployment-cache-recovery-plan.v1",
            "environment": "dev",
            "status": "repairable",
            "current_deployment_id": deployment_id,
            "current_path_deployment_id": deployment_id,
            "preferred_repair_deployment_id": deployment_id,
            "issues": [
                {
                    "code": "DPONE_CACHE_CHECKSUM_MISMATCH",
                    "severity": "error",
                    "message": "artifact digest differs",
                    "path": ".dpone-cache/releases/sha256-a/packs/orders.json",
                }
            ],
            "repair_candidates": [
                {
                    "deployment_id": deployment_id,
                    "path": ".dpone-cache/deployments/dev/sha256-a",
                    "reason": "current_state",
                }
            ],
        },
        expected_kind="dpone.deployment-cache-recovery-plan.v1",
    )

    assert issues == ()


@pytest.mark.parametrize(
    ("kind", "payload"),
    [
        (
            "dpone.deployment-cache-retention-plan.v1",
            {
                "schema": "dpone.deployment-cache-retention-plan.v1",
                "environment": "dev",
                "current_deployment_id": None,
                "protected_deployment_ids": [],
                "items": [
                    {
                        "deployment_id": None,
                        "action": "delete",
                        "reason": "unreferenced",
                        "path": ".dpone-cache/deployments/dev/invalid",
                    }
                ],
                "delete_candidates": [],
            },
        ),
        (
            "dpone.deployment-cache-retention-apply.v1",
            {
                "schema": "dpone.deployment-cache-retention-apply.v1",
                "environment": "dev",
                "promoted_by": "ci://retention",
                "current_deployment_id": None,
                "items": [
                    {
                        "deployment_id": None,
                        "action": "deleted",
                        "reason": "unreferenced",
                        "path": ".dpone-cache/deployments/dev/invalid",
                    }
                ],
                "deleted_deployment_ids": [],
                "skipped_deployment_ids": [],
            },
        ),
    ],
)
def test_retention_schemas_reject_null_destructive_actions(kind: str, payload: dict[str, object]) -> None:
    issues = GitOpsSchemaValidator().validate(payload, expected_kind=kind)

    assert issues


@pytest.mark.parametrize(
    ("kind", "payload"),
    [
        (
            "dpone.deployment-cache-retention-plan.v1",
            {
                "schema": "dpone.deployment-cache-retention-plan.v1",
                "environment": "dev",
                "current_deployment_id": None,
                "protected_deployment_ids": [],
                "items": [],
                "delete_candidates": [],
            },
        ),
        (
            "dpone.deployment-cache-retention-apply.v1",
            {
                "schema": "dpone.deployment-cache-retention-apply.v1",
                "environment": "dev",
                "promoted_by": "ci://retention",
                "current_deployment_id": None,
                "items": [],
                "deleted_deployment_ids": [],
                "skipped_deployment_ids": [],
            },
        ),
    ],
)
def test_retention_v1_accepts_pre_digest_evidence(kind: str, payload: dict[str, object]) -> None:
    assert GitOpsSchemaValidator().validate(payload, expected_kind=kind) == ()


def test_retention_apply_v1_preserves_historical_deleted_reason_grammar() -> None:
    deployment_id = "sha256:" + "a" * 64
    payload = {
        "schema": "dpone.deployment-cache-retention-apply.v1",
        "environment": "dev",
        "promoted_by": "ci://retention",
        "current_deployment_id": None,
        "items": [
            {
                "deployment_id": deployment_id,
                "action": "deleted",
                "reason": "current",
                "path": ".dpone-cache/deployments/dev/sha256-a",
            }
        ],
        "deleted_deployment_ids": [deployment_id],
        "skipped_deployment_ids": [],
    }

    assert GitOpsSchemaValidator().validate(payload, expected_kind=payload["schema"]) == ()

    payload["schema"] = "dpone.deployment-cache-retention-apply.v2"
    payload["reviewed_plan_sha256"] = "sha256:" + "b" * 64
    payload["activation_history_revision"] = "sha256:" + "c" * 64
    assert GitOpsSchemaValidator().validate(payload, expected_kind=payload["schema"])


def test_retention_apply_v2_preserves_pre_receipt_contract_and_v3_requires_receipt() -> None:
    payload = {
        "schema": "dpone.deployment-cache-retention-apply.v2",
        "environment": "dev",
        "promoted_by": "ci://retention",
        "current_deployment_id": "sha256:" + "a" * 64,
        "items": [
            {
                "deployment_id": "sha256:" + "b" * 64,
                "action": "deleted",
                "reason": "unreferenced",
                "path": ".dpone-cache/deployments/dev/sha256-b",
            }
        ],
        "deleted_deployment_ids": ["sha256:" + "b" * 64],
        "skipped_deployment_ids": [],
    }

    issues = GitOpsSchemaValidator().validate(
        payload,
        expected_kind="dpone.deployment-cache-retention-apply.v2",
    )

    assert issues
    payload["reviewed_plan_sha256"] = "sha256:" + "c" * 64
    payload["activation_history_revision"] = "sha256:" + "d" * 64
    assert (
        GitOpsSchemaValidator().validate(
            payload,
            expected_kind="dpone.deployment-cache-retention-apply.v2",
        )
        == ()
    )

    payload["schema"] = "dpone.deployment-cache-retention-apply.v3"
    payload["review_id"] = "00000000-0000-4000-8000-000000000001"
    payload["operation_id"] = retention_operation_id(
        environment="dev",
        reviewed_plan_sha256=str(payload["reviewed_plan_sha256"]),
        review_id=str(payload["review_id"]),
    )
    payload["receipt_revision"] = "sha256:" + "f" * 64
    payload["transaction_status"] = "committed"
    assert (
        GitOpsSchemaValidator().validate(
            payload,
            expected_kind="dpone.deployment-cache-retention-apply.v3",
        )
        == ()
    )


def test_airflow_pack_schema_documents_self_contained_runtime_fields() -> None:
    schema = get_gitops_schema_contract("gitops.airflow_pack").schema
    properties = schema["properties"]

    assert properties["runtime_command"]["type"] == "string"
    assert properties["pack_identity"]["properties"]["schema"]["const"] == ("dpone.airflow-pack-identity.v1")
    assert properties["pack_fingerprint"]["pattern"] == "^sha256:[0-9a-f]{64}$"
    assert properties["pod_spec"]["type"] == "object"
    assert properties["connection_projection"]["type"] == "object"
    assert properties["xcom"]["type"] == "object"
    assert properties["outcome_gate"]["type"] == "object"
    assert properties["kpo_kwargs"]["type"] == "object"
    provider_execution = properties["provider_execution"]
    assert provider_execution["required"] == ["schema", "kpo_kwargs", "pod_spec"]
    assert provider_execution["additionalProperties"] is False
    assert provider_execution["properties"]["schema"]["const"] == "dpone.airflow-provider-execution.v1"
    assert provider_execution["properties"]["kpo_kwargs"]["additionalProperties"] is False
    assert provider_execution["properties"]["pod_spec"]["additionalProperties"] is False
    assert set(provider_execution["properties"]["pod_spec"]["properties"]["spec"]["properties"]) == {
        "nodeSelector",
        "tolerations",
        "imagePullSecrets",
        "containers",
    }
    assert properties["artifact_index"]["type"] == "object"
    assert properties["pack_fingerprint"]["type"] == "string"
    runtime_command = properties["steps"]["items"]["properties"]["runtime_command"]
    assert runtime_command["additionalProperties"] is False


def test_airflow_pack_runtime_command_rejects_unknown_fields_in_both_inventories() -> None:
    in_memory = get_gitops_schema_contract("gitops.airflow_pack").schema
    published = json.loads((SCHEMA_DIR / "airflow-pack.schema.json").read_text(encoding="utf-8"))
    command = {
        "schema": "dpone.airflow-pre-hook-command.v1",
        "hook_id": "refresh_orders",
        "process_selector": None,
        "argv": [
            "dpone",
            "hooks",
            "execute",
            "runtime/orders.yaml",
            "--phase",
            "pre_hook",
            "--hook-id",
            "refresh_orders",
        ],
    }

    for schema in (in_memory, published):
        properties = schema["properties"]
        runtime_schemas = (
            properties["steps"]["items"]["properties"]["runtime_command"],
            properties["process_plans"]["additionalProperties"]["properties"]["steps"]["items"]["properties"][
                "runtime_command"
            ],
        )
        for runtime_schema in runtime_schemas:
            jsonschema.validate(command, runtime_schema)
            with pytest.raises(jsonschema.ValidationError):
                jsonschema.validate(
                    {**command, "unexpected": "forbidden"},
                    runtime_schema,
                )


def test_airflow_advisory_contract_schemas_allow_null_image_digest() -> None:
    validator = GitOpsSchemaValidator()

    run_spec = {
        "kind": "gitops.airflow_run_spec",
        "schema_version": "1",
        "producer": "dpone gitops airflow run-spec",
        "bundle_path": ".dpone/gitops/bundle/bundle.json",
        "bundle_digest": "sha256:abc",
        "image": "dpone-runtime-local:live-smoke",
        "image_digest": None,
        "worktree": ".",
        "evidence_output": ".dpone/gitops/airflow/runtime-evidence.json",
        "entries": [],
        "steps": [],
    }
    runtime_profile = {
        "kind": "gitops.airflow_runtime_profile",
        "schema_version": "1",
        "producer": "dpone gitops airflow runtime-profile",
        "bundle_path": ".dpone/gitops/bundle/bundle.json",
        "bundle_digest": "sha256:abc",
        "run_spec_path": ".dpone/gitops/airflow/run-spec.json",
        "runtime_evidence_path": ".dpone/gitops/airflow/runtime-evidence.json",
        "xcom_summary_path": ".dpone/gitops/airflow/xcom-summary.json",
        "dag_factory_path": ".dpone/gitops/airflow/airflow_dag_factory.py",
        "outcome_gate_path": ".dpone/gitops/airflow/outcome_gate.py",
        "image": "dpone-runtime-local:live-smoke",
        "image_digest": None,
        "namespace": "dpone-airflow",
        "service_account": "dpone-airflow-worker",
        "resources": {"requests": {"cpu": "50m", "memory": "128Mi"}, "limits": {"cpu": "250m", "memory": "512Mi"}},
        "artifact_sink": {"kind": "local", "path": ".dpone/gitops/airflow"},
        "runner_policy": "advisory",
        "outcome_mode": "xcom_then_gate",
    }

    assert validator.validate(run_spec, expected_kind="gitops.airflow_run_spec") == ()
    assert validator.validate(runtime_profile, expected_kind="gitops.airflow_runtime_profile") == ()


def test_gitops_schema_validator_rejects_missing_required_bundle_field() -> None:
    issues = GitOpsSchemaValidator().validate(
        {"kind": "gitops.bundle", "entries": []},
        expected_kind="gitops.bundle",
    )

    assert [issue.code for issue in issues] == [
        "schema_required_field_missing",
        "schema_required_field_missing",
        "schema_required_field_missing",
        "schema_required_field_missing",
    ]
    assert {issue.path for issue in issues} == {"output_dir", "affected_path", "summary_path", "policy"}


def test_gitops_schema_validator_rejects_credential_runtime_secret_material() -> None:
    issues = GitOpsSchemaValidator().validate(
        {
            "kind": "dpone.credential-runtime.v1",
            "schema": "dpone.credential-runtime.v1",
            "environment": "dev",
            "vault": {
                "address": "https://vault.internal",
                "auth": {
                    "method": "kubernetes",
                    "role": "dpone-runtime-dev",
                    "token": "must-not-leak",
                },
            },
        },
        expected_kind="dpone.credential-runtime.v1",
    )

    assert [(issue.code, issue.path) for issue in issues] == [
        ("schema_validation_failed", "vault.auth"),
    ]
    assert "must-not-leak" not in " ".join(issue.message for issue in issues)


def test_gitops_schema_validator_rejects_credential_runtime_password_material() -> None:
    issues = GitOpsSchemaValidator().validate(
        {
            "kind": "dpone.credential-runtime.v1",
            "schema": "dpone.credential-runtime.v1",
            "environment": "dev",
            "vault": {
                "address": "https://vault.internal",
                "auth": {
                    "method": "kubernetes",
                    "role": "dpone-runtime-dev",
                    "password": "must-not-leak",
                },
            },
        },
        expected_kind="dpone.credential-runtime.v1",
    )

    assert [(issue.code, issue.path) for issue in issues] == [
        ("schema_validation_failed", "vault.auth"),
    ]
    assert "must-not-leak" not in " ".join(issue.message for issue in issues)


def test_gitops_schema_validator_rejects_credential_runtime_derived_secret_material() -> None:
    issues = GitOpsSchemaValidator().validate(
        {
            "kind": "dpone.credential-runtime.v1",
            "schema": "dpone.credential-runtime.v1",
            "environment": "dev",
            "vault": {
                "address": "https://vault.internal",
                "auth": {
                    "method": "kubernetes",
                    "role": "dpone-runtime-dev",
                    "client_secret": "must-not-leak",
                },
            },
        },
        expected_kind="dpone.credential-runtime.v1",
    )

    assert [(issue.code, issue.path) for issue in issues] == [
        ("schema_validation_failed", "vault.auth"),
    ]
    assert "must-not-leak" not in " ".join(issue.message for issue in issues)


def test_gitops_schema_validator_rejects_credential_runtime_missing_environment() -> None:
    issues = GitOpsSchemaValidator().validate(
        {
            "schema": "dpone.credential-runtime.v1",
            "vault": {
                "address": "https://vault.internal",
                "auth": {
                    "method": "kubernetes",
                    "role": "dpone-runtime-dev",
                },
            },
        },
        expected_kind="dpone.credential-runtime.v1",
    )

    assert [(issue.code, issue.path) for issue in issues] == [
        ("schema_required_field_missing", "environment"),
    ]


def test_gitops_schema_validator_accepts_schema_identity_for_dpone_contracts() -> None:
    issues = GitOpsSchemaValidator().validate(
        {
            "schema": "dpone.credential-runtime.v1",
            "environment": "dev",
            "vault": {
                "address": "https://vault.internal",
                "auth": {
                    "method": "kubernetes",
                    "role": "dpone-runtime-dev",
                },
            },
        },
        expected_kind="dpone.credential-runtime.v1",
    )

    assert issues == ()


def test_gitops_schema_validator_rejects_empty_connection_registry_field_mapping() -> None:
    issues = GitOpsSchemaValidator().validate(
        {
            "schema": "dpone.connection-registry.v1",
            "connections": {
                "pg_prod": {
                    "type": "postgres",
                    "credentials": {
                        "resolver": "vault_kv",
                        "mount": "kv",
                        "path": "dpone/prod/credentials/pg_source",
                        "fields": {"": "username", "password": ""},
                        "version_policy": "latest",
                        "resolution_scope": "workload_start",
                    },
                }
            },
        },
        expected_kind="dpone.connection-registry.v1",
    )

    assert [(issue.code, issue.path) for issue in issues] == [
        ("schema_min_length_violation", "connections.pg_prod.credentials.fields.<key>"),
        ("schema_min_length_violation", "connections.pg_prod.credentials.fields.password"),
    ]


def test_gitops_schema_validator_rejects_airflow_bridge_uri_connection_ids() -> None:
    issues = GitOpsSchemaValidator().validate(
        {
            "kind": "gitops.airflow_runtime_profile",
            "schema_version": "1",
            "producer": "dpone gitops airflow runtime-profile",
            "bundle_path": ".dpone/gitops/bundle/bundle.json",
            "run_spec_path": ".dpone/gitops/airflow/run-spec.json",
            "runtime_evidence_path": ".dpone/gitops/airflow/runtime-evidence.json",
            "xcom_summary_path": ".dpone/gitops/airflow/xcom-summary.json",
            "dag_factory_path": ".dpone/gitops/airflow/airflow_dag_factory.py",
            "outcome_gate_path": ".dpone/gitops/airflow/outcome_gate.py",
            "image": "ghcr.io/acme/dpone:2026.06.17",
            "namespace": "dpone-runners",
            "service_account": "dpone-runner",
            "resources": {"requests": {"cpu": "250m"}, "limits": {"cpu": "1"}},
            "artifact_sink": {"kind": "local", "path": ".dpone/gitops/airflow"},
            "runner_policy": "release",
            "outcome_mode": "strict_fail",
            "connection_bridge": {
                "enabled": True,
                "mode": "k8s_secret",
                "runtime_mode": "runtime_only",
                "secret_name": "dpone-airflow-connections",
                "required_connection_ids": ["postgres://etl:secret@pg.internal:5432/dwh"],
                "env": [
                    {
                        "connection_id": "postgres://etl:secret@pg.internal:5432/dwh",
                        "env_name": "AIRFLOW_CONN_PG_PROD",
                        "secret_ref": {"name": "dpone-airflow-connections", "key": "AIRFLOW_CONN_PG_PROD"},
                    }
                ],
            },
        },
        expected_kind="gitops.airflow_runtime_profile",
    )

    assert [(issue.code, issue.path) for issue in issues] == [
        ("schema_pattern_mismatch", "connection_bridge.required_connection_ids[0]"),
        ("schema_pattern_mismatch", "connection_bridge.env[0].connection_id"),
    ]
    assert "PASSWORD=secret" not in " ".join(issue.message for issue in issues)


def test_gitops_schema_validator_rejects_airflow_bridge_invalid_secret_key() -> None:
    issues = GitOpsSchemaValidator().validate(
        {
            "kind": "gitops.airflow_runtime_profile",
            "schema_version": "1",
            "producer": "dpone gitops airflow runtime-profile",
            "bundle_path": ".dpone/gitops/bundle/bundle.json",
            "run_spec_path": ".dpone/gitops/airflow/run-spec.json",
            "runtime_evidence_path": ".dpone/gitops/airflow/runtime-evidence.json",
            "xcom_summary_path": ".dpone/gitops/airflow/xcom-summary.json",
            "dag_factory_path": ".dpone/gitops/airflow/airflow_dag_factory.py",
            "outcome_gate_path": ".dpone/gitops/airflow/outcome_gate.py",
            "image": "ghcr.io/acme/dpone:2026.06.17",
            "namespace": "dpone-runners",
            "service_account": "dpone-runner",
            "resources": {"requests": {"cpu": "250m"}, "limits": {"cpu": "1"}},
            "artifact_sink": {"kind": "local", "path": ".dpone/gitops/airflow"},
            "runner_policy": "release",
            "outcome_mode": "strict_fail",
            "connection_bridge": {
                "enabled": True,
                "mode": "k8s_secret",
                "runtime_mode": "runtime_only",
                "secret_name": "dpone-airflow-connections",
                "required_connection_ids": ["pg_prod"],
                "env": [
                    {
                        "connection_id": "pg_prod",
                        "env_name": "AIRFLOW_CONN_PG_PROD",
                        "secret_ref": {
                            "name": "dpone-airflow-connections",
                            "key": "AIRFLOW_CONN_PG_PROD\nPASSWORD=secret",
                        },
                    }
                ],
            },
        },
        expected_kind="gitops.airflow_runtime_profile",
    )

    assert [(issue.code, issue.path) for issue in issues] == [
        ("schema_pattern_mismatch", "connection_bridge.env[0].secret_ref.key"),
    ]
    assert "PASSWORD=secret" not in " ".join(issue.message for issue in issues)


def test_gitops_schema_validator_rejects_airflow_bridge_invalid_secret_name() -> None:
    issues = GitOpsSchemaValidator().validate(
        {
            "kind": "gitops.airflow_runtime_profile",
            "schema_version": "1",
            "producer": "dpone gitops airflow runtime-profile",
            "bundle_path": ".dpone/gitops/bundle/bundle.json",
            "run_spec_path": ".dpone/gitops/airflow/run-spec.json",
            "runtime_evidence_path": ".dpone/gitops/airflow/runtime-evidence.json",
            "xcom_summary_path": ".dpone/gitops/airflow/xcom-summary.json",
            "dag_factory_path": ".dpone/gitops/airflow/airflow_dag_factory.py",
            "outcome_gate_path": ".dpone/gitops/airflow/outcome_gate.py",
            "image": "ghcr.io/acme/dpone:2026.06.17",
            "namespace": "dpone-runners",
            "service_account": "dpone-runner",
            "resources": {"requests": {"cpu": "250m"}, "limits": {"cpu": "1"}},
            "artifact_sink": {"kind": "local", "path": ".dpone/gitops/airflow"},
            "runner_policy": "release",
            "outcome_mode": "strict_fail",
            "connection_bridge": {
                "enabled": True,
                "mode": "k8s_secret",
                "runtime_mode": "runtime_only",
                "secret_name": "dpone-airflow-connections\nPASSWORD=secret",
                "required_connection_ids": ["pg_prod"],
                "env": [
                    {
                        "connection_id": "pg_prod",
                        "env_name": "AIRFLOW_CONN_PG_PROD",
                        "secret_ref": {
                            "name": "dpone-airflow-connections\nPASSWORD=secret",
                            "key": "AIRFLOW_CONN_PG_PROD",
                        },
                    }
                ],
            },
        },
        expected_kind="gitops.airflow_runtime_profile",
    )

    assert [(issue.code, issue.path) for issue in issues] == [
        ("schema_pattern_mismatch", "connection_bridge.secret_name"),
        ("schema_pattern_mismatch", "connection_bridge.env[0].secret_ref.name"),
    ]
    assert "PASSWORD=secret" not in " ".join(issue.message for issue in issues)


def test_gitops_schema_validator_rejects_extra_binding_set_entry_fields() -> None:
    issues = GitOpsSchemaValidator().validate(
        {
            "schema": "dpone.binding-set.v1",
            "environment": "prod",
            "bindings": {
                "pg_source": {
                    "connection_ref": "pg_prod",
                    "resolver": "vault_kv",
                    "vault_path": "dpone/prod/credentials/pg_source",
                }
            },
            "runtime": {
                "kubernetes_namespace": "airflow-example",
                "service_account": "dpone-runtime",
            },
        },
        expected_kind="dpone.binding-set.v1",
    )

    assert [(issue.code, issue.path) for issue in issues] == [
        ("schema_additional_property_forbidden", "bindings.pg_source.resolver"),
        ("schema_additional_property_forbidden", "bindings.pg_source.vault_path"),
    ]


def test_gitops_schema_validator_rejects_invalid_airflow_index_artifact_items() -> None:
    issues = GitOpsSchemaValidator().validate(
        {
            "schema": "dpone.airflow-deployment-index.v1",
            "release_id": "sha256:" + "a" * 64,
            "deployment_id": "sha256:" + "b" * 64,
            "dag_specs": [
                {
                    "id": "orders_daily",
                    "artifact_ref": "../releases/orders_daily.dag-spec.json",
                    "sha256": "sha256:" + "c" * 64,
                    "bytes": 4096,
                }
            ],
            "workload_packs": [
                {
                    "id": "load_orders",
                    "artifact_ref": "cache://releases/sha256-release/packs/load_orders.airflow-pack.json",
                    "sha256": "not-a-sha",
                    "bytes": 8192,
                }
            ],
            "runtime_artifact_delivery": _complete_init_fetch_delivery(),
        },
        expected_kind="dpone.airflow-deployment-index.v1",
    )

    assert [(issue.code, issue.path) for issue in issues] == [
        ("schema_pattern_mismatch", "dag_specs[0].artifact_ref"),
        ("schema_pattern_mismatch", "workload_packs[0].sha256"),
    ]


def test_gitops_schema_validator_accepts_legacy_v1_index_without_declared_bytes() -> None:
    issues = GitOpsSchemaValidator().validate(
        {
            "schema": "dpone.airflow-deployment-index.v1",
            "release_id": "sha256:" + "a" * 64,
            "deployment_id": "sha256:" + "b" * 64,
            "dag_specs": [
                {
                    "id": "orders_daily",
                    "artifact_ref": "cache://releases/sha256-release/dags/orders_daily.dag-spec.json",
                    "sha256": "sha256:" + "c" * 64,
                }
            ],
            "workload_packs": [],
            "runtime_artifact_delivery": _complete_init_fetch_delivery(),
        },
        expected_kind="dpone.airflow-deployment-index.v1",
    )

    assert issues == ()


def test_gitops_schema_validator_rejects_invalid_nullable_deployment_index_refs() -> None:
    issues = GitOpsSchemaValidator().validate(
        {
            "schema": "dpone.airflow-deployment-index.v1",
            "release_id": "sha256:" + "a" * 64,
            "deployment_id": "sha256:" + "b" * 64,
            "dag_specs": [],
            "workload_packs": [],
            "binding_set_ref": 7,
            "connection_registry_ref": {"bad": "shape"},
            "runtime_artifact_delivery": _complete_init_fetch_delivery(),
        },
        expected_kind="dpone.airflow-deployment-index.v1",
    )

    assert [(issue.code, issue.path) for issue in issues] == [
        ("schema_any_of_mismatch", "binding_set_ref"),
        ("schema_any_of_mismatch", "connection_registry_ref"),
    ]


def test_gitops_schema_validator_rejects_non_digest_deployment_index_refs() -> None:
    issues = GitOpsSchemaValidator().validate(
        {
            "schema": "dpone.airflow-deployment-index.v1",
            "release_id": "sha256:" + "a" * 64,
            "deployment_id": "sha256:" + "b" * 64,
            "dag_specs": [],
            "workload_packs": [],
            "binding_set_ref": "bindings-prod",
            "connection_registry_ref": "registry-prod",
            "credential_runtime_ref": "credential-runtime-prod",
            "runtime_image_digest": "dpone-runtime:latest",
            "runtime_artifact_delivery": _complete_init_fetch_delivery(),
        },
        expected_kind="dpone.airflow-deployment-index.v1",
    )

    assert [(issue.code, issue.path) for issue in issues] == [
        ("schema_any_of_mismatch", "binding_set_ref"),
        ("schema_any_of_mismatch", "connection_registry_ref"),
        ("schema_any_of_mismatch", "credential_runtime_ref"),
        ("schema_any_of_mismatch", "runtime_image_digest"),
    ]


def test_gitops_schema_validator_rejects_non_digest_safe_sample_deployment_context_refs() -> None:
    issues = GitOpsSchemaValidator().validate(
        {
            "schema": "dpone.safe-sample-airflow-deployment-context.v1",
            "release_id": "sha256:" + "a" * 64,
            "deployment_id": "sha256:" + "b" * 64,
            "deployment_type": "environment",
            "environment": "prod",
            "runnable": True,
            "runtime_artifact_delivery": _complete_init_fetch_delivery(),
            "workload_packs": [],
            "binding_set_ref": "bindings-prod",
            "connection_registry_ref": "registry-prod",
            "credential_runtime_ref": "credential-runtime-prod",
            "runtime_image_digest": "dpone-runtime:latest",
            "airflow_bundle_ref": "git:7ac31f2",
            "index_path": ".dpone-cache/current/airflow-index.json",
            "deployment_path": ".dpone-cache/deployments/prod/sha256-b",
            "parse_side_effects": {"network": False, "secrets": False},
        },
        expected_kind="dpone.safe-sample-airflow-deployment-context.v1",
    )

    assert [(issue.code, issue.path) for issue in issues] == [
        ("schema_any_of_mismatch", "binding_set_ref"),
        ("schema_any_of_mismatch", "connection_registry_ref"),
        ("schema_any_of_mismatch", "credential_runtime_ref"),
        ("schema_any_of_mismatch", "runtime_image_digest"),
    ]


def test_gitops_schema_validator_rejects_non_digest_safe_sample_runtime_identity_refs() -> None:
    issues = GitOpsSchemaValidator().validate(
        {
            "schema": "dpone.safe-sample-runtime-execution.v1",
            "execution_status": "succeeded",
            "data_outcome": "passed",
            "release_id": "sha256:" + "a" * 64,
            "deployment_id": "sha256:" + "b" * 64,
            "deployment_identity": {
                "release_id": "sha256:" + "a" * 64,
                "deployment_id": "sha256:" + "b" * 64,
                "binding_set_ref": "bindings-prod",
                "connection_registry_ref": "registry-prod",
                "credential_runtime_ref": "credential-runtime-prod",
                "runtime_image_digest": "dpone-runtime:latest",
                "airflow_bundle_ref": "git:7ac31f2",
                "workload_packs": [],
                "runtime_artifact_delivery": _complete_init_fetch_delivery(),
            },
            "errors": [],
        },
        expected_kind="dpone.safe-sample-runtime-execution.v1",
    )

    assert [(issue.code, issue.path) for issue in issues] == [
        ("schema_any_of_mismatch", "deployment_identity.binding_set_ref"),
        ("schema_any_of_mismatch", "deployment_identity.connection_registry_ref"),
        ("schema_any_of_mismatch", "deployment_identity.credential_runtime_ref"),
        ("schema_any_of_mismatch", "deployment_identity.runtime_image_digest"),
    ]


def test_gitops_schema_validator_rejects_incomplete_runtime_identity_init_fetch_delivery() -> None:
    issues = GitOpsSchemaValidator().validate(
        {
            "schema": "dpone.safe-sample-runtime-execution.v1",
            "execution_status": "succeeded",
            "data_outcome": "passed",
            "release_id": "sha256:" + "a" * 64,
            "deployment_id": "sha256:" + "b" * 64,
            "deployment_identity": {
                "release_id": "sha256:" + "a" * 64,
                "deployment_id": "sha256:" + "b" * 64,
                "workload_packs": [],
                "runtime_artifact_delivery": {"mode": "init_fetch"},
            },
            "errors": [],
        },
        expected_kind="dpone.safe-sample-runtime-execution.v1",
    )

    assert [(issue.code, issue.path) for issue in issues] == [
        ("schema_required_field_missing", "deployment_identity.runtime_artifact_delivery.artifact_registry_ref"),
        ("schema_required_field_missing", "deployment_identity.runtime_artifact_delivery.identity"),
        ("schema_required_field_missing", "deployment_identity.runtime_artifact_delivery.source"),
        ("schema_required_field_missing", "deployment_identity.runtime_artifact_delivery.verify"),
    ]


def test_gitops_schema_validator_rejects_values_below_minimum() -> None:
    issues = GitOpsSchemaValidator().validate(
        {
            "schema": "dpone.safe-sample-certified-copy-request.v1",
            "certification_id": "mssql-clickhouse-incremental-sample",
            "source": {
                "type": "mssql",
                "connection_ref": "mssql_prod",
                "table": {"schema": "dbo", "name": "orders"},
            },
            "sink": {
                "type": "clickhouse",
                "connection_ref": "clickhouse_prod",
                "temporary_table": {"schema": "tmp", "name": "orders_sample"},
            },
            "strategy": "incremental_merge",
            "sample_rows": 0,
            "max_bytes": -1,
            "timeout_seconds": 0,
            "source_read_only": True,
            "pii_policy": "masked",
            "proof": "route_certification:mssql_clickhouse_incremental_sample",
        },
        expected_kind="dpone.safe-sample-certified-copy-request.v1",
    )

    assert [(issue.code, issue.path) for issue in issues] == [
        ("schema_minimum_violation", "sample_rows"),
        ("schema_minimum_violation", "max_bytes"),
        ("schema_minimum_violation", "timeout_seconds"),
    ]


def test_gitops_schema_validator_rejects_invalid_safe_sample_connection_ref_aliases() -> None:
    issues = GitOpsSchemaValidator().validate(
        {
            "schema": "dpone.safe-sample-certified-copy-request.v1",
            "certification_id": "mssql-clickhouse-incremental-sample",
            "source": {
                "type": "mssql",
                "connection_ref": "mssql_prod\nPASSWORD=secret",
                "table": {"schema": "dbo", "name": "orders"},
            },
            "sink": {
                "type": "clickhouse",
                "connection_ref": "../clickhouse_prod",
                "temporary_table": {"schema": "tmp", "name": "orders_sample"},
            },
            "strategy": "incremental_merge",
            "sample_rows": 1000,
            "max_bytes": 1024,
            "timeout_seconds": 60,
            "source_read_only": True,
            "pii_policy": "masked",
            "proof": "route_certification:mssql_clickhouse_incremental_sample",
        },
        expected_kind="dpone.safe-sample-certified-copy-request.v1",
    )

    assert [(issue.code, issue.path) for issue in issues] == [
        ("schema_pattern_mismatch", "source.connection_ref"),
        ("schema_pattern_mismatch", "sink.connection_ref"),
    ]


def test_gitops_schema_validator_rejects_invalid_connection_check_report_refs() -> None:
    issues = GitOpsSchemaValidator().validate(
        {
            "schema": "dpone.connection-check.v1",
            "passed": True,
            "changes": [],
            "errors": [],
            "mode": "connections",
            "network": False,
            "secrets": False,
            "source_queries": False,
            "handshake": "configuration_only",
            "environment": "prod",
            "connection_refs": ["orders_source\nPASSWORD=secret"],
            "resolved_connection_refs": ["../pg_prod"],
            "airflow_connection_bridge": {
                "required": True,
                "execution_mode": "operator_bridge",
                "resolver_location": "operator_execution",
                "parse_safe": True,
                "secrets": False,
                "required_connection_ids": ["pg_prod"],
                "connections": [
                    {
                        "connection_ref": "orders_source\nPASSWORD=secret",
                        "registry_connection_ref": "../pg_prod",
                        "connection_id": "pg_prod",
                        "env_name": "AIRFLOW_CONN_PG_PROD",
                    }
                ],
                "projection": {
                    "mode": "kubernetes_secret_volume",
                    "secret_name": "dpone-airflow-connection-bridge",
                    "mount_path": "/run/secrets/dpone/airflow-connections",
                    "payload_format": "airflow_connection_uri",
                    "secret_values": False,
                    "connections": [
                        {
                            "connection_ref": "orders_source\nPASSWORD=secret",
                            "registry_connection_ref": "../pg_prod",
                            "connection_id": "pg_prod",
                            "secret_key": "AIRFLOW_CONN_PG_PROD",
                            "mount_path": "/run/secrets/dpone/airflow-connections/orders_source",
                            "fields": {"uri": "uri"},
                        }
                    ],
                },
                "next_actions": [],
            },
            "binding_set_path": "environments/prod/binding-set.yaml",
            "connection_registry_path": "platform/connection-registries/prod.yaml",
            "credential_runtime_path": "environments/prod/credential-runtime.yaml",
        },
        expected_kind="dpone.connection-check.v1",
    )

    assert [(issue.code, issue.path) for issue in issues] == [
        ("schema_pattern_mismatch", "connection_refs[0]"),
        ("schema_pattern_mismatch", "resolved_connection_refs[0]"),
        ("schema_pattern_mismatch", "airflow_connection_bridge.connections[0].connection_ref"),
        ("schema_pattern_mismatch", "airflow_connection_bridge.connections[0].registry_connection_ref"),
        ("schema_any_of_mismatch", "airflow_connection_bridge.projection"),
    ]


def test_gitops_schema_validator_rejects_invalid_live_preflight_refs() -> None:
    issues = GitOpsSchemaValidator().validate(
        {
            "schema": "dpone.live-preflight.v1",
            "passed": False,
            "runner": "not_configured",
            "network": False,
            "secrets": False,
            "source_queries": False,
            "planned_network": True,
            "planned_secrets": True,
            "planned_source_queries": "bounded_probes",
            "environment": "prod",
            "connection_refs": ["orders_source\nPASSWORD=secret"],
            "resolved_connection_refs": ["../pg_prod"],
            "probes": [
                {
                    "connection_ref": "../pg_prod",
                    "probe": "bounded_source_probe",
                    "status": "blocked",
                    "runner": "not_configured",
                    "network": True,
                    "secrets": True,
                    "source_queries": True,
                }
            ],
            "errors": [],
        },
        expected_kind="dpone.live-preflight.v1",
    )

    assert [(issue.code, issue.path) for issue in issues] == [
        ("schema_pattern_mismatch", "connection_refs[0]"),
        ("schema_pattern_mismatch", "resolved_connection_refs[0]"),
        ("schema_pattern_mismatch", "probes[0].connection_ref"),
    ]


def test_gitops_schema_validator_rejects_connection_check_bridge_mount_path_escape() -> None:
    issues = GitOpsSchemaValidator().validate(
        {
            "schema": "dpone.connection-check.v1",
            "passed": True,
            "changes": [],
            "errors": [],
            "mode": "connections",
            "network": False,
            "secrets": False,
            "source_queries": False,
            "handshake": "configuration_only",
            "environment": "prod",
            "connection_refs": ["orders_source"],
            "resolved_connection_refs": ["pg_prod"],
            "airflow_connection_bridge": {
                "required": True,
                "execution_mode": "operator_bridge",
                "resolver_location": "operator_execution",
                "parse_safe": True,
                "secrets": False,
                "required_connection_ids": ["pg_prod"],
                "connections": [
                    {
                        "connection_ref": "orders_source",
                        "registry_connection_ref": "pg_prod",
                        "connection_id": "pg_prod",
                        "env_name": "AIRFLOW_CONN_PG_PROD",
                    }
                ],
                "projection": {
                    "mode": "kubernetes_secret_volume",
                    "secret_name": "dpone-airflow-connection-bridge",
                    "mount_path": "/run/secrets/dpone/../outside",
                    "payload_format": "airflow_connection_uri",
                    "secret_values": False,
                    "connections": [
                        {
                            "connection_ref": "orders_source",
                            "registry_connection_ref": "pg_prod",
                            "connection_id": "pg_prod",
                            "secret_key": "AIRFLOW_CONN_PG_PROD",
                            "mount_path": "/run/secrets/dpone/airflow-connections/../orders_source",
                            "fields": {"uri": "uri"},
                        }
                    ],
                },
                "next_actions": [],
            },
            "binding_set_path": "environments/prod/binding-set.yaml",
            "connection_registry_path": "platform/connection-registries/prod.yaml",
            "credential_runtime_path": "environments/prod/credential-runtime.yaml",
        },
        expected_kind="dpone.connection-check.v1",
    )

    assert ("schema_any_of_mismatch", "airflow_connection_bridge.projection") in [
        (issue.code, issue.path) for issue in issues
    ]


def test_gitops_schema_validator_rejects_connection_check_bridge_uri_connection_ids() -> None:
    issues = GitOpsSchemaValidator().validate(
        {
            "schema": "dpone.connection-check.v1",
            "passed": True,
            "changes": [],
            "errors": [],
            "mode": "connections",
            "network": False,
            "secrets": False,
            "source_queries": False,
            "handshake": "configuration_only",
            "environment": "prod",
            "connection_refs": ["orders_source"],
            "resolved_connection_refs": ["pg_prod"],
            "airflow_connection_bridge": {
                "required": True,
                "execution_mode": "operator_bridge",
                "resolver_location": "operator_execution",
                "parse_safe": True,
                "secrets": False,
                "required_connection_ids": ["postgres://user:password@host/db"],
                "connections": [
                    {
                        "connection_ref": "orders_source",
                        "registry_connection_ref": "pg_prod",
                        "connection_id": "postgres://user:password@host/db",
                        "env_name": "AIRFLOW_CONN_PG_PROD",
                    }
                ],
                "projection": {
                    "mode": "kubernetes_secret_volume",
                    "secret_name": "dpone-airflow-connection-bridge",
                    "mount_path": "/run/secrets/dpone/airflow-connections",
                    "payload_format": "airflow_connection_uri",
                    "secret_values": False,
                    "connections": [
                        {
                            "connection_ref": "orders_source",
                            "registry_connection_ref": "pg_prod",
                            "connection_id": "postgres://user:password@host/db",
                            "secret_key": "AIRFLOW_CONN_PG_PROD",
                            "mount_path": "/run/secrets/dpone/airflow-connections/orders_source",
                            "fields": {"uri": "uri"},
                        }
                    ],
                },
                "next_actions": [],
            },
            "binding_set_path": "environments/prod/binding-set.yaml",
            "connection_registry_path": "platform/connection-registries/prod.yaml",
            "credential_runtime_path": "environments/prod/credential-runtime.yaml",
        },
        expected_kind="dpone.connection-check.v1",
    )

    assert {issue.path for issue in issues} >= {
        "airflow_connection_bridge.connections[0].connection_id",
        "airflow_connection_bridge.projection",
    }


def test_gitops_schema_validator_rejects_invalid_number_type() -> None:
    issues = GitOpsSchemaValidator().validate(
        {
            "kind": "gitops.airflow_runtime_evidence",
            "schema_version": "1",
            "producer": "dpone gitops airflow runtime-evidence",
            "run_spec_path": ".dpone/gitops/airflow/run-spec.json",
            "bundle_path": ".dpone/gitops/bundle/bundle.json",
            "image": "dpone-runtime-local:live-smoke",
            "status": "succeeded",
            "started_at": "2026-07-11T12:00:00Z",
            "finished_at": "2026-07-11T12:00:01Z",
            "duration_seconds": "1.0",
            "steps": [],
        },
        expected_kind="gitops.airflow_runtime_evidence",
    )

    assert [(issue.code, issue.path) for issue in issues] == [
        ("schema_type_mismatch", "duration_seconds"),
    ]


def test_gitops_schema_validator_rejects_arrays_with_too_few_items() -> None:
    issues = GitOpsSchemaValidator().validate(
        {
            "schema": "dpone.safe-sample-cli-argument-validation.v1",
            "errors": [],
        },
        expected_kind="dpone.safe-sample-cli-argument-validation.v1",
    )

    assert [(issue.code, issue.path) for issue in issues] == [
        ("schema_min_items_violation", "errors"),
    ]


def test_gitops_schema_validator_rejects_duplicate_unique_items() -> None:
    issues = GitOpsSchemaValidator().validate(
        {
            "schema": "dpone.safe-sample-runtime-readiness.v1",
            "ready": False,
            "available_contracts": ["binding-set", "binding-set"],
            "blockers": ["vault_unavailable", "vault_unavailable"],
            "errors": [],
        },
        expected_kind="dpone.safe-sample-runtime-readiness.v1",
    )

    assert [(issue.code, issue.path) for issue in issues] == [
        ("schema_unique_items_violation", "available_contracts"),
        ("schema_unique_items_violation", "blockers"),
    ]


def test_gitops_schema_validator_rejects_invalid_uri_format() -> None:
    issues = GitOpsSchemaValidator().validate(
        {
            "schema": "dpone.credential-runtime.v1",
            "environment": "dev",
            "vault": {
                "address": "vault internal",
                "auth": {
                    "method": "kubernetes",
                    "role": "dpone-runtime-dev",
                },
            },
        },
        expected_kind="dpone.credential-runtime.v1",
    )

    assert [(issue.code, issue.path) for issue in issues] == [
        ("schema_format_mismatch", "vault.address"),
    ]


def test_gitops_schema_validator_rejects_invalid_date_time_format() -> None:
    issues = GitOpsSchemaValidator().validate(
        {
            "schema": "dpone.current-pointer.v1",
            "environment": "prod",
            "deployment_id": "sha256:" + "a" * 64,
            "release_id": "sha256:" + "b" * 64,
            "promoted_by": "ci://github-actions/dpone-release",
            "promoted_at": "not-a-date-time",
        },
        expected_kind="dpone.current-pointer.v1",
    )

    assert [(issue.code, issue.path) for issue in issues] == [
        ("schema_format_mismatch", "promoted_at"),
    ]


def test_gitops_schema_validator_rejects_invalid_previous_deployment_id() -> None:
    issues = GitOpsSchemaValidator().validate(
        {
            "schema": "dpone.current-pointer.v1",
            "environment": "prod",
            "deployment_id": "sha256:" + "a" * 64,
            "release_id": "sha256:" + "b" * 64,
            "promoted_by": "ci://github-actions/dpone-release",
            "promoted_at": "2026-07-12T09:30:00Z",
            "previous_deployment_id": "not-a-digest",
        },
        expected_kind="dpone.current-pointer.v1",
    )

    assert [(issue.code, issue.path) for issue in issues] == [
        ("schema_any_of_mismatch", "previous_deployment_id"),
    ]


def test_gitops_schema_validator_rejects_invalid_workspace_authority_ref() -> None:
    issues = GitOpsSchemaValidator().validate(
        {
            "schema": "dpone.current-pointer.v1",
            "environment": "prod",
            "deployment_id": "sha256:" + "a" * 64,
            "release_id": "sha256:" + "b" * 64,
            "promoted_by": "ci://github-actions/dpone-release",
            "promoted_at": "2026-07-12T09:30:00Z",
            "workspace_authority_connection_ref": "NOT SAFE",
        },
        expected_kind="dpone.current-pointer.v1",
    )

    assert [(issue.code, issue.path) for issue in issues] == [
        ("schema_pattern_mismatch", "workspace_authority_connection_ref"),
    ]


def test_gitops_schema_validator_rejects_any_of_mismatch() -> None:
    issues = GitOpsSchemaValidator().validate(
        {
            "schema": "dpone.deployment-set.v1",
            "deployment_id": "sha256:" + "a" * 64,
            "environment": "prod",
            "release_ref": "sha256:" + "b" * 64,
            "binding_set_ref": "not-a-sha",
            "runtime_artifact_delivery": {"mode": "local_preview"},
        },
        expected_kind="dpone.deployment-set.v1",
    )

    assert [(issue.code, issue.path) for issue in issues] == [
        ("schema_any_of_mismatch", "binding_set_ref"),
    ]


def test_airflow_architecture_safe_sample_examples_match_public_schemas() -> None:
    document = (ROOT / "docs" / "airflow-self-service-architecture.md").read_text(encoding="utf-8")
    examples = (
        _yaml_block_after(document, "preview-only current deployment still blocks safely"),
        _yaml_block_after(document, "certified_copy_request` preview"),
    )

    for payload in examples:
        assert GitOpsSchemaValidator().validate(payload, expected_kind=str(payload["schema"])) == ()


@pytest.mark.parametrize("value", ["", "   ", "TODO", "  TODO  "])
def test_domain_ownership_schema_rejects_runtime_placeholders(value: str) -> None:
    payload = {
        "schema": "dpone.domain-ownership.v1",
        "domain": "crm",
        "owner": {"team": value, "contact": "crm@example.com"},
        "approvers": {"github_team": "data-platform"},
    }

    assert GitOpsSchemaValidator().validate(payload, expected_kind="dpone.domain-ownership.v1")


@pytest.mark.parametrize(
    "root",
    (
        " ",
        " workloads",
        "workloads ",
        "\x00unsafe",
        ".",
        "owned/./pipelines",
        "owned//pipelines",
        "owned/pipelines/",
        "../unsafe",
        "/unsafe",
        r"owned\pipelines",
    ),
)
def test_project_schema_rejects_runtime_invalid_layout_roots(root: str) -> None:
    payload = {
        "schema": "dpone.project.v1",
        "layout": {
            "mode": "domain_first",
            "root": root,
            "pipeline_id_scope": "project",
        },
    }

    assert GitOpsSchemaValidator().validate(payload, expected_kind="dpone.project.v1")


def test_project_schema_accepts_confined_nested_layout_root() -> None:
    payload = {
        "schema": "dpone.project.v1",
        "layout": {
            "mode": "domain_first",
            "root": "data-products/workloads",
            "pipeline_id_scope": "project",
        },
    }

    assert GitOpsSchemaValidator().validate(payload, expected_kind="dpone.project.v1") == ()


@pytest.mark.parametrize(
    "root",
    (
        " ",
        " workloads",
        "workloads ",
        "\x00unsafe",
        ".",
        "owned/./pipelines",
        "owned//pipelines",
        "owned/pipelines/",
        "../unsafe",
        "/unsafe",
        r"owned\pipelines",
    ),
)
def test_workload_index_schema_rejects_runtime_invalid_layout_roots(root: str) -> None:
    payload = {
        "schema": "dpone.workload-index.v1",
        "project_fingerprint": "sha256:" + "0" * 64,
        "layout_mode": "domain_first",
        "layout_root": root,
        "pipeline_id_scope": "project",
        "workloads": [],
    }

    assert GitOpsSchemaValidator().validate(payload, expected_kind="dpone.workload-index.v1")


def test_workload_index_schema_rejects_identical_duplicate_entries(tmp_path: Path) -> None:
    service = build_airflow_self_service_service(root=tmp_path)
    assert service.init_project(airflow=True, layout="domain_first").passed
    assert service.init_domain(
        domain="crm",
        owner_team="data-crm",
        owner_contact="crm@example.com",
        approver_team="data-platform",
    ).passed
    assert service.init_pipeline(
        pipeline_id="orders_daily",
        domain="crm",
        recipe="mssql-to-clickhouse-incremental",
        airflow=True,
    ).passed
    payload = workload_index_from_snapshot(ProjectDiscoveryService(tmp_path).discover())
    payload["workloads"].append(dict(payload["workloads"][0]))

    issues = GitOpsSchemaValidator().validate(payload, expected_kind="dpone.workload-index.v1")

    assert ("schema_unique_items_violation", "workloads") in {(issue.code, issue.path) for issue in issues}
