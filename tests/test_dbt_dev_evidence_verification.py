from __future__ import annotations

import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from dpone_airflow_pack.dev_evidence_export import (
    export_dev_evidence_if_requested,
)

from dpone.adapters.dbt_artifacts import (
    CampaignDbtExecutionEvidenceWriter,
    LocalDbtExecutionEvidenceWriter,
)
from dpone.adapters.dbt_dev_evidence_campaign_journal import (
    ConfinedDbtDevEvidenceCampaignJournal,
)
from dpone.contracts.airflow_correlation import (
    AirflowAttemptCorrelation,
    build_airflow_correlation,
)
from dpone.contracts.airflow_deployment import release_id as compute_release_id
from dpone.contracts.airflow_run_identity import (
    AirflowArtifactIdentity,
    AirflowRunIdentity,
)
from dpone.contracts.dbt_invocation import DbtInvocationContext
from dpone.contracts.dbt_publishing import (
    DbtExecutionPack,
    DbtProfileSpec,
    DbtSelectionLock,
    DbtSqlServerAdapterPolicy,
    DbtSqlServerRuntimePolicy,
    canonical_dbt_execution_evidence_bytes,
    dbt_target_identity_sha256,
    sha256_bytes,
)
from dpone.contracts.dbt_runtime import dbt_target_binding_identity_sha256
from dpone.contracts.dbt_sqlserver_graph_policy import (
    DBT_SQLSERVER_GRAPH_POLICY_ID,
    DBT_SQLSERVER_GRAPH_POLICY_SHA256,
)
from dpone.contracts.dbt_toolchain import DBT_SQLSERVER_1_12_CERTIFIED
from dpone.gitops.airflow_xcom_from_evidence import (
    write_airflow_xcom_from_evidence,
)
from dpone.gitops.airflow_xcom_outcome import (
    AIRFLOW_RUN_IDENTITY_ENV,
)
from dpone.readiness.dbt_airflow_execution_pack import (
    DbtAirflowExecutionPackBuilder,
)
from dpone.services.dbt_dev_evidence_bundle import (
    DBT_DEV_EVIDENCE_SUBJECTS_FILENAME,
    DbtDevEvidenceBundleError,
    DbtDevEvidenceBundleService,
)
from dpone.services.dbt_dev_evidence_campaign_receipt import (
    DbtDevEvidenceCampaignReceipt,
)
from dpone.services.dbt_dev_evidence_contracts import (
    validate_dbt_execution_evidence_contract,
)
from dpone.services.dbt_dev_evidence_provenance import (
    campaign_request_bytes,
    campaign_request_sha256,
)
from dpone.services.dbt_dev_evidence_request import DbtDevEvidenceRequest
from dpone.services.dbt_dev_evidence_verification import (
    DBT_DEV_EVIDENCE_UNVERIFIED,
    DBT_DEV_EVIDENCE_VERIFIED,
    DbtDevEvidenceVerificationService,
)

DEPLOYMENT_ID = "sha256:" + "b" * 64
TRANSFER_PACK_SHA = "sha256:" + "d" * 64
SECOND_TRANSFER_PACK_SHA = "sha256:" + "8" * 64
PROJECT_SHA = "sha256:" + "1" * 64
MANIFEST_SHA = "sha256:" + "2" * 64
TOOLCHAIN_SHA = DBT_SQLSERVER_1_12_CERTIFIED.sha256
INVOCATION_CONTEXT = DbtInvocationContext.canonical()
GRAPH_CONTRACT_SHA = "sha256:" + "3" * 64
AIRFLOW_RUN_ID = "manual__2026-07-27T00:00:00+00:00"
SECOND_AIRFLOW_RUN_ID = "manual__2026-07-27T01:00:00+00:00"
DAILY_DAG_ID = "DAG__daily_marts"
HOURLY_DAG_ID = "DAG__hourly_marts"
EVIDENCE_SET_ID = "sha256:" + "a" * 64
ACTIVATION_ID = "3f60628e-ef48-48b0-84c3-a9e27a82a7f2"


def _deployment_identity(release_id: str) -> dict[str, str]:
    return {
        "schema": "dpone.airflow-deployment-identity.v1",
        "release_id": release_id,
        "deployment_id": DEPLOYMENT_ID,
        "activation_id": ACTIVATION_ID,
    }


def test_dev_evidence_proves_every_pinned_workload(tmp_path: Path) -> None:
    compiled, evidence, release_id = _valid_evidence_tree(tmp_path)

    report = DbtDevEvidenceVerificationService().verify(
        compiled_root=compiled,
        evidence_root=evidence,
        expected_release_id=release_id,
        expected_deployment_id=DEPLOYMENT_ID,
    )

    assert report.passed
    assert report.code == DBT_DEV_EVIDENCE_VERIFIED
    assert report.required_workloads == ("dbt__daily_marts", "publish_orders")
    assert report.verified_workloads == report.required_workloads
    assert report.verified_dbt_workflows == ("dbt__daily_marts",)


def test_dev_evidence_proves_provider_attempt_set_and_completion_receipt(
    tmp_path: Path,
) -> None:
    compiled, evidence, release_id = _valid_evidence_tree(tmp_path)
    _upgrade_to_attempt_evidence_set(evidence)

    report = DbtDevEvidenceVerificationService().verify(
        compiled_root=compiled,
        evidence_root=evidence,
        expected_release_id=release_id,
        expected_deployment_id=DEPLOYMENT_ID,
    )

    assert report.passed
    assert report.evidence_set_id == EVIDENCE_SET_ID
    assert report.verified_workloads == (
        "dbt__daily_marts",
        "publish_orders",
    )


def test_dev_evidence_rejects_different_requested_evidence_set(
    tmp_path: Path,
) -> None:
    compiled, evidence, release_id = _valid_evidence_tree(tmp_path)
    _upgrade_to_attempt_evidence_set(evidence)

    report = DbtDevEvidenceVerificationService().verify(
        compiled_root=compiled,
        evidence_root=evidence,
        expected_release_id=release_id,
        expected_deployment_id=DEPLOYMENT_ID,
        expected_evidence_set_id="sha256:" + "f" * 64,
    )

    assert not report.passed
    assert report.reason_codes == ("airflow_evidence_set_mismatch",)


def test_dev_evidence_rejects_receipt_inventory_digest_mismatch(
    tmp_path: Path,
) -> None:
    compiled, evidence, release_id = _valid_evidence_tree(tmp_path)
    _upgrade_to_attempt_evidence_set(evidence)
    outcome_path = evidence / "outcomes" / "daily_marts.json"
    outcome = json.loads(outcome_path.read_text(encoding="utf-8"))
    outcome["artifacts"][0]["sha256"] = "sha256:" + "f" * 64
    _write_json(outcome_path, outcome)

    report = DbtDevEvidenceVerificationService().verify(
        compiled_root=compiled,
        evidence_root=evidence,
        expected_release_id=release_id,
        expected_deployment_id=DEPLOYMENT_ID,
    )

    assert not report.passed
    assert report.reason_codes == ("workflow_outcome_evidence_invalid",)


def test_dev_evidence_rejects_tampered_provider_xcom_digest(
    tmp_path: Path,
) -> None:
    compiled, evidence, release_id = _valid_evidence_tree(tmp_path)
    _upgrade_to_attempt_evidence_set(evidence)
    attempt_path = next((evidence / "airflow").glob("sha256-*.json"))
    attempt = json.loads(attempt_path.read_text(encoding="utf-8"))
    attempt["xcom_summary"]["status"] = "failed"
    _write_json(attempt_path, attempt)

    report = DbtDevEvidenceVerificationService().verify(
        compiled_root=compiled,
        evidence_root=evidence,
        expected_release_id=release_id,
        expected_deployment_id=DEPLOYMENT_ID,
    )

    assert not report.passed
    assert "airflow_evidence_contract_invalid" in report.reason_codes


def test_dev_evidence_rejects_attempt_without_activation_identity(
    tmp_path: Path,
) -> None:
    compiled, evidence, release_id = _valid_evidence_tree(tmp_path)
    _upgrade_to_attempt_evidence_set(evidence)
    attempt_path = next((evidence / "airflow").glob("sha256-*.json"))
    attempt = json.loads(attempt_path.read_text(encoding="utf-8"))
    attempt.pop("deployment_identity")
    attempt["xcom_summary"].pop("deployment_identity")
    attempt["xcom_summary_sha256"] = _canonical_sha256(attempt["xcom_summary"])
    _write_json(attempt_path, attempt)

    report = DbtDevEvidenceVerificationService().verify(
        compiled_root=compiled,
        evidence_root=evidence,
        expected_release_id=release_id,
        expected_deployment_id=DEPLOYMENT_ID,
    )

    assert not report.passed
    assert report.reason_codes == ("airflow_evidence_contract_invalid",)


def test_dev_evidence_rejects_workflow_activation_mismatch(
    tmp_path: Path,
) -> None:
    compiled, evidence, release_id = _valid_evidence_tree(tmp_path)
    _upgrade_to_attempt_evidence_set(evidence)
    outcome_path = evidence / "outcomes" / "daily_marts.json"
    outcome = json.loads(outcome_path.read_text(encoding="utf-8"))
    outcome["deployment_identity"]["activation_id"] = "4f60628e-ef48-48b0-84c3-a9e27a82a7f2"
    _write_json(outcome_path, outcome)

    report = DbtDevEvidenceVerificationService().verify(
        compiled_root=compiled,
        evidence_root=evidence,
        expected_release_id=release_id,
        expected_deployment_id=DEPLOYMENT_ID,
    )

    assert not report.passed
    assert report.reason_codes == ("workflow_outcome_evidence_invalid",)


def test_exact_dev_evidence_rejects_legacy_attempt_and_workflow_envelopes(
    tmp_path: Path,
) -> None:
    compiled, evidence, release_id = _valid_evidence_tree(tmp_path)
    _upgrade_to_attempt_evidence_set(evidence)
    for path in (evidence / "airflow").glob("*.json"):
        payload = json.loads(path.read_text(encoding="utf-8"))
        payload["schema"] = "dpone.dbt-airflow-attempt-evidence.v1"
        payload.pop("deployment_identity")
        payload["xcom_summary"].pop("deployment_identity")
        payload["xcom_summary_sha256"] = _canonical_sha256(payload["xcom_summary"])
        _write_json(path, payload)
    outcome_path = evidence / "outcomes" / "daily_marts.json"
    outcome = json.loads(outcome_path.read_text(encoding="utf-8"))
    outcome["schema"] = "dpone.dbt-workflow-evidence-outcome.v1"
    outcome.pop("deployment_identity")
    _write_json(outcome_path, outcome)

    report = DbtDevEvidenceVerificationService().verify(
        compiled_root=compiled,
        evidence_root=evidence,
        expected_release_id=release_id,
        expected_deployment_id=DEPLOYMENT_ID,
        expected_activation_id=ACTIVATION_ID,
        require_exact_activation=True,
    )

    assert not report.passed
    assert report.reason_codes == ("exact_activation_evidence_required",)


def test_exact_dev_evidence_rejects_a_different_expected_activation(
    tmp_path: Path,
) -> None:
    compiled, evidence, release_id = _valid_evidence_tree(tmp_path)
    _upgrade_to_attempt_evidence_set(evidence)

    report = DbtDevEvidenceVerificationService().verify(
        compiled_root=compiled,
        evidence_root=evidence,
        expected_release_id=release_id,
        expected_deployment_id=DEPLOYMENT_ID,
        expected_activation_id="4f60628e-ef48-48b0-84c3-a9e27a82a7f2",
        require_exact_activation=True,
    )

    assert not report.passed
    assert report.reason_codes == ("airflow_deployment_identity_mismatch",)


def test_dev_evidence_rejects_mixed_provider_evidence_sets(
    tmp_path: Path,
) -> None:
    compiled, evidence, release_id = _valid_evidence_tree(tmp_path)
    _upgrade_to_attempt_evidence_set(evidence)
    attempt_path = next((evidence / "airflow").glob("sha256-*.json"))
    attempt = json.loads(attempt_path.read_text(encoding="utf-8"))
    attempt["evidence_set_id"] = "sha256:" + "f" * 64
    _write_json(attempt_path, attempt)

    report = DbtDevEvidenceVerificationService().verify(
        compiled_root=compiled,
        evidence_root=evidence,
        expected_release_id=release_id,
        expected_deployment_id=DEPLOYMENT_ID,
    )

    assert not report.passed
    assert report.evidence_set_id is None


def test_dev_evidence_accepts_exact_correlation_per_workflow(
    tmp_path: Path,
) -> None:
    compiled, evidence, release_id = _valid_two_workflow_evidence_tree(tmp_path)

    report = DbtDevEvidenceVerificationService().verify(
        compiled_root=compiled,
        evidence_root=evidence,
        expected_release_id=release_id,
        expected_deployment_id=DEPLOYMENT_ID,
    )

    assert report.passed
    assert report.required_workloads == (
        "dbt__daily_marts",
        "dbt__hourly_marts",
        "publish_customers",
        "publish_orders",
    )
    assert report.verified_workloads == report.required_workloads
    assert report.verified_dbt_workflows == (
        "dbt__daily_marts",
        "dbt__hourly_marts",
    )


def test_dev_evidence_rejects_cross_workflow_airflow_mixing(
    tmp_path: Path,
) -> None:
    compiled, evidence, release_id = _valid_two_workflow_evidence_tree(tmp_path)
    _write_json(
        evidence / "airflow" / "publish_customers.json",
        _airflow_evidence(
            "publish_customers",
            SECOND_TRANSFER_PACK_SHA,
            release_id,
            dag_id=DAILY_DAG_ID,
            run_id=AIRFLOW_RUN_ID,
        ),
    )

    report = DbtDevEvidenceVerificationService().verify(
        compiled_root=compiled,
        evidence_root=evidence,
        expected_release_id=release_id,
        expected_deployment_id=DEPLOYMENT_ID,
    )

    assert not report.passed
    assert report.reason_codes == ("airflow_evidence_identity_mismatch",)


def test_dev_evidence_rejects_cross_workflow_outcome_mixing(
    tmp_path: Path,
) -> None:
    compiled, evidence, release_id = _valid_two_workflow_evidence_tree(tmp_path)
    path = evidence / "outcomes" / "hourly_marts.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["dag_run_id"] = AIRFLOW_RUN_ID
    _write_json(path, payload)

    report = DbtDevEvidenceVerificationService().verify(
        compiled_root=compiled,
        evidence_root=evidence,
        expected_release_id=release_id,
        expected_deployment_id=DEPLOYMENT_ID,
    )

    assert not report.passed
    assert report.reason_codes == ("workflow_outcome_evidence_invalid",)


def test_dev_evidence_rejects_missing_transfer_attempt(tmp_path: Path) -> None:
    compiled, evidence, release_id = _valid_evidence_tree(tmp_path)
    (evidence / "airflow" / "publish_orders.json").unlink()

    report = DbtDevEvidenceVerificationService().verify(
        compiled_root=compiled,
        evidence_root=evidence,
        expected_release_id=release_id,
        expected_deployment_id=DEPLOYMENT_ID,
    )

    assert not report.passed
    assert report.code == DBT_DEV_EVIDENCE_UNVERIFIED
    assert report.reason_codes == ("airflow_workload_coverage_incomplete",)


def test_dev_evidence_rejects_failed_dbt_execution(tmp_path: Path) -> None:
    compiled, evidence, release_id = _valid_evidence_tree(tmp_path)
    path = evidence / "dbt" / "daily_marts.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload.update(status="failed", dbt_exit_code=1)
    path.write_text(json.dumps(payload), encoding="utf-8")

    report = DbtDevEvidenceVerificationService().verify(
        compiled_root=compiled,
        evidence_root=evidence,
        expected_release_id=release_id,
        expected_deployment_id=DEPLOYMENT_ID,
    )

    assert not report.passed
    assert report.reason_codes == ("dbt_execution_evidence_invalid",)


def test_dev_evidence_cannot_weaken_pack_warning_policy(tmp_path: Path) -> None:
    compiled, evidence, release_id = _valid_evidence_tree(tmp_path)
    path = evidence / "dbt" / "daily_marts.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["dbt_warning_policy"] = "allow"
    payload["dbt_warning_count"] = 1
    payload["nodes"][0]["status"] = "warn"
    _write_json(path, payload)

    report = DbtDevEvidenceVerificationService().verify(
        compiled_root=compiled,
        evidence_root=evidence,
        expected_release_id=release_id,
        expected_deployment_id=DEPLOYMENT_ID,
    )

    assert not report.passed
    assert report.reason_codes == ("dbt_execution_evidence_invalid",)


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("dbt_schema_version", "https://schemas.getdbt.com/dbt/run-results/v5.json"),
        ("dbt_version", "1.11.12"),
        ("invocation_id", None),
    ),
)
def test_dev_evidence_requires_pack_bound_invocation_metadata(
    tmp_path: Path,
    field: str,
    value: object,
) -> None:
    compiled, evidence, release_id = _valid_evidence_tree(tmp_path)
    path = evidence / "dbt" / "daily_marts.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload[field] = value
    _write_json(path, payload)

    report = DbtDevEvidenceVerificationService().verify(
        compiled_root=compiled,
        evidence_root=evidence,
        expected_release_id=release_id,
        expected_deployment_id=DEPLOYMENT_ID,
    )

    assert not report.passed
    assert report.reason_codes == ("dbt_execution_evidence_invalid",)


@pytest.mark.parametrize(
    "field",
    (
        "invocation_context_sha256",
        "logical_target_sha256",
        "target_binding_sha256",
    ),
)
def test_dev_evidence_rejects_tampered_runtime_identity_digest(
    tmp_path: Path,
    field: str,
) -> None:
    compiled, evidence, release_id = _valid_evidence_tree(tmp_path)
    path = evidence / "dbt" / "daily_marts.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload[field] = "sha256:" + "0" * 64
    _write_json(path, payload)

    report = DbtDevEvidenceVerificationService().verify(
        compiled_root=compiled,
        evidence_root=evidence,
        expected_release_id=release_id,
        expected_deployment_id=DEPLOYMENT_ID,
    )

    assert not report.passed
    assert report.reason_codes == ("dbt_execution_evidence_invalid",)


@pytest.mark.parametrize(
    ("field", "value"),
    (
        (
            "adapter_runtime",
            DbtSqlServerRuntimePolicy.for_process_timeout(600).to_dict(),
        ),
        ("adapter_policy_sha256", "sha256:" + "0" * 64),
        ("graph_policy_sha256", "sha256:" + "1" * 64),
    ),
)
def test_dev_evidence_rejects_policy_identity_not_bound_to_release(
    tmp_path: Path,
    field: str,
    value: object,
) -> None:
    compiled, evidence, release_id = _valid_evidence_tree(tmp_path)
    path = evidence / "dbt" / "daily_marts.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload[field] = value
    _write_json(path, payload)

    report = DbtDevEvidenceVerificationService().verify(
        compiled_root=compiled,
        evidence_root=evidence,
        expected_release_id=release_id,
        expected_deployment_id=DEPLOYMENT_ID,
    )

    assert not report.passed
    assert report.reason_codes == ("dbt_execution_evidence_invalid",)


def test_dev_evidence_rejects_duplicate_node_outcomes(tmp_path: Path) -> None:
    compiled, evidence, release_id = _valid_evidence_tree(tmp_path)
    path = evidence / "dbt" / "daily_marts.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["nodes"].append(dict(payload["nodes"][0]))
    _write_json(path, payload)

    report = DbtDevEvidenceVerificationService().verify(
        compiled_root=compiled,
        evidence_root=evidence,
        expected_release_id=release_id,
        expected_deployment_id=DEPLOYMENT_ID,
    )

    assert not report.passed
    assert report.reason_codes == ("dbt_execution_evidence_invalid",)


def test_dev_evidence_rejects_incomplete_airflow_correlation(
    tmp_path: Path,
) -> None:
    compiled, evidence, release_id = _valid_evidence_tree(tmp_path)
    path = evidence / "airflow" / "publish_orders.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["correlation"]["pod"]["uid"] = None
    path.write_text(json.dumps(payload), encoding="utf-8")

    report = DbtDevEvidenceVerificationService().verify(
        compiled_root=compiled,
        evidence_root=evidence,
        expected_release_id=release_id,
        expected_deployment_id=DEPLOYMENT_ID,
    )

    assert not report.passed
    assert report.reason_codes == ("airflow_evidence_identity_mismatch",)


def test_dev_evidence_rejects_symlinked_inventory(tmp_path: Path) -> None:
    compiled, evidence, release_id = _valid_evidence_tree(tmp_path)
    target = evidence / "airflow" / "publish_orders.json"
    target.unlink()
    target.symlink_to(evidence / "airflow" / "dbt__daily_marts.json")

    report = DbtDevEvidenceVerificationService().verify(
        compiled_root=compiled,
        evidence_root=evidence,
        expected_release_id=release_id,
        expected_deployment_id=DEPLOYMENT_ID,
    )

    assert not report.passed
    assert report.reason_codes == ("airflow_evidence_inventory_invalid",)


def test_dev_evidence_rejects_secret_bearing_fields(tmp_path: Path) -> None:
    compiled, evidence, release_id = _valid_evidence_tree(tmp_path)
    path = evidence / "dbt" / "daily_marts.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["password"] = "must-not-be-promoted"
    _write_json(path, payload)

    report = DbtDevEvidenceVerificationService().verify(
        compiled_root=compiled,
        evidence_root=evidence,
        expected_release_id=release_id,
        expected_deployment_id=DEPLOYMENT_ID,
    )

    assert not report.passed
    assert report.reason_codes == ("evidence_secret_field_forbidden",)


@pytest.mark.parametrize(
    "field",
    ("api_key", "authorization", "client_secret", "session_token"),
)
def test_dev_evidence_rejects_all_canonical_secret_aliases(
    tmp_path: Path,
    field: str,
) -> None:
    compiled, evidence, release_id = _valid_evidence_tree(tmp_path)
    path = evidence / "dbt" / "daily_marts.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload[field] = "must-not-be-promoted"
    _write_json(path, payload)

    report = DbtDevEvidenceVerificationService().verify(
        compiled_root=compiled,
        evidence_root=evidence,
        expected_release_id=release_id,
        expected_deployment_id=DEPLOYMENT_ID,
    )

    assert not report.passed
    assert report.reason_codes == ("evidence_secret_field_forbidden",)


def test_dev_evidence_rejects_unknown_dbt_evidence_fields(tmp_path: Path) -> None:
    compiled, evidence, release_id = _valid_evidence_tree(tmp_path)
    path = evidence / "dbt" / "daily_marts.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["unexpected"] = "not part of dpone.dbt-execution-evidence.v1"
    _write_json(path, payload)

    report = DbtDevEvidenceVerificationService().verify(
        compiled_root=compiled,
        evidence_root=evidence,
        expected_release_id=release_id,
        expected_deployment_id=DEPLOYMENT_ID,
    )

    assert not report.passed
    assert report.reason_codes == ("dbt_execution_evidence_invalid",)


def test_dev_evidence_requires_complete_airflow_artifact_set(
    tmp_path: Path,
) -> None:
    compiled, evidence, release_id = _valid_evidence_tree(tmp_path)
    path = evidence / "airflow" / "publish_orders.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["artifacts"] = []
    _write_json(path, payload)

    report = DbtDevEvidenceVerificationService().verify(
        compiled_root=compiled,
        evidence_root=evidence,
        expected_release_id=release_id,
        expected_deployment_id=DEPLOYMENT_ID,
    )

    assert not report.passed
    assert report.reason_codes == ("airflow_evidence_artifact_failed",)


@pytest.mark.parametrize("mutation", ("missing", "optional", "duplicate", "wrong_kind"))
def test_dev_evidence_rejects_required_airflow_artifact_downgrades(
    tmp_path: Path,
    mutation: str,
) -> None:
    compiled, evidence, release_id = _valid_evidence_tree(tmp_path)
    path = evidence / "airflow" / "publish_orders.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    artifacts = payload["artifacts"]
    if mutation == "missing":
        artifacts.pop()
    elif mutation == "optional":
        artifacts[0]["required"] = False
    elif mutation == "duplicate":
        artifacts.append(dict(artifacts[0]))
    else:
        artifacts[0]["actual_kind"] = "gitops.airflow_runtime_evidence"
    _write_json(path, payload)

    report = DbtDevEvidenceVerificationService().verify(
        compiled_root=compiled,
        evidence_root=evidence,
        expected_release_id=release_id,
        expected_deployment_id=DEPLOYMENT_ID,
    )

    assert not report.passed
    assert report.reason_codes == ("airflow_evidence_artifact_failed",)


def test_dev_evidence_binds_top_level_attempt_to_correlation(tmp_path: Path) -> None:
    compiled, evidence, release_id = _valid_evidence_tree(tmp_path)
    path = evidence / "airflow" / "publish_orders.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["attempt"]["try_number"] = 2
    _write_json(path, payload)

    report = DbtDevEvidenceVerificationService().verify(
        compiled_root=compiled,
        evidence_root=evidence,
        expected_release_id=release_id,
        expected_deployment_id=DEPLOYMENT_ID,
    )

    assert not report.passed
    assert report.reason_codes == ("airflow_evidence_identity_mismatch",)


def test_dev_evidence_rejects_mixed_airflow_runs(tmp_path: Path) -> None:
    compiled, evidence, release_id = _valid_evidence_tree(tmp_path)
    path = evidence / "airflow" / "publish_orders.json"
    foreign_run = "manual__2026-07-27T01:00:00+00:00"
    _write_json(
        path,
        _airflow_evidence(
            "publish_orders",
            TRANSFER_PACK_SHA,
            release_id,
            run_id=foreign_run,
        ),
    )

    report = DbtDevEvidenceVerificationService().verify(
        compiled_root=compiled,
        evidence_root=evidence,
        expected_release_id=release_id,
        expected_deployment_id=DEPLOYMENT_ID,
    )

    assert not report.passed
    assert report.reason_codes == ("airflow_evidence_run_mismatch",)


def test_dev_evidence_binds_dbt_evidence_to_airflow_attempt(tmp_path: Path) -> None:
    compiled, evidence, release_id = _valid_evidence_tree(tmp_path)
    path = evidence / "dbt" / "daily_marts.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["airflow"]["try_number"] = 2
    _write_json(path, payload)

    report = DbtDevEvidenceVerificationService().verify(
        compiled_root=compiled,
        evidence_root=evidence,
        expected_release_id=release_id,
        expected_deployment_id=DEPLOYMENT_ID,
    )

    assert not report.passed
    assert report.reason_codes == ("dbt_execution_evidence_invalid",)


def test_dev_evidence_binds_outcome_to_airflow_run(tmp_path: Path) -> None:
    compiled, evidence, release_id = _valid_evidence_tree(tmp_path)
    path = evidence / "outcomes" / "daily_marts.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["dag_run_id"] = "manual__2026-07-27T01:00:00+00:00"
    _write_json(path, payload)

    report = DbtDevEvidenceVerificationService().verify(
        compiled_root=compiled,
        evidence_root=evidence,
        expected_release_id=release_id,
        expected_deployment_id=DEPLOYMENT_ID,
    )

    assert not report.passed
    assert report.reason_codes == ("workflow_outcome_evidence_invalid",)


def test_dev_evidence_rejects_release_digest_or_node_substitution(tmp_path: Path) -> None:
    compiled, evidence, release_id = _valid_evidence_tree(tmp_path)
    path = evidence / "dbt" / "daily_marts.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["project_bundle_sha256"] = "sha256:" + "9" * 64
    payload["nodes"] = payload["nodes"][:-1]
    _write_json(path, payload)

    report = DbtDevEvidenceVerificationService().verify(
        compiled_root=compiled,
        evidence_root=evidence,
        expected_release_id=release_id,
        expected_deployment_id=DEPLOYMENT_ID,
    )

    assert not report.passed
    assert report.reason_codes == ("dbt_execution_evidence_invalid",)


def test_dev_evidence_requires_successful_terminal_workflow_outcome(tmp_path: Path) -> None:
    compiled, evidence, release_id = _valid_evidence_tree(tmp_path)
    path = evidence / "outcomes" / "daily_marts.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["status"] = "failed"
    payload["code"] = "DPONE_DBT_WORKFLOW_FAILED"
    payload["tasks"][0]["state"] = "failed"
    _write_json(path, payload)

    report = DbtDevEvidenceVerificationService().verify(
        compiled_root=compiled,
        evidence_root=evidence,
        expected_release_id=release_id,
        expected_deployment_id=DEPLOYMENT_ID,
    )

    assert not report.passed
    assert report.reason_codes == ("workflow_outcome_evidence_invalid",)


def test_dev_evidence_rejects_incomplete_terminal_workflow_task_set(
    tmp_path: Path,
) -> None:
    compiled, evidence, release_id = _valid_evidence_tree(tmp_path)
    path = evidence / "outcomes" / "daily_marts.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["tasks"] = [
        {
            "task_id": "different_task",
            "state": "success",
        }
    ]
    _write_json(path, payload)

    report = DbtDevEvidenceVerificationService().verify(
        compiled_root=compiled,
        evidence_root=evidence,
        expected_release_id=release_id,
        expected_deployment_id=DEPLOYMENT_ID,
    )

    assert not report.passed
    assert report.reason_codes == ("workflow_outcome_evidence_invalid",)


def test_dev_evidence_bundle_is_atomic_idempotent_and_tamper_evident(
    tmp_path: Path,
) -> None:
    compiled, evidence, release_id = _valid_evidence_tree(tmp_path)
    output = tmp_path / "trusted-evidence"
    service = DbtDevEvidenceBundleService()

    created = service.finalize(
        compiled_root=compiled,
        source_evidence_root=evidence,
        output_root=output,
        expected_release_id=release_id,
        expected_deployment_id=DEPLOYMENT_ID,
        producer_repository="PaulKov/airflow-dev",
        producer_workflow="dbt-dev-evidence.yml",
        source_commit="8" * 40,
    )
    repeated = service.finalize(
        compiled_root=compiled,
        source_evidence_root=evidence,
        output_root=output,
        expected_release_id=release_id,
        expected_deployment_id=DEPLOYMENT_ID,
        producer_repository="PaulKov/airflow-dev",
        producer_workflow="dbt-dev-evidence.yml",
        source_commit="8" * 40,
    )

    assert created.no_op is False
    assert repeated.no_op is True
    assert repeated.subject_sha256 == created.subject_sha256
    assert (output / DBT_DEV_EVIDENCE_SUBJECTS_FILENAME).is_file()

    path = output / "dbt" / "daily_marts.json"
    path.write_text(path.read_text(encoding="utf-8") + "\n", encoding="utf-8")
    with pytest.raises(DbtDevEvidenceBundleError):
        service.verify(
            compiled_root=compiled,
            evidence_root=output,
            expected_release_id=release_id,
            expected_deployment_id=DEPLOYMENT_ID,
        )


def test_dev_evidence_bundle_v2_binds_campaign_identity(
    tmp_path: Path,
) -> None:
    compiled, evidence, release_id = _valid_evidence_tree(tmp_path)
    request = _campaign_request(compiled, release_id)
    _upgrade_to_attempt_evidence_set(evidence, request=request)
    _close_campaign(evidence, request)
    output = tmp_path / "trusted-evidence"
    service = DbtDevEvidenceBundleService()

    report = service.finalize(
        compiled_root=compiled,
        source_evidence_root=evidence,
        output_root=output,
        expected_release_id=release_id,
        expected_deployment_id=DEPLOYMENT_ID,
        expected_evidence_set_id=request.evidence_set_id,
        expected_activation_id=ACTIVATION_ID,
        campaign_request=request,
        producer_repository="PaulKov/airflow-dev",
        producer_workflow="dbt-dev-evidence.yml",
        source_commit="8" * 40,
    )

    payload = report.to_dict()
    provenance = json.loads((output / "provenance.json").read_text(encoding="utf-8"))
    assert payload["schema"] == "dpone.dbt-dev-evidence-bundle.v2"
    assert payload["evidence_set_id"] == request.evidence_set_id
    assert payload["campaign_request_sha256"] == campaign_request_sha256(request)
    assert provenance["schema"] == "dpone.dbt-dev-evidence-provenance.v2"
    assert provenance["evidence_set_id"] == request.evidence_set_id
    assert provenance["campaign_request_sha256"] == campaign_request_sha256(request)
    with pytest.raises(DbtDevEvidenceBundleError):
        service.verify(
            compiled_root=compiled,
            evidence_root=output,
            expected_release_id=release_id,
            expected_deployment_id=DEPLOYMENT_ID,
            expected_evidence_set_id="sha256:" + "f" * 64,
            expected_activation_id=ACTIVATION_ID,
        )


def test_runtime_writer_provider_verifier_and_finalizer_form_one_chain(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    compiled, legacy, release_id = _valid_evidence_tree(tmp_path)
    request = _campaign_request(compiled, release_id)
    workflow = request.workflows[0]
    shared = tmp_path / "shared-evidence"
    shared.mkdir()
    journal = ConfinedDbtDevEvidenceCampaignJournal(shared)
    journal.open(request)
    spool = shared / "dbt-spool"
    spool.mkdir()

    dbt_payload = json.loads((legacy / "dbt" / "daily_marts.json").read_text(encoding="utf-8"))
    dbt_payload["airflow"]["run_id"] = workflow.dag_run_id
    dbt_evidence = validate_dbt_execution_evidence_contract(dbt_payload)
    local_evidence = tmp_path / "runtime" / "dbt-evidence.json"
    CampaignDbtExecutionEvidenceWriter(
        LocalDbtExecutionEvidenceWriter(local_evidence),
        evidence_root=str(spool),
        evidence_set_id=request.evidence_set_id,
    ).write(dbt_evidence)

    dbt_identity = json.loads((legacy / "airflow" / "dbt__daily_marts.json").read_text(encoding="utf-8"))[
        "run_identity"
    ]
    transfer_identity = json.loads((legacy / "airflow" / "publish_orders.json").read_text(encoding="utf-8"))[
        "run_identity"
    ]
    monkeypatch.setenv(
        AIRFLOW_RUN_IDENTITY_ENV,
        json.dumps(dbt_identity),
    )
    deployment_identity = _deployment_identity(release_id)
    monkeypatch.setenv(
        "DPONE_AIRFLOW_DEPLOYMENT_IDENTITY",
        json.dumps(deployment_identity),
    )
    xcom_path = tmp_path / "xcom" / "return.json"
    write_airflow_xcom_from_evidence(
        evidence_path=local_evidence,
        xcom_output=xcom_path,
        status="passed",
    )
    summaries = {
        "dbt__daily_marts__dpone_runtime": json.loads(xcom_path.read_text(encoding="utf-8")),
        "publish_orders__dpone_runtime": _xcom_summary(transfer_identity),
    }
    summaries["publish_orders__dpone_runtime"]["deployment_identity"] = deployment_identity
    context = {key: value for key, value in dbt_identity.items() if key != "workload_pack"}
    context["_workload_pack_sha256"] = {
        "dbt__daily_marts": dbt_identity["workload_pack"]["sha256"],
        "publish_orders": TRANSFER_PACK_SHA,
    }
    context["_activation_id"] = ACTIVATION_ID
    task_instances = [
        SimpleNamespace(
            task_id=task_id,
            state="success",
            try_number=1,
            map_index=-1,
        )
        for task_id in summaries
    ]
    dag_run = SimpleNamespace(
        dag_id=workflow.dag_id,
        run_id=workflow.dag_run_id,
        conf={
            "dpone_evidence_authority": {
                "schema": "dpone.dbt-dev-evidence-authority.v1",
                "request_id": request.evidence_set_id,
                "evidence_set_id": request.evidence_set_id,
                "release_id": release_id,
                "deployment_id": DEPLOYMENT_ID,
                "workflow_id": workflow.workflow_id,
                "dag_id": workflow.dag_id,
                "dag_run_id": workflow.dag_run_id,
            }
        },
        get_task_instances=lambda: task_instances,
    )

    class _TaskInstance:
        def xcom_pull(
            self,
            *,
            task_ids: str,
            key: str | None = None,
        ) -> object:
            assert key in (None, "return_value")
            return summaries[task_ids]

    source_outcome = json.loads((legacy / "outcomes" / "daily_marts.json").read_text(encoding="utf-8"))
    source_outcome["dag_run_id"] = workflow.dag_run_id
    source_outcome["schema"] = "dpone.dbt-workflow-outcome.v2"
    source_outcome["deployment_identity"] = deployment_identity
    export_report = export_dev_evidence_if_requested(
        workflow_id=workflow.workflow_id,
        workflow_outcome=source_outcome,
        workload_tasks=(
            {
                "workload_id": "dbt__daily_marts",
                "runtime_task_id": ("dbt__daily_marts__dpone_runtime"),
            },
            {
                "workload_id": "publish_orders",
                "runtime_task_id": ("publish_orders__dpone_runtime"),
            },
        ),
        run_identity_context=context,
        ti=_TaskInstance(),
        dag_run=dag_run,
        evidence_root=shared,
    )
    journal.close(
        DbtDevEvidenceCampaignReceipt.build(
            request=request,
            status="passed",
            code="DPONE_DBT_DEV_EVIDENCE_CAMPAIGN_PASSED",
            workflow_states={workflow.workflow_id: "success"},
        )
    )

    output = tmp_path / "trusted-evidence"
    report = DbtDevEvidenceBundleService().finalize(
        compiled_root=compiled,
        source_evidence_root=Path(str(export_report["evidence_root"])),
        output_root=output,
        expected_release_id=release_id,
        expected_deployment_id=DEPLOYMENT_ID,
        expected_evidence_set_id=request.evidence_set_id,
        expected_activation_id=ACTIVATION_ID,
        campaign_request=request,
        producer_repository="PaulKov/airflow-dev",
        producer_workflow="dbt-self-service-dev-evidence.yml",
        source_commit="8" * 40,
    )

    assert report.evidence_set_id == request.evidence_set_id
    assert report.verified_workloads == (
        "dbt__daily_marts",
        "publish_orders",
    )
    assert len(tuple((output / "airflow").glob("sha256-*.json"))) == 2


def test_bundle_rejects_failed_campaign_closure(
    tmp_path: Path,
) -> None:
    compiled, evidence, release_id = _valid_evidence_tree(tmp_path)
    request = _campaign_request(compiled, release_id)
    _upgrade_to_attempt_evidence_set(evidence, request=request)
    (evidence / "campaign-request.json").write_bytes(campaign_request_bytes(request))
    (evidence / "campaign-outcome.json").write_bytes(
        DbtDevEvidenceCampaignReceipt.build(
            request=request,
            status="failed",
            code="DPONE_DBT_DEV_EVIDENCE_CAMPAIGN_FAILED",
            workflow_states={workflow.workflow_id: "failed" for workflow in request.workflows},
        ).to_bytes()
    )

    with pytest.raises(
        DbtDevEvidenceBundleError,
        match="terminal success",
    ):
        DbtDevEvidenceBundleService().finalize(
            compiled_root=compiled,
            source_evidence_root=evidence,
            output_root=tmp_path / "trusted-evidence",
            expected_release_id=release_id,
            expected_deployment_id=DEPLOYMENT_ID,
            expected_evidence_set_id=request.evidence_set_id,
            expected_activation_id=ACTIVATION_ID,
            campaign_request=request,
            producer_repository="PaulKov/airflow-dev",
            producer_workflow="dbt-self-service-dev-evidence.yml",
            source_commit="8" * 40,
        )


def test_dev_evidence_bundle_rejects_ambiguous_source_and_bad_provenance(
    tmp_path: Path,
) -> None:
    compiled, evidence, release_id = _valid_evidence_tree(tmp_path)
    (evidence / "unexpected.json").write_text("{}\n", encoding="utf-8")

    with pytest.raises(DbtDevEvidenceBundleError):
        DbtDevEvidenceBundleService().finalize(
            compiled_root=compiled,
            source_evidence_root=evidence,
            output_root=tmp_path / "trusted-evidence",
            expected_release_id=release_id,
            expected_deployment_id=DEPLOYMENT_ID,
            producer_repository="PaulKov/airflow-dev",
            producer_workflow="dbt-dev-evidence.yml",
            source_commit="main",
        )

    assert not (tmp_path / "trusted-evidence").exists()


def test_dev_evidence_bundle_rejects_unknown_provenance_fields(
    tmp_path: Path,
) -> None:
    compiled, evidence, release_id = _valid_evidence_tree(tmp_path)
    output = tmp_path / "trusted-evidence"
    service = DbtDevEvidenceBundleService()
    service.finalize(
        compiled_root=compiled,
        source_evidence_root=evidence,
        output_root=output,
        expected_release_id=release_id,
        expected_deployment_id=DEPLOYMENT_ID,
        producer_repository="PaulKov/airflow-dev",
        producer_workflow="dbt-dev-evidence.yml",
        source_commit="8" * 40,
    )
    provenance = output / "provenance.json"
    payload = json.loads(provenance.read_text(encoding="utf-8"))
    payload["untrusted"] = True
    _write_json(provenance, payload)

    with pytest.raises(DbtDevEvidenceBundleError):
        service.verify(
            compiled_root=compiled,
            evidence_root=output,
            expected_release_id=release_id,
            expected_deployment_id=DEPLOYMENT_ID,
        )


def _valid_evidence_tree(tmp_path: Path) -> tuple[Path, Path, str]:
    compiled = tmp_path / "compiled"
    compiled.mkdir()
    selection = DbtSelectionLock.build(
        manifest_sha256=MANIFEST_SHA,
        toolchain_sha256=TOOLCHAIN_SHA,
        invocation_context_sha256=(INVOCATION_CONTEXT.invocation_context_sha256),
        graph_contract_sha256=GRAPH_CONTRACT_SHA,
        graph_policy_id=DBT_SQLSERVER_GRAPH_POLICY_ID,
        graph_policy_sha256=DBT_SQLSERVER_GRAPH_POLICY_SHA256,
        selectors=("+fqn:analytics.orders",),
        selected_graph_unique_ids=(
            "model.analytics.orders",
            "test.analytics.orders_not_null",
        ),
        expected_run_result_unique_ids=(
            "model.analytics.orders",
            "test.analytics.orders_not_null",
        ),
        publish_model_unique_ids=("model.analytics.orders",),
    )
    selection_path = "runtime/dbt/daily_marts.selection-lock.json"
    (compiled / "runtime" / "dbt").mkdir(parents=True)
    _write_json(compiled / selection_path, selection.to_dict())
    selection_bytes = (compiled / selection_path).read_bytes()
    dbt_pack_bytes, dbt_pack_sha, dbt_pack_fingerprint = _write_dbt_workload_pack(
        compiled,
        workflow_id="daily_marts",
        selection=selection,
    )
    logical_target_sha = dbt_target_identity_sha256(
        _dbt_profile_spec(),
    )
    dag_path = "dags/DAG__daily_marts.dag-spec.json"
    (compiled / "dags").mkdir()
    _write_json(
        compiled / dag_path,
        {
            "kind": "gitops.airflow_dag_spec",
            "dag_id": DAILY_DAG_ID,
            "source": {"type": "dbt", "workflow": "daily_marts"},
            "workflow_outcome": {
                "schema": "dpone.dbt-workflow-outcome.v1",
                "task_id": "workflow_outcome",
                "workflow_id": "daily_marts",
                "expected_terminal_task_ids": ["publish_orders__outcome_gate"],
            },
            "nodes": [
                {
                    "node_id": "dbt__daily_marts",
                    "workload_id": "dbt__daily_marts",
                },
                {
                    "node_id": "publish_orders",
                    "workload_id": "publish_orders",
                },
            ],
        },
    )
    dag_bytes = (compiled / dag_path).read_bytes()
    release = {
        "schema": "dpone.release-set.v2",
        "release_id": "",
        "producer": {
            "dpone_version": "0.73.20",
            "wire_contract": "dpone.dbt-airflow-self-service.v1",
        },
        "artifacts": {
            "dag_specs": [
                {
                    "id": "DAG__daily_marts",
                    "path": dag_path,
                    "sha256": sha256_bytes(dag_bytes),
                    "bytes": len(dag_bytes),
                }
            ],
            "workload_packs": [
                {
                    "id": "dbt__daily_marts",
                    "path": "packs/dbt__daily_marts.airflow-pack.json",
                    "sha256": dbt_pack_sha,
                    "bytes": len(dbt_pack_bytes),
                    "pack_fingerprint": dbt_pack_fingerprint,
                },
                {
                    "id": "publish_orders",
                    "path": "packs/publish_orders.airflow-pack.json",
                    "sha256": TRANSFER_PACK_SHA,
                    "bytes": 100,
                    "pack_fingerprint": "sha256:" + "5" * 64,
                },
            ],
            "canonical_schemas": [],
            "runtime_payloads": [
                {
                    "id": "dbt_project",
                    "kind": "dbt_project_bundle",
                    "path": "runtime/dbt/project.tar.gz",
                    "sha256": PROJECT_SHA,
                    "bytes": 100,
                    "media_type": "application/vnd.dpone.dbt-project-bundle+gzip",
                },
                {
                    "id": "dbt_manifest",
                    "kind": "dbt_manifest",
                    "path": "runtime/dbt/manifest.json",
                    "sha256": MANIFEST_SHA,
                    "bytes": 100,
                    "media_type": "application/vnd.dbt.manifest+json",
                },
                {
                    "id": "dbt_selection_daily_marts",
                    "kind": "dbt_selection_lock",
                    "path": selection_path,
                    "sha256": sha256_bytes(selection_bytes),
                    "bytes": len(selection_bytes),
                    "media_type": "application/vnd.dpone.dbt-selection-lock+json",
                },
            ],
        },
        "selection_authority": "dbt_cli",
        "selection_fingerprint": "sha256:" + "7" * 64,
        "provenance": {},
    }
    release_id = compute_release_id(release)
    release["release_id"] = release_id
    _write_json(
        compiled / "release-set.json",
        release,
    )
    evidence = tmp_path / "evidence"
    (evidence / "airflow").mkdir(parents=True)
    (evidence / "dbt").mkdir()
    (evidence / "outcomes").mkdir()
    _write_json(
        evidence / "airflow" / "dbt__daily_marts.json",
        _airflow_evidence("dbt__daily_marts", dbt_pack_sha, release_id),
    )
    _write_json(
        evidence / "airflow" / "publish_orders.json",
        _airflow_evidence("publish_orders", TRANSFER_PACK_SHA, release_id),
    )
    _write_json(
        evidence / "dbt" / "daily_marts.json",
        {
            "schema": "dpone.dbt-execution-evidence.v1",
            "status": "passed",
            "code": "DPONE_DBT_EXECUTION_PASSED",
            "workflow_id": "daily_marts",
            "release_id": release_id,
            "deployment_id": DEPLOYMENT_ID,
            "workload_pack_sha256": dbt_pack_sha,
            "project_bundle_sha256": PROJECT_SHA,
            "manifest_sha256": MANIFEST_SHA,
            "selection_sha256": selection.selection_sha256,
            "toolchain_sha256": TOOLCHAIN_SHA,
            "invocation_context_sha256": (INVOCATION_CONTEXT.invocation_context_sha256),
            "adapter_runtime": DbtSqlServerRuntimePolicy.for_process_timeout(3600).to_dict(),
            "adapter_policy_sha256": (DbtSqlServerAdapterPolicy.canonical().adapter_policy_sha256),
            "graph_policy_sha256": selection.graph_policy_sha256,
            "logical_target_sha256": logical_target_sha,
            "target_binding_sha256": dbt_target_binding_identity_sha256(
                logical_target_sha256=logical_target_sha,
                run_identity=_airflow_run_identity(
                    "dbt__daily_marts",
                    dbt_pack_sha,
                    release_id,
                ),
            ),
            "preflight_status": "passed",
            "build_started": True,
            "dbt_exit_code": 0,
            "dbt_warning_policy": "fail",
            "dbt_warning_count": 0,
            "dbt_schema_version": "https://schemas.getdbt.com/dbt/run-results/v6.json",
            "dbt_version": "1.12.3",
            "invocation_id": "invocation-daily-marts",
            "started_at": "2026-07-27T00:00:00+00:00",
            "finished_at": "2026-07-27T00:01:00+00:00",
            "airflow": {
                "dag_id": DAILY_DAG_ID,
                "task_id": "dbt__daily_marts__dpone_runtime",
                "run_id": AIRFLOW_RUN_ID,
                "try_number": 1,
                "map_index": -1,
            },
            "credential_versions": [],
            "nodes": [
                {"unique_id": unique_id, "status": status, "execution_time": 0.1}
                for unique_id, status in zip(
                    selection.expected_run_result_unique_ids,
                    ("success", "pass"),
                    strict=True,
                )
            ],
        },
    )
    _write_json(
        evidence / "outcomes" / "daily_marts.json",
        {
            "schema": "dpone.dbt-workflow-outcome.v1",
            "status": "passed",
            "code": "DPONE_DBT_WORKFLOW_PASSED",
            "workflow_id": "daily_marts",
            "release_id": release_id,
            "deployment_id": DEPLOYMENT_ID,
            "dag_run_id": AIRFLOW_RUN_ID,
            "tasks": [
                {
                    "task_id": "publish_orders__outcome_gate",
                    "state": "success",
                },
            ],
        },
    )
    return compiled, evidence, release_id


def _valid_two_workflow_evidence_tree(
    tmp_path: Path,
) -> tuple[Path, Path, str]:
    compiled, evidence, _ = _valid_evidence_tree(tmp_path)
    second_selection = DbtSelectionLock.build(
        manifest_sha256=MANIFEST_SHA,
        toolchain_sha256=TOOLCHAIN_SHA,
        invocation_context_sha256=(INVOCATION_CONTEXT.invocation_context_sha256),
        graph_contract_sha256=GRAPH_CONTRACT_SHA,
        graph_policy_id=DBT_SQLSERVER_GRAPH_POLICY_ID,
        graph_policy_sha256=DBT_SQLSERVER_GRAPH_POLICY_SHA256,
        selectors=("+fqn:analytics.customers",),
        selected_graph_unique_ids=(
            "model.analytics.customers",
            "test.analytics.customers_not_null",
        ),
        expected_run_result_unique_ids=(
            "model.analytics.customers",
            "test.analytics.customers_not_null",
        ),
        publish_model_unique_ids=("model.analytics.customers",),
    )
    selection_path = "runtime/dbt/hourly_marts.selection-lock.json"
    _write_json(compiled / selection_path, second_selection.to_dict())
    selection_bytes = (compiled / selection_path).read_bytes()
    second_pack_bytes, second_pack_sha, second_pack_fingerprint = _write_dbt_workload_pack(
        compiled,
        workflow_id="hourly_marts",
        selection=second_selection,
    )

    dag_path = "dags/DAG__hourly_marts.dag-spec.json"
    _write_json(
        compiled / dag_path,
        {
            "kind": "gitops.airflow_dag_spec",
            "dag_id": HOURLY_DAG_ID,
            "source": {"type": "dbt", "workflow": "hourly_marts"},
            "workflow_outcome": {
                "schema": "dpone.dbt-workflow-outcome.v1",
                "task_id": "workflow_outcome",
                "workflow_id": "hourly_marts",
                "expected_terminal_task_ids": ["publish_customers__outcome_gate"],
            },
            "nodes": [
                {
                    "node_id": "dbt__hourly_marts",
                    "workload_id": "dbt__hourly_marts",
                },
                {
                    "node_id": "publish_customers",
                    "workload_id": "publish_customers",
                },
            ],
        },
    )
    dag_bytes = (compiled / dag_path).read_bytes()

    release_path = compiled / "release-set.json"
    release = json.loads(release_path.read_text(encoding="utf-8"))
    artifacts = release["artifacts"]
    artifacts["dag_specs"].append(
        {
            "id": HOURLY_DAG_ID,
            "path": dag_path,
            "sha256": sha256_bytes(dag_bytes),
            "bytes": len(dag_bytes),
        }
    )
    artifacts["workload_packs"].extend(
        [
            {
                "id": "dbt__hourly_marts",
                "path": "packs/dbt__hourly_marts.airflow-pack.json",
                "sha256": second_pack_sha,
                "bytes": len(second_pack_bytes),
                "pack_fingerprint": second_pack_fingerprint,
            },
            {
                "id": "publish_customers",
                "path": "packs/publish_customers.airflow-pack.json",
                "sha256": SECOND_TRANSFER_PACK_SHA,
                "bytes": 100,
                "pack_fingerprint": "sha256:" + "a" * 64,
            },
        ]
    )
    artifacts["runtime_payloads"].append(
        {
            "id": "dbt_selection_hourly_marts",
            "kind": "dbt_selection_lock",
            "path": selection_path,
            "sha256": sha256_bytes(selection_bytes),
            "bytes": len(selection_bytes),
            "media_type": "application/vnd.dpone.dbt-selection-lock+json",
        }
    )
    release_id = compute_release_id(release)
    release["release_id"] = release_id
    _write_json(release_path, release)

    daily_pack_sha = next(item["sha256"] for item in artifacts["workload_packs"] if item["id"] == "dbt__daily_marts")
    _write_json(
        evidence / "airflow" / "dbt__daily_marts.json",
        _airflow_evidence(
            "dbt__daily_marts",
            daily_pack_sha,
            release_id,
            dag_id=DAILY_DAG_ID,
        ),
    )
    _write_json(
        evidence / "airflow" / "publish_orders.json",
        _airflow_evidence(
            "publish_orders",
            TRANSFER_PACK_SHA,
            release_id,
            dag_id=DAILY_DAG_ID,
        ),
    )
    _write_json(
        evidence / "airflow" / "dbt__hourly_marts.json",
        _airflow_evidence(
            "dbt__hourly_marts",
            second_pack_sha,
            release_id,
            dag_id=HOURLY_DAG_ID,
            run_id=SECOND_AIRFLOW_RUN_ID,
        ),
    )
    _write_json(
        evidence / "airflow" / "publish_customers.json",
        _airflow_evidence(
            "publish_customers",
            SECOND_TRANSFER_PACK_SHA,
            release_id,
            dag_id=HOURLY_DAG_ID,
            run_id=SECOND_AIRFLOW_RUN_ID,
        ),
    )

    daily_dbt_path = evidence / "dbt" / "daily_marts.json"
    daily_dbt = json.loads(daily_dbt_path.read_text(encoding="utf-8"))
    daily_dbt["release_id"] = release_id
    _write_json(daily_dbt_path, daily_dbt)
    _write_json(
        evidence / "dbt" / "hourly_marts.json",
        {
            **daily_dbt,
            "workflow_id": "hourly_marts",
            "workload_pack_sha256": second_pack_sha,
            "selection_sha256": second_selection.selection_sha256,
            "invocation_id": "invocation-hourly-marts",
            "airflow": {
                "dag_id": HOURLY_DAG_ID,
                "task_id": "dbt__hourly_marts__dpone_runtime",
                "run_id": SECOND_AIRFLOW_RUN_ID,
                "try_number": 1,
                "map_index": -1,
            },
            "nodes": [
                {
                    "unique_id": unique_id,
                    "status": status,
                    "execution_time": 0.1,
                }
                for unique_id, status in zip(
                    second_selection.expected_run_result_unique_ids,
                    ("success", "pass"),
                    strict=True,
                )
            ],
        },
    )

    daily_outcome_path = evidence / "outcomes" / "daily_marts.json"
    daily_outcome = json.loads(daily_outcome_path.read_text(encoding="utf-8"))
    daily_outcome["release_id"] = release_id
    _write_json(daily_outcome_path, daily_outcome)
    _write_json(
        evidence / "outcomes" / "hourly_marts.json",
        {
            **daily_outcome,
            "workflow_id": "hourly_marts",
            "dag_run_id": SECOND_AIRFLOW_RUN_ID,
            "tasks": [
                {
                    "task_id": "publish_customers__outcome_gate",
                    "state": "success",
                }
            ],
        },
    )
    return compiled, evidence, release_id


def _airflow_evidence(
    workload_id: str,
    pack_sha256: str,
    release_id: str,
    *,
    dag_id: str = DAILY_DAG_ID,
    run_id: str = AIRFLOW_RUN_ID,
) -> dict[str, object]:
    identity = _airflow_run_identity(
        workload_id,
        pack_sha256,
        release_id,
        dag_id=dag_id,
    )
    attempt = AirflowAttemptCorrelation(
        dag_id=dag_id,
        task_id=f"{workload_id}__dpone_runtime",
        run_id=run_id,
        try_number=1,
        map_index=-1,
    )
    correlation = build_airflow_correlation(
        run_identity=identity,
        attempt=attempt,
        dpone_run_id=f"run-{workload_id}",
        dpone_process=workload_id,
        runtime_evidence_sha256="sha256:" + "1" * 64,
        pod={
            "name": f"pod-{workload_id}",
            "uid": f"uid-{workload_id}",
            "namespace": "airflow-dev",
            "image_digest": "sha256:" + "f" * 64,
        },
    )
    return {
        "kind": "gitops.airflow_evidence_bundle",
        "schema_version": "1",
        "producer": "dpone gitops airflow evidence-bundle",
        "runner_policy": "release",
        "attempt": attempt.to_dict(),
        "pod": {
            "pod_name": f"pod-{workload_id}",
            "pod_uid": f"uid-{workload_id}",
            "namespace": "airflow-dev",
            "service_account": "dpone-runtime",
            "image": "registry.example/dpone-runtime",
            "image_digest": "sha256:" + "f" * 64,
        },
        "artifacts": _required_airflow_artifacts(),
        "run_identity": identity.to_dict(),
        "correlation": correlation.to_dict(),
        "warnings": [],
        "blockers": [],
    }


def _airflow_run_identity(
    workload_id: str,
    pack_sha256: str,
    release_id: str,
    *,
    dag_id: str = DAILY_DAG_ID,
) -> AirflowRunIdentity:
    return AirflowRunIdentity(
        release_id=release_id,
        deployment_id=DEPLOYMENT_ID,
        workload_pack=AirflowArtifactIdentity(workload_id, pack_sha256),
        dag_spec=AirflowArtifactIdentity(
            dag_id,
            "sha256:" + "e" * 64,
        ),
        runtime_image_digest="sha256:" + "f" * 64,
    )


def _upgrade_to_attempt_evidence_set(
    evidence: Path,
    *,
    request: DbtDevEvidenceRequest | None = None,
) -> None:
    evidence_set_id = request.evidence_set_id if request is not None else EVIDENCE_SET_ID
    dag_runs = {item.dag_id: item.dag_run_id for item in request.workflows} if request is not None else {}
    inventory: list[dict[str, object]] = []
    for path in sorted((evidence / "airflow").glob("*.json")):
        legacy = json.loads(path.read_text(encoding="utf-8"))
        workload_id = legacy["run_identity"]["workload_pack"]["id"]
        attempt = dict(legacy["attempt"])
        attempt["run_id"] = dag_runs.get(
            str(attempt["dag_id"]),
            attempt["run_id"],
        )
        summary = _xcom_summary(legacy["run_identity"])
        deployment_identity = _deployment_identity(legacy["run_identity"]["release_id"])
        summary["deployment_identity"] = deployment_identity
        envelope = {
            "schema": "dpone.dbt-airflow-attempt-evidence.v2",
            "status": "passed",
            "evidence_set_id": evidence_set_id,
            "run_identity": legacy["run_identity"],
            "attempt": attempt,
            "xcom_summary_sha256": _canonical_sha256(summary),
            "xcom_summary": summary,
            "deployment_identity": deployment_identity,
        }
        canonical_path = (
            evidence / "airflow" / ("sha256-" + hashlib.sha256(str(workload_id).encode("utf-8")).hexdigest() + ".json")
        )
        path.unlink()
        _write_json(canonical_path, envelope)
        payload = _canonical_bytes(envelope)
        inventory.append(
            {
                "category": "airflow",
                "logical_id": workload_id,
                "sha256": _sha256(payload),
                "bytes": len(payload),
            }
        )
    dbt_path = evidence / "dbt" / "daily_marts.json"
    dbt_evidence = json.loads(dbt_path.read_text(encoding="utf-8"))
    if request is not None:
        dbt_evidence["airflow"]["run_id"] = dag_runs[DAILY_DAG_ID]
        _write_json(dbt_path, dbt_evidence)
    dbt_payload = canonical_dbt_execution_evidence_bytes(dbt_evidence)
    inventory.append(
        {
            "category": "dbt",
            "logical_id": "daily_marts",
            "sha256": _sha256(dbt_payload),
            "bytes": len(dbt_payload),
        }
    )
    outcome_path = evidence / "outcomes" / "daily_marts.json"
    outcome = json.loads(outcome_path.read_text(encoding="utf-8"))
    if request is not None:
        outcome["dag_run_id"] = dag_runs[DAILY_DAG_ID]
    outcome.update(
        schema="dpone.dbt-workflow-evidence-outcome.v2",
        evidence_set_id=evidence_set_id,
        artifacts=sorted(
            inventory,
            key=lambda item: (str(item["category"]), str(item["logical_id"])),
        ),
        deployment_identity=_deployment_identity(outcome["release_id"]),
    )
    _write_json(outcome_path, outcome)


def _campaign_request(
    compiled: Path,
    release_id: str,
) -> DbtDevEvidenceRequest:
    return DbtDevEvidenceRequest.build(
        compiled_root=compiled,
        release_id=release_id,
        deployment_id=DEPLOYMENT_ID,
        producer_repository="PaulKov/airflow-dev",
        producer_workflow="dbt-self-service-dev-evidence.yml",
        source_commit="7" * 40,
        orchestration_run_id="12345",
        orchestration_run_attempt=1,
    )


def _close_campaign(
    evidence: Path,
    request: DbtDevEvidenceRequest,
) -> None:
    (evidence / "campaign-request.json").write_bytes(campaign_request_bytes(request))
    receipt = DbtDevEvidenceCampaignReceipt.build(
        request=request,
        status="passed",
        code="DPONE_DBT_DEV_EVIDENCE_CAMPAIGN_PASSED",
        workflow_states={workflow.workflow_id: "success" for workflow in request.workflows},
    )
    (evidence / "campaign-outcome.json").write_bytes(receipt.to_bytes())


def _write_dbt_workload_pack(
    compiled: Path,
    *,
    workflow_id: str,
    selection: DbtSelectionLock,
) -> tuple[bytes, str, str]:
    execution = DbtExecutionPack.build(
        workflow_id=workflow_id,
        project_bundle_sha256=PROJECT_SHA,
        project_subdir="dbt-project",
        target_path="target",
        profile=_dbt_profile_spec(),
        selection_lock=selection,
        invocation_context=INVOCATION_CONTEXT,
        adapter_runtime=DbtSqlServerRuntimePolicy.for_process_timeout(3600),
        adapter_policy=DbtSqlServerAdapterPolicy.canonical(),
        dbt_warning_policy="fail",
        timeout_seconds=3600,
    )
    pack = DbtAirflowExecutionPackBuilder().build(
        workflow_id=workflow_id,
        execution_pack=execution,
        runtime_payload_ids=(
            "dbt_project",
            "dbt_manifest",
            f"dbt_selection_{workflow_id}",
        ),
        xcom_sidecar_image=("registry.example/dpone-xcom@sha256:" + "e" * 64),
        pool="dpone_dbt",
    )
    payload = _canonical_bytes(pack)
    path = compiled / "packs" / f"dbt__{workflow_id}.airflow-pack.json"
    path.parent.mkdir(exist_ok=True)
    path.write_bytes(payload)
    return payload, sha256_bytes(payload), str(pack["pack_fingerprint"])


def _dbt_profile_spec() -> DbtProfileSpec:
    return DbtProfileSpec(
        profile_name="analytics",
        target_name="runtime",
        connection_ref="mssql_source",
        adapter_type="sqlserver",
        database="analytics",
        schema="mart",
        threads=4,
    )


def _xcom_summary(run_identity: object) -> dict[str, object]:
    return {
        "kind": "gitops.airflow_xcom_summary",
        "schema_version": "1",
        "producer": "dpone gitops airflow run-spec-exec",
        "status": "passed",
        "runtime_profile_path": "runtime-profile.json",
        "run_spec_path": "run-spec.json",
        "runtime_evidence_path": "runtime-evidence.json",
        "runtime_evidence_sha256": "sha256:" + "0" * 64,
        "runtime_evidence": {
            "schema_version": "dpone.airflow.inline_runtime_evidence.v1",
            "status": "passed",
            "metrics": {"duration_seconds": 1.0, "step_count": 1},
            "step_timeline": [],
            "warnings": [],
        },
        "failed_step": None,
        "warnings": [],
        "blockers": [],
        "run_identity": run_identity,
    }


def _canonical_sha256(payload: object) -> str:
    return _sha256(_canonical_bytes(payload))


def _canonical_bytes(payload: object) -> bytes:
    return (
        json.dumps(
            payload,
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")


def _sha256(payload: bytes) -> str:
    return "sha256:" + hashlib.sha256(payload).hexdigest()


def _required_airflow_artifacts() -> list[dict[str, object]]:
    kinds = {
        "bundle": "gitops.bundle",
        "run_spec": "gitops.airflow_run_spec",
        "runtime_profile": "gitops.airflow_runtime_profile",
        "pod_contract": "gitops.airflow_pod_contract",
        "runtime_evidence": "gitops.airflow_runtime_evidence",
        "xcom_summary": "gitops.airflow_xcom_summary",
    }
    return [
        {
            "name": name,
            "path": f"{name}.json",
            "expected_kind": kind,
            "actual_kind": kind,
            "required": True,
            "exists": True,
            "sha256": str(index) * 64,
            "bytes": 100,
            "passed": True,
            "reason": "passed",
        }
        for index, (name, kind) in enumerate(kinds.items(), start=1)
    ]


def _write_json(path: Path, payload: object) -> None:
    path.write_text(json.dumps(payload), encoding="utf-8")
