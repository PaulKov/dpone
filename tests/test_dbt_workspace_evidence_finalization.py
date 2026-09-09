"""Complete two-project provider evidence, not live database certification."""

from __future__ import annotations

import json

import pytest
from dpone_airflow_pack.dev_evidence_export_contract import canonical_bytes, inventory_item
from dpone_airflow_pack.dev_evidence_store import logical_evidence_filename

from dpone.app.dbt_promotion_composition import (
    build_dbt_dev_evidence_bundle_service,
    build_dbt_dev_evidence_verification_service,
)
from dpone.services.dbt_dev_evidence_bundle import DbtDevEvidenceBundleError
from tests.dbt_workspace_evidence_helpers import workspace_evidence
from tests.test_dbt_dev_evidence_verification import ACTIVATION_ID


def _arguments(compiled, source, request, output):
    return dict(
        compiled_root=compiled,
        source_evidence_root=source,
        output_root=output,
        expected_release_id=request.release_id,
        expected_deployment_id=request.deployment_id,
        expected_evidence_set_id=request.evidence_set_id,
        expected_activation_id=ACTIVATION_ID,
        campaign_request=request,
        producer_repository="example/repo",
        producer_workflow=".github/workflows/accept.yml",
        source_commit="a" * 40,
    )


def _assert_semantic_reason(compiled, source, request, reason):
    report = build_dbt_dev_evidence_verification_service().verify(
        compiled_root=compiled,
        evidence_root=source,
        expected_release_id=request.release_id,
        expected_deployment_id=request.deployment_id,
        expected_evidence_set_id=request.evidence_set_id,
        expected_campaign_request=request,
        expected_activation_id=ACTIVATION_ID,
        require_exact_activation=True,
    )
    assert not report.passed
    assert report.reason_codes == (reason,)


def test_finalizes_every_project_transfer_and_terminal_outcome_idempotently(tmp_path, monkeypatch) -> None:
    compiled, source, _, request = workspace_evidence(tmp_path, monkeypatch)
    arguments = _arguments(compiled, source, request, tmp_path / "trusted")
    service = build_dbt_dev_evidence_bundle_service()
    first = service.finalize(**arguments)
    second = service.finalize(**arguments)
    assert first.evidence_set_id == request.evidence_set_id
    assert first.subject_sha256 == second.subject_sha256
    assert first.no_op is False and second.no_op is True
    assert set(first.verified_workloads) == {"dbt__alpha", "publish_alpha", "dbt__beta", "publish_beta"}
    verified = service.verify(
        compiled_root=compiled,
        evidence_root=arguments["output_root"],
        expected_release_id=request.release_id,
        expected_deployment_id=request.deployment_id,
        expected_evidence_set_id=request.evidence_set_id,
        expected_activation_id=ACTIVATION_ID,
        expected_campaign_request=request,
    )
    assert verified.subject_sha256 == first.subject_sha256


@pytest.mark.parametrize(
    ("category", "logical_id"),
    [("airflow", "dbt__beta"), ("airflow", "publish_beta"), ("dbt", "beta"), ("outcomes", "beta")],
)
def test_project_a_cannot_substitute_for_missing_project_b(tmp_path, monkeypatch, category, logical_id) -> None:
    compiled, source, _, request = workspace_evidence(tmp_path, monkeypatch)
    (source / category / logical_evidence_filename(logical_id)).unlink()
    _assert_semantic_reason(
        compiled,
        source,
        request,
        {
            "airflow": "airflow_workload_coverage_incomplete",
            "dbt": "dbt_workflow_coverage_incomplete",
            "outcomes": "workflow_outcome_coverage_incomplete",
        }[category],
    )
    output = tmp_path / "trusted"
    with pytest.raises(DbtDevEvidenceBundleError, match="does not prove the complete release workflow"):
        build_dbt_dev_evidence_bundle_service().finalize(**_arguments(compiled, source, request, output))
    assert not output.exists()
    assert not list(tmp_path.glob(".trusted.*"))


@pytest.mark.parametrize("mutation", ["project", "results", "failed_terminal", "wrong_attempt"])
def test_rehashed_beta_evidence_cannot_hide_semantic_failure(tmp_path, monkeypatch, mutation) -> None:
    compiled, source, _, request = workspace_evidence(tmp_path, monkeypatch)
    outcome_path = source / "outcomes" / logical_evidence_filename("beta")
    outcome = json.loads(outcome_path.read_bytes())
    if mutation == "failed_terminal":
        outcome["tasks"][0]["state"] = "failed"
    else:
        dbt_path = source / "dbt" / logical_evidence_filename("beta")
        dbt = json.loads(dbt_path.read_bytes())
        if mutation == "project":
            alpha = json.loads((source / "dbt" / logical_evidence_filename("alpha")).read_bytes())
            dbt["project_bundle_sha256"] = alpha["project_bundle_sha256"]
        elif mutation == "results":
            dbt["nodes"][0]["unique_id"] = "model.analytics.other"
        else:
            dbt["airflow"]["try_number"] = 2
        payload = canonical_bytes(dbt)
        dbt_path.write_bytes(payload)
        outcome["artifacts"] = [
            inventory_item(category="dbt", logical_id="beta", payload=payload) if item["category"] == "dbt" else item
            for item in outcome["artifacts"]
        ]
    outcome_path.write_bytes(canonical_bytes(outcome))
    _assert_semantic_reason(
        compiled,
        source,
        request,
        "workflow_outcome_evidence_invalid" if mutation == "failed_terminal" else "dbt_execution_evidence_invalid",
    )
    output = tmp_path / "trusted"
    with pytest.raises(DbtDevEvidenceBundleError, match="does not prove the complete release workflow"):
        build_dbt_dev_evidence_bundle_service().finalize(**_arguments(compiled, source, request, output))
    assert not output.exists()
