"""Release matching is independent of envelope acquisition and SDK validation."""

import hashlib
import json
from dataclasses import replace

import pytest

from dpone.contracts.airflow_correlation import AirflowAttemptCorrelation, parse_airflow_correlation
from dpone.contracts.airflow_run_identity import AirflowArtifactIdentity, AirflowRunIdentity
from dpone.contracts.dbt_runtime import dbt_target_binding_identity_sha256
from dpone.services.dbt_dev_evidence_release import load_expected_dbt_release
from dpone.services.dbt_dev_evidence_verification import _verified_airflow_evidence
from dpone.services.dbt_dev_evidence_workflow_verification import EvidenceViolation
from tests.test_dbt_dev_evidence_verification import (
    DEPLOYMENT_ID,
    _airflow_evidence,
    _canonical_bytes,
    _canonical_sha256,
    _upgrade_to_attempt_evidence_set,
    _valid_evidence_tree,
)


@pytest.fixture
def binding_inputs(tmp_path):
    root, evidence, release_id = _valid_evidence_tree(tmp_path)
    expected = load_expected_dbt_release(root, release_id)
    workflow = expected.dbt_workflows["daily_marts"]
    payload = _airflow_evidence(workflow.workload_id, workflow.workload_pack_sha256, release_id)
    return (
        expected,
        evidence,
        payload,
        dict(
            workflow=workflow,
            identity=AirflowRunIdentity.from_mapping(payload["run_identity"]),
            attempt=AirflowAttemptCorrelation.from_mapping(payload["attempt"]),
            correlation=parse_airflow_correlation(payload["correlation"]),
            release_id=release_id,
            deployment_id=DEPLOYMENT_ID,
            expected_pack_sha256=workflow.workload_pack_sha256,
            duplicate_workload=False,
        ),
    )


@pytest.mark.parametrize("legacy_correlation", [False, True])
def test_release_binding_preserves_target_identity(binding_inputs, legacy_correlation):
    from dpone.contracts.dbt_release_expectations import airflow_evidence_target_binding

    _, _, _, args = binding_inputs
    if not legacy_correlation:
        args["correlation"] = None
    expected = dbt_target_binding_identity_sha256(
        logical_target_sha256=args["workflow"].logical_target_sha256, run_identity=args["identity"]
    )
    assert airflow_evidence_target_binding(**args) == expected


@pytest.mark.parametrize(
    "mutation",
    [
        "release",
        "deployment",
        "pack",
        "missing-workflow",
        "dag",
        "missing-dag",
        "attempt-dag",
        "duplicate",
        "correlation-incomplete",
        "correlation-workload",
        "correlation-release",
        "correlation-deployment",
        "correlation-attempt",
    ],
)
def test_release_binding_rejects_each_semantic_mismatch(binding_inputs, mutation):
    from dpone.contracts.dbt_release_expectations import airflow_evidence_target_binding

    _, _, _, args = binding_inputs
    if mutation in {"release", "deployment"}:
        args[mutation + "_id"] = "sha256:" + "9" * 64
    elif mutation == "pack":
        args["expected_pack_sha256"] = "sha256:" + "9" * 64
    elif mutation == "missing-workflow":
        args["workflow"] = None
    elif mutation == "dag":
        args["identity"] = replace(args["identity"], dag_spec=AirflowArtifactIdentity("other", "sha256:" + "9" * 64))
    elif mutation == "missing-dag":
        args["identity"] = replace(args["identity"], dag_spec=None)
    elif mutation == "attempt-dag":
        args["attempt"] = replace(args["attempt"], dag_id="other")
    elif mutation == "duplicate":
        args["duplicate_workload"] = True
    else:
        correlation = args["correlation"]
        if mutation == "correlation-incomplete":
            correlation = replace(correlation, pod=replace(correlation.pod, uid=None))
        elif mutation == "correlation-attempt":
            correlation = replace(correlation, airflow=replace(correlation.airflow, try_number=2))
        else:
            field = mutation.removeprefix("correlation-") + "_id"
            value = "other" if field == "workload_id" else "sha256:" + "9" * 64
            correlation = replace(correlation, artifacts=replace(correlation.artifacts, **{field: value}))
        args["correlation"] = correlation
    assert airflow_evidence_target_binding(**args) is None


@pytest.mark.parametrize("version", ["legacy", "v1", "v2"])
def test_service_keeps_full_envelope_ascii_bytes_and_exact_mode(binding_inputs, version):
    expected, evidence, payload, args = binding_inputs
    if version != "legacy":
        _upgrade_to_attempt_evidence_set(evidence)
        payload = next(
            row
            for row in (json.loads(path.read_bytes()) for path in (evidence / "airflow").glob("*.json"))
            if row["run_identity"]["workload_pack"]["id"] == args["workflow"].workload_id
        )
        payload["attempt"]["run_id"] += "__тест"
        if version == "v1":
            payload["schema"] = "dpone.dbt-airflow-attempt-evidence.v1"
            payload.pop("deployment_identity")
            payload["xcom_summary"].pop("deployment_identity")
            payload["xcom_summary_sha256"] = _canonical_sha256(payload["xcom_summary"])
    else:
        payload["pod"]["pod_name"] += "__тест"
    verified = _verify(expected, (payload,), exact=version == "v2")
    item = verified[args["workflow"].workload_id]
    body = _canonical_bytes(payload)
    assert b"\\u0442" in body and body.endswith(b"\n")
    assert item.evidence_bytes == len(body)
    assert item.evidence_sha256 == "sha256:" + hashlib.sha256(body).hexdigest()
    assert item.exact_activation == (version == "v2")


def _verify(expected, payloads, *, exact=False, required=None):
    return _verified_airflow_evidence(
        payloads,
        required=expected.required_workloads if required is None else required,
        expected=expected.dbt_workflows,
        release_id=expected.release_id,
        deployment_id=DEPLOYMENT_ID,
        require_exact_activation=exact,
    )


def test_membership_is_checked_before_any_payload(binding_inputs):
    expected, _, _, _ = binding_inputs
    with pytest.raises(EvidenceViolation) as caught:
        _verify(expected, ({},), required={})
    assert caught.value.reason == "release_evidence_contract_invalid"


def test_first_payload_binding_failure_precedes_second_envelope_failure(binding_inputs):
    expected, _, _, args = binding_inputs
    payload = _airflow_evidence(
        args["workflow"].workload_id, args["workflow"].workload_pack_sha256, "sha256:" + "9" * 64
    )
    with pytest.raises(EvidenceViolation) as caught:
        _verify(expected, (payload, {}))
    assert caught.value.reason == "airflow_evidence_identity_mismatch"


def test_duplicate_preserves_identity_mismatch_reason(binding_inputs):
    expected, _, payload, _ = binding_inputs
    with pytest.raises(EvidenceViolation) as caught:
        _verify(expected, (payload, payload))
    assert caught.value.reason == "airflow_evidence_identity_mismatch"
