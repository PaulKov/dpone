"""Pure expectations preserve the legacy reader's diagnostics and source scope."""

import json
import subprocess
import sys

import pytest

from dpone.contracts.airflow_deployment import release_id as compute_release_id
from dpone.contracts.dbt_contract_validation import canonical_fingerprint, sha256_bytes
from dpone.services.dbt_dev_evidence_release import DbtDevEvidenceReleaseError, load_expected_dbt_release
from tests.test_dbt_dev_evidence_verification import _valid_evidence_tree


def _save(root, release):
    release["release_id"] = compute_release_id(release)
    (root / "release-set.json").write_text(json.dumps(release))
    return release["release_id"]


def _replace_artifact(root, descriptor, value):
    body = json.dumps(value).encode()
    (root / descriptor["path"]).write_bytes(body)
    descriptor.update(sha256=sha256_bytes(body), bytes=len(body))


def test_legacy_expectations_do_not_require_v2_complete_source_bytes(tmp_path):
    root, _, release_id = _valid_evidence_tree(tmp_path)
    assert not (root / "runtime/dbt/project.tar.gz").exists()
    assert not (root / "runtime/dbt/manifest.json").exists()
    expected = load_expected_dbt_release(root, release_id)
    assert set(expected.dbt_workflows) == {"daily_marts"}
    assert set(expected.required_workloads) == {"dbt__daily_marts", "publish_orders"}


@pytest.mark.parametrize("other_defect", [False, True])
def test_dag_failure_precedes_missing_project_descriptor(tmp_path, other_defect):
    root, _, _ = _valid_evidence_tree(tmp_path)
    release = json.loads((root / "release-set.json").read_bytes())
    dag = release["artifacts"]["dag_specs"][0]
    (root / dag["path"]).write_bytes(b"corrupt")
    if other_defect:
        release["artifacts"]["runtime_payloads"].pop(0)
    with pytest.raises(DbtDevEvidenceReleaseError, match="^DAG spec descriptor differs from DAG spec bytes$"):
        load_expected_dbt_release(root, _save(root, release))


@pytest.mark.parametrize("other_defect", [False, True])
def test_selection_failure_precedes_bad_execution_pack(tmp_path, other_defect):
    root, _, _ = _valid_evidence_tree(tmp_path)
    release = json.loads((root / "release-set.json").read_bytes())
    selection = release["artifacts"]["runtime_payloads"][2]
    (root / selection["path"]).write_bytes(b"corrupt")
    if other_defect:
        pack = release["artifacts"]["workload_packs"][0]
        (root / pack["path"]).write_bytes(b"corrupt")
    with pytest.raises(DbtDevEvidenceReleaseError, match="^selection descriptor differs from selection lock bytes$"):
        load_expected_dbt_release(root, _save(root, release))


@pytest.mark.parametrize("other_defect", [False, True])
def test_execution_selection_mismatch_precedes_missing_workflow_dag(tmp_path, other_defect):
    root, _, _ = _valid_evidence_tree(tmp_path)
    release = json.loads((root / "release-set.json").read_bytes())
    descriptor = release["artifacts"]["runtime_payloads"][2]
    selection = json.loads((root / descriptor["path"]).read_bytes())
    selection["graph_contract_sha256"] = "sha256:" + "c" * 64
    selection["selection_sha256"] = canonical_fingerprint(
        {key: value for key, value in selection.items() if key != "selection_sha256"}
    )
    _replace_artifact(root, descriptor, selection)
    if other_defect:
        descriptor = release["artifacts"]["dag_specs"][0]
        dag = json.loads((root / descriptor["path"]).read_bytes())
        dag["source"]["workflow"] = "other"
        dag["workflow_outcome"]["workflow_id"] = "other"
        _replace_artifact(root, descriptor, dag)
    with pytest.raises(DbtDevEvidenceReleaseError, match="^dbt execution pack differs from release selection$"):
        load_expected_dbt_release(root, _save(root, release))


def test_expectation_policy_has_no_service_or_sdk_imports():
    result = subprocess.run(
        [
            sys.executable,
            "-I",
            "-c",
            "from dpone.contracts.dbt_release_expectations import DbtLegacyEvidencePlan; "
            "import sys; "
            "assert not any(n.startswith(('dpone.services.', 'dpone.adapters.', 'dbt.', 'airflow.', "
            "'dpone_airflow_pack.')) for n in sys.modules)",
        ],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    assert result.returncode == 0, result.stderr


def test_service_reexports_same_expected_types():
    from dpone.contracts import dbt_release_expectations as policy
    from dpone.services import dbt_dev_evidence_release as reader

    assert reader.ExpectedDbtWorkflow is policy.ExpectedDbtWorkflow
    assert reader.ExpectedDbtRelease is policy.ExpectedDbtRelease
    assert reader.DbtDevEvidenceReleaseError is policy.DbtDevEvidenceReleaseError
