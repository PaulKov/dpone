from __future__ import annotations

import json
import math
from pathlib import Path

import pytest

from dpone.adapters.dbt_artifacts import LocalDbtExecutionEvidenceWriter, LocalDbtRunResultsReader
from dpone.contracts.dbt_publishing import (
    DbtExecutionEvidence,
    DbtNodeOutcome,
    DbtPublishingError,
)
from dpone.services.dbt_dev_evidence_contracts import (
    DbtDevEvidenceContractError,
    read_evidence_object,
)
from dpone.services.dbt_prod_mirror import DbtProdMirrorError
from dpone.services.dbt_prod_promotion_contract import dbt_json_object


@pytest.mark.parametrize(
    "secret_text",
    (
        "password=hunter2",
        "Authorization: Bearer token-value",
        "https://example.invalid/path?X-Amz-Signature=abc123",
        "-----BEGIN PRIVATE KEY-----\nabc\n-----END PRIVATE KEY-----",
    ),
)
def test_evidence_reader_rejects_secret_patterns_in_safe_named_values(
    tmp_path: Path,
    secret_text: str,
) -> None:
    path = tmp_path / "evidence.json"
    path.write_text(json.dumps({"reason": secret_text}), encoding="utf-8")

    with pytest.raises(DbtDevEvidenceContractError) as exc:
        read_evidence_object(tmp_path, path.name, max_bytes=4096)

    assert exc.value.reason == "evidence_secret_value_forbidden"


@pytest.mark.parametrize("constant", ("NaN", "Infinity", "-Infinity"))
def test_evidence_and_run_results_readers_reject_non_finite_json(
    tmp_path: Path,
    constant: str,
) -> None:
    evidence = tmp_path / "evidence.json"
    evidence.write_text(f'{{"value":{constant}}}', encoding="utf-8")
    run_results = tmp_path / "run_results.json"
    run_results.write_text(f'{{"elapsed_time":{constant}}}', encoding="utf-8")

    with pytest.raises(DbtDevEvidenceContractError):
        read_evidence_object(tmp_path, evidence.name, max_bytes=4096)
    with pytest.raises(DbtPublishingError):
        LocalDbtRunResultsReader().read(run_results, root=tmp_path, max_bytes=4096)


@pytest.mark.parametrize(
    "payload",
    (
        '{"results":[],"results":[]}',
        '{"elapsed_time":1e999}',
    ),
)
def test_run_results_reader_rejects_ambiguous_json(
    tmp_path: Path,
    payload: str,
) -> None:
    run_results = tmp_path / "run_results.json"
    run_results.write_text(payload, encoding="utf-8")

    with pytest.raises(DbtPublishingError) as exc:
        LocalDbtRunResultsReader().read(run_results, root=tmp_path, max_bytes=4096)

    assert exc.value.code == "DPONE_DBT_RESULTS_INVALID"


@pytest.mark.parametrize(
    "payload",
    (
        b'{"release_id":"first","release_id":"second"}',
        b'{"release_id":NaN}',
        b'{"release_id":1e999}',
    ),
)
def test_promotion_metadata_rejects_ambiguous_json(payload: bytes) -> None:
    with pytest.raises(DbtProdMirrorError):
        dbt_json_object(payload)


@pytest.mark.parametrize("value", (math.nan, math.inf, -math.inf))
def test_node_outcome_rejects_non_finite_execution_time(value: float) -> None:
    with pytest.raises(DbtPublishingError):
        DbtNodeOutcome(
            unique_id="model.project.orders",
            status="success",
            execution_time=value,
        )


def test_execution_evidence_rejects_contradictory_success() -> None:
    with pytest.raises(DbtPublishingError):
        _execution_evidence(status="passed", code="DPONE_DBT_EXECUTION_FAILED", exit_code=17)


def test_execution_evidence_writer_rejects_non_finite_json_before_publication(
    tmp_path: Path,
) -> None:
    evidence = _execution_evidence()
    object.__setattr__(evidence.nodes[0], "execution_time", math.nan)

    with pytest.raises(DbtPublishingError):
        LocalDbtExecutionEvidenceWriter(tmp_path / "evidence.json").write(evidence)

    assert not (tmp_path / "evidence.json").exists()


def _execution_evidence(
    *,
    status: str = "passed",
    code: str = "DPONE_DBT_EXECUTION_PASSED",
    exit_code: int = 0,
) -> DbtExecutionEvidence:
    from dpone.contracts.dbt_publishing import (
        DbtSqlServerAdapterPolicy,
        DbtSqlServerRuntimePolicy,
    )
    from dpone.contracts.dbt_sqlserver_graph_policy import (
        DBT_SQLSERVER_GRAPH_POLICY_SHA256,
    )

    digest = "sha256:" + "a" * 64
    return DbtExecutionEvidence(
        status=status,
        code=code,
        workflow_id="daily_marts",
        release_id=digest,
        deployment_id=digest,
        workload_pack_sha256=digest,
        project_bundle_sha256=digest,
        manifest_sha256=digest,
        selection_sha256=digest,
        toolchain_sha256=digest,
        invocation_context_sha256=digest,
        logical_target_sha256=digest,
        target_binding_sha256=digest,
        adapter_runtime=DbtSqlServerRuntimePolicy.for_process_timeout(3600),
        adapter_policy_sha256=(DbtSqlServerAdapterPolicy.canonical().adapter_policy_sha256),
        graph_policy_sha256=DBT_SQLSERVER_GRAPH_POLICY_SHA256,
        preflight_status="passed",
        build_started=True,
        dbt_exit_code=exit_code,
        dbt_warning_policy="fail",
        dbt_warning_count=0,
        dbt_schema_version="v6",
        dbt_version="1.12.3",
        invocation_id="invocation-id",
        started_at="2026-07-27T00:00:00+00:00",
        finished_at="2026-07-27T00:01:00+00:00",
        airflow={
            "dag_id": "DAG__daily_marts",
            "task_id": "dbt__daily_marts__dpone_runtime",
            "run_id": "manual__2026-07-27",
            "try_number": 1,
            "map_index": -1,
        },
        credential_versions=(),
        nodes=(DbtNodeOutcome("model.project.orders", "success", 0.1),),
    )
