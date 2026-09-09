from __future__ import annotations

import json
from pathlib import Path

import pytest

from dpone.adapters.dbt_artifacts import (
    CampaignDbtExecutionEvidenceWriter,
    LocalDbtExecutionEvidenceWriter,
)
from dpone.contracts.dbt_publishing import (
    DbtExecutionEvidence,
    DbtNodeOutcome,
    DbtPublishingError,
    DbtSqlServerAdapterPolicy,
    DbtSqlServerRuntimePolicy,
)
from dpone.contracts.dbt_sqlserver_graph_policy import (
    DBT_SQLSERVER_GRAPH_POLICY_SHA256,
)

RELEASE_ID = "sha256:" + "a" * 64
DEPLOYMENT_ID = "sha256:" + "b" * 64
EVIDENCE_SET_ID = "sha256:" + "c" * 64


def _evidence() -> DbtExecutionEvidence:
    return DbtExecutionEvidence(
        status="passed",
        code="DPONE_DBT_EXECUTION_PASSED",
        workflow_id="daily_marts",
        release_id=RELEASE_ID,
        deployment_id=DEPLOYMENT_ID,
        workload_pack_sha256="sha256:" + "d" * 64,
        project_bundle_sha256="sha256:" + "e" * 64,
        manifest_sha256="sha256:" + "f" * 64,
        selection_sha256="sha256:" + "1" * 64,
        toolchain_sha256="sha256:" + "2" * 64,
        invocation_context_sha256="sha256:" + "3" * 64,
        logical_target_sha256="sha256:" + "4" * 64,
        target_binding_sha256="sha256:" + "5" * 64,
        adapter_runtime=DbtSqlServerRuntimePolicy.for_process_timeout(3600),
        adapter_policy_sha256=(DbtSqlServerAdapterPolicy.canonical().adapter_policy_sha256),
        graph_policy_sha256=DBT_SQLSERVER_GRAPH_POLICY_SHA256,
        preflight_status="passed",
        build_started=True,
        dbt_exit_code=0,
        dbt_warning_policy="fail",
        dbt_warning_count=0,
        dbt_schema_version="v6",
        dbt_version="1.12.3",
        invocation_id="invocation",
        started_at="2026-07-27T00:00:00+00:00",
        finished_at="2026-07-27T00:01:00+00:00",
        airflow={
            "dag_id": "DAG__daily_marts",
            "task_id": "dbt__daily_marts__dpone_runtime",
            "run_id": "manual__evidence",
            "try_number": 1,
            "map_index": -1,
        },
        credential_versions=(),
        nodes=(
            DbtNodeOutcome(
                unique_id="model.analytics.orders",
                status="success",
                execution_time=1.0,
            ),
        ),
    )


def _writer(tmp_path: Path, shared_root: Path, *, set_id: str | None) -> CampaignDbtExecutionEvidenceWriter:
    return CampaignDbtExecutionEvidenceWriter(
        LocalDbtExecutionEvidenceWriter(tmp_path / "local.json"),
        evidence_root=str(shared_root),
        evidence_set_id=set_id,
    )


def test_runtime_writer_exports_exact_evidence_only_for_campaign(tmp_path: Path) -> None:
    shared_root = tmp_path / "shared"
    shared_root.mkdir()

    local_path = _writer(tmp_path, shared_root, set_id=None).write(_evidence())

    assert local_path.is_file()
    assert not tuple(shared_root.iterdir())

    _writer(tmp_path, shared_root, set_id=EVIDENCE_SET_ID).write(_evidence())
    exported = tuple(shared_root.rglob("*.json"))
    assert len(exported) == 1
    assert json.loads(exported[0].read_text(encoding="utf-8"))["workflow_id"] == "daily_marts"


def test_runtime_writer_is_byte_idempotent_and_conflicts_fail_closed(tmp_path: Path) -> None:
    shared_root = tmp_path / "shared"
    shared_root.mkdir()
    writer = _writer(tmp_path, shared_root, set_id=EVIDENCE_SET_ID)
    writer.write(_evidence())
    writer.write(_evidence())

    exported = next(shared_root.rglob("*.json"))
    exported.write_text('{"status":"failed"}\n', encoding="utf-8")

    with pytest.raises(DbtPublishingError) as raised:
        writer.write(_evidence())
    assert raised.value.code == "DPONE_DBT_DEV_EVIDENCE_EXPORT_FAILED"


def test_runtime_writer_rejects_symlinked_shared_root(tmp_path: Path) -> None:
    actual = tmp_path / "actual"
    actual.mkdir()
    link = tmp_path / "shared"
    link.symlink_to(actual, target_is_directory=True)

    with pytest.raises(DbtPublishingError) as raised:
        _writer(tmp_path, link, set_id=EVIDENCE_SET_ID).write(_evidence())
    assert raised.value.code == "DPONE_DBT_DEV_EVIDENCE_EXPORT_FAILED"
