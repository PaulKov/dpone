"""Offline two-project source → writer → provider-export evidence fixture.

Task states and database outcomes are synthetic. Production contract producers
write the evidence; this helper is not a live route or runtime certification.
"""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

from dpone_airflow_pack.dev_evidence_export import export_dev_evidence_if_requested
from dpone_airflow_pack.pack_identity import PACK_IDENTITY_SCHEMA, compute_pack_fingerprint

from dpone.adapters.dbt_artifacts import CampaignDbtExecutionEvidenceWriter, LocalDbtExecutionEvidenceWriter
from dpone.adapters.dbt_dev_evidence_campaign_journal import ConfinedDbtDevEvidenceCampaignJournal
from dpone.app.dbt_promotion_composition import build_dbt_expected_release_loader
from dpone.contracts.airflow_run_identity import AirflowArtifactIdentity, AirflowRunIdentity
from dpone.contracts.dbt_contract_validation import sha256_bytes
from dpone.contracts.dbt_execution_evidence import DbtExecutionEvidence, DbtNodeOutcome
from dpone.contracts.dbt_runtime import dbt_target_binding_identity_sha256
from dpone.gitops.airflow_compact_pack_bootstrap import RuntimePayloadBuilder
from dpone.gitops.airflow_xcom_from_evidence import write_airflow_xcom_from_evidence
from dpone.gitops.airflow_xcom_outcome import AIRFLOW_RUN_IDENTITY_ENV
from dpone.services.dbt_dev_evidence_campaign_receipt import DbtDevEvidenceCampaignReceipt
from dpone.services.dbt_dev_evidence_request import DbtDevEvidenceRequest
from tests.test_dbt_dev_evidence_verification import ACTIVATION_ID, DEPLOYMENT_ID, _deployment_identity, _xcom_summary
from tests.test_dbt_release_source_reader import _json, _seal, _tree, _write


def _add_transfer_fixtures(root: Path, release: dict) -> None:
    for dag in release["artifacts"]["dag_specs"]:
        value = json.loads((root / dag["path"]).read_bytes())
        workflow = value["source"]["workflow"]
        workload_id = f"publish_{workflow}"
        # Source/evidence fixture with an exact embedded, nonconflicting sink;
        # empty steps are deliberate: this is not executed or live-certified.
        manifest_path = f"_dbt/manifests/{workload_id}.yaml"
        manifest = {
            "name": workload_id,
            "sink": {
                "type": "clickhouse",
                "connection_ref": "sink-main",
                "table": {"schema": f"delivery_{workflow}", "name": "orders"},
            },
        }
        pack = {
            "kind": "gitops.airflow_pack",
            "schema_version": "3",
            "pack_identity": {"schema": PACK_IDENTITY_SCHEMA},
            "workload": {"workload_id": workload_id, "manifest": manifest_path},
            "runtime_payload": RuntimePayloadBuilder(
                repo_root=root, paths=(), generated_files={manifest_path: _json(manifest)}
            )
            .build()
            .to_jsonable(),
            "steps": [],
        }
        pack["pack_fingerprint"] = compute_pack_fingerprint(pack)
        body = _json(pack)
        descriptor = {
            "id": workload_id,
            "path": f"packs/{workload_id}.airflow-pack.json",
            "sha256": sha256_bytes(body),
            "bytes": len(body),
            "pack_fingerprint": pack["pack_fingerprint"],
        }
        _write(root, descriptor["path"], body)
        release["artifacts"]["workload_packs"].append(descriptor)
        value["nodes"].append({"workload_id": workload_id})
        value["workflow_outcome"]["expected_terminal_task_ids"] = [f"{workload_id}__outcome_gate"]
        body = _json(value)
        dag.update(sha256=sha256_bytes(body), bytes=len(body))
        _write(root, dag["path"], body)
    _seal(root, release)


def workspace_evidence(tmp_path: Path, monkeypatch):
    """Build complete v2 source inputs and export both workflow evidence trees."""

    compiled, release, _ = _tree(tmp_path)
    _add_transfer_fixtures(compiled, release)
    loader = build_dbt_expected_release_loader()
    expected = loader(compiled, release["release_id"])
    request = DbtDevEvidenceRequest.build(
        compiled_root=compiled,
        release_id=release["release_id"],
        deployment_id=DEPLOYMENT_ID,
        producer_repository="example/repo",
        producer_workflow=".github/workflows/accept.yml",
        source_commit="a" * 40,
        orchestration_run_id="123",
        orchestration_run_attempt=1,
        release_loader=loader,
    )
    shared = tmp_path / "shared-evidence"
    shared.mkdir()
    spool = shared / "dbt-spool"
    spool.mkdir()
    journal = ConfinedDbtDevEvidenceCampaignJournal(shared)
    journal.open(request)
    deployment_identity = _deployment_identity(release["release_id"])
    dag_descriptors = {item["id"]: item for item in release["artifacts"]["dag_specs"]}
    evidence_root = None
    for workflow_request in request.workflows:
        workflow = expected.dbt_workflows[workflow_request.workflow_id]
        summaries = {}
        identities = {}
        for workload_id in workflow.expected_workload_ids:
            identity = AirflowRunIdentity(
                release_id=request.release_id,
                deployment_id=request.deployment_id,
                workload_pack=AirflowArtifactIdentity(workload_id, expected.required_workloads[workload_id]),
                dag_spec=AirflowArtifactIdentity(workflow.dag_id, dag_descriptors[workflow.dag_id]["sha256"]),
                runtime_image_digest="sha256:" + "f" * 64,
            )
            identities[workload_id] = identity
            summaries[f"{workload_id}__dpone_runtime"] = {
                **_xcom_summary(identity.to_dict()),
                "deployment_identity": deployment_identity,
            }
        identity = identities[workflow.workload_id]
        runtime_evidence = _execution_evidence(workflow, workflow_request.dag_run_id, identity)
        local = tmp_path / "runtime" / workflow.workflow_id / "dbt-evidence.json"
        CampaignDbtExecutionEvidenceWriter(
            LocalDbtExecutionEvidenceWriter(local),
            evidence_root=str(spool),
            evidence_set_id=request.evidence_set_id,
        ).write(runtime_evidence)
        monkeypatch.setenv(AIRFLOW_RUN_IDENTITY_ENV, json.dumps(identity.to_dict()))
        monkeypatch.setenv("DPONE_AIRFLOW_DEPLOYMENT_IDENTITY", json.dumps(deployment_identity))
        xcom_path = local.parent / "return.json"
        write_airflow_xcom_from_evidence(evidence_path=local, xcom_output=xcom_path, status="passed")
        summaries[f"{workflow.workload_id}__dpone_runtime"] = json.loads(xcom_path.read_bytes())
        context = {key: value for key, value in identity.to_dict().items() if key != "workload_pack"}
        context["_workload_pack_sha256"] = {
            item_id: expected.required_workloads[item_id] for item_id in workflow.expected_workload_ids
        }
        context["_activation_id"] = ACTIVATION_ID
        task_instances = [
            SimpleNamespace(task_id=item_id, state="success", try_number=1, map_index=-1) for item_id in summaries
        ]
        dag_run = SimpleNamespace(
            dag_id=workflow.dag_id,
            run_id=workflow_request.dag_run_id,
            conf={
                "dpone_evidence_authority": {
                    "schema": "dpone.dbt-dev-evidence-authority.v1",
                    "request_id": request.evidence_set_id,
                    "evidence_set_id": request.evidence_set_id,
                    "release_id": request.release_id,
                    "deployment_id": request.deployment_id,
                    "workflow_id": workflow.workflow_id,
                    "dag_id": workflow.dag_id,
                    "dag_run_id": workflow_request.dag_run_id,
                }
            },
            get_task_instances=lambda task_instances=task_instances: task_instances,
        )

        class TaskInstance:
            def xcom_pull(self, *, task_ids: str, key: str | None = None):
                assert key in (None, "return_value")
                return summaries[task_ids]

        outcome = {
            "schema": "dpone.dbt-workflow-outcome.v2",
            "status": "passed",
            "code": "DPONE_DBT_WORKFLOW_PASSED",
            "workflow_id": workflow.workflow_id,
            "release_id": request.release_id,
            "deployment_id": request.deployment_id,
            "dag_run_id": workflow_request.dag_run_id,
            "deployment_identity": deployment_identity,
            "tasks": [{"task_id": task_id, "state": "success"} for task_id in workflow.expected_terminal_task_ids],
        }
        exported = export_dev_evidence_if_requested(
            workflow_id=workflow.workflow_id,
            workflow_outcome=outcome,
            workload_tasks=tuple(
                {"workload_id": item_id, "runtime_task_id": f"{item_id}__dpone_runtime"}
                for item_id in workflow.expected_workload_ids
            ),
            run_identity_context=context,
            ti=TaskInstance(),
            dag_run=dag_run,
            evidence_root=shared,
        )
        current = Path(exported["evidence_root"])
        assert evidence_root is None or evidence_root == current
        evidence_root = current
    journal.close(
        DbtDevEvidenceCampaignReceipt.build(
            request=request,
            status="passed",
            code="DPONE_DBT_DEV_EVIDENCE_CAMPAIGN_PASSED",
            workflow_states={item.workflow_id: "success" for item in request.workflows},
        )
    )
    return compiled, evidence_root, release, request


def _execution_evidence(workflow, run_id: str, identity: AirflowRunIdentity) -> DbtExecutionEvidence:
    return DbtExecutionEvidence(
        status="passed",
        code="DPONE_DBT_EXECUTION_PASSED",
        workflow_id=workflow.workflow_id,
        release_id=identity.release_id,
        deployment_id=identity.deployment_id,
        workload_pack_sha256=workflow.workload_pack_sha256,
        project_bundle_sha256=workflow.project_bundle_sha256,
        manifest_sha256=workflow.manifest_sha256,
        selection_sha256=workflow.selection_sha256,
        toolchain_sha256=workflow.toolchain_sha256,
        invocation_context_sha256=workflow.invocation_context_sha256,
        logical_target_sha256=workflow.logical_target_sha256,
        target_binding_sha256=dbt_target_binding_identity_sha256(
            logical_target_sha256=workflow.logical_target_sha256, run_identity=identity
        ),
        adapter_runtime=workflow.adapter_runtime,
        adapter_policy_sha256=workflow.adapter_policy_sha256,
        graph_policy_sha256=workflow.graph_policy_sha256,
        preflight_status="passed",
        build_started=True,
        dbt_exit_code=0,
        dbt_warning_policy=workflow.dbt_warning_policy,
        dbt_warning_count=0,
        dbt_schema_version=workflow.dbt_schema_version,
        dbt_version=workflow.dbt_version,
        invocation_id=f"fixture-{workflow.workflow_id}",
        started_at="2026-08-28T00:00:00+00:00",
        finished_at="2026-08-28T00:01:00+00:00",
        airflow={
            "dag_id": workflow.dag_id,
            "task_id": f"{workflow.workload_id}__dpone_runtime",
            "run_id": run_id,
            "try_number": 1,
            "map_index": -1,
        },
        credential_versions=(),
        nodes=tuple(DbtNodeOutcome(item_id, "success", 0.1) for item_id in workflow.expected_run_result_unique_ids),
    )
