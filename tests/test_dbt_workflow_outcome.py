from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from dpone_airflow_pack.dag_spec_loader import (
    compute_dag_spec_fingerprint,
    load_dag_spec_file,
)
from dpone_airflow_pack.deployment_index import AirflowDeploymentIndexError
from dpone_airflow_pack.dev_evidence_export import DevEvidenceExportError
from dpone_airflow_pack.launch_pin_cleanup import (
    AIRFLOW_OUTCOME_GATE_RESULT_XCOM_KEY,
)
from dpone_airflow_pack.pack_wiring import WiredPackWorkload
from dpone_airflow_pack.workflow_outcome import (
    evaluate_workflow_outcome,
    wire_workflow_outcome,
)


class _TaskInstance:
    def __init__(self) -> None:
        self.values: dict[str, object] = {}

    def xcom_push(self, *, key: str, value: object) -> None:
        self.values[key] = value


class _TaskSdkTaskInstance(_TaskInstance):
    def __init__(self, receipts: dict[str, object]) -> None:
        super().__init__()
        self.receipts = receipts

    def xcom_pull(self, *, task_ids: str, key: str) -> object:
        assert key == AIRFLOW_OUTCOME_GATE_RESULT_XCOM_KEY
        return self.receipts.get(task_ids)


def _dag_run(*states: tuple[str, str]) -> SimpleNamespace:
    instances = [SimpleNamespace(task_id=task_id, state=state) for task_id, state in states]
    return SimpleNamespace(
        run_id="scheduled__2026-07-27T00:00:00+00:00",
        get_task_instances=lambda: instances,
    )


def test_workflow_outcome_records_exact_successful_terminal_set() -> None:
    ti = _TaskInstance()

    payload = evaluate_workflow_outcome(
        workflow_id="daily_marts",
        expected_task_ids=("publish_b", "publish_a"),
        release_id="sha256:" + "a" * 64,
        deployment_id="sha256:" + "b" * 64,
        activation_id="3f60628e-ef48-48b0-84c3-a9e27a82a7f2",
        ti=ti,
        dag_run=_dag_run(("publish_a", "success"), ("publish_b", "success")),
    )

    assert payload["status"] == "passed"
    assert payload["schema"] == "dpone.dbt-workflow-outcome.v2"
    assert [item["task_id"] for item in payload["tasks"]] == ["publish_a", "publish_b"]
    assert payload["deployment_identity"] == {
        "schema": "dpone.airflow-deployment-identity.v1",
        "release_id": "sha256:" + "a" * 64,
        "deployment_id": "sha256:" + "b" * 64,
        "activation_id": "3f60628e-ef48-48b0-84c3-a9e27a82a7f2",
    }
    assert ti.values["dpone_workflow_outcome"] == payload


def test_airflow_3_task_sdk_uses_exact_outcome_gate_receipt() -> None:
    gate_task_id = "publish_orders__outcome_gate"
    ti = _TaskSdkTaskInstance(
        {
            gate_task_id: {
                "passed": True,
                "payload": {"passed": True, "status": "passed"},
            }
        }
    )

    payload = evaluate_workflow_outcome(
        workflow_id="daily_marts",
        expected_task_ids=(gate_task_id,),
        release_id="sha256:" + "a" * 64,
        deployment_id="sha256:" + "b" * 64,
        activation_id="3f60628e-ef48-48b0-84c3-a9e27a82a7f2",
        ti=ti,
        dag_run=SimpleNamespace(run_id="manual__airflow3"),
    )

    assert payload["status"] == "passed"
    assert payload["tasks"] == [
        {
            "task_id": gate_task_id,
            "state": "success",
            "mapped_instances": 1,
        }
    ]


@pytest.mark.parametrize(
    ("receipt", "expected_state"),
    [
        (None, "missing"),
        ({"passed": True, "payload": {"passed": False}}, "invalid"),
        ({"passed": False, "payload": {"passed": False}}, "failed"),
    ],
)
def test_airflow_3_task_sdk_receipt_fallback_remains_fail_closed(
    receipt: object,
    expected_state: str,
) -> None:
    gate_task_id = "publish_orders__outcome_gate"
    ti = _TaskSdkTaskInstance({gate_task_id: receipt})

    with pytest.raises(RuntimeError, match="DPONE_DBT_WORKFLOW_FAILED"):
        evaluate_workflow_outcome(
            workflow_id="daily_marts",
            expected_task_ids=(gate_task_id,),
            release_id="sha256:" + "a" * 64,
            deployment_id="sha256:" + "b" * 64,
            activation_id="3f60628e-ef48-48b0-84c3-a9e27a82a7f2",
            ti=ti,
            dag_run=SimpleNamespace(run_id="manual__airflow3"),
        )

    receipt_payload = ti.values["dpone_workflow_outcome"]
    assert isinstance(receipt_payload, dict)
    assert receipt_payload["tasks"][0]["state"] == expected_state


def test_workflow_outcome_omits_occurrence_identity_for_legacy_index() -> None:
    payload = evaluate_workflow_outcome(
        workflow_id="daily_marts",
        expected_task_ids=("publish_a",),
        release_id="sha256:" + "a" * 64,
        deployment_id="sha256:" + "b" * 64,
        ti=_TaskInstance(),
        dag_run=_dag_run(("publish_a", "success")),
    )

    assert "deployment_identity" not in payload
    assert payload["schema"] == "dpone.dbt-workflow-outcome.v1"


def test_workflow_outcome_rejects_invalid_nonlegacy_activation_identity() -> None:
    ti = _TaskInstance()

    with pytest.raises(RuntimeError, match="DPONE_DBT_WORKFLOW_IDENTITY_MISSING"):
        evaluate_workflow_outcome(
            workflow_id="daily_marts",
            expected_task_ids=("publish_a",),
            release_id="sha256:" + "a" * 64,
            deployment_id="sha256:" + "b" * 64,
            activation_id="latest",
            ti=ti,
            dag_run=_dag_run(("publish_a", "success")),
        )

    receipt = ti.values["dpone_workflow_outcome"]
    assert isinstance(receipt, dict)
    assert receipt["status"] == "failed"


def test_workflow_outcome_never_publishes_passed_before_requested_export(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ti = _TaskInstance()

    def fail_export(**_kwargs: object) -> dict[str, object]:
        raise DevEvidenceExportError("evidence storage unavailable")

    monkeypatch.setattr(
        "dpone_airflow_pack.workflow_outcome.export_dev_evidence_if_requested",
        fail_export,
    )

    with pytest.raises(DevEvidenceExportError):
        evaluate_workflow_outcome(
            workflow_id="daily_marts",
            expected_task_ids=("publish_orders",),
            release_id="sha256:" + "a" * 64,
            deployment_id="sha256:" + "b" * 64,
            ti=ti,
            dag_run=_dag_run(("publish_orders", "success")),
        )

    receipt = ti.values["dpone_workflow_outcome"]
    assert isinstance(receipt, dict)
    assert receipt["status"] == "failed"
    assert receipt["code"] == "DPONE_DBT_WORKFLOW_EVIDENCE_EXPORT_FAILED"
    assert "dpone_dev_evidence_export" not in ti.values


def test_workflow_outcome_persists_failure_receipt_before_failing_task() -> None:
    ti = _TaskInstance()

    with pytest.raises(RuntimeError, match="DPONE_DBT_WORKFLOW_FAILED"):
        evaluate_workflow_outcome(
            workflow_id="daily_marts",
            expected_task_ids=("publish_a", "publish_b"),
            release_id="sha256:" + "a" * 64,
            deployment_id="sha256:" + "b" * 64,
            ti=ti,
            dag_run=_dag_run(("publish_a", "success"), ("publish_b", "failed")),
        )

    receipt = ti.values["dpone_workflow_outcome"]
    assert isinstance(receipt, dict)
    assert receipt["status"] == "failed"


def test_workflow_outcome_never_hides_failed_mapped_instance() -> None:
    ti = _TaskInstance()

    with pytest.raises(RuntimeError, match="DPONE_DBT_WORKFLOW_FAILED"):
        evaluate_workflow_outcome(
            workflow_id="daily_marts",
            expected_task_ids=("publish_orders",),
            release_id="sha256:" + "a" * 64,
            deployment_id="sha256:" + "b" * 64,
            ti=ti,
            dag_run=_dag_run(
                ("publish_orders", "failed"),
                ("publish_orders", "success"),
            ),
        )

    receipt = ti.values["dpone_workflow_outcome"]
    assert isinstance(receipt, dict)
    assert receipt["tasks"] == [
        {
            "task_id": "publish_orders",
            "state": "failed",
            "mapped_instances": 2,
        }
    ]


@pytest.mark.parametrize("missing_field", ["release_id", "deployment_id"])
def test_workflow_outcome_requires_pinned_release_and_deployment(
    missing_field: str,
) -> None:
    ti = _TaskInstance()
    identities: dict[str, str | None] = {
        "release_id": "sha256:" + "a" * 64,
        "deployment_id": "sha256:" + "b" * 64,
    }
    identities[missing_field] = None

    with pytest.raises(RuntimeError, match="DPONE_DBT_WORKFLOW_IDENTITY_MISSING"):
        evaluate_workflow_outcome(
            workflow_id="daily_marts",
            expected_task_ids=("publish_orders",),
            release_id=identities["release_id"],
            deployment_id=identities["deployment_id"],
            ti=ti,
            dag_run=_dag_run(("publish_orders", "success")),
        )

    receipt = ti.values["dpone_workflow_outcome"]
    assert isinstance(receipt, dict)
    assert receipt["status"] == "failed"


def test_dbt_dag_spec_requires_explicit_terminal_task_ids(tmp_path: Path) -> None:
    payload = {
        "kind": "gitops.airflow_dag_spec",
        "schema_version": "1",
        "producer": "dpone dbt compile",
        "dag_id": "DAG__daily_marts",
        "schedule": None,
        "start_date": "2026-01-01",
        "nodes": [
            {
                "node_id": "publish_orders",
                "workload_id": "publish_orders",
                "pack_ref": "cached://workloads/publish_orders",
            }
        ],
        "edges": [],
        "topological_order": ["publish_orders"],
        "workflow_outcome": {
            "schema": "dpone.dbt-workflow-outcome.v1",
            "task_id": "workflow_outcome",
            "workflow_id": "daily_marts",
        },
        "warnings": [],
    }
    payload["spec_fingerprint"] = compute_dag_spec_fingerprint(payload)
    path = tmp_path / "DAG__daily_marts.dag-spec.json"
    path.write_text(json.dumps(payload), encoding="utf-8")

    loaded, issues = load_dag_spec_file(path)

    assert loaded is None
    assert [issue.code for issue in issues] == ["DPONE_DBT_WORKFLOW_OUTCOME_INVALID"]


def test_materializer_rejects_terminal_task_identity_drift() -> None:
    configured_terminal = "publish_orders__outcome_gate"
    actual_terminal = SimpleNamespace(task_id="publish_orders__renamed_gate")
    wired = {
        "publish_orders": WiredPackWorkload(
            entrypoints=(actual_terminal,),
            terminal=actual_terminal,
        )
    }

    with pytest.raises(
        AirflowDeploymentIndexError,
        match="terminal task identities differ",
    ):
        wire_workflow_outcome(
            {
                "dag_id": "DAG__daily_marts",
                "edges": [],
                "workflow_outcome": {
                    "schema": "dpone.dbt-workflow-outcome.v1",
                    "task_id": "workflow_outcome",
                    "workflow_id": "daily_marts",
                    "expected_terminal_task_ids": [configured_terminal],
                },
            },
            dag=object(),
            wired_nodes=wired,
            run_identity_context=None,
        )


def test_non_dbt_dag_without_workflow_outcome_remains_compatible() -> None:
    assert (
        wire_workflow_outcome(
            {"dag_id": "ordinary_ingestion", "edges": []},
            dag=object(),
            wired_nodes={},
            run_identity_context=None,
        )
        is None
    )
