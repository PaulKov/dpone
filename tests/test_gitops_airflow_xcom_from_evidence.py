from __future__ import annotations

import hashlib
import json
import logging
from argparse import Namespace

from dpone.commands.gitops.airflow_xcom_cmd import cmd_gitops_airflow_xcom_from_evidence
from dpone.contracts.dbt_publishing import canonical_dbt_execution_evidence_bytes


def test_xcom_from_evidence_includes_redacted_stderr_tail_for_empty_evidence(tmp_path) -> None:
    evidence_path = tmp_path / "runtime-evidence.json"
    stderr_path = tmp_path / "runtime-stderr.log"
    xcom_path = tmp_path / "return.json"
    evidence_path.write_text("", encoding="utf-8")
    stderr_path.write_text(
        "Runtime failed: password=secret-token ClickHouse get_records error: Code: 102",
        encoding="utf-8",
    )

    code = cmd_gitops_airflow_xcom_from_evidence(
        Namespace(
            evidence_path=str(evidence_path),
            runtime_evidence_path=".dpone/runs/orders/runtime-evidence.json",
            stderr_path=str(stderr_path),
            xcom_output=str(xcom_path),
            status="failed",
        ),
        ctx=object(),
        logger=logging.getLogger("test"),
    )

    payload = json.loads(xcom_path.read_text(encoding="utf-8"))

    assert code == 0
    assert payload["status"] == "failed"
    assert payload["blockers"][0]["code"] == "runtime_evidence_xcom_build_unavailable"
    assert "ClickHouse get_records error: Code: 102" in payload["blockers"][0]["message"]
    assert "secret-token" not in payload["blockers"][0]["message"]
    assert "password=<redacted>" in payload["blockers"][0]["message"]


def test_xcom_from_evidence_redacts_json_shaped_secret_tail(tmp_path) -> None:
    evidence_path = tmp_path / "runtime-evidence.json"
    stderr_path = tmp_path / "runtime-stderr.log"
    xcom_path = tmp_path / "return.json"
    evidence_path.write_text("", encoding="utf-8")
    stderr_path.write_text(
        'Runtime failed: {"password": "json-secret", "token": "runtime-token"} ClickHouse failed',
        encoding="utf-8",
    )

    code = cmd_gitops_airflow_xcom_from_evidence(
        Namespace(
            evidence_path=str(evidence_path),
            runtime_evidence_path=".dpone/runs/orders/runtime-evidence.json",
            stderr_path=str(stderr_path),
            xcom_output=str(xcom_path),
            status="failed",
        ),
        ctx=object(),
        logger=logging.getLogger("test"),
    )

    payload = json.loads(xcom_path.read_text(encoding="utf-8"))
    message = payload["blockers"][0]["message"]

    assert code == 0
    assert "json-secret" not in message
    assert "runtime-token" not in message
    assert '"password": "<redacted>"' in message
    assert '"token": "<redacted>"' in message


def test_xcom_fallback_does_not_publish_physical_paths(tmp_path) -> None:
    evidence_path = tmp_path / "runtime-evidence.json"
    stderr_path = tmp_path / "runtime-stderr.log"
    xcom_path = tmp_path / "return.json"
    evidence_path.write_text("", encoding="utf-8")
    stderr_path.write_text(f"failed while reading {tmp_path / 'pipeline.yaml'}", encoding="utf-8")

    code = cmd_gitops_airflow_xcom_from_evidence(
        Namespace(
            evidence_path=str(evidence_path),
            runtime_evidence_path=str(evidence_path),
            stderr_path=str(stderr_path),
            xcom_output=str(xcom_path),
            status="failed",
        ),
        ctx=object(),
        logger=logging.getLogger("test"),
    )

    payload = json.loads(xcom_path.read_text(encoding="utf-8"))
    serialized = json.dumps(payload)
    assert code == 0
    assert tmp_path.as_posix() not in serialized
    assert payload["runtime_evidence_path"] == "runtime-evidence.json"
    assert payload["blockers"][0]["path"] == "runtime-evidence.json"
    assert "$ABSOLUTE_PATH" in payload["blockers"][0]["message"]


def test_failed_quality_report_survives_xcom_projection(tmp_path) -> None:
    evidence_path = tmp_path / "runtime-evidence.json"
    xcom_path = tmp_path / "return.json"
    evidence_path.write_text(
        json.dumps(
            {
                "status": "failed",
                "result": {
                    "status": "error",
                    "quality_gates": {
                        "kind": "dpone.data_quality.gates.v1",
                        "passed": False,
                        "results": [
                            {
                                "gate_id": "target_not_empty",
                                "type": "min_rows",
                                "status": "failed",
                                "severity": "error",
                                "metrics": {"target_row_count": 0},
                                "message": "target row count is below threshold",
                            }
                        ],
                    },
                },
            }
        ),
        encoding="utf-8",
    )

    code = cmd_gitops_airflow_xcom_from_evidence(
        Namespace(
            evidence_path=str(evidence_path),
            runtime_evidence_path="runtime-evidence.json",
            stderr_path=None,
            xcom_output=str(xcom_path),
            status="failed",
        ),
        ctx=object(),
        logger=logging.getLogger("test"),
    )

    payload = json.loads(xcom_path.read_text(encoding="utf-8"))
    quality_gates = payload["runtime_evidence"]["quality_gates"]
    assert code == 0
    assert payload["status"] == "failed"
    assert quality_gates["passed"] is False
    assert quality_gates["results"][0]["gate_id"] == "target_not_empty"


def test_dbt_evidence_uses_bounded_exact_file_descriptor_in_xcom(tmp_path) -> None:
    evidence_path = tmp_path / "runtime-evidence.json"
    xcom_path = tmp_path / "return.json"
    evidence = {
        "schema": "dpone.dbt-execution-evidence.v1",
        "status": "passed",
        "code": "DPONE_DBT_EXECUTION_PASSED",
        "workflow_id": "daily_marts",
        "release_id": "sha256:" + "a" * 64,
        "deployment_id": "sha256:" + "b" * 64,
        "workload_pack_sha256": "sha256:" + "c" * 64,
        "project_bundle_sha256": "sha256:" + "d" * 64,
        "manifest_sha256": "sha256:" + "e" * 64,
        "selection_sha256": "sha256:" + "f" * 64,
        "toolchain_sha256": "sha256:" + "1" * 64,
        "dbt_exit_code": 0,
        "dbt_warning_policy": "fail",
        "dbt_warning_count": 0,
        "dbt_schema_version": "v6",
        "dbt_version": "1.12.3",
        "invocation_id": "invocation",
        "started_at": "2026-07-27T00:00:00+00:00",
        "finished_at": "2026-07-27T00:01:00+00:00",
        "airflow": {
            "dag_id": "DAG__daily_marts",
            "task_id": "dbt__daily_marts__dpone_runtime",
            "run_id": "manual__evidence",
            "try_number": 1,
            "map_index": -1,
        },
        "credential_versions": [],
        "nodes": [
            {
                "unique_id": "model.analytics.orders",
                "status": "success",
                "execution_time": 1.0,
            }
        ],
    }
    evidence_path.write_text(json.dumps(evidence), encoding="utf-8")

    code = cmd_gitops_airflow_xcom_from_evidence(
        Namespace(
            evidence_path=str(evidence_path),
            runtime_evidence_path="runtime-evidence.json",
            stderr_path=None,
            xcom_output=str(xcom_path),
            status="passed",
        ),
        ctx=object(),
        logger=logging.getLogger("test"),
    )

    payload = json.loads(xcom_path.read_text(encoding="utf-8"))
    exact = canonical_dbt_execution_evidence_bytes(evidence)
    assert code == 0
    assert "dbt_execution_evidence" not in payload
    assert payload["dbt_execution_evidence_ref"] == {
        "schema": "dpone.dbt-execution-evidence-ref.v1",
        "workflow_id": "daily_marts",
        "sha256": "sha256:" + hashlib.sha256(exact).hexdigest(),
        "bytes": len(exact),
        "storage_scope": "dbt_spool",
    }


def test_dpone_run_errors_become_xcom_blockers(tmp_path) -> None:
    """init_fetch pack-exec must not drop ValueError/errors into an empty blockers list."""

    evidence_path = tmp_path / "runtime-evidence.json"
    xcom_path = tmp_path / "return.json"
    evidence_path.write_text(
        json.dumps(
            {
                "manifest": "dpone_workloads/manifests/mssql/account_orders.yaml",
                "selector": "work-item_account_orders",
                "passed": False,
                "result": {
                    "status": "error",
                    "error_code": "DPONE_RUNTIME_CONNECTION_CONTEXT_REQUIRED",
                    "errors": [
                        "DPONE_RUNTIME_CONNECTION_CONTEXT_REQUIRED: Canonical "
                        "connection_ref execution requires a verified runtime "
                        "connection context."
                    ],
                },
            }
        ),
        encoding="utf-8",
    )

    code = cmd_gitops_airflow_xcom_from_evidence(
        Namespace(
            evidence_path=str(evidence_path),
            runtime_evidence_path="runtime-evidence.json",
            stderr_path=None,
            xcom_output=str(xcom_path),
            status="failed",
        ),
        ctx=object(),
        logger=logging.getLogger("test"),
    )

    payload = json.loads(xcom_path.read_text(encoding="utf-8"))
    assert code == 0
    assert payload["status"] == "failed"
    assert payload["blockers"], payload
    codes = {item["code"] for item in payload["blockers"]}
    messages = " ".join(item["message"] for item in payload["blockers"])
    assert (
        "DPONE_RUNTIME_CONNECTION_CONTEXT_REQUIRED" in codes or "DPONE_RUNTIME_CONNECTION_CONTEXT_REQUIRED" in messages
    )
    assert "connection_ref" in messages


def test_commit_unknown_keeps_recovery_contract_in_xcom(tmp_path) -> None:
    evidence_path = tmp_path / "runtime-evidence.json"
    xcom_path = tmp_path / "return.json"
    evidence_path.write_text(
        json.dumps(
            {
                "passed": False,
                "result": {
                    "status": "COMMIT_UNKNOWN",
                    "error_code": "COMMIT_UNKNOWN",
                    "errors": ["COMMIT_UNKNOWN: operator verification is required"],
                    "failure_boundary": "checkpoint_persistence",
                    "target_state": "unknown",
                    "checkpoint_state": "incomplete",
                    "source_state": "not_advanced",
                    "safe_to_retry": False,
                    "operator_verification_required": True,
                    "recovery_action": "operator_verification_required",
                },
            }
        ),
        encoding="utf-8",
    )

    code = cmd_gitops_airflow_xcom_from_evidence(
        Namespace(
            evidence_path=str(evidence_path),
            runtime_evidence_path="runtime-evidence.json",
            stderr_path=None,
            xcom_output=str(xcom_path),
            status="failed",
        ),
        ctx=object(),
        logger=logging.getLogger("test"),
    )

    payload = json.loads(xcom_path.read_text(encoding="utf-8"))
    assert code == 0
    assert payload["status"] == "failed"
    assert payload["blockers"][0]["code"] == "COMMIT_UNKNOWN"
    assert payload["recovery"] == {
        "code": "COMMIT_UNKNOWN",
        "failure_boundary": "checkpoint_persistence",
        "target_state": "unknown",
        "checkpoint_state": "incomplete",
        "source_state": "not_advanced",
        "safe_to_retry": False,
        "operator_verification_required": True,
        "recovery_action": "operator_verification_required",
    }


def test_commit_unknown_status_wins_over_passed_status_hint(tmp_path) -> None:
    evidence_path = tmp_path / "runtime-evidence.json"
    xcom_path = tmp_path / "return.json"
    evidence_path.write_text(
        json.dumps(
            {
                "result": {
                    "status": "COMMIT_UNKNOWN",
                    "error_code": "COMMIT_UNKNOWN",
                    "errors": ["COMMIT_UNKNOWN: operator verification is required"],
                    "failure_boundary": "target_invocation",
                    "target_state": "unknown",
                    "checkpoint_state": "not_advanced",
                    "source_state": "not_advanced",
                    "safe_to_retry": False,
                    "operator_verification_required": True,
                    "recovery_action": "operator_verification_required",
                }
            }
        ),
        encoding="utf-8",
    )

    code = cmd_gitops_airflow_xcom_from_evidence(
        Namespace(
            evidence_path=str(evidence_path),
            runtime_evidence_path="runtime-evidence.json",
            stderr_path=None,
            xcom_output=str(xcom_path),
            status="passed",
        ),
        ctx=object(),
        logger=logging.getLogger("test"),
    )

    payload = json.loads(xcom_path.read_text(encoding="utf-8"))
    assert code == 0
    assert payload["status"] == "failed"
    assert payload["recovery"]["code"] == "COMMIT_UNKNOWN"


def test_duplicate_key_evidence_fails_closed_instead_of_last_wins_passed(tmp_path) -> None:
    evidence_path = tmp_path / "runtime-evidence.json"
    xcom_path = tmp_path / "return.json"
    evidence_path.write_text(
        '{"passed": false, "result": {"status": "failed", "status": "passed", "errors": []}}',
        encoding="utf-8",
    )

    code = cmd_gitops_airflow_xcom_from_evidence(
        Namespace(
            evidence_path=str(evidence_path),
            runtime_evidence_path="runtime-evidence.json",
            stderr_path=None,
            xcom_output=str(xcom_path),
            status="failed",
        ),
        ctx=object(),
        logger=logging.getLogger("test"),
    )

    payload = json.loads(xcom_path.read_text(encoding="utf-8"))
    assert code == 0
    assert payload["status"] == "failed"
    assert payload["blockers"]


def test_dpone_run_value_error_becomes_xcom_blocker(tmp_path) -> None:
    evidence_path = tmp_path / "runtime-evidence.json"
    xcom_path = tmp_path / "return.json"
    evidence_path.write_text(
        json.dumps(
            {
                "manifest": "dpone_workloads/manifests/mssql/account_orders.yaml",
                "selector": "work-item_account_orders",
                "passed": False,
                "result": {
                    "status": "error",
                    "errors": ["ValueError: object_storage.runtime_access.connection_id is required"],
                },
            }
        ),
        encoding="utf-8",
    )

    code = cmd_gitops_airflow_xcom_from_evidence(
        Namespace(
            evidence_path=str(evidence_path),
            runtime_evidence_path="runtime-evidence.json",
            stderr_path=None,
            xcom_output=str(xcom_path),
            status="failed",
        ),
        ctx=object(),
        logger=logging.getLogger("test"),
    )

    payload = json.loads(xcom_path.read_text(encoding="utf-8"))
    assert code == 0
    assert payload["blockers"], payload
    messages = " ".join(item["message"] for item in payload["blockers"])
    assert "connection_id is required" in messages


def test_provider_outcome_accepts_complete_current_xcom_from_canonical_evidence(
    tmp_path,
    monkeypatch,
) -> None:
    from dpone_airflow_pack.outcome import evaluate_inline_pack_outcome

    identity = {
        "schema": "dpone.airflow-run-identity.v1",
        "release_id": "sha256:" + "a" * 64,
        "deployment_id": "sha256:" + "b" * 64,
        "dag_spec": None,
        "workload_pack": {"id": "orders", "sha256": "sha256:" + "c" * 64},
        "runtime_image_digest": None,
        "binding_set_ref": None,
        "connection_registry_ref": None,
        "credential_runtime_ref": None,
        "airflow_bundle": None,
    }
    monkeypatch.setenv("DPONE_AIRFLOW_RUN_IDENTITY", json.dumps(identity))
    evidence_path = tmp_path / "runtime-evidence.json"
    xcom_path = tmp_path / "return.json"
    evidence_path.write_text(json.dumps({"status": "passed"}), encoding="utf-8")

    code = cmd_gitops_airflow_xcom_from_evidence(
        Namespace(
            evidence_path=str(evidence_path),
            runtime_evidence_path="runtime-evidence.json",
            stderr_path=None,
            xcom_output=str(xcom_path),
            status="passed",
        ),
        ctx=object(),
        logger=logging.getLogger("test"),
    )
    payload = json.loads(xcom_path.read_text(encoding="utf-8"))

    assert code == 0
    assert (
        evaluate_inline_pack_outcome(
            payload,
            task_id="orders__runtime",
            expected_run_identity=identity,
            expected_runtime_evidence_sha256=payload["runtime_evidence_sha256"],
        )
        is payload
    )
