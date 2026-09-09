from __future__ import annotations

import json
import math
from argparse import Namespace
from pathlib import Path

from dpone.gitops.airflow_runtime_models import GitOpsAirflowRuntimeEvidence, GitOpsAirflowRuntimeStep
from dpone.gitops.airflow_xcom_outcome import GitOpsAirflowXComOutcomeBuilder
from dpone.gitops.models import GitOpsIssue


def test_xcom_outcome_embeds_bounded_redacted_runtime_evidence() -> None:
    evidence = GitOpsAirflowRuntimeEvidence(
        run_spec_path=".dpone/gitops/airflow/run-spec.json",
        bundle_path=".dpone/gitops/bundle/bundle.json",
        image="registry.example/dpone:dev",
        image_digest=None,
        status="passed",
        started_at="2026-07-03T00:00:00Z",
        finished_at="2026-07-03T00:00:10Z",
        duration_seconds=10.0,
        steps=(
            GitOpsAirflowRuntimeStep(
                name="dpone_run_orders",
                kind="dpone_run",
                command="dpone run manifests/orders.yaml --format json",
                required=True,
                status="passed",
                exit_code=0,
                started_at="2026-07-03T00:00:01Z",
                finished_at="2026-07-03T00:00:09Z",
                duration_seconds=8.0,
                stdout=json.dumps(
                    {
                        "data_quality": {"row_count": {"source": 10, "target": 10}},
                        "load_steps": [{"name": "stage", "status": "passed"}],
                        "route_capabilities": {"selected_route": "typed_binary_streaming"},
                        "throughput": {"overall_rps": 1250, "session_token": "do-not-leak"},
                    }
                ),
            ),
        ),
    )

    summary = (
        GitOpsAirflowXComOutcomeBuilder()
        .build(
            evidence=evidence,
            runtime_evidence_path=".dpone/runs/orders/runtime-evidence.json",
            runtime_evidence_sha256="sha256:" + "a" * 64,
        )
        .to_jsonable()
    )

    inline = summary["runtime_evidence"]
    assert inline["schema_version"] == "dpone.airflow.inline_runtime_evidence.v1"
    assert inline["status"] == "passed"
    assert inline["metrics"] == {"duration_seconds": 10.0, "step_count": 1}
    assert inline["step_timeline"] == [
        {
            "duration_seconds": 8.0,
            "exit_code": 0,
            "kind": "dpone_run",
            "manifest": None,
            "name": "dpone_run_orders",
            "required": True,
            "status": "passed",
        }
    ]
    assert inline["data_quality"]["row_count"] == {"source": 10, "target": 10}
    assert inline["load_steps"] == [{"name": "stage", "status": "passed"}]
    assert inline["route_capabilities"] == {"selected_route": "typed_binary_streaming"}
    assert inline["throughput"] == {"overall_rps": 1250, "session_token": "***REDACTED***"}
    assert "stdout" not in json.dumps(inline)
    assert "do-not-leak" not in json.dumps(summary)


def test_xcom_preserves_quality_gates_from_reconciliation_metrics() -> None:
    evidence = GitOpsAirflowRuntimeEvidence(
        run_spec_path="run-spec.json",
        bundle_path="bundle.json",
        image="dpone:dev",
        image_digest=None,
        status="passed",
        started_at="",
        finished_at="",
        duration_seconds=1.0,
        steps=(),
    )
    quality_gates = {
        "kind": "dpone.data_quality.gates.v1",
        "passed": True,
        "results": [
            {
                "gate_id": "target_not_empty",
                "status": "passed",
                "severity": "error",
                "metrics": {"target_row_count": 10, "password": "must-not-leak"},
            }
        ],
    }

    summary = (
        GitOpsAirflowXComOutcomeBuilder()
        .build(
            evidence=evidence,
            runtime_evidence_path="runtime-evidence.json",
            runtime_evidence_sha256="sha256:" + "a" * 64,
            inline_payload={"result": {"reconciliation_metrics": {"quality_gates": quality_gates}}},
        )
        .to_jsonable()
    )

    projected = summary["runtime_evidence"]["quality_gates"]
    assert projected["results"][0]["gate_id"] == "target_not_empty"
    assert projected["results"][0]["metrics"]["password"] == "***REDACTED***"
    assert "must-not-leak" not in json.dumps(summary)


def test_xcom_projection_does_not_publish_physical_paths(tmp_path: Path) -> None:
    source = tmp_path / "pipelines" / "orders" / "pipeline.yaml"
    evidence = GitOpsAirflowRuntimeEvidence(
        run_spec_path=str(tmp_path / "run-spec.json"),
        bundle_path=str(tmp_path / "bundle.json"),
        image="dpone:dev",
        image_digest=None,
        status="failed",
        started_at="",
        finished_at="",
        duration_seconds=1.0,
        steps=(
            GitOpsAirflowRuntimeStep(
                name="dpone_run_orders",
                kind="dpone_run",
                command="dpone run pipeline.yaml --format json",
                required=True,
                status="failed",
                exit_code=1,
                started_at="",
                finished_at="",
                duration_seconds=1.0,
                manifest=str(source),
                stdout=json.dumps({"quality": {"diagnostic_path": str(tmp_path / "quality.json")}}),
            ),
        ),
        warnings=(
            GitOpsIssue(
                code="warning",
                message=f"See {tmp_path / 'warning.log'}",
                path=str(tmp_path / "warning.log"),
                source="test",
            ),
        ),
    )

    payload = (
        GitOpsAirflowXComOutcomeBuilder()
        .build(
            evidence=evidence,
            runtime_evidence_path=str(tmp_path / "runtime-evidence.json"),
            runtime_evidence_sha256="sha256:" + "a" * 64,
            runtime_profile_path=str(tmp_path / "runtime-profile.json"),
        )
        .to_jsonable()
    )

    serialized = json.dumps(payload)
    assert tmp_path.as_posix() not in serialized
    assert payload["runtime_profile_path"] == "runtime-profile.json"
    assert payload["run_spec_path"] == "run-spec.json"
    assert payload["runtime_evidence_path"] == "runtime-evidence.json"
    assert payload["runtime_evidence"]["step_timeline"][0]["manifest"] == "pipeline.yaml"
    assert payload["warnings"][0]["path"] == "warning.log"
    assert "$ABSOLUTE_PATH" in payload["warnings"][0]["message"]


def test_xcom_from_evidence_cli_writes_inline_summary(tmp_path: Path) -> None:
    from dpone.commands.gitops.airflow_xcom_cmd import cmd_gitops_airflow_xcom_from_evidence

    evidence_path = tmp_path / "runtime-evidence.json"
    xcom_path = tmp_path / "return.json"
    evidence_path.write_text(
        json.dumps(
            {
                "kind": "gitops.airflow_runtime_evidence",
                "run_spec_path": "",
                "bundle_path": "",
                "image": "dpone:dev",
                "image_digest": None,
                "status": "passed",
                "started_at": "2026-07-03T00:00:00Z",
                "finished_at": "2026-07-03T00:00:01Z",
                "duration_seconds": 1.0,
                "steps": [],
                "warnings": [],
                "blockers": [],
                "data_quality": {"row_count": {"status": "passed"}},
                "route_capabilities": {"selected_route": "object_storage_pull"},
            }
        ),
        encoding="utf-8",
    )

    code = cmd_gitops_airflow_xcom_from_evidence(
        Namespace(
            evidence_path=str(evidence_path),
            runtime_evidence_path=".dpone/runs/orders/runtime-evidence.json",
            xcom_output=str(xcom_path),
            status=None,
        ),
        ctx=None,
        logger=None,
    )
    payload = json.loads(xcom_path.read_text(encoding="utf-8"))

    assert code == 0
    assert payload["kind"] == "gitops.airflow_xcom_summary"
    assert payload["status"] == "passed"
    assert payload["runtime_evidence_path"] == ".dpone/runs/orders/runtime-evidence.json"
    assert payload["runtime_evidence"]["metrics"] == {"duration_seconds": 1.0, "step_count": 0}
    assert payload["runtime_evidence"]["data_quality"] == {"row_count": {"status": "passed"}}
    assert payload["runtime_evidence"]["route_capabilities"] == {"selected_route": "object_storage_pull"}


def test_xcom_from_evidence_preserves_valid_provider_run_identity(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from dpone.commands.gitops.airflow_xcom_cmd import cmd_gitops_airflow_xcom_from_evidence

    identity = _run_identity_payload()
    monkeypatch.setenv("DPONE_AIRFLOW_RUN_IDENTITY", json.dumps(identity))
    deployment_identity = _deployment_identity_payload()
    monkeypatch.setenv("DPONE_AIRFLOW_DEPLOYMENT_IDENTITY", json.dumps(deployment_identity))
    evidence_path = tmp_path / "runtime-evidence.json"
    xcom_path = tmp_path / "return.json"
    evidence_path.write_text(json.dumps({"status": "passed"}), encoding="utf-8")

    code = cmd_gitops_airflow_xcom_from_evidence(
        Namespace(
            evidence_path=str(evidence_path),
            runtime_evidence_path="runtime-evidence.json",
            xcom_output=str(xcom_path),
            status="passed",
            stderr_path=None,
        ),
        ctx=None,
        logger=None,
    )

    payload = json.loads(xcom_path.read_text(encoding="utf-8"))
    assert code == 0
    assert payload["status"] == "passed"
    assert payload["run_identity"] == identity
    assert payload["deployment_identity"] == deployment_identity


def test_xcom_from_evidence_fails_closed_for_invalid_provider_run_identity(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from dpone.commands.gitops.airflow_xcom_cmd import cmd_gitops_airflow_xcom_from_evidence

    monkeypatch.setenv("DPONE_AIRFLOW_RUN_IDENTITY", '{"password":"must-not-leak"}')
    evidence_path = tmp_path / "runtime-evidence.json"
    xcom_path = tmp_path / "return.json"
    evidence_path.write_text(json.dumps({"status": "passed"}), encoding="utf-8")

    code = cmd_gitops_airflow_xcom_from_evidence(
        Namespace(
            evidence_path=str(evidence_path),
            runtime_evidence_path="runtime-evidence.json",
            xcom_output=str(xcom_path),
            status="passed",
            stderr_path=None,
        ),
        ctx=None,
        logger=None,
    )

    payload = json.loads(xcom_path.read_text(encoding="utf-8"))
    assert code == 0
    assert payload["status"] == "failed"
    assert payload["blockers"][0]["code"] == "DPONE_AIRFLOW_RUN_IDENTITY_INVALID"
    assert "must-not-leak" not in json.dumps(payload)


def test_xcom_from_evidence_fails_closed_for_invalid_deployment_identity(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from dpone.commands.gitops.airflow_xcom_cmd import cmd_gitops_airflow_xcom_from_evidence

    monkeypatch.setenv("DPONE_AIRFLOW_RUN_IDENTITY", json.dumps(_run_identity_payload()))
    monkeypatch.setenv(
        "DPONE_AIRFLOW_DEPLOYMENT_IDENTITY",
        json.dumps({**_deployment_identity_payload(), "activation_id": "latest"}),
    )
    evidence_path = tmp_path / "runtime-evidence.json"
    xcom_path = tmp_path / "return.json"
    evidence_path.write_text(json.dumps({"status": "passed"}), encoding="utf-8")

    code = cmd_gitops_airflow_xcom_from_evidence(
        Namespace(
            evidence_path=str(evidence_path),
            runtime_evidence_path="runtime-evidence.json",
            xcom_output=str(xcom_path),
            status="passed",
            stderr_path=None,
        ),
        ctx=None,
        logger=None,
    )

    payload = json.loads(xcom_path.read_text(encoding="utf-8"))
    assert code == 0
    assert payload["status"] == "failed"
    assert payload["blockers"][0]["code"] == "DPONE_AIRFLOW_DEPLOYMENT_IDENTITY_INVALID"


def test_xcom_from_evidence_fails_closed_for_mismatched_deployment_identity(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from dpone.commands.gitops.airflow_xcom_cmd import cmd_gitops_airflow_xcom_from_evidence

    monkeypatch.setenv("DPONE_AIRFLOW_RUN_IDENTITY", json.dumps(_run_identity_payload()))
    monkeypatch.setenv(
        "DPONE_AIRFLOW_DEPLOYMENT_IDENTITY",
        json.dumps({**_deployment_identity_payload(), "deployment_id": "sha256:" + "9" * 64}),
    )
    evidence_path = tmp_path / "runtime-evidence.json"
    xcom_path = tmp_path / "return.json"
    evidence_path.write_text(json.dumps({"status": "passed"}), encoding="utf-8")

    code = cmd_gitops_airflow_xcom_from_evidence(
        Namespace(
            evidence_path=str(evidence_path),
            runtime_evidence_path="runtime-evidence.json",
            xcom_output=str(xcom_path),
            status="passed",
            stderr_path=None,
        ),
        ctx=None,
        logger=None,
    )

    payload = json.loads(xcom_path.read_text(encoding="utf-8"))
    assert code == 0
    assert payload["status"] == "failed"
    assert payload["blockers"][0]["code"] == "DPONE_AIRFLOW_DEPLOYMENT_IDENTITY_INVALID"


def _run_identity_payload() -> dict[str, object]:
    return {
        "schema": "dpone.airflow-run-identity.v1",
        "release_id": "sha256:" + "a" * 64,
        "deployment_id": "sha256:" + "b" * 64,
        "dag_spec": {"id": "orders_daily", "sha256": "sha256:" + "c" * 64},
        "workload_pack": {"id": "load_orders", "sha256": "sha256:" + "d" * 64},
        "runtime_image_digest": "sha256:" + "e" * 64,
        "binding_set_ref": "sha256:" + "1" * 64,
        "connection_registry_ref": "sha256:" + "2" * 64,
        "credential_runtime_ref": "sha256:" + "3" * 64,
        "airflow_bundle": {
            "backend": "git",
            "ref": "git:7ac31f2",
            "versioned": True,
            "version": "7ac31f2",
            "snapshot_ref": None,
        },
    }


def _deployment_identity_payload() -> dict[str, str]:
    return {
        "schema": "dpone.airflow-deployment-identity.v1",
        "release_id": "sha256:" + "a" * 64,
        "deployment_id": "sha256:" + "b" * 64,
        "activation_id": "3f60628e-ef48-48b0-84c3-a9e27a82a7f2",
    }


def test_xcom_from_evidence_uses_status_hint_for_non_contract_runtime_json(tmp_path: Path) -> None:
    from dpone.commands.gitops.airflow_xcom_cmd import cmd_gitops_airflow_xcom_from_evidence

    evidence_path = tmp_path / "runtime-evidence.json"
    xcom_path = tmp_path / "return.json"
    evidence_path.write_text(
        json.dumps({"status": "success", "loaded_rows": 1102, "warnings": []}),
        encoding="utf-8",
    )

    code = cmd_gitops_airflow_xcom_from_evidence(
        Namespace(
            evidence_path=str(evidence_path),
            runtime_evidence_path=".dpone/runs/smoke/runtime-evidence.json",
            xcom_output=str(xcom_path),
            status="passed",
        ),
        ctx=None,
        logger=None,
    )
    payload = json.loads(xcom_path.read_text(encoding="utf-8"))

    assert code == 0
    assert payload["status"] == "passed"
    assert payload["blockers"] == []
    assert payload["runtime_evidence"]["status"] == "passed"
    assert payload["runtime_evidence"]["metrics"] == {"duration_seconds": 0.0, "step_count": 0}
    assert payload["runtime_evidence_sha256"].startswith("sha256:")


def test_xcom_from_evidence_cli_sanitizes_non_finite_runtime_metrics(tmp_path: Path) -> None:
    from dpone.commands.gitops.airflow_xcom_cmd import cmd_gitops_airflow_xcom_from_evidence

    evidence_path = tmp_path / "runtime-evidence.json"
    xcom_path = tmp_path / "return.json"
    evidence_path.write_text(
        json.dumps(
            {
                "kind": "gitops.airflow_runtime_evidence",
                "schema_version": "1",
                "producer": "dpone gitops airflow run-spec-exec",
                "run_spec_path": ".dpone/gitops/airflow/run-spec.json",
                "bundle_path": ".dpone/gitops/bundle/bundle.json",
                "image": "dpone:dev",
                "image_digest": None,
                "status": "passed",
                "started_at": "2026-07-03T00:00:00Z",
                "finished_at": "2026-07-03T00:00:01Z",
                "duration_seconds": 1.0,
                "steps": [],
                "warnings": [],
                "blockers": [],
                "throughput": {
                    "overall_rps": math.nan,
                    "stage_rps": math.inf,
                    "finalize_rps": -math.inf,
                },
            }
        ),
        encoding="utf-8",
    )

    code = cmd_gitops_airflow_xcom_from_evidence(
        Namespace(
            evidence_path=str(evidence_path),
            runtime_evidence_path=".dpone/runs/orders/runtime-evidence.json",
            xcom_output=str(xcom_path),
            status="passed",
        ),
        ctx=None,
        logger=None,
    )
    raw_xcom = xcom_path.read_text(encoding="utf-8")
    payload = json.loads(raw_xcom)

    assert code == 0
    assert payload["status"] == "passed"
    assert payload["blockers"] == []
    assert "runtime_evidence_xcom_build_failed" not in raw_xcom
    assert "NaN" not in raw_xcom
    assert "Infinity" not in raw_xcom
    assert payload["runtime_evidence"]["throughput"] == {
        "overall_rps": None,
        "stage_rps": None,
        "finalize_rps": None,
    }


def test_xcom_from_evidence_cli_extracts_json_from_mixed_stdout(tmp_path: Path) -> None:
    from dpone.commands.gitops.airflow_xcom_cmd import cmd_gitops_airflow_xcom_from_evidence

    evidence_path = tmp_path / "runtime-evidence.json"
    xcom_path = tmp_path / "return.json"
    evidence_path.write_text(
        "\n".join(
            (
                "[base] INFO: source export started",
                json.dumps(
                    {
                        "status": "success",
                        "duration_seconds": 714.2,
                        "warnings": [],
                        "blockers": [],
                        "load_steps": [
                            {
                                "step_id": "staged",
                                "kind": "columnar_window_load",
                                "status": "passed",
                                "duration_seconds": 690.0,
                                "rows": 34_917_463,
                            }
                        ],
                    }
                ),
                "[base] INFO: pod cleanup finished",
            )
        ),
        encoding="utf-8",
    )

    code = cmd_gitops_airflow_xcom_from_evidence(
        Namespace(
            evidence_path=str(evidence_path),
            runtime_evidence_path=".dpone/runs/dim/runtime-evidence.json",
            xcom_output=str(xcom_path),
            status="passed",
        ),
        ctx=None,
        logger=None,
    )
    payload = json.loads(xcom_path.read_text(encoding="utf-8"))

    assert code == 0
    assert payload["status"] == "passed"
    assert payload["blockers"] == []
    assert payload["runtime_evidence"]["metrics"] == {"duration_seconds": 714.2, "step_count": 0}
    assert payload["runtime_evidence"]["step_timeline"][0]["name"] == "staged"


def test_xcom_from_evidence_builds_timeline_from_top_level_load_steps(tmp_path: Path) -> None:
    from dpone.commands.gitops.airflow_xcom_cmd import cmd_gitops_airflow_xcom_from_evidence

    evidence_path = tmp_path / "runtime-evidence.json"
    xcom_path = tmp_path / "return.json"
    evidence_path.write_text(
        json.dumps(
            {
                "status": "success",
                "duration_seconds": 12.0,
                "warnings": [],
                "blockers": [],
                "load_steps": [
                    {
                        "step_id": "staged",
                        "phase": "stage",
                        "kind": "clickhouse_columnar_pull",
                        "status": "passed",
                        "duration_seconds": 10.5,
                        "rows": 34_917_463,
                        "details_json": {
                            "throughput": {"rows_per_second": 48_282.27},
                            "window_metrics": [
                                {
                                    "window_index": 1,
                                    "source_read_seconds": 18.0,
                                    "parquet_write_seconds": 2.0,
                                    "object_upload_seconds": 1.0,
                                    "clickhouse_pull_seconds": 1.2,
                                    "window_cleanup_seconds": 0.2,
                                }
                            ],
                        },
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    code = cmd_gitops_airflow_xcom_from_evidence(
        Namespace(
            evidence_path=str(evidence_path),
            runtime_evidence_path=".dpone/runs/dim/runtime-evidence.json",
            xcom_output=str(xcom_path),
            status=None,
        ),
        ctx=None,
        logger=None,
    )
    payload = json.loads(xcom_path.read_text(encoding="utf-8"))

    assert code == 0
    assert payload["status"] == "passed"
    assert payload["blockers"] == []
    assert payload["runtime_evidence"]["step_timeline"] == [
        {
            "name": "staged",
            "kind": "clickhouse_columnar_pull",
            "phase": "stage",
            "status": "passed",
            "duration_seconds": 10.5,
            "rows": 34_917_463,
            "rows_per_second": 48_282.27,
        }
    ]
    assert payload["runtime_evidence"]["load_steps"][0]["details_json"]["window_metrics"][0]["window_index"] == 1


def test_xcom_from_evidence_extracts_nested_run_result_throughput(tmp_path: Path) -> None:
    from dpone.commands.gitops.airflow_xcom_cmd import cmd_gitops_airflow_xcom_from_evidence

    evidence_path = tmp_path / "runtime-evidence.json"
    xcom_path = tmp_path / "return.json"
    evidence_path.write_text(
        json.dumps(
            {
                "status": "success",
                "duration_seconds": 25.0,
                "warnings": [],
                "blockers": [],
                "result": {
                    "loaded_rows": 10_000,
                    "run_throughput": {
                        "rows_per_second": 400.0,
                        "row_count": 10_000,
                    },
                    "details": {
                        "load_steps": [
                            {
                                "step_id": "staging_load",
                                "kind": "bulk_insert",
                                "status": "passed",
                                "duration_seconds": 20.0,
                                "rows": 10_000,
                                "details_json": {
                                    "throughput": {"rows_per_second": 500.0},
                                },
                            }
                        ]
                    },
                },
            }
        ),
        encoding="utf-8",
    )

    code = cmd_gitops_airflow_xcom_from_evidence(
        Namespace(
            evidence_path=str(evidence_path),
            runtime_evidence_path=".dpone/runs/smoke/runtime-evidence.json",
            xcom_output=str(xcom_path),
            status=None,
        ),
        ctx=None,
        logger=None,
    )
    payload = json.loads(xcom_path.read_text(encoding="utf-8"))

    assert code == 0
    assert payload["runtime_evidence"]["throughput"]["rows_per_second"] == 400.0
    assert payload["runtime_evidence"]["throughput"]["row_count"] == 10_000
    assert payload["runtime_evidence"]["step_timeline"][0]["name"] == "staging_load"
    assert payload["runtime_evidence"]["step_timeline"][0]["rows_per_second"] == 500.0


def test_xcom_from_evidence_extracts_process_result_details_throughput(tmp_path: Path) -> None:
    from dpone.commands.gitops.airflow_xcom_cmd import cmd_gitops_airflow_xcom_from_evidence

    evidence_path = tmp_path / "runtime-evidence.json"
    xcom_path = tmp_path / "return.json"
    evidence_path.write_text(
        json.dumps(
            {
                "manifest": "manifests/orders.yaml",
                "process": "orders",
                "run_id": "run-1",
                "passed": True,
                "result": {
                    "status": "success",
                    "inserted_rows": 10_000,
                    "duration_seconds": 25.0,
                    "details": {
                        "run_throughput": {
                            "rows_per_second": 400.0,
                            "row_count": 10_000,
                        },
                        "load_steps": [
                            {
                                "step_id": "staging_load",
                                "kind": "bulk_insert",
                                "status": "succeeded",
                                "duration_seconds": 20.0,
                                "details_json": {
                                    "throughput": {"rows_per_second": 500.0, "row_count": 10_000},
                                },
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
            runtime_evidence_path=".dpone/runs/orders/runtime-evidence.json",
            xcom_output=str(xcom_path),
            status="passed",
        ),
        ctx=None,
        logger=None,
    )
    payload = json.loads(xcom_path.read_text(encoding="utf-8"))

    assert code == 0
    assert payload["runtime_evidence"]["throughput"]["rows_per_second"] == 400.0
    assert payload["runtime_evidence"]["step_timeline"][0]["rows_per_second"] == 500.0
    assert payload["runtime_evidence"]["dpone_run"] == {"run_id": "run-1", "process": "orders"}


def test_xcom_from_evidence_rejects_conflicting_dpone_run_identity(tmp_path: Path) -> None:
    from dpone.commands.gitops.airflow_xcom_cmd import cmd_gitops_airflow_xcom_from_evidence

    evidence_path = tmp_path / "runtime-evidence.json"
    xcom_path = tmp_path / "return.json"
    evidence_path.write_text(
        json.dumps(
            {
                "kind": "gitops.airflow_runtime_evidence",
                "status": "passed",
                "steps": [
                    {"stdout": json.dumps({"run_id": "run-1", "process": "orders"})},
                    {"stdout": json.dumps({"run_id": "run-2", "process": "orders"})},
                ],
            }
        ),
        encoding="utf-8",
    )

    code = cmd_gitops_airflow_xcom_from_evidence(
        Namespace(
            evidence_path=str(evidence_path),
            runtime_evidence_path="runtime-evidence.json",
            xcom_output=str(xcom_path),
            status="passed",
            stderr_path=None,
        ),
        ctx=None,
        logger=None,
    )

    payload = json.loads(xcom_path.read_text(encoding="utf-8"))
    assert code == 0
    assert payload["status"] == "failed"
    assert payload["blockers"][0]["code"] == "DPONE_AIRFLOW_CORRELATION_MISMATCH"
