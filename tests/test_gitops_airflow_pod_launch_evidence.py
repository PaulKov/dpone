from __future__ import annotations

import json
import subprocess
from pathlib import Path

from dpone.gitops.airflow_pod_launch_evidence import GitOpsAirflowPodLaunchEvidencePlanner
from dpone.gitops.airflow_pod_launch_evidence_models import (
    GitOpsAirflowPodLaunchEvidenceCommand,
    GitOpsAirflowPodLaunchEvidenceCommandResult,
)
from dpone.gitops.airflow_pod_launch_evidence_runner import SubprocessAirflowPodLaunchEvidenceRunner

_DIGEST = "sha256:" + "a" * 64
_IMAGE = "ghcr.io/acme/dpone:0.12.0"


def _runtime_profile() -> dict[str, object]:
    return {
        "kind": "gitops.airflow_runtime_profile",
        "run_spec_path": ".dpone/gitops/airflow/run-spec.json",
        "runtime_evidence_path": ".dpone/gitops/airflow/runtime-evidence.json",
        "xcom_summary_path": ".dpone/gitops/airflow/xcom-summary.json",
        "image": _IMAGE,
        "image_digest": _DIGEST,
        "namespace": "dpone-runners",
        "service_account": "dpone-runner",
        "runner_policy": "release",
        "outcome_mode": "xcom_then_gate",
    }


def _pod_contract() -> dict[str, object]:
    return {
        "kind": "gitops.airflow_pod_contract",
        "run_spec_path": ".dpone/gitops/airflow/run-spec.json",
        "runtime_profile_path": ".dpone/gitops/airflow/runtime-profile.json",
        "image": _IMAGE,
        "namespace": "dpone-runners",
        "service_account": "dpone-runner",
        "xcom": {
            "enabled": True,
            "return_path": "/airflow/xcom/return.json",
            "summary_path": ".dpone/gitops/airflow/xcom-summary.json",
            "outcome_mode": "xcom_then_gate",
        },
        "pod_spec": {
            "metadata": {"name": "dpone-orders"},
            "spec": {
                "serviceAccountName": "dpone-runner",
                "containers": [{"name": "base", "image": _IMAGE}],
            },
        },
        "kpo_kwargs": {
            "namespace": "dpone-runners",
            "name": "dpone-orders",
            "image": _IMAGE,
            "do_xcom_push": True,
        },
    }


def _image_contract() -> dict[str, object]:
    return {
        "kind": "gitops.airflow_image_contract.v1",
        "image": _IMAGE,
        "image_digest": _DIGEST,
        "tools": ["dpone", "kubectl"],
    }


def _runtime_evidence() -> dict[str, object]:
    return {
        "kind": "gitops.airflow_runtime_evidence",
        "status": "passed",
        "run_spec_path": ".dpone/gitops/airflow/run-spec.json",
        "steps": [{"name": "Run manifest orders", "status": "passed", "exit_code": 0}],
        "blockers": [],
    }


def _xcom_summary() -> dict[str, object]:
    return {
        "kind": "gitops.airflow_xcom_summary",
        "status": "passed",
        "runtime_evidence_path": ".dpone/gitops/airflow/runtime-evidence.json",
        "failed_step": None,
        "step_counts": {"passed": 1, "failed": 0},
        "blockers": [],
    }


def _pod_json(
    *,
    phase: str = "Succeeded",
    restart_count: int = 0,
    exit_code: int = 0,
    reason: str = "Completed",
    image_id: str | None = None,
) -> dict[str, object]:
    return {
        "kind": "Pod",
        "metadata": {"name": "dpone-orders", "namespace": "dpone-runners"},
        "spec": {"serviceAccountName": "dpone-runner", "nodeName": "node-a"},
        "status": {
            "phase": phase,
            "containerStatuses": [
                {
                    "name": "base",
                    "image": _IMAGE,
                    "imageID": image_id or f"{_IMAGE}@{_DIGEST}",
                    "ready": False,
                    "restartCount": restart_count,
                    "state": {
                        "terminated": {
                            "exitCode": exit_code,
                            "reason": reason,
                            "message": "container completed",
                        }
                    },
                }
            ],
        },
    }


def _events_json(*, reason: str = "Started", event_type: str = "Normal") -> dict[str, object]:
    return {
        "items": [
            {
                "type": event_type,
                "reason": reason,
                "message": "Started container base",
                "count": 1,
            }
        ]
    }


def _plan() -> object:
    return GitOpsAirflowPodLaunchEvidencePlanner().plan(
        runtime_profile_path=".dpone/gitops/airflow/runtime-profile.json",
        runtime_profile=_runtime_profile(),
        pod_contract_path=".dpone/gitops/airflow/pod-contract.json",
        pod_contract=_pod_contract(),
        image_contract_path=".dpone/gitops/airflow/image-contract.json",
        image_contract=_image_contract(),
        runtime_evidence_path=".dpone/gitops/airflow/runtime-evidence.json",
        runtime_evidence=_runtime_evidence(),
        xcom_summary_path=".dpone/gitops/airflow/xcom-summary.json",
        xcom_summary=_xcom_summary(),
        mode="plan",
        runner_policy="release",
        pod_name=None,
        expected_phase="Succeeded",
        timeout_seconds=300,
        log_tail_lines=200,
        kubectl="kubectl",
    )


def test_airflow_pod_launch_evidence_plan_builds_watch_commands_without_local_paths() -> None:
    report = _plan()
    payload = report.to_jsonable()

    assert report.passed
    assert payload["kind"] == "gitops.airflow_pod_launch_evidence"
    assert payload["pod_name"] == "dpone-orders"
    assert payload["namespace"] == "dpone-runners"
    assert [command["name"] for command in payload["commands"]] == [
        "kubectl_get_pod",
        "kubectl_get_events",
        "kubectl_logs_base",
    ]
    assert payload["commands"][0]["command"] == "kubectl get pod dpone-orders -n dpone-runners -o json"
    assert all(not command["executed"] for command in payload["commands"])
    assert "/Users/" not in json.dumps(payload)


def test_airflow_pod_launch_evidence_live_results_parse_observed_pod_events_and_logs() -> None:
    planner = GitOpsAirflowPodLaunchEvidencePlanner()
    plan = _plan()

    report = planner.with_results(
        plan,
        results=(
            GitOpsAirflowPodLaunchEvidenceCommandResult(
                name="kubectl_get_pod",
                command="kubectl get pod",
                exit_code=0,
                stdout=json.dumps(_pod_json()),
            ),
            GitOpsAirflowPodLaunchEvidenceCommandResult(
                name="kubectl_get_events",
                command="kubectl get events",
                exit_code=0,
                stdout=json.dumps(_events_json()),
            ),
            GitOpsAirflowPodLaunchEvidenceCommandResult(
                name="kubectl_logs_base",
                command="kubectl logs",
                exit_code=0,
                stdout="dpone run completed\nxcom summary written\n",
            ),
        ),
    )
    payload = report.to_jsonable()

    assert report.passed
    assert payload["observed_pod"]["phase"] == "Succeeded"
    assert payload["observed_pod"]["service_account"] == "dpone-runner"
    assert payload["observed_pod"]["containers"][0]["name"] == "base"
    assert payload["observed_pod"]["containers"][0]["exit_code"] == 0
    assert payload["observed_pod"]["events"][0]["reason"] == "Started"
    assert "xcom summary written" in payload["observed_pod"]["logs_tail"]
    assert all(command["executed"] for command in payload["commands"])


def test_airflow_pod_launch_evidence_release_blocks_failed_pod_event_restart_and_exit_code() -> None:
    planner = GitOpsAirflowPodLaunchEvidencePlanner()
    plan = _plan()

    report = planner.with_results(
        plan,
        results=(
            GitOpsAirflowPodLaunchEvidenceCommandResult(
                name="kubectl_get_pod",
                command="kubectl get pod",
                exit_code=0,
                stdout=json.dumps(_pod_json(phase="Failed", restart_count=1, exit_code=137, reason="OOMKilled")),
            ),
            GitOpsAirflowPodLaunchEvidenceCommandResult(
                name="kubectl_get_events",
                command="kubectl get events",
                exit_code=0,
                stdout=json.dumps(_events_json(reason="FailedScheduling", event_type="Warning")),
            ),
            GitOpsAirflowPodLaunchEvidenceCommandResult(
                name="kubectl_logs_base",
                command="kubectl logs",
                exit_code=0,
                stdout="pod failed\n",
            ),
        ),
    )

    assert not report.passed
    blocker_codes = {blocker.code for blocker in report.blockers}
    assert {
        "airflow_pod_phase_mismatch",
        "airflow_pod_container_failed",
        "airflow_pod_restart_detected",
        "airflow_pod_event_blocker",
    }.issubset(blocker_codes)


def test_airflow_pod_launch_evidence_release_requires_digest_in_observed_image_id() -> None:
    planner = GitOpsAirflowPodLaunchEvidencePlanner()
    plan = _plan()

    report = planner.with_results(
        plan,
        results=(
            GitOpsAirflowPodLaunchEvidenceCommandResult(
                name="kubectl_get_pod",
                command="kubectl get pod",
                exit_code=0,
                stdout=json.dumps(_pod_json(image_id=f"{_IMAGE}@sha256:" + "b" * 64)),
            ),
            GitOpsAirflowPodLaunchEvidenceCommandResult(
                name="kubectl_get_events",
                command="kubectl get events",
                exit_code=0,
                stdout=json.dumps(_events_json()),
            ),
        ),
    )

    assert not report.passed
    assert any(blocker.code == "airflow_pod_image_digest_mismatch" for blocker in report.blockers)


def test_airflow_pod_launch_evidence_subprocess_runner_returns_timeout_result(monkeypatch) -> None:
    def timeout_run(*_args: object, **_kwargs: object) -> object:
        raise subprocess.TimeoutExpired(cmd="kubectl get pod dpone-orders", timeout=1, stderr=b"still pending")

    monkeypatch.setattr(subprocess, "run", timeout_run)
    command = GitOpsAirflowPodLaunchEvidenceCommand(
        name="kubectl_get_pod",
        kind="kubernetes_pod",
        command="kubectl get pod dpone-orders",
        required=True,
        timeout_seconds=1,
    )

    result = SubprocessAirflowPodLaunchEvidenceRunner().run(command=command, cwd=Path("."))

    assert result.exit_code == 124
    assert result.stderr == "still pending"
