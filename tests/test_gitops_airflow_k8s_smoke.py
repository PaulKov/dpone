from __future__ import annotations

import json
import subprocess
from pathlib import Path

from dpone.gitops.airflow_k8s_smoke import GitOpsAirflowK8sSmokePlanner
from dpone.gitops.airflow_k8s_smoke_models import (
    GitOpsAirflowK8sCommandResult,
    GitOpsAirflowK8sSmokeCommand,
)
from dpone.gitops.airflow_k8s_smoke_runner import (
    SubprocessAirflowK8sSmokeRunner,
)


def _run_spec() -> dict[str, object]:
    return {
        "kind": "gitops.airflow_run_spec",
        "bundle_path": ".dpone/gitops/bundle/bundle.json",
        "image": "ghcr.io/acme/dpone:0.12.0",
        "image_digest": "sha256:" + "a" * 64,
        "worktree": ".",
        "evidence_output": ".dpone/gitops/airflow/runtime-evidence.json",
        "steps": [
            {
                "name": "Run manifest orders",
                "kind": "dpone_run",
                "command": "dpone run dpone_workloads/manifests/orders.yaml",
                "required": True,
                "manifest": "dpone_workloads/manifests/orders.yaml",
            }
        ],
    }


def _runtime_profile() -> dict[str, object]:
    return {
        "kind": "gitops.airflow_runtime_profile",
        "run_spec_path": ".dpone/gitops/airflow/run-spec.json",
        "runtime_evidence_path": ".dpone/gitops/airflow/runtime-evidence.json",
        "xcom_summary_path": ".dpone/gitops/airflow/xcom-summary.json",
        "image": "ghcr.io/acme/dpone:0.12.0",
        "image_digest": "sha256:" + "a" * 64,
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
        "pod_spec_path": ".dpone/gitops/airflow/pod-spec.yaml",
        "kpo_kwargs_path": ".dpone/gitops/airflow/kpo-kwargs.json",
        "image": "ghcr.io/acme/dpone:0.12.0",
        "namespace": "dpone-runners",
        "service_account": "dpone-runner",
        "xcom": {
            "enabled": True,
            "return_path": "/airflow/xcom/return.json",
            "summary_path": ".dpone/gitops/airflow/xcom-summary.json",
            "outcome_mode": "xcom_then_gate",
        },
        "pod_spec": {
            "spec": {
                "serviceAccountName": "dpone-runner",
                "containers": [
                    {
                        "name": "base",
                        "image": "ghcr.io/acme/dpone:0.12.0",
                        "resources": {"requests": {"cpu": "250m"}, "limits": {"cpu": "2"}},
                    }
                ],
            }
        },
        "kpo_kwargs": {
            "namespace": "dpone-runners",
            "name": "dpone-orders",
            "image": "ghcr.io/acme/dpone:0.12.0",
            "do_xcom_push": True,
            "pod_template_file": ".dpone/gitops/airflow/pod-spec.yaml",
        },
    }


def _image_contract(*, digest: str | None = "sha256:" + "a" * 64) -> dict[str, object]:
    return {
        "kind": "gitops.airflow_image_contract.v1",
        "image": "ghcr.io/acme/dpone:0.12.0",
        "image_digest": digest,
        "dpone_version": "0.12.0",
        "python_version": "3.12",
        "airflow_provider_version": "10.18.0",
        "tools": ["dpone", "kubectl"],
    }


def _xcom_summary() -> dict[str, object]:
    return {
        "kind": "gitops.airflow_xcom_summary",
        "status": "passed",
        "run_spec_path": ".dpone/gitops/airflow/run-spec.json",
        "runtime_evidence_path": ".dpone/gitops/airflow/runtime-evidence.json",
        "runtime_evidence_sha256": "sha256:" + "b" * 64,
        "failed_step": None,
        "step_counts": {"passed": 3, "failed": 0},
        "artifact_paths": {"runtime_evidence": ".dpone/gitops/airflow/runtime-evidence.json"},
        "blockers": [],
    }


def test_airflow_k8s_smoke_plan_builds_release_commands_without_local_paths() -> None:
    report = GitOpsAirflowK8sSmokePlanner().plan(
        run_spec_path=".dpone/gitops/airflow/run-spec.json",
        run_spec=_run_spec(),
        runtime_profile_path=".dpone/gitops/airflow/runtime-profile.json",
        runtime_profile=_runtime_profile(),
        pod_contract_path=".dpone/gitops/airflow/pod-contract.json",
        pod_contract=_pod_contract(),
        image_contract_path=".dpone/gitops/airflow/image-contract.json",
        image_contract=_image_contract(),
        xcom_summary_path=".dpone/gitops/airflow/xcom-summary.json",
        xcom_summary=_xcom_summary(),
        mode="plan",
        runner_kind="kubernetes_pod_operator",
        runner_policy="release",
        smoke_name="orders-smoke",
        timeout_seconds=300,
    )

    payload = report.to_jsonable()

    assert report.passed
    assert payload["kind"] == "gitops.airflow_k8s_smoke"
    assert payload["image_ref"] == "ghcr.io/acme/dpone:0.12.0@sha256:" + "a" * 64
    assert [command["name"] for command in payload["commands"]] == [
        "kubectl_auth_can_i",
        "kubectl_image_version_smoke",
        "kubectl_run_spec_smoke",
        "airflow_outcome_gate",
    ]
    command_text = "\n".join(command["command"] for command in payload["commands"])
    assert "--serviceaccount" not in command_text
    assert "--overrides=" in command_text
    assert "serviceAccountName" in command_text
    assert all(check["passed"] for check in payload["checks"])
    assert all(not command["executed"] for command in payload["commands"])
    assert "/Users/" not in json.dumps(payload)


def test_airflow_k8s_smoke_release_policy_blocks_unpinned_image() -> None:
    report = GitOpsAirflowK8sSmokePlanner().plan(
        run_spec_path=".dpone/gitops/airflow/run-spec.json",
        run_spec=_run_spec() | {"image_digest": None},
        runtime_profile_path=".dpone/gitops/airflow/runtime-profile.json",
        runtime_profile=_runtime_profile() | {"image_digest": None},
        pod_contract_path=".dpone/gitops/airflow/pod-contract.json",
        pod_contract=_pod_contract(),
        image_contract_path=".dpone/gitops/airflow/image-contract.json",
        image_contract=_image_contract(digest=None),
        xcom_summary_path=".dpone/gitops/airflow/xcom-summary.json",
        xcom_summary=_xcom_summary(),
        mode="plan",
        runner_kind="kubernetes_executor",
        runner_policy="release",
        smoke_name="orders-smoke",
        timeout_seconds=300,
    )

    assert not report.passed
    assert any(blocker.code == "airflow_k8s_image_digest_required" for blocker in report.blockers)


def test_airflow_k8s_smoke_live_results_block_failed_required_command() -> None:
    planner = GitOpsAirflowK8sSmokePlanner()
    plan = planner.plan(
        run_spec_path=".dpone/gitops/airflow/run-spec.json",
        run_spec=_run_spec(),
        runtime_profile_path=".dpone/gitops/airflow/runtime-profile.json",
        runtime_profile=_runtime_profile(),
        pod_contract_path=".dpone/gitops/airflow/pod-contract.json",
        pod_contract=_pod_contract(),
        image_contract_path=".dpone/gitops/airflow/image-contract.json",
        image_contract=_image_contract(),
        xcom_summary_path=".dpone/gitops/airflow/xcom-summary.json",
        xcom_summary=_xcom_summary(),
        mode="live",
        runner_kind="kubernetes_pod_operator",
        runner_policy="release",
        smoke_name="orders-smoke",
        timeout_seconds=300,
    )
    failed = GitOpsAirflowK8sCommandResult(
        name="kubectl_run_spec_smoke",
        command="kubectl run orders-smoke",
        exit_code=1,
        stdout="",
        stderr="ImagePullBackOff",
        duration_seconds=2.1,
    )

    report = planner.with_results(plan, results=(failed,))

    assert not report.passed
    assert any(blocker.code == "airflow_k8s_command_failed" for blocker in report.blockers)
    assert report.results[0].stderr == "ImagePullBackOff"


def test_airflow_k8s_subprocess_runner_returns_timeout_result(monkeypatch) -> None:
    def timeout_run(*_args: object, **_kwargs: object) -> object:
        raise subprocess.TimeoutExpired(cmd="kubectl run smoke", timeout=1, stderr=b"still pending")

    monkeypatch.setattr(subprocess, "run", timeout_run)
    command = GitOpsAirflowK8sSmokeCommand(
        name="kubectl_run_spec_smoke",
        kind="kubernetes_pod_operator",
        command="kubectl run smoke",
        required=True,
        timeout_seconds=1,
    )

    result = SubprocessAirflowK8sSmokeRunner().run(command=command, cwd=Path("."))

    assert result.exit_code == 124
    assert result.stderr == "still pending"
