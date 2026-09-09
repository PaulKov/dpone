from __future__ import annotations

import json
import logging
from argparse import Namespace
from pathlib import Path

from dpone.adapters.fs_local import LocalFileSystem
from dpone.adapters.yaml_pyyaml import PyYamlCodec
from dpone.app.context import AppContext
from dpone.app.settings import Settings
from dpone.gitops.airflow_runtime_executor import AirflowCommandResult
from dpone.services.gitops.airflow_runtime_exec_service import GitOpsAirflowRunSpecExecService


def test_run_spec_exec_preserves_step_stdout_and_stderr(tmp_path: Path) -> None:
    _write_run_spec(tmp_path)

    GitOpsAirflowRunSpecExecService(
        ctx=_ctx(tmp_path),
        runner=_OutputRunner(),
    ).build_view(_exec_args())
    evidence = json.loads((tmp_path / ".dpone/gitops/airflow/runtime-evidence.json").read_text(encoding="utf-8"))
    failed_step = evidence["steps"][-1]

    assert failed_step["status"] == "failed"
    assert failed_step["stdout"] == "stdout from dpone run dpone_workloads/manifests/orders.yaml"
    assert failed_step["stderr"] == "stderr from dpone run dpone_workloads/manifests/orders.yaml"


def test_run_spec_exec_blocks_invalid_identity_before_running_commands(tmp_path: Path) -> None:
    _write_run_spec(tmp_path)
    secret_marker = "must-not-leak"

    view = GitOpsAirflowRunSpecExecService(
        ctx=_ctx(tmp_path),
        runner=_FailIfCalledRunner(),
        run_identity_json=json.dumps({"token": secret_marker}),
    ).build_view(_exec_args())
    evidence_text = (tmp_path / ".dpone/gitops/airflow/runtime-evidence.json").read_text(encoding="utf-8")
    xcom_text = (tmp_path / ".dpone/gitops/airflow/xcom-summary.json").read_text(encoding="utf-8")

    assert view.exit_code == 2
    assert "DPONE_AIRFLOW_RUN_IDENTITY_INVALID" in evidence_text
    assert "DPONE_AIRFLOW_RUN_IDENTITY_INVALID" in xcom_text
    assert secret_marker not in evidence_text
    assert secret_marker not in xcom_text


class _OutputRunner:
    def run(self, *, command: str, cwd: Path) -> AirflowCommandResult:
        _ = cwd
        return AirflowCommandResult(
            exit_code=7 if command.startswith("dpone run ") else 0,
            stdout=f"stdout from {command}",
            stderr=f"stderr from {command}",
        )


class _FailIfCalledRunner:
    def run(self, *, command: str, cwd: Path) -> AirflowCommandResult:
        raise AssertionError(f"runner must not be called: command={command!r}, cwd={cwd!s}")


def _ctx(tmp_path: Path) -> AppContext:
    settings = Settings(repo_root=tmp_path, project_dir=tmp_path, manifest_dir=tmp_path, sources_registry_paths=())
    return AppContext(settings=settings, logger=logging.getLogger("test"), fs=LocalFileSystem(), yaml=PyYamlCodec())


def _exec_args() -> Namespace:
    return Namespace(
        run_spec_path=".dpone/gitops/airflow/run-spec.json",
        evidence_output=".dpone/gitops/airflow/runtime-evidence.json",
        xcom_output=".dpone/gitops/airflow/xcom-summary.json",
        format="json",
        output=None,
    )


def _write_run_spec(tmp_path: Path) -> None:
    run_spec_path = tmp_path / ".dpone/gitops/airflow/run-spec.json"
    run_spec_path.parent.mkdir(parents=True)
    run_spec_path.write_text(json.dumps(_run_spec(), indent=2), encoding="utf-8")


def _run_spec() -> dict[str, object]:
    return {
        "bundle_path": ".dpone/gitops/bundle/bundle.json",
        "image": "ghcr.io/acme/dpone:runtime",
        "image_digest": "sha256:" + "d" * 64,
        "worktree": ".",
        "steps": [
            {
                "name": "bundle_verify",
                "kind": "bundle_verify",
                "command": "dpone gitops bundle verify .dpone/gitops/bundle/bundle.json",
                "required": True,
            },
            {
                "name": "run_orders",
                "kind": "dpone_run",
                "command": "dpone run dpone_workloads/manifests/orders.yaml",
                "required": True,
                "manifest": "dpone_workloads/manifests/orders.yaml",
            },
        ],
    }
