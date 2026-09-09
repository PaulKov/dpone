from __future__ import annotations

import json
import logging
from argparse import Namespace
from pathlib import Path

from dpone.adapters.fs_local import LocalFileSystem
from dpone.adapters.yaml_pyyaml import PyYamlCodec
from dpone.app.context import AppContext
from dpone.app.settings import Settings
from dpone.commands.gitops.airflow_admission_check_cmd import cmd_gitops_airflow_admission_check
from dpone.gitops.airflow_admission_check_runner import StaticAirflowAdmissionCheckRunner


def _ctx(tmp_path: Path) -> AppContext:
    settings = Settings(repo_root=tmp_path, project_dir=tmp_path, manifest_dir=tmp_path, sources_registry_paths=())
    return AppContext(settings=settings, logger=logging.getLogger("test"), fs=LocalFileSystem(), yaml=PyYamlCodec())


def _args(**overrides: object) -> Namespace:
    data = {
        "artifact_dir": ".dpone/gitops/airflow",
        "manifest_path": ".dpone/gitops/airflow/airflow-k8s-manifests.yaml",
        "pod_spec_path": ".dpone/gitops/airflow/pod-spec.yaml",
        "mode": "plan",
        "runner_policy": "release",
        "timeout_seconds": 120,
        "kubectl": "kubectl",
        "output": None,
        "format": "json",
    }
    data.update(overrides)
    return Namespace(**data)


def test_admission_check_plan_builds_server_dry_run_commands(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    monkeypatch.chdir(tmp_path)
    artifact_dir = _write_admission_artifacts(tmp_path)

    code = cmd_gitops_airflow_admission_check(
        _args(output=".dpone/gitops/airflow/airflow-admission-check.json"),
        ctx=_ctx(tmp_path),
        logger=logging.getLogger("test"),
    )
    payload = json.loads(capsys.readouterr().out)
    file_payload = json.loads((artifact_dir / "airflow-admission-check.json").read_text("utf-8"))

    assert code == 0
    assert payload == file_payload
    assert payload["kind"] == "gitops.airflow_admission_check"
    assert payload["mode"] == "plan"
    assert [command["name"] for command in payload["commands"]] == [
        "kubectl_apply_airflow_k8s_manifests",
        "kubectl_apply_pod_spec",
    ]
    assert all("--dry-run=server" in command["command"] for command in payload["commands"])
    assert all(command["executed"] is False for command in payload["commands"])
    assert str(tmp_path) not in json.dumps(payload)


def test_admission_check_live_uses_injected_runner(tmp_path: Path, monkeypatch, capsys) -> None:
    monkeypatch.chdir(tmp_path)
    _write_admission_artifacts(tmp_path)

    code = cmd_gitops_airflow_admission_check(
        _args(mode="live"),
        ctx=_ctx(tmp_path),
        logger=logging.getLogger("test"),
        runner=StaticAirflowAdmissionCheckRunner(exit_codes={}),
    )
    payload = json.loads(capsys.readouterr().out)

    assert code == 0
    assert all(command["executed"] for command in payload["commands"])
    assert all(result["exit_code"] == 0 for result in payload["results"])


def test_admission_check_blocks_server_side_policy_failure(tmp_path: Path, monkeypatch, capsys) -> None:
    monkeypatch.chdir(tmp_path)
    _write_admission_artifacts(tmp_path)

    code = cmd_gitops_airflow_admission_check(
        _args(mode="live"),
        ctx=_ctx(tmp_path),
        logger=logging.getLogger("test"),
        runner=StaticAirflowAdmissionCheckRunner(
            exit_codes={"kubectl_apply_pod_spec": 1},
            stderr={"kubectl_apply_pod_spec": "denied by PodSecurity: hostPath volumes are forbidden"},
        ),
    )
    payload = json.loads(capsys.readouterr().out)

    assert code == 2
    assert any(blocker["code"] == "airflow_admission_command_failed" for blocker in payload["blockers"])
    assert "hostPath" in json.dumps(payload["blockers"])


def _write_admission_artifacts(tmp_path: Path) -> Path:
    artifact_dir = tmp_path / ".dpone/gitops/airflow"
    artifact_dir.mkdir(parents=True)
    (artifact_dir / "airflow-k8s-manifests.yaml").write_text(
        "\n".join(
            [
                "apiVersion: v1",
                "kind: ServiceAccount",
                "metadata:",
                "  name: dpone-runner",
                "  namespace: dpone-runners",
                "",
            ]
        ),
        encoding="utf-8",
    )
    (artifact_dir / "pod-spec.yaml").write_text(
        "\n".join(
            [
                "apiVersion: v1",
                "kind: Pod",
                "metadata:",
                "  name: dpone-runtime",
                "  namespace: dpone-runners",
                "spec:",
                "  containers:",
                "    - name: base",
                "      image: ghcr.io/acme/dpone:2026.06.17",
                "",
            ]
        ),
        encoding="utf-8",
    )
    return artifact_dir
