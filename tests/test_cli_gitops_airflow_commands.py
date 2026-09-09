from __future__ import annotations

import json
import logging
import subprocess
from argparse import Namespace
from pathlib import Path

from dpone.adapters.fs_local import LocalFileSystem
from dpone.adapters.yaml_pyyaml import PyYamlCodec
from dpone.app.context import AppContext
from dpone.app.settings import Settings
from dpone.commands.gitops.airflow_cmd import (
    cmd_gitops_airflow_doctor,
    cmd_gitops_airflow_evidence_bundle,
    cmd_gitops_airflow_evidence_verify,
    cmd_gitops_airflow_image_contract,
    cmd_gitops_airflow_k8s_smoke,
    cmd_gitops_airflow_outcome_gate,
    cmd_gitops_airflow_pod_contract,
    cmd_gitops_airflow_pod_doctor,
    cmd_gitops_airflow_pod_watch,
    cmd_gitops_airflow_render,
    cmd_gitops_airflow_run_spec,
    cmd_gitops_airflow_run_spec_exec,
    cmd_gitops_airflow_runtime_profile,
)
from dpone.commands.registry_gitops import gitops_group
from dpone.services.gitops.bundle_service import GitOpsBundleService

ROOT = Path(__file__).resolve().parents[1]


def _airflow_run_identity_json() -> str:
    return json.dumps(
        {
            "schema": "dpone.airflow-run-identity.v1",
            "release_id": "sha256:" + "1" * 64,
            "deployment_id": "sha256:" + "2" * 64,
            "dag_spec": {"id": "dpone_gitops", "sha256": "sha256:" + "3" * 64},
            "workload_pack": {"id": "dpone_orders", "sha256": "sha256:" + "4" * 64},
            "runtime_image_digest": "sha256:" + "a" * 64,
            "binding_set_ref": "sha256:" + "5" * 64,
            "connection_registry_ref": "sha256:" + "6" * 64,
            "credential_runtime_ref": "sha256:" + "7" * 64,
            "airflow_bundle": {
                "backend": "git",
                "ref": "git:7ac31f2",
                "versioned": True,
                "version": "7ac31f2",
                "snapshot_ref": None,
            },
        },
        sort_keys=True,
    )


def test_airflow_cli_facade_stays_below_hard_sloc_limit() -> None:
    command_module = ROOT / "src" / "dpone" / "commands" / "gitops" / "airflow_cmd.py"
    runtime_profile_module = ROOT / "src" / "dpone" / "commands" / "gitops" / "airflow_runtime_profile_cmd.py"

    assert _nonblank_sloc(command_module) <= 400
    assert _nonblank_sloc(runtime_profile_module) <= 400


def _nonblank_sloc(path: Path) -> int:
    return sum(1 for line in path.read_text(encoding="utf-8").splitlines() if line.strip())


def _ctx(tmp_path: Path) -> AppContext:
    settings = Settings(repo_root=tmp_path, project_dir=tmp_path, manifest_dir=tmp_path, sources_registry_paths=())
    return AppContext(settings=settings, logger=logging.getLogger("test"), fs=LocalFileSystem(), yaml=PyYamlCodec())


def _bundle_args(**overrides: object) -> Namespace:
    data = {
        "changed_files": ["dpone_workloads/manifests/seed.yaml"],
        "changed_files_file": None,
        "from_ref": None,
        "to_ref": None,
        "workload_root": None,
        "manifest_glob": "manifests/**/*.yaml",
        "include_global_overrides": False,
        "include_env_overrides": [],
        "include_registry": False,
        "registry": [],
        "support_path": [],
        "runner": "airflow",
        "worktree": ".",
        "verify_lock": True,
        "fail_on_empty_impact": False,
        "fail_on_warnings": False,
        "require_lock": True,
        "policy_profile": "release",
        "attest": True,
        "output_dir": ".dpone/gitops/bundle",
        "output": None,
        "format": "json",
    }
    data.update(overrides)
    return Namespace(**data)


def _render_args(**overrides: object) -> Namespace:
    data = {
        "bundle_path": ".dpone/gitops/bundle/bundle.json",
        "output_dir": ".dpone/gitops/airflow",
        "image": "ghcr.io/acme/dpone:2026.06.16",
        "image_digest": None,
        "dpone_version": None,
        "python_version": None,
        "airflow_provider_version": None,
        "tool": [],
        "user": None,
        "workdir": None,
        "entrypoint": None,
        "image_contract": ".dpone/gitops/airflow/image-contract.json",
        "namespace": "dpone-runners",
        "service_account": "dpone-runner",
        "dag_id": "dpone_gitops",
        "task_id": "dpone_orders",
        "require_attestation": True,
        "format": "json",
        "output": None,
    }
    data.update(overrides)
    return Namespace(**data)


def _doctor_args(**overrides: object) -> Namespace:
    data = {
        "bundle_path": ".dpone/gitops/bundle/bundle.json",
        "pod_template": ".dpone/gitops/airflow/pod_template.yaml",
        "image_contract": ".dpone/gitops/airflow/image-contract.json",
        "image": "ghcr.io/acme/dpone:2026.06.16",
        "require_attestation": True,
        "runner_policy": "advisory",
        "format": "json",
        "output": None,
    }
    data.update(overrides)
    return Namespace(**data)


def _image_contract_args(**overrides: object) -> Namespace:
    data = {
        "image": "ghcr.io/acme/dpone:2026.06.16",
        "image_digest": None,
        "dpone_version": "0.11.0",
        "python_version": "3.12",
        "airflow_provider_version": "10.18.0",
        "tool": ["dpone", "bcp"],
        "user": None,
        "workdir": None,
        "entrypoint": None,
        "output_path": ".dpone/gitops/airflow/image-contract.json",
        "format": "json",
        "output": None,
    }
    data.update(overrides)
    return Namespace(**data)


def _run_spec_args(**overrides: object) -> Namespace:
    data = {
        "bundle_path": ".dpone/gitops/bundle/bundle.json",
        "output_path": ".dpone/gitops/airflow/run-spec.json",
        "evidence_output": ".dpone/gitops/airflow/runtime-evidence.json",
        "image": "ghcr.io/acme/dpone:2026.06.16",
        "image_digest": "sha256:" + "e" * 64,
        "worktree": ".",
        "require_attestation": True,
        "format": "json",
        "output": None,
    }
    data.update(overrides)
    return Namespace(**data)


def _run_spec_exec_args(**overrides: object) -> Namespace:
    data = {
        "run_spec_path": ".dpone/gitops/airflow/run-spec.json",
        "evidence_output": ".dpone/gitops/airflow/runtime-evidence.json",
        "xcom_output": ".dpone/gitops/airflow/xcom-summary.json",
        "format": "json",
        "output": None,
    }
    data.update(overrides)
    return Namespace(**data)


def _runtime_profile_args(**overrides: object) -> Namespace:
    data = {
        "bundle_path": ".dpone/gitops/bundle/bundle.json",
        "run_spec_path": ".dpone/gitops/airflow/run-spec.json",
        "runtime_evidence_path": ".dpone/gitops/airflow/runtime-evidence.json",
        "output_path": ".dpone/gitops/airflow/runtime-profile.json",
        "xcom_summary_path": ".dpone/gitops/airflow/xcom-summary.json",
        "dag_factory_path": ".dpone/gitops/airflow/airflow_dag_factory.py",
        "outcome_gate_path": ".dpone/gitops/airflow/outcome_gate.py",
        "image": "ghcr.io/acme/dpone:2026.06.16",
        "image_digest": "sha256:" + "f" * 64,
        "namespace": "dpone-runners",
        "service_account": "dpone-runner",
        "cpu_request": "250m",
        "memory_request": "512Mi",
        "cpu_limit": "2",
        "memory_limit": "2Gi",
        "artifact_sink_kind": "local",
        "artifact_sink_path": ".dpone/gitops/airflow",
        "label": ["app.kubernetes.io/name=dpone"],
        "annotation": ["dpone.dev/runner=airflow"],
        "env": ["DPONE_PROFILE=release"],
        "runner_policy": "release",
        "outcome_mode": "strict_fail",
        "format": "json",
        "output": None,
    }
    data.update(overrides)
    return Namespace(**data)


def _pod_contract_args(**overrides: object) -> Namespace:
    data = {
        "bundle_path": ".dpone/gitops/bundle/bundle.json",
        "run_spec_path": ".dpone/gitops/airflow/run-spec.json",
        "runtime_profile_path": ".dpone/gitops/airflow/runtime-profile.json",
        "output_path": ".dpone/gitops/airflow/pod-contract.json",
        "pod_spec_path": ".dpone/gitops/airflow/pod-spec.yaml",
        "kpo_kwargs_path": ".dpone/gitops/airflow/kpo-kwargs.json",
        "image_pull_secret": ["regcred"],
        "volume": ["dpone-artifacts=.dpone/gitops/airflow"],
        "volume_mount": ["dpone-artifacts=/workspace/.dpone/gitops/airflow:ro"],
        "env_from_configmap": ["dpone-runner-config"],
        "env_secret": ["DPONE_TOKEN=dpone-token:token"],
        "node_selector": ["workload=dpone"],
        "toleration": ["dedicated=dpone:NoSchedule"],
        "label": ["app.kubernetes.io/component=dpone-runner"],
        "annotation": ["dpone.dev/pod-contract=enabled"],
        "on_finish_action": "delete_pod",
        "get_logs": True,
        "deferrable": False,
        "outcome_mode": "strict_fail",
        "format": "json",
        "output": None,
    }
    data.update(overrides)
    return Namespace(**data)


def _pod_doctor_args(**overrides: object) -> Namespace:
    data = {
        "artifact_dir": ".dpone/gitops/airflow",
        "pod_contract_path": ".dpone/gitops/airflow/pod-contract.json",
        "pod_spec_path": ".dpone/gitops/airflow/pod-spec.yaml",
        "kpo_kwargs_path": ".dpone/gitops/airflow/kpo-kwargs.json",
        "runner_policy": "release",
        "format": "json",
        "output": None,
    }
    data.update(overrides)
    return Namespace(**data)


def _evidence_verify_args(**overrides: object) -> Namespace:
    data = {
        "run_spec_path": ".dpone/gitops/airflow/run-spec.json",
        "evidence_path": ".dpone/gitops/airflow/runtime-evidence.json",
        "require_all_steps": True,
        "format": "json",
        "output": None,
    }
    data.update(overrides)
    return Namespace(**data)


def _outcome_gate_args(**overrides: object) -> Namespace:
    data = {
        "xcom_summary_path": ".dpone/gitops/airflow/xcom-summary.json",
        "required_status": "passed",
        "format": "json",
        "output": None,
    }
    data.update(overrides)
    return Namespace(**data)


def _k8s_smoke_args(**overrides: object) -> Namespace:
    data = {
        "run_spec_path": ".dpone/gitops/airflow/run-spec.json",
        "runtime_profile_path": ".dpone/gitops/airflow/runtime-profile.json",
        "pod_contract_path": ".dpone/gitops/airflow/pod-contract.json",
        "image_contract_path": ".dpone/gitops/airflow/image-contract.json",
        "xcom_summary_path": ".dpone/gitops/airflow/xcom-summary.json",
        "mode": "plan",
        "runner_kind": "kubernetes_pod_operator",
        "runner_policy": "release",
        "smoke_name": "orders-smoke",
        "timeout_seconds": 300,
        "kubectl": "kubectl",
        "airflow_cmd": "airflow",
        "format": "json",
        "output": None,
    }
    data.update(overrides)
    return Namespace(**data)


def _pod_watch_args(**overrides: object) -> Namespace:
    data = {
        "runtime_profile_path": ".dpone/gitops/airflow/runtime-profile.json",
        "pod_contract_path": ".dpone/gitops/airflow/pod-contract.json",
        "image_contract_path": ".dpone/gitops/airflow/image-contract.json",
        "runtime_evidence_path": ".dpone/gitops/airflow/runtime-evidence.json",
        "xcom_summary_path": ".dpone/gitops/airflow/xcom-summary.json",
        "pod_name": None,
        "mode": "plan",
        "runner_policy": "release",
        "expected_phase": "Succeeded",
        "timeout_seconds": 300,
        "log_tail_lines": 200,
        "kubectl": "kubectl",
        "format": "json",
        "output": None,
    }
    data.update(overrides)
    return Namespace(**data)


def _evidence_bundle_args(**overrides: object) -> Namespace:
    data = {
        "bundle_path": ".dpone/gitops/bundle/bundle.json",
        "run_spec_path": ".dpone/gitops/airflow/run-spec.json",
        "runtime_profile_path": ".dpone/gitops/airflow/runtime-profile.json",
        "pod_contract_path": ".dpone/gitops/airflow/pod-contract.json",
        "runtime_evidence_path": ".dpone/gitops/airflow/runtime-evidence.json",
        "xcom_summary_path": ".dpone/gitops/airflow/xcom-summary.json",
        "k8s_smoke_path": ".dpone/gitops/airflow/airflow-k8s-smoke.json",
        "pod_launch_evidence_path": ".dpone/gitops/airflow/airflow-pod-launch-evidence.json",
        "dag_id": "dpone_gitops",
        "task_id": "dpone_orders",
        "run_id": "manual__2026-06-16T10:00:00+00:00",
        "try_number": 2,
        "map_index": -1,
        "pod_name": "dpone-gitops-runtime",
        "pod_uid": "pod-uid-456",
        "runner_policy": "release",
        "require_k8s_smoke": False,
        "require_pod_launch_evidence": False,
        "format": "json",
        "output": None,
    }
    data.update(overrides)
    return Namespace(**data)


def _write(path: Path, text: str = "source: {}\nsink: {}\n") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


def _build_airflow_runtime_artifacts(tmp_path: Path, capsys) -> None:
    from dpone.gitops.airflow_runtime_executor import AirflowCommandResult

    class _CorrelatedRunner:
        def run(self, *, command: str, cwd: Path) -> AirflowCommandResult:
            del cwd
            stdout = json.dumps({"run_id": "orders-run-1", "process": "orders"}) if "dpone run" in command else ""
            return AirflowCommandResult(exit_code=0, stdout=stdout)

    _build_attested_bundle(tmp_path)
    cmd_gitops_airflow_image_contract(
        _image_contract_args(image_digest="sha256:" + "a" * 64, dpone_version="0.12.0", tool=["dpone", "kubectl"]),
        ctx=_ctx(tmp_path),
        logger=logging.getLogger("test"),
    )
    cmd_gitops_airflow_run_spec(
        _run_spec_args(image_digest="sha256:" + "a" * 64),
        ctx=_ctx(tmp_path),
        logger=logging.getLogger("test"),
    )
    cmd_gitops_airflow_runtime_profile(
        _runtime_profile_args(image_digest="sha256:" + "a" * 64, outcome_mode="xcom_then_gate"),
        ctx=_ctx(tmp_path),
        logger=logging.getLogger("test"),
    )
    cmd_gitops_airflow_pod_contract(
        _pod_contract_args(outcome_mode="xcom_then_gate"),
        ctx=_ctx(tmp_path),
        logger=logging.getLogger("test"),
    )
    cmd_gitops_airflow_run_spec_exec(
        _run_spec_exec_args(),
        ctx=_ctx(tmp_path),
        logger=logging.getLogger("test"),
        runner=_CorrelatedRunner(),
    )
    capsys.readouterr()


def _pod_watch_pod_stdout(
    *,
    phase: str = "Succeeded",
    restart_count: int = 0,
    exit_code: int = 0,
    reason: str = "Completed",
) -> str:
    image = "ghcr.io/acme/dpone:2026.06.16"
    digest = "sha256:" + "a" * 64
    return json.dumps(
        {
            "kind": "Pod",
            "metadata": {"name": "dpone-gitops-runtime", "namespace": "dpone-runners"},
            "spec": {"serviceAccountName": "dpone-runner", "nodeName": "node-a"},
            "status": {
                "phase": phase,
                "containerStatuses": [
                    {
                        "name": "base",
                        "image": image,
                        "imageID": f"{image}@{digest}",
                        "ready": False,
                        "restartCount": restart_count,
                        "state": {"terminated": {"exitCode": exit_code, "reason": reason, "message": reason}},
                    }
                ],
            },
        }
    )


def _pod_watch_events_stdout(*, reason: str = "Started", event_type: str = "Normal") -> str:
    return json.dumps(
        {
            "items": [
                {
                    "type": event_type,
                    "reason": reason,
                    "message": "Started container base",
                    "count": 1,
                }
            ]
        }
    )


def _build_attested_bundle(tmp_path: Path) -> None:
    workload = tmp_path / "dpone_workloads"
    _write(workload / "manifests" / "orders.yaml", "depends_on:\n  - path: seed.yaml\nsource: {}\nsink: {}\n")
    _write(workload / "manifests" / "seed.yaml")
    subprocess.run(["git", "init"], cwd=tmp_path, check=True, capture_output=True, text=True)
    subprocess.run(["git", "config", "user.email", "test@example.com"], cwd=tmp_path, check=True)
    subprocess.run(["git", "config", "user.name", "Test User"], cwd=tmp_path, check=True)
    subprocess.run(["git", "add", "."], cwd=tmp_path, check=True, capture_output=True, text=True)
    subprocess.run(["git", "commit", "-m", "baseline"], cwd=tmp_path, check=True, capture_output=True, text=True)
    assert GitOpsBundleService(ctx=_ctx(tmp_path)).build_view(_bundle_args()).exit_code == 0


def test_gitops_airflow_render_cli_prints_json_and_writes_optional_output(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    monkeypatch.chdir(tmp_path)
    _build_attested_bundle(tmp_path)

    code = cmd_gitops_airflow_render(
        _render_args(output=".dpone/gitops/airflow-render.json"),
        ctx=_ctx(tmp_path),
        logger=logging.getLogger("test"),
    )
    stdout_payload = json.loads(capsys.readouterr().out)
    file_payload = json.loads((tmp_path / ".dpone/gitops/airflow-render.json").read_text(encoding="utf-8"))

    assert code == 0
    assert stdout_payload["kind"] == "gitops.airflow_render"
    assert stdout_payload["artifacts"][0]["path"] == ".dpone/gitops/airflow/pod_template.yaml"
    assert any(artifact["kind"] == "run_spec" for artifact in stdout_payload["artifacts"])
    assert file_payload == stdout_payload
    assert str(tmp_path) not in json.dumps(stdout_payload)


def test_gitops_airflow_render_cli_writes_image_digest_to_contract(tmp_path: Path, monkeypatch, capsys) -> None:
    monkeypatch.chdir(tmp_path)
    _build_attested_bundle(tmp_path)

    code = cmd_gitops_airflow_render(
        _render_args(image_digest="sha256:" + "c" * 64, tool=["dpone", "clickhouse-client"]),
        ctx=_ctx(tmp_path),
        logger=logging.getLogger("test"),
    )
    payload = json.loads(capsys.readouterr().out)
    contract = json.loads((tmp_path / ".dpone/gitops/airflow/image-contract.json").read_text(encoding="utf-8"))

    assert code == 0
    assert payload["kind"] == "gitops.airflow_render"
    assert contract["image_digest"] == "sha256:" + "c" * 64
    assert contract["tools"] == ["dpone", "clickhouse-client"]


def test_gitops_airflow_doctor_cli_blocks_release_policy_without_image_digest(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    monkeypatch.chdir(tmp_path)
    _build_attested_bundle(tmp_path)
    cmd_gitops_airflow_render(_render_args(), ctx=_ctx(tmp_path), logger=logging.getLogger("test"))
    capsys.readouterr()

    code = cmd_gitops_airflow_doctor(
        _doctor_args(runner_policy="release"),
        ctx=_ctx(tmp_path),
        logger=logging.getLogger("test"),
    )
    payload = json.loads(capsys.readouterr().out)

    assert code == 2
    assert payload["runner_policy"] == "release"
    assert any(blocker["code"] == "image_digest_required" for blocker in payload["blockers"])


def test_gitops_airflow_doctor_cli_prints_markdown(tmp_path: Path, monkeypatch, capsys) -> None:
    monkeypatch.chdir(tmp_path)
    _build_attested_bundle(tmp_path)
    cmd_gitops_airflow_render(_render_args(), ctx=_ctx(tmp_path), logger=logging.getLogger("test"))
    capsys.readouterr()

    code = cmd_gitops_airflow_doctor(
        _doctor_args(format="markdown"),
        ctx=_ctx(tmp_path),
        logger=logging.getLogger("test"),
    )
    output = capsys.readouterr().out

    assert code == 0
    assert "# GitOps Airflow doctor" in output
    assert "pod_template_base_container" in output
    assert "ghcr.io/acme/dpone:2026.06.16" in output


def test_gitops_airflow_image_contract_cli_prints_json(tmp_path: Path, monkeypatch, capsys) -> None:
    monkeypatch.chdir(tmp_path)

    code = cmd_gitops_airflow_image_contract(
        _image_contract_args(),
        ctx=_ctx(tmp_path),
        logger=logging.getLogger("test"),
    )
    payload = json.loads(capsys.readouterr().out)

    assert code == 0
    assert payload["kind"] == "gitops.airflow_image_contract"
    assert payload["contract"]["tools"] == ["dpone", "bcp"]
    assert (tmp_path / ".dpone/gitops/airflow/image-contract.json").exists()


def test_gitops_airflow_run_spec_cli_prints_json_and_writes_contract(tmp_path: Path, monkeypatch, capsys) -> None:
    monkeypatch.chdir(tmp_path)
    _build_attested_bundle(tmp_path)

    code = cmd_gitops_airflow_run_spec(
        _run_spec_args(output=".dpone/gitops/airflow-run-spec.json"),
        ctx=_ctx(tmp_path),
        logger=logging.getLogger("test"),
    )
    stdout_payload = json.loads(capsys.readouterr().out)
    file_payload = json.loads((tmp_path / ".dpone/gitops/airflow-run-spec.json").read_text(encoding="utf-8"))
    run_spec = json.loads((tmp_path / ".dpone/gitops/airflow/run-spec.json").read_text(encoding="utf-8"))

    assert code == 0
    assert stdout_payload["kind"] == "gitops.airflow_run_spec"
    assert stdout_payload["image_digest"] == "sha256:" + "e" * 64
    assert [step["kind"] for step in stdout_payload["steps"]] == ["bundle_verify", "gitops_verify", "dpone_run"]
    assert file_payload == stdout_payload
    assert run_spec == {key: value for key, value in stdout_payload.items() if key != "meta"}
    assert str(tmp_path) not in json.dumps(stdout_payload)


def test_gitops_airflow_runtime_profile_cli_prints_json_and_writes_artifacts(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    monkeypatch.chdir(tmp_path)
    _build_attested_bundle(tmp_path)
    cmd_gitops_airflow_run_spec(_run_spec_args(), ctx=_ctx(tmp_path), logger=logging.getLogger("test"))
    capsys.readouterr()

    code = cmd_gitops_airflow_runtime_profile(
        _runtime_profile_args(output=".dpone/gitops/airflow-runtime-profile-report.json"),
        ctx=_ctx(tmp_path),
        logger=logging.getLogger("test"),
    )
    stdout_payload = json.loads(capsys.readouterr().out)
    file_payload = json.loads(
        (tmp_path / ".dpone/gitops/airflow-runtime-profile-report.json").read_text(encoding="utf-8")
    )
    profile = json.loads((tmp_path / ".dpone/gitops/airflow/runtime-profile.json").read_text(encoding="utf-8"))

    assert code == 0
    assert stdout_payload["kind"] == "gitops.airflow_runtime_profile"
    assert stdout_payload["image_digest"] == "sha256:" + "f" * 64
    assert stdout_payload["xcom_summary_path"] == ".dpone/gitops/airflow/xcom-summary.json"
    assert stdout_payload["outcome_mode"] == "strict_fail"
    assert stdout_payload["outcome_gate_path"] == ".dpone/gitops/airflow/outcome_gate.py"
    assert stdout_payload["artifact_sink"] == {"kind": "local", "path": ".dpone/gitops/airflow"}
    assert file_payload == stdout_payload
    assert profile == {key: value for key, value in stdout_payload.items() if key != "meta"}
    assert (tmp_path / ".dpone/gitops/airflow/xcom-summary.json").exists()
    assert (tmp_path / ".dpone/gitops/airflow/airflow_dag_factory.py").exists()
    assert (tmp_path / ".dpone/gitops/airflow/outcome_gate.py").exists()
    assert str(tmp_path) not in json.dumps(stdout_payload)


def test_gitops_airflow_runtime_profile_cli_serializes_git_sync_partial_clone(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    monkeypatch.chdir(tmp_path)
    _build_attested_bundle(tmp_path)
    cmd_gitops_airflow_run_spec(_run_spec_args(), ctx=_ctx(tmp_path), logger=logging.getLogger("test"))
    capsys.readouterr()

    code = cmd_gitops_airflow_runtime_profile(
        _runtime_profile_args(
            git_sync_repo="ssh://git@git.example.test/platform/example-workloads.git",
            git_sync_ref="main",
            git_sync_image="registry.k8s.io/git-sync/git-sync:v4.7.0",
            git_sync_depth=2,
            git_sync_filter="blob:none",
            git_sync_auth_mode="ssh_secret",
            git_sync_ssh_secret="example-workloads-git",
        ),
        ctx=_ctx(tmp_path),
        logger=logging.getLogger("test"),
    )
    stdout_payload = json.loads(capsys.readouterr().out)

    assert code == 0
    assert stdout_payload["git_sync"]["clone"] == {"depth": 2, "filter": "blob:none"}
    assert stdout_payload["git_sync"]["image"] == "registry.k8s.io/git-sync/git-sync:v4.7.0"

    cmd_gitops_airflow_pod_contract(_pod_contract_args(), ctx=_ctx(tmp_path), logger=logging.getLogger("test"))
    pod_spec = PyYamlCodec().load((tmp_path / ".dpone/gitops/airflow/pod-spec.yaml").read_text(encoding="utf-8"))
    git_sync_args = pod_spec["spec"]["initContainers"][1]["args"]
    assert "--depth=2" in git_sync_args
    assert "--filter=blob:none" in git_sync_args


def test_gitops_airflow_pod_contract_cli_prints_json_and_writes_artifacts(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    monkeypatch.chdir(tmp_path)
    _build_attested_bundle(tmp_path)
    cmd_gitops_airflow_run_spec(_run_spec_args(), ctx=_ctx(tmp_path), logger=logging.getLogger("test"))
    cmd_gitops_airflow_runtime_profile(_runtime_profile_args(), ctx=_ctx(tmp_path), logger=logging.getLogger("test"))
    capsys.readouterr()

    code = cmd_gitops_airflow_pod_contract(
        _pod_contract_args(output=".dpone/gitops/airflow-pod-contract-report.json"),
        ctx=_ctx(tmp_path),
        logger=logging.getLogger("test"),
    )
    stdout_payload = json.loads(capsys.readouterr().out)
    file_payload = json.loads((tmp_path / ".dpone/gitops/airflow-pod-contract-report.json").read_text("utf-8"))

    assert code == 0
    assert stdout_payload["kind"] == "gitops.airflow_pod_contract"
    assert stdout_payload["pod_spec_path"] == ".dpone/gitops/airflow/pod-spec.yaml"
    assert stdout_payload["kpo_kwargs_path"] == ".dpone/gitops/airflow/kpo-kwargs.json"
    assert stdout_payload["xcom"]["return_path"] == "/airflow/xcom/return.json"
    assert stdout_payload["xcom"]["outcome_mode"] == "strict_fail"
    assert file_payload == stdout_payload
    assert (tmp_path / ".dpone/gitops/airflow/pod-contract.json").exists()
    assert (tmp_path / ".dpone/gitops/airflow/pod-spec.yaml").exists()
    assert (tmp_path / ".dpone/gitops/airflow/kpo-kwargs.json").exists()
    assert str(tmp_path) not in json.dumps(stdout_payload)


def test_gitops_airflow_pod_doctor_cli_validates_artifact_dir(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    monkeypatch.chdir(tmp_path)
    _build_attested_bundle(tmp_path)
    cmd_gitops_airflow_run_spec(_run_spec_args(), ctx=_ctx(tmp_path), logger=logging.getLogger("test"))
    cmd_gitops_airflow_runtime_profile(_runtime_profile_args(), ctx=_ctx(tmp_path), logger=logging.getLogger("test"))
    cmd_gitops_airflow_pod_contract(_pod_contract_args(), ctx=_ctx(tmp_path), logger=logging.getLogger("test"))
    capsys.readouterr()

    code = cmd_gitops_airflow_pod_doctor(
        _pod_doctor_args(
            artifact_dir=".dpone/gitops/airflow",
            pod_contract_path=None,
            pod_spec_path=None,
            kpo_kwargs_path=None,
            output=".dpone/gitops/airflow-pod-doctor-report.json",
        ),
        ctx=_ctx(tmp_path),
        logger=logging.getLogger("test"),
    )
    stdout_payload = json.loads(capsys.readouterr().out)
    file_payload = json.loads((tmp_path / ".dpone/gitops/airflow-pod-doctor-report.json").read_text("utf-8"))

    assert code == 0
    assert stdout_payload["kind"] == "gitops.airflow_pod_doctor"
    assert stdout_payload["artifact_dir"] == ".dpone/gitops/airflow"
    assert stdout_payload["pod_contract_path"] == ".dpone/gitops/airflow/pod-contract.json"
    assert stdout_payload["pod_spec_path"] == ".dpone/gitops/airflow/pod-spec.yaml"
    assert stdout_payload["kpo_kwargs_path"] == ".dpone/gitops/airflow/kpo-kwargs.json"
    assert stdout_payload["blockers"] == []
    assert file_payload == stdout_payload
    assert str(tmp_path) not in json.dumps(stdout_payload)


def test_gitops_airflow_pod_doctor_cli_blocks_invalid_contract(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    monkeypatch.chdir(tmp_path)
    _build_attested_bundle(tmp_path)
    cmd_gitops_airflow_run_spec(_run_spec_args(), ctx=_ctx(tmp_path), logger=logging.getLogger("test"))
    cmd_gitops_airflow_runtime_profile(_runtime_profile_args(), ctx=_ctx(tmp_path), logger=logging.getLogger("test"))
    cmd_gitops_airflow_pod_contract(_pod_contract_args(), ctx=_ctx(tmp_path), logger=logging.getLogger("test"))
    capsys.readouterr()
    kpo_kwargs_path = tmp_path / ".dpone/gitops/airflow/kpo-kwargs.json"
    kpo_kwargs = json.loads(kpo_kwargs_path.read_text("utf-8"))
    kpo_kwargs["do_xcom_push"] = False
    kpo_kwargs_path.write_text(json.dumps(kpo_kwargs, indent=2), encoding="utf-8")

    code = cmd_gitops_airflow_pod_doctor(
        _pod_doctor_args(),
        ctx=_ctx(tmp_path),
        logger=logging.getLogger("test"),
    )
    payload = json.loads(capsys.readouterr().out)

    assert code == 2
    assert payload["kind"] == "gitops.airflow_pod_doctor"
    assert any(blocker["code"] == "pod_contract_xcom_push_required" for blocker in payload["blockers"])


def test_gitops_airflow_run_spec_exec_cli_writes_evidence_with_static_runner(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    from dpone.gitops.airflow_runtime_executor import StaticAirflowCommandRunner

    monkeypatch.chdir(tmp_path)
    _build_attested_bundle(tmp_path)
    cmd_gitops_airflow_run_spec(_run_spec_args(), ctx=_ctx(tmp_path), logger=logging.getLogger("test"))
    capsys.readouterr()

    code = cmd_gitops_airflow_run_spec_exec(
        _run_spec_exec_args(),
        ctx=_ctx(tmp_path),
        logger=logging.getLogger("test"),
        runner=StaticAirflowCommandRunner(exit_codes={}),
    )
    payload = json.loads(capsys.readouterr().out)

    assert code == 0
    assert payload["kind"] == "gitops.airflow_runtime_evidence"
    assert payload["status"] == "passed"
    assert all(step["exit_code"] == 0 for step in payload["steps"])
    assert (tmp_path / ".dpone/gitops/airflow/runtime-evidence.json").exists()
    xcom_summary = json.loads((tmp_path / ".dpone/gitops/airflow/xcom-summary.json").read_text(encoding="utf-8"))
    assert xcom_summary["kind"] == "gitops.airflow_xcom_summary"
    assert xcom_summary["status"] == "passed"
    assert xcom_summary["runtime_evidence_sha256"].startswith("sha256:")


def test_gitops_airflow_outcome_gate_cli_blocks_failed_xcom_summary(tmp_path: Path, monkeypatch, capsys) -> None:
    from dpone.gitops.airflow_runtime_executor import StaticAirflowCommandRunner

    monkeypatch.chdir(tmp_path)
    _build_attested_bundle(tmp_path)
    cmd_gitops_airflow_run_spec(_run_spec_args(), ctx=_ctx(tmp_path), logger=logging.getLogger("test"))
    capsys.readouterr()
    cmd_gitops_airflow_run_spec_exec(
        _run_spec_exec_args(),
        ctx=_ctx(tmp_path),
        logger=logging.getLogger("test"),
        runner=StaticAirflowCommandRunner(exit_codes={"dpone run dpone_workloads/manifests/orders.yaml": 7}),
    )
    capsys.readouterr()

    code = cmd_gitops_airflow_outcome_gate(
        _outcome_gate_args(output=".dpone/gitops/airflow-outcome-gate.json"),
        ctx=_ctx(tmp_path),
        logger=logging.getLogger("test"),
    )
    stdout_payload = json.loads(capsys.readouterr().out)
    file_payload = json.loads((tmp_path / ".dpone/gitops/airflow-outcome-gate.json").read_text(encoding="utf-8"))

    assert code == 2
    assert stdout_payload["kind"] == "gitops.airflow_outcome_gate"
    assert stdout_payload["status"] == "failed"
    assert stdout_payload["required_status"] == "passed"
    assert any(blocker["code"] == "airflow_outcome_failed" for blocker in stdout_payload["blockers"])
    assert file_payload == stdout_payload
    assert str(tmp_path) not in json.dumps(stdout_payload)


def test_gitops_airflow_evidence_verify_cli_blocks_failed_evidence(tmp_path: Path, monkeypatch, capsys) -> None:
    from dpone.gitops.airflow_runtime_executor import StaticAirflowCommandRunner

    monkeypatch.chdir(tmp_path)
    _build_attested_bundle(tmp_path)
    cmd_gitops_airflow_run_spec(_run_spec_args(), ctx=_ctx(tmp_path), logger=logging.getLogger("test"))
    capsys.readouterr()
    cmd_gitops_airflow_run_spec_exec(
        _run_spec_exec_args(),
        ctx=_ctx(tmp_path),
        logger=logging.getLogger("test"),
        runner=StaticAirflowCommandRunner(exit_codes={"dpone run dpone_workloads/manifests/orders.yaml": 7}),
    )
    capsys.readouterr()

    code = cmd_gitops_airflow_evidence_verify(
        _evidence_verify_args(),
        ctx=_ctx(tmp_path),
        logger=logging.getLogger("test"),
    )
    payload = json.loads(capsys.readouterr().out)

    assert code == 2
    assert payload["kind"] == "gitops.airflow_runtime_evidence"
    assert payload["status"] == "failed"
    assert any(blocker["code"] == "runtime_step_failed" for blocker in payload["blockers"])


def test_gitops_airflow_k8s_smoke_cli_builds_plan_report(tmp_path: Path, monkeypatch, capsys) -> None:
    monkeypatch.chdir(tmp_path)
    _build_attested_bundle(tmp_path)
    cmd_gitops_airflow_image_contract(
        _image_contract_args(image_digest="sha256:" + "a" * 64, dpone_version="0.12.0", tool=["dpone", "kubectl"]),
        ctx=_ctx(tmp_path),
        logger=logging.getLogger("test"),
    )
    cmd_gitops_airflow_run_spec(
        _run_spec_args(image_digest="sha256:" + "a" * 64), ctx=_ctx(tmp_path), logger=logging.getLogger("test")
    )
    cmd_gitops_airflow_runtime_profile(
        _runtime_profile_args(image_digest="sha256:" + "a" * 64, outcome_mode="xcom_then_gate"),
        ctx=_ctx(tmp_path),
        logger=logging.getLogger("test"),
    )
    cmd_gitops_airflow_pod_contract(
        _pod_contract_args(outcome_mode="xcom_then_gate"),
        ctx=_ctx(tmp_path),
        logger=logging.getLogger("test"),
    )
    from dpone.gitops.airflow_runtime_executor import StaticAirflowCommandRunner

    cmd_gitops_airflow_run_spec_exec(
        _run_spec_exec_args(),
        ctx=_ctx(tmp_path),
        logger=logging.getLogger("test"),
        runner=StaticAirflowCommandRunner(exit_codes={}),
    )
    capsys.readouterr()

    code = cmd_gitops_airflow_k8s_smoke(
        _k8s_smoke_args(output=".dpone/gitops/airflow-k8s-smoke.json"),
        ctx=_ctx(tmp_path),
        logger=logging.getLogger("test"),
    )
    stdout_payload = json.loads(capsys.readouterr().out)
    file_payload = json.loads((tmp_path / ".dpone/gitops/airflow-k8s-smoke.json").read_text(encoding="utf-8"))

    assert code == 0
    assert stdout_payload["kind"] == "gitops.airflow_k8s_smoke"
    assert stdout_payload["mode"] == "plan"
    assert stdout_payload["runner_kind"] == "kubernetes_pod_operator"
    assert stdout_payload["image_ref"] == "ghcr.io/acme/dpone:2026.06.16@sha256:" + "a" * 64
    assert [command["name"] for command in stdout_payload["commands"]] == [
        "kubectl_auth_can_i",
        "kubectl_image_version_smoke",
        "kubectl_run_spec_smoke",
        "airflow_outcome_gate",
    ]
    assert file_payload == stdout_payload
    assert str(tmp_path) not in json.dumps(stdout_payload)


def test_gitops_airflow_k8s_smoke_cli_live_mode_uses_injected_runner(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    from dpone.gitops.airflow_k8s_smoke_runner import StaticAirflowK8sSmokeRunner
    from dpone.gitops.airflow_runtime_executor import StaticAirflowCommandRunner

    monkeypatch.chdir(tmp_path)
    _build_attested_bundle(tmp_path)
    cmd_gitops_airflow_image_contract(
        _image_contract_args(image_digest="sha256:" + "a" * 64, dpone_version="0.12.0", tool=["dpone", "kubectl"]),
        ctx=_ctx(tmp_path),
        logger=logging.getLogger("test"),
    )
    cmd_gitops_airflow_run_spec(
        _run_spec_args(image_digest="sha256:" + "a" * 64), ctx=_ctx(tmp_path), logger=logging.getLogger("test")
    )
    cmd_gitops_airflow_runtime_profile(
        _runtime_profile_args(image_digest="sha256:" + "a" * 64), ctx=_ctx(tmp_path), logger=logging.getLogger("test")
    )
    cmd_gitops_airflow_pod_contract(_pod_contract_args(), ctx=_ctx(tmp_path), logger=logging.getLogger("test"))
    cmd_gitops_airflow_run_spec_exec(
        _run_spec_exec_args(),
        ctx=_ctx(tmp_path),
        logger=logging.getLogger("test"),
        runner=StaticAirflowCommandRunner(exit_codes={}),
    )
    capsys.readouterr()

    code = cmd_gitops_airflow_k8s_smoke(
        _k8s_smoke_args(mode="live"),
        ctx=_ctx(tmp_path),
        logger=logging.getLogger("test"),
        runner=StaticAirflowK8sSmokeRunner(exit_codes={}),
    )
    payload = json.loads(capsys.readouterr().out)

    assert code == 0
    assert payload["mode"] == "live"
    assert [result["exit_code"] for result in payload["results"]] == [0, 0, 0, 0]
    assert all(command["executed"] for command in payload["commands"])


def test_gitops_airflow_k8s_smoke_cli_blocks_live_runner_failure(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    from dpone.gitops.airflow_k8s_smoke_runner import StaticAirflowK8sSmokeRunner
    from dpone.gitops.airflow_runtime_executor import StaticAirflowCommandRunner

    monkeypatch.chdir(tmp_path)
    _build_attested_bundle(tmp_path)
    cmd_gitops_airflow_image_contract(
        _image_contract_args(image_digest="sha256:" + "a" * 64, dpone_version="0.12.0", tool=["dpone", "kubectl"]),
        ctx=_ctx(tmp_path),
        logger=logging.getLogger("test"),
    )
    cmd_gitops_airflow_run_spec(
        _run_spec_args(image_digest="sha256:" + "a" * 64), ctx=_ctx(tmp_path), logger=logging.getLogger("test")
    )
    cmd_gitops_airflow_runtime_profile(
        _runtime_profile_args(image_digest="sha256:" + "a" * 64), ctx=_ctx(tmp_path), logger=logging.getLogger("test")
    )
    cmd_gitops_airflow_pod_contract(_pod_contract_args(), ctx=_ctx(tmp_path), logger=logging.getLogger("test"))
    cmd_gitops_airflow_run_spec_exec(
        _run_spec_exec_args(),
        ctx=_ctx(tmp_path),
        logger=logging.getLogger("test"),
        runner=StaticAirflowCommandRunner(exit_codes={}),
    )
    capsys.readouterr()

    code = cmd_gitops_airflow_k8s_smoke(
        _k8s_smoke_args(mode="live"),
        ctx=_ctx(tmp_path),
        logger=logging.getLogger("test"),
        runner=StaticAirflowK8sSmokeRunner(exit_codes={"kubectl_run_spec_smoke": 1}),
    )
    payload = json.loads(capsys.readouterr().out)

    assert code == 2
    assert any(blocker["code"] == "airflow_k8s_command_failed" for blocker in payload["blockers"])
    assert any(result["name"] == "kubectl_run_spec_smoke" and result["exit_code"] == 1 for result in payload["results"])


def test_gitops_airflow_pod_watch_cli_builds_plan_report(tmp_path: Path, monkeypatch, capsys) -> None:
    monkeypatch.chdir(tmp_path)
    _build_airflow_runtime_artifacts(tmp_path, capsys)

    code = cmd_gitops_airflow_pod_watch(
        _pod_watch_args(output=".dpone/gitops/airflow-pod-launch-evidence.json"),
        ctx=_ctx(tmp_path),
        logger=logging.getLogger("test"),
    )
    stdout_payload = json.loads(capsys.readouterr().out)
    file_payload = json.loads((tmp_path / ".dpone/gitops/airflow-pod-launch-evidence.json").read_text(encoding="utf-8"))

    assert code == 0
    assert stdout_payload["kind"] == "gitops.airflow_pod_launch_evidence"
    assert stdout_payload["mode"] == "plan"
    assert stdout_payload["pod_name"] == "dpone-gitops-runtime"
    assert stdout_payload["namespace"] == "dpone-runners"
    assert [command["name"] for command in stdout_payload["commands"]] == [
        "kubectl_get_pod",
        "kubectl_get_events",
        "kubectl_logs_base",
    ]
    assert file_payload == stdout_payload
    assert str(tmp_path) not in json.dumps(stdout_payload)


def test_gitops_airflow_pod_watch_cli_live_mode_uses_injected_runner(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    from dpone.gitops.airflow_pod_launch_evidence_runner import StaticAirflowPodLaunchEvidenceRunner

    monkeypatch.chdir(tmp_path)
    _build_airflow_runtime_artifacts(tmp_path, capsys)

    code = cmd_gitops_airflow_pod_watch(
        _pod_watch_args(mode="live"),
        ctx=_ctx(tmp_path),
        logger=logging.getLogger("test"),
        runner=StaticAirflowPodLaunchEvidenceRunner(
            stdout_by_name={
                "kubectl_get_pod": _pod_watch_pod_stdout(),
                "kubectl_get_events": _pod_watch_events_stdout(),
                "kubectl_logs_base": "dpone run completed\nxcom summary written\n",
            },
        ),
    )
    payload = json.loads(capsys.readouterr().out)

    assert code == 0
    assert payload["mode"] == "live"
    assert payload["observed_pod"]["phase"] == "Succeeded"
    assert payload["observed_pod"]["containers"][0]["exit_code"] == 0
    assert payload["observed_pod"]["events"][0]["reason"] == "Started"
    assert "xcom summary written" in payload["observed_pod"]["logs_tail"]
    assert [result["exit_code"] for result in payload["results"]] == [0, 0, 0]
    assert all(command["executed"] for command in payload["commands"])


def test_gitops_airflow_pod_watch_cli_blocks_failed_pod_event(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    from dpone.gitops.airflow_pod_launch_evidence_runner import StaticAirflowPodLaunchEvidenceRunner

    monkeypatch.chdir(tmp_path)
    _build_airflow_runtime_artifacts(tmp_path, capsys)

    code = cmd_gitops_airflow_pod_watch(
        _pod_watch_args(mode="live"),
        ctx=_ctx(tmp_path),
        logger=logging.getLogger("test"),
        runner=StaticAirflowPodLaunchEvidenceRunner(
            stdout_by_name={
                "kubectl_get_pod": _pod_watch_pod_stdout(
                    phase="Failed",
                    restart_count=1,
                    exit_code=137,
                    reason="OOMKilled",
                ),
                "kubectl_get_events": _pod_watch_events_stdout(reason="FailedScheduling", event_type="Warning"),
                "kubectl_logs_base": "pod failed\n",
            },
        ),
    )
    payload = json.loads(capsys.readouterr().out)

    assert code == 2
    blocker_codes = {blocker["code"] for blocker in payload["blockers"]}
    assert {
        "airflow_pod_phase_mismatch",
        "airflow_pod_container_failed",
        "airflow_pod_restart_detected",
        "airflow_pod_event_blocker",
    }.issubset(blocker_codes)


def test_gitops_airflow_evidence_bundle_cli_collects_attempt_pod_and_artifacts(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("DPONE_AIRFLOW_RUN_IDENTITY", _airflow_run_identity_json())
    _build_airflow_runtime_artifacts(tmp_path, capsys)
    cmd_gitops_airflow_k8s_smoke(
        _k8s_smoke_args(output=".dpone/gitops/airflow/airflow-k8s-smoke.json"),
        ctx=_ctx(tmp_path),
        logger=logging.getLogger("test"),
    )
    cmd_gitops_airflow_pod_watch(
        _pod_watch_args(output=".dpone/gitops/airflow/airflow-pod-launch-evidence.json"),
        ctx=_ctx(tmp_path),
        logger=logging.getLogger("test"),
    )
    capsys.readouterr()

    code = cmd_gitops_airflow_evidence_bundle(
        _evidence_bundle_args(
            require_k8s_smoke=True,
            require_pod_launch_evidence=True,
            output=".dpone/gitops/airflow/airflow-evidence-bundle.json",
        ),
        ctx=_ctx(tmp_path),
        logger=logging.getLogger("test"),
    )
    stdout_payload = json.loads(capsys.readouterr().out)
    file_payload = json.loads((tmp_path / ".dpone/gitops/airflow/airflow-evidence-bundle.json").read_text("utf-8"))

    assert code == 0, stdout_payload
    assert stdout_payload["kind"] == "gitops.airflow_evidence_bundle"
    assert stdout_payload["attempt"]["dag_id"] == "dpone_gitops"
    assert stdout_payload["attempt"]["task_id"] == "dpone_orders"
    assert stdout_payload["attempt"]["try_number"] == 2
    assert stdout_payload["pod"]["pod_name"] == "dpone-gitops-runtime"
    assert stdout_payload["pod"]["pod_uid"] == "pod-uid-456"
    assert [artifact["name"] for artifact in stdout_payload["artifacts"]] == [
        "bundle",
        "run_spec",
        "runtime_profile",
        "pod_contract",
        "runtime_evidence",
        "xcom_summary",
        "k8s_smoke",
        "pod_launch_evidence",
    ]
    assert all(artifact["exists"] for artifact in stdout_payload["artifacts"])
    assert all(artifact["sha256"] for artifact in stdout_payload["artifacts"])
    assert file_payload == stdout_payload
    assert str(tmp_path) not in json.dumps(stdout_payload)


def test_gitops_airflow_evidence_bundle_cli_prints_markdown(tmp_path: Path, monkeypatch, capsys) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("DPONE_AIRFLOW_RUN_IDENTITY", _airflow_run_identity_json())
    _build_airflow_runtime_artifacts(tmp_path, capsys)

    code = cmd_gitops_airflow_evidence_bundle(
        _evidence_bundle_args(format="markdown", k8s_smoke_path=None, pod_launch_evidence_path=None),
        ctx=_ctx(tmp_path),
        logger=logging.getLogger("test"),
    )
    output = capsys.readouterr().out

    assert code == 0, output
    assert "# GitOps Airflow evidence bundle" in output
    assert "dpone_gitops" in output
    assert "runtime-evidence.json" in output


def test_gitops_airflow_evidence_bundle_cli_blocks_missing_required_artifact(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    monkeypatch.chdir(tmp_path)
    _build_airflow_runtime_artifacts(tmp_path, capsys)
    (tmp_path / ".dpone/gitops/airflow/runtime-evidence.json").unlink()

    code = cmd_gitops_airflow_evidence_bundle(
        _evidence_bundle_args(k8s_smoke_path=None, pod_launch_evidence_path=None),
        ctx=_ctx(tmp_path),
        logger=logging.getLogger("test"),
    )
    payload = json.loads(capsys.readouterr().out)

    assert code == 2
    assert any(blocker["code"] == "airflow_evidence_artifact_missing" for blocker in payload["blockers"])


def test_gitops_group_registers_airflow_nested_command() -> None:
    group = gitops_group()

    names = {command.name for command in group.subcommands}

    assert "airflow" in names

    airflow = next(command for command in group.subcommands if command.name == "airflow")
    airflow_names = {command.name for command in airflow.subcommands}
    assert {
        "pod-contract",
        "pod-doctor",
        "connection-bridge-plan",
        "cluster-doctor",
        "k8s-manifests",
        "admission-check",
        "pack",
        "outcome-gate",
        "k8s-smoke",
        "pod-watch",
        "evidence-bundle",
    }.issubset(airflow_names)
