from __future__ import annotations

import json
import logging
from argparse import Namespace
from pathlib import Path

from dpone.adapters.fs_local import LocalFileSystem
from dpone.adapters.yaml_pyyaml import PyYamlCodec
from dpone.app.context import AppContext
from dpone.app.settings import Settings
from dpone.commands.gitops.airflow_cluster_doctor_cmd import cmd_gitops_airflow_cluster_doctor
from dpone.gitops.airflow_cluster_doctor import GitOpsAirflowClusterDoctorPlanner
from dpone.gitops.airflow_cluster_doctor_runner import StaticAirflowClusterDoctorRunner


def _ctx(tmp_path: Path) -> AppContext:
    settings = Settings(repo_root=tmp_path, project_dir=tmp_path, manifest_dir=tmp_path, sources_registry_paths=())
    return AppContext(settings=settings, logger=logging.getLogger("test"), fs=LocalFileSystem(), yaml=PyYamlCodec())


def _args(**overrides: object) -> Namespace:
    data = {
        "artifact_dir": ".dpone/gitops/airflow",
        "runtime_profile_path": None,
        "pod_contract_path": None,
        "connection_bridge_plan_path": None,
        "mode": "plan",
        "runner_policy": "release",
        "timeout_seconds": 120,
        "kubectl": "kubectl",
        "external_secret": [],
        "require_external_secret": False,
        "format": "json",
        "output": None,
    }
    data.update(overrides)
    return Namespace(**data)


def _write_artifacts(tmp_path: Path) -> Path:
    artifact_dir = tmp_path / ".dpone/gitops/airflow"
    artifact_dir.mkdir(parents=True)
    runtime_profile = {
        "kind": "gitops.airflow_runtime_profile",
        "image": "ghcr.io/acme/dpone:2026.06.17",
        "namespace": "dpone-runners",
        "service_account": "dpone-runner",
        "resources": {"requests": {"cpu": "250m", "memory": "512Mi"}, "limits": {"cpu": "2", "memory": "2Gi"}},
        "runner_policy": "release",
        "git_sync": {
            "enabled": True,
            "repo": "ssh://git@git.example/platform/example-workloads.git",
            "ref": "main",
            "image": "registry.k8s.io/git-sync/git-sync:v4.7.0",
            "auth": {
                "mode": "ssh_secret",
                "ssh_secret": {"name": "example-workloads-git", "ssh_key": "ssh", "known_hosts_key": "known_hosts"},
            },
            "sparse_paths": [],
        },
    }
    pod_contract = {
        "kind": "gitops.airflow_pod_contract",
        "image": "ghcr.io/acme/dpone:2026.06.17",
        "namespace": "dpone-runners",
        "service_account": "dpone-runner",
        "connection_bridge": _bridge(),
        "pod_spec": {
            "spec": {
                "serviceAccountName": "dpone-runner",
                "imagePullSecrets": [{"name": "regcred"}],
                "containers": [
                    {
                        "name": "base",
                        "image": "ghcr.io/acme/dpone:2026.06.17",
                        "resources": {
                            "requests": {"cpu": "250m", "memory": "512Mi"},
                            "limits": {"cpu": "2", "memory": "2Gi"},
                        },
                    }
                ],
            }
        },
    }
    connection_bridge_plan = {
        "kind": "gitops.airflow_connection_bridge_plan",
        "schema_version": "1",
        "producer": "dpone gitops airflow connection-bridge-plan",
        "artifact_dir": ".dpone/gitops/airflow",
        "output_path": ".dpone/gitops/airflow/connection-bridge-plan.json",
        "runtime_profile_path": ".dpone/gitops/airflow/runtime-profile.json",
        "pod_contract_path": ".dpone/gitops/airflow/pod-contract.json",
        "mode": "k8s_secret",
        "runtime_mode": "runtime_only",
        "secret_name": "dpone-airflow-connections",
        "required_connection_ids": ["mssql_dwh"],
        "env": _bridge()["env"],
        "artifacts": [
            {
                "path": ".dpone/gitops/airflow/airflow-connections-externalsecret.yaml",
                "kind": "kubernetes.ExternalSecret",
                "required": False,
                "exists": True,
                "reason": "written",
            }
        ],
        "warnings": [],
        "blockers": [],
    }
    _write_json(artifact_dir / "runtime-profile.json", runtime_profile)
    _write_json(artifact_dir / "pod-contract.json", pod_contract)
    _write_json(artifact_dir / "connection-bridge-plan.json", connection_bridge_plan)
    return artifact_dir


def _bridge() -> dict[str, object]:
    return {
        "enabled": True,
        "mode": "k8s_secret",
        "runtime_mode": "runtime_only",
        "secret_name": "dpone-airflow-connections",
        "required_connection_ids": ["mssql_dwh"],
        "env": [
            {
                "connection_id": "mssql_dwh",
                "env_name": "AIRFLOW_CONN_MSSQL_DWH",
                "secret_ref": {"name": "dpone-airflow-connections", "key": "AIRFLOW_CONN_MSSQL_DWH"},
            }
        ],
        "warnings": [],
        "blockers": [],
    }


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def test_cluster_doctor_plan_discovers_cluster_refs_without_secret_values() -> None:
    runtime_profile = json.loads(json.dumps((_write_artifact_payloads())["runtime_profile"]))
    pod_contract = json.loads(json.dumps((_write_artifact_payloads())["pod_contract"]))
    connection_bridge_plan = json.loads(json.dumps((_write_artifact_payloads())["connection_bridge_plan"]))

    report = GitOpsAirflowClusterDoctorPlanner().plan(
        artifact_dir=".dpone/gitops/airflow",
        runtime_profile_path=".dpone/gitops/airflow/runtime-profile.json",
        runtime_profile=runtime_profile,
        pod_contract_path=".dpone/gitops/airflow/pod-contract.json",
        pod_contract=pod_contract,
        connection_bridge_plan_path=".dpone/gitops/airflow/connection-bridge-plan.json",
        connection_bridge_plan=connection_bridge_plan,
        mode="plan",
        runner_policy="release",
        timeout_seconds=120,
    )
    payload = report.to_jsonable()

    assert report.passed
    assert payload["kind"] == "gitops.airflow_cluster_doctor"
    assert {ref["name"] for ref in payload["secret_refs"]} == {
        "example-workloads-git",
        "dpone-airflow-connections",
        "regcred",
    }
    assert any(ref["required_keys"] == ["known_hosts", "ssh"] for ref in payload["secret_refs"])
    assert any(command["name"] == "kubectl_can_create_pods" for command in payload["commands"])
    assert any(command["kind"] == "kubernetes_secret_keys" for command in payload["commands"])
    assert "real:secret" not in json.dumps(payload)
    assert "/Users/" not in json.dumps(payload)


def test_cluster_doctor_cli_live_mode_uses_injected_runner(tmp_path: Path, monkeypatch, capsys) -> None:
    monkeypatch.chdir(tmp_path)
    artifact_dir = _write_artifacts(tmp_path)
    ctx = _ctx(tmp_path)

    code = cmd_gitops_airflow_cluster_doctor(
        _args(mode="live", output=".dpone/gitops/airflow/airflow-cluster-doctor.json"),
        ctx=ctx,
        logger=logging.getLogger("test"),
        runner=StaticAirflowClusterDoctorRunner(exit_codes={}),
    )
    payload = json.loads(capsys.readouterr().out)
    file_payload = json.loads((artifact_dir / "airflow-cluster-doctor.json").read_text("utf-8"))

    assert code == 0
    assert payload["mode"] == "live"
    assert all(command["executed"] for command in payload["commands"])
    assert all(result["exit_code"] == 0 for result in payload["results"])
    assert file_payload == payload


def test_cluster_doctor_blocks_missing_secret_keys(tmp_path: Path, monkeypatch, capsys) -> None:
    monkeypatch.chdir(tmp_path)
    _write_artifacts(tmp_path)

    code = cmd_gitops_airflow_cluster_doctor(
        _args(mode="live"),
        ctx=_ctx(tmp_path),
        logger=logging.getLogger("test"),
        runner=StaticAirflowClusterDoctorRunner(
            exit_codes={},
            stdout={"kubectl_secret_keys_example_workloads_git": "ssh\nSHOULD_NOT_LEAK=top-secret\n"},
        ),
    )
    payload = json.loads(capsys.readouterr().out)
    serialized = json.dumps(payload)

    assert code == 2
    assert any(blocker["code"] == "airflow_cluster_secret_key_missing" for blocker in payload["blockers"])
    assert "known_hosts" in json.dumps(payload["blockers"])
    assert "top-secret" not in serialized


def test_cluster_doctor_blocks_external_secret_not_ready(tmp_path: Path, monkeypatch, capsys) -> None:
    monkeypatch.chdir(tmp_path)
    _write_artifacts(tmp_path)

    code = cmd_gitops_airflow_cluster_doctor(
        _args(mode="live", require_external_secret_ready=True),
        ctx=_ctx(tmp_path),
        logger=logging.getLogger("test"),
        runner=StaticAirflowClusterDoctorRunner(
            exit_codes={},
            stdout={
                "kubectl_external_secret_dpone_airflow_connections": json.dumps(
                    {"status": {"conditions": [{"type": "Ready", "status": "False"}]}}
                )
            },
        ),
    )
    payload = json.loads(capsys.readouterr().out)

    assert code == 2
    assert any(blocker["code"] == "airflow_cluster_external_secret_not_ready" for blocker in payload["blockers"])


def test_cluster_doctor_blocks_rbac_denied(tmp_path: Path, monkeypatch, capsys) -> None:
    monkeypatch.chdir(tmp_path)
    _write_artifacts(tmp_path)

    code = cmd_gitops_airflow_cluster_doctor(
        _args(mode="live"),
        ctx=_ctx(tmp_path),
        logger=logging.getLogger("test"),
        runner=StaticAirflowClusterDoctorRunner(
            exit_codes={},
            stdout={"kubectl_can_create_pods": "no\n"},
        ),
    )
    payload = json.loads(capsys.readouterr().out)

    assert code == 2
    assert any(blocker["code"] == "airflow_cluster_rbac_denied" for blocker in payload["blockers"])


def _write_artifact_payloads() -> dict[str, object]:
    bridge = _bridge()
    return {
        "runtime_profile": {
            "kind": "gitops.airflow_runtime_profile",
            "namespace": "dpone-runners",
            "service_account": "dpone-runner",
            "git_sync": {
                "auth": {
                    "mode": "ssh_secret",
                    "ssh_secret": {"name": "example-workloads-git", "ssh_key": "ssh", "known_hosts_key": "known_hosts"},
                }
            },
        },
        "pod_contract": {
            "kind": "gitops.airflow_pod_contract",
            "namespace": "dpone-runners",
            "service_account": "dpone-runner",
            "connection_bridge": bridge,
            "pod_spec": {
                "spec": {
                    "imagePullSecrets": [{"name": "regcred"}],
                    "containers": [
                        {
                            "name": "base",
                            "resources": {
                                "requests": {"cpu": "250m", "memory": "512Mi"},
                                "limits": {"cpu": "2", "memory": "2Gi"},
                            },
                        }
                    ],
                }
            },
        },
        "connection_bridge_plan": {
            "kind": "gitops.airflow_connection_bridge_plan",
            "secret_name": "dpone-airflow-connections",
            "env": bridge["env"],
            "artifacts": [{"kind": "kubernetes.ExternalSecret"}],
        },
    }
