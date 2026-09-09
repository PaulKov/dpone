from __future__ import annotations

import json
import logging
from argparse import Namespace
from pathlib import Path

import yaml

from dpone.adapters.fs_local import LocalFileSystem
from dpone.adapters.yaml_pyyaml import PyYamlCodec
from dpone.app.context import AppContext
from dpone.app.settings import Settings
from dpone.commands.gitops.airflow_k8s_manifests_cmd import cmd_gitops_airflow_k8s_manifests


def _ctx(tmp_path: Path) -> AppContext:
    settings = Settings(repo_root=tmp_path, project_dir=tmp_path, manifest_dir=tmp_path, sources_registry_paths=())
    return AppContext(settings=settings, logger=logging.getLogger("test"), fs=LocalFileSystem(), yaml=PyYamlCodec())


def _args(**overrides: object) -> Namespace:
    data = {
        "artifact_dir": ".dpone/gitops/airflow",
        "runtime_profile_path": None,
        "pod_contract_path": None,
        "connection_bridge_plan_path": None,
        "manifest_output": ".dpone/gitops/airflow/airflow-k8s-manifests.yaml",
        "include_network_policy": False,
        "gitops_controller": "plain",
        "output": None,
        "format": "json",
    }
    data.update(overrides)
    return Namespace(**data)


def test_k8s_manifests_writes_secret_safe_deployable_pack(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    monkeypatch.chdir(tmp_path)
    artifact_dir = _write_airflow_artifacts(tmp_path)

    code = cmd_gitops_airflow_k8s_manifests(
        _args(
            include_network_policy=True,
            output=".dpone/gitops/airflow/airflow-k8s-manifests.json",
        ),
        ctx=_ctx(tmp_path),
        logger=logging.getLogger("test"),
    )
    payload = json.loads(capsys.readouterr().out)
    report_payload = json.loads((artifact_dir / "airflow-k8s-manifests.json").read_text("utf-8"))
    manifest_text = (artifact_dir / "airflow-k8s-manifests.yaml").read_text("utf-8")
    documents = [doc for doc in yaml.safe_load_all(manifest_text) if doc]

    assert code == 0
    assert payload == report_payload
    assert payload["kind"] == "gitops.airflow_k8s_manifests"
    assert payload["namespace"] == "dpone-runners"
    assert payload["service_account"] == "dpone-runner"
    assert payload["gitops_controller"] == "plain"
    assert payload["manifest_path"] == ".dpone/gitops/airflow/airflow-k8s-manifests.yaml"
    assert [(doc["kind"], doc["metadata"]["name"]) for doc in documents] == [
        ("ServiceAccount", "dpone-runner"),
        ("Role", "dpone-runner"),
        ("RoleBinding", "dpone-runner"),
        ("Secret", "example-workloads-git"),
        ("Secret", "dpone-airflow-connections"),
        ("Secret", "regcred"),
        ("ExternalSecret", "dpone-airflow-connections"),
        ("NetworkPolicy", "dpone-runner-allow-all-egress"),
    ]
    assert documents[0]["imagePullSecrets"] == [{"name": "regcred"}]
    assert documents[3]["metadata"]["annotations"]["dpone.io/required-keys"] == "known_hosts,ssh"
    assert documents[4]["stringData"] == {}
    assert documents[4]["metadata"]["annotations"]["dpone.io/required-keys"] == "AIRFLOW_CONN_MSSQL_DWH"
    assert documents[6]["spec"]["target"]["name"] == "dpone-airflow-connections"
    assert "mssql://user:secret" not in manifest_text
    assert "real:secret" not in json.dumps(payload)
    assert str(tmp_path) not in json.dumps(payload)


def test_k8s_manifests_adds_argocd_metadata(tmp_path: Path, monkeypatch, capsys) -> None:
    monkeypatch.chdir(tmp_path)
    artifact_dir = _write_airflow_artifacts(tmp_path)

    code = cmd_gitops_airflow_k8s_manifests(
        _args(gitops_controller="argocd"),
        ctx=_ctx(tmp_path),
        logger=logging.getLogger("test"),
    )
    payload = json.loads(capsys.readouterr().out)
    documents = [
        doc for doc in yaml.safe_load_all((artifact_dir / "airflow-k8s-manifests.yaml").read_text("utf-8")) if doc
    ]

    assert code == 0
    assert payload["gitops_controller"] == "argocd"
    assert any("Argo CD Application path" in hint for hint in payload["controller_hints"])
    assert documents[0]["metadata"]["labels"]["dpone.io/gitops-controller"] == "argocd"
    assert documents[0]["metadata"]["annotations"]["argocd.argoproj.io/sync-wave"] == "-20"
    assert documents[2]["metadata"]["annotations"]["argocd.argoproj.io/sync-wave"] == "-5"
    assert documents[3]["metadata"]["annotations"]["dpone.io/required-keys"] == "known_hosts,ssh"


def test_k8s_manifests_adds_flux_metadata_without_argocd_annotations(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    monkeypatch.chdir(tmp_path)
    artifact_dir = _write_airflow_artifacts(tmp_path)

    code = cmd_gitops_airflow_k8s_manifests(
        _args(gitops_controller="flux"),
        ctx=_ctx(tmp_path),
        logger=logging.getLogger("test"),
    )
    payload = json.loads(capsys.readouterr().out)
    documents = [
        doc for doc in yaml.safe_load_all((artifact_dir / "airflow-k8s-manifests.yaml").read_text("utf-8")) if doc
    ]

    assert code == 0
    assert payload["gitops_controller"] == "flux"
    assert any("Flux Kustomization path" in hint for hint in payload["controller_hints"])
    assert documents[0]["metadata"]["labels"]["dpone.io/gitops-controller"] == "flux"
    assert "argocd.argoproj.io/sync-wave" not in documents[0]["metadata"].get("annotations", {})


def test_k8s_manifests_blocks_invalid_runtime_artifacts(tmp_path: Path, monkeypatch, capsys) -> None:
    monkeypatch.chdir(tmp_path)
    artifact_dir = tmp_path / ".dpone/gitops/airflow"
    artifact_dir.mkdir(parents=True)
    _write_json(artifact_dir / "runtime-profile.json", {"kind": "wrong"})
    _write_json(artifact_dir / "pod-contract.json", {"kind": "gitops.airflow_pod_contract"})

    code = cmd_gitops_airflow_k8s_manifests(
        _args(),
        ctx=_ctx(tmp_path),
        logger=logging.getLogger("test"),
    )
    payload = json.loads(capsys.readouterr().out)

    assert code == 2
    assert any(blocker["code"] == "airflow_k8s_manifests_runtime_profile_kind" for blocker in payload["blockers"])
    assert not (artifact_dir / "airflow-k8s-manifests.yaml").exists()


def _write_airflow_artifacts(tmp_path: Path) -> Path:
    artifact_dir = tmp_path / ".dpone/gitops/airflow"
    artifact_dir.mkdir(parents=True)
    bridge_env = [
        {
            "connection_id": "mssql_dwh",
            "env_name": "AIRFLOW_CONN_MSSQL_DWH",
            "secret_ref": {"name": "dpone-airflow-connections", "key": "AIRFLOW_CONN_MSSQL_DWH"},
        }
    ]
    _write_json(
        artifact_dir / "runtime-profile.json",
        {
            "kind": "gitops.airflow_runtime_profile",
            "namespace": "dpone-runners",
            "service_account": "dpone-runner",
            "git_sync": {
                "enabled": True,
                "auth": {
                    "mode": "ssh_secret",
                    "ssh_secret": {
                        "name": "example-workloads-git",
                        "ssh_key": "ssh",
                        "known_hosts_key": "known_hosts",
                    },
                },
            },
        },
    )
    _write_json(
        artifact_dir / "pod-contract.json",
        {
            "kind": "gitops.airflow_pod_contract",
            "namespace": "dpone-runners",
            "service_account": "dpone-runner",
            "connection_bridge": {
                "enabled": True,
                "mode": "k8s_secret",
                "runtime_mode": "runtime_only",
                "required_connection_ids": ["mssql_dwh"],
                "env": bridge_env,
            },
            "pod_spec": {
                "spec": {
                    "imagePullSecrets": [{"name": "regcred"}],
                    "containers": [{"name": "base", "image": "ghcr.io/acme/dpone:2026.06.17"}],
                }
            },
        },
    )
    _write_json(
        artifact_dir / "connection-bridge-plan.json",
        {
            "kind": "gitops.airflow_connection_bridge_plan",
            "mode": "k8s_secret",
            "runtime_mode": "runtime_only",
            "secret_name": "dpone-airflow-connections",
            "env": bridge_env,
            "artifacts": [
                {
                    "path": ".dpone/gitops/airflow/airflow-connections-externalsecret.yaml",
                    "kind": "kubernetes.ExternalSecret",
                    "required": False,
                    "exists": True,
                    "reason": "written",
                }
            ],
        },
    )
    return artifact_dir


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
