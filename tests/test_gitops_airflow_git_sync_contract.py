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
from dpone.gitops.airflow_git_sync_capabilities import git_sync_filter_capability
from dpone.services.gitops.airflow_pod_contract_service import GitOpsAirflowPodContractService
from dpone.services.gitops.airflow_pod_doctor_service import GitOpsAirflowPodDoctorService
from dpone.services.gitops.airflow_runtime_profile_service import GitOpsAirflowRuntimeProfileService
from dpone.services.gitops.airflow_runtime_service import GitOpsAirflowRunSpecService
from dpone.services.gitops.bundle_service import GitOpsBundleService

GIT_SYNC_IMAGE = "registry.k8s.io/git-sync/git-sync:v4.4.0"
GIT_SYNC_FILTER_IMAGE = "registry.k8s.io/git-sync/git-sync:v4.7.0"
GIT_SYNC_REPO = "ssh://git@git.example.test/platform/example-workloads.git"


def _ctx(tmp_path: Path) -> AppContext:
    settings = Settings(repo_root=tmp_path, project_dir=tmp_path, manifest_dir=tmp_path, sources_registry_paths=())
    return AppContext(settings=settings, logger=logging.getLogger("test"), fs=LocalFileSystem(), yaml=PyYamlCodec())


def _write(path: Path, text: str = "source: {}\nsink: {}\n") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def test_git_sync_filter_capability_is_versioned() -> None:
    legacy = git_sync_filter_capability(GIT_SYNC_IMAGE)
    current = git_sync_filter_capability(GIT_SYNC_FILTER_IMAGE)
    custom = git_sync_filter_capability("registry.example.com/git-sync/custom")

    assert legacy.support == "unsupported"
    assert current.support == "supported"
    assert custom.support == "unknown"


def _workload(tmp_path: Path) -> None:
    workload = tmp_path / "dpone_workloads"
    _write(workload / "manifests" / "orders.yaml", "depends_on:\n  - path: seed.yaml\nsource: {}\nsink: {}\n")
    _write(workload / "manifests" / "seed.yaml")


def _git_baseline(tmp_path: Path) -> None:
    subprocess.run(["git", "init"], cwd=tmp_path, check=True, capture_output=True, text=True)
    subprocess.run(["git", "config", "user.email", "test@example.com"], cwd=tmp_path, check=True)
    subprocess.run(["git", "config", "user.name", "Test User"], cwd=tmp_path, check=True)
    subprocess.run(["git", "add", "."], cwd=tmp_path, check=True, capture_output=True, text=True)
    subprocess.run(["git", "commit", "-m", "baseline"], cwd=tmp_path, check=True, capture_output=True, text=True)


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
        "git_sync_repo": None,
        "git_sync_ref": None,
        "git_sync_image": None,
        "git_sync_depth": 1,
        "git_sync_filter": None,
        "git_sync_auth_mode": "image",
        "git_sync_ssh_secret": None,
        "git_sync_ssh_key": "ssh",
        "git_sync_ssh_known_hosts_key": "known_hosts",
        "git_sync_https_secret": None,
        "git_sync_https_username_key": "username",
        "git_sync_https_password_key": "password",
        "airflow_connection_bridge": "k8s_secret",
        "airflow_connection_secret": "dpone-airflow-connections",
        "airflow_runtime_mode": "runtime_only",
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
        "volume": [],
        "volume_mount": [],
        "env_from_configmap": [],
        "env_secret": [],
        "node_selector": [],
        "toleration": [],
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
        "pod_contract_path": ".dpone/gitops/airflow/pod-contract.json",
        "pod_spec_path": ".dpone/gitops/airflow/pod-spec.yaml",
        "kpo_kwargs_path": ".dpone/gitops/airflow/kpo-kwargs.json",
        "runner_policy": "release",
        "format": "json",
        "output": None,
    }
    data.update(overrides)
    return Namespace(**data)


def _build_runtime_profile_with_git_sync(tmp_path: Path, **overrides: object) -> None:
    _workload(tmp_path)
    _git_baseline(tmp_path)
    GitOpsBundleService(ctx=_ctx(tmp_path)).build_view(_bundle_args())
    GitOpsAirflowRunSpecService(ctx=_ctx(tmp_path)).build_view(_run_spec_args())
    options = {
        "git_sync_repo": GIT_SYNC_REPO,
        "git_sync_ref": "main",
        "git_sync_image": GIT_SYNC_IMAGE,
        "git_sync_auth_mode": "ssh_secret",
        "git_sync_ssh_secret": "example-workloads-git",
    }
    options.update(overrides)
    GitOpsAirflowRuntimeProfileService(ctx=_ctx(tmp_path)).build_view(_runtime_profile_args(**options))


def test_runtime_profile_builds_git_sync_contract_from_bundle_plans(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    _build_runtime_profile_with_git_sync(tmp_path)

    payload = json.loads((tmp_path / ".dpone/gitops/airflow/runtime-profile.json").read_text(encoding="utf-8"))
    git_sync = payload["git_sync"]
    sparse_paths = [entry["path"] for entry in git_sync["sparse_paths"]]

    assert git_sync["enabled"] is True
    assert git_sync["repo"] == GIT_SYNC_REPO
    assert git_sync["ref"] == "main"
    assert git_sync["image"] == GIT_SYNC_IMAGE
    assert git_sync["root"] == "/workspace/.git-sync"
    assert git_sync["link"] == "/workspace/repo"
    assert git_sync["worktree_path"] == "/workspace/repo"
    assert git_sync["sparse_checkout_file"] == "/workspace/.dpone/git-sync/sparse-checkout"
    assert git_sync["clone"] == {"depth": 1, "filter": None}
    assert git_sync["auth"] == {
        "mode": "ssh_secret",
        "ssh_secret": {
            "name": "example-workloads-git",
            "ssh_key": "ssh",
            "known_hosts_key": "known_hosts",
        },
    }
    assert ".dpone/gitops/bundle/bundle.json" in sparse_paths
    assert ".dpone/gitops/airflow/run-spec.json" in sparse_paths
    assert "dpone_workloads/manifests/orders.yaml" in sparse_paths
    assert "dpone_workloads/manifests/seed.yaml" in sparse_paths
    assert any(path.endswith("gitops_plan.json") for path in sparse_paths)
    assert str(tmp_path) not in json.dumps(payload)


def test_runtime_profile_discovers_airflow_connections_and_pod_contract_injects_secret_env(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    workload = tmp_path / "dpone_workloads"
    _write(
        workload / "manifests" / "orders.yaml",
        """
depends_on:
  - path: seed.yaml
source:
  type: mssql
  connection_type: airflow
  connection_id: mssql_dwh
  table: {database: analytics_staging, schema: clickhouse, name: orders}
sink:
  type: clickhouse
  connection_type: airflow
  connection_id: clickhouse_prod
  table: {schema: analytics, name: orders}
""",
    )
    _write(workload / "manifests" / "seed.yaml")
    _git_baseline(tmp_path)
    GitOpsBundleService(ctx=_ctx(tmp_path)).build_view(_bundle_args())
    GitOpsAirflowRunSpecService(ctx=_ctx(tmp_path)).build_view(_run_spec_args())

    profile_view = GitOpsAirflowRuntimeProfileService(ctx=_ctx(tmp_path)).build_view(_runtime_profile_args())
    GitOpsAirflowPodContractService(ctx=_ctx(tmp_path)).build_view(_pod_contract_args())
    pod_spec = PyYamlCodec().load((tmp_path / ".dpone/gitops/airflow/pod-spec.yaml").read_text(encoding="utf-8"))
    profile = profile_view.report.to_jsonable()
    contract = json.loads((tmp_path / ".dpone/gitops/airflow/pod-contract.json").read_text(encoding="utf-8"))
    env = {item["name"]: item for item in pod_spec["spec"]["containers"][0]["env"]}

    assert profile_view.exit_code == 0
    assert profile["connection_bridge"]["mode"] == "k8s_secret"
    assert profile["connection_bridge"]["required_connection_ids"] == ["mssql_dwh", "clickhouse_prod"]
    assert env["AIRFLOW_CONN_MSSQL_DWH"]["valueFrom"]["secretKeyRef"] == {
        "name": "dpone-airflow-connections",
        "key": "AIRFLOW_CONN_MSSQL_DWH",
    }
    assert env["AIRFLOW_CONN_CLICKHOUSE_PROD"]["valueFrom"]["secretKeyRef"] == {
        "name": "dpone-airflow-connections",
        "key": "AIRFLOW_CONN_CLICKHOUSE_PROD",
    }
    assert "mssql://actual-secret" not in json.dumps(contract)
    doctor = GitOpsAirflowPodDoctorService(ctx=_ctx(tmp_path)).build_view(_pod_doctor_args())
    assert doctor.exit_code == 0
    checks = {check["name"]: check["passed"] for check in doctor.report.to_jsonable()["checks"]}
    assert checks["pod_contract_airflow_connection_secret_env"] is True


def test_pod_doctor_blocks_runtime_only_airflow_connections_without_bridge(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    workload = tmp_path / "dpone_workloads"
    _write(
        workload / "manifests" / "orders.yaml",
        "source: {connection_type: airflow, connection_id: mssql_dwh}\nsink: {}\n",
    )
    _git_baseline(tmp_path)
    GitOpsBundleService(ctx=_ctx(tmp_path)).build_view(
        _bundle_args(changed_files=["dpone_workloads/manifests/orders.yaml"])
    )
    GitOpsAirflowRunSpecService(ctx=_ctx(tmp_path)).build_view(_run_spec_args())
    GitOpsAirflowRuntimeProfileService(ctx=_ctx(tmp_path)).build_view(
        _runtime_profile_args(airflow_connection_bridge="disabled")
    )
    GitOpsAirflowPodContractService(ctx=_ctx(tmp_path)).build_view(_pod_contract_args())

    doctor = GitOpsAirflowPodDoctorService(ctx=_ctx(tmp_path)).build_view(_pod_doctor_args())
    blocker_codes = {blocker["code"] for blocker in doctor.report.to_jsonable()["blockers"]}

    assert doctor.exit_code == 2
    assert "pod_contract_airflow_connection_bridge_required" in blocker_codes


def test_pod_doctor_allows_airflow_image_mode_without_env_bridge(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    workload = tmp_path / "dpone_workloads"
    _write(
        workload / "manifests" / "orders.yaml",
        "source: {connection_type: airflow, connection_id: mssql_dwh}\nsink: {}\n",
    )
    _git_baseline(tmp_path)
    GitOpsBundleService(ctx=_ctx(tmp_path)).build_view(
        _bundle_args(changed_files=["dpone_workloads/manifests/orders.yaml"])
    )
    GitOpsAirflowRunSpecService(ctx=_ctx(tmp_path)).build_view(_run_spec_args())
    GitOpsAirflowRuntimeProfileService(ctx=_ctx(tmp_path)).build_view(
        _runtime_profile_args(airflow_connection_bridge="disabled", airflow_runtime_mode="airflow_image")
    )
    GitOpsAirflowPodContractService(ctx=_ctx(tmp_path)).build_view(_pod_contract_args())

    doctor = GitOpsAirflowPodDoctorService(ctx=_ctx(tmp_path)).build_view(_pod_doctor_args())
    checks = {check["name"]: check["passed"] for check in doctor.report.to_jsonable()["checks"]}

    assert doctor.exit_code == 0
    assert checks["pod_contract_airflow_runtime_image"] is True


def test_pod_contract_materializes_sparse_git_sync_init_containers(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    _build_runtime_profile_with_git_sync(tmp_path)

    view = GitOpsAirflowPodContractService(ctx=_ctx(tmp_path)).build_view(_pod_contract_args())
    pod_spec = PyYamlCodec().load((tmp_path / ".dpone/gitops/airflow/pod-spec.yaml").read_text(encoding="utf-8"))
    kpo_kwargs = json.loads((tmp_path / ".dpone/gitops/airflow/kpo-kwargs.json").read_text(encoding="utf-8"))
    dag_factory = (tmp_path / ".dpone/gitops/airflow/airflow_dag_factory.py").read_text(encoding="utf-8")
    contract = view.report.to_jsonable()

    assert view.exit_code == 0
    assert contract["git_sync"]["enabled"] is True
    assert [container["name"] for container in pod_spec["spec"]["initContainers"]] == [
        "dpone-sparse-checkout",
        "dpone-git-sync",
    ]
    assert {"name": "dpone-worktree", "emptyDir": {}} in pod_spec["spec"]["volumes"]
    assert {"name": "dpone-git-sync-ssh", "secret": {"secretName": "example-workloads-git"}} in (
        pod_spec["spec"]["volumes"]
    )
    base_container = pod_spec["spec"]["containers"][0]
    assert base_container["workingDir"] == "/workspace/repo"
    assert {"name": "dpone-worktree", "mountPath": "/workspace", "readOnly": False} in (base_container["volumeMounts"])
    git_sync_container = pod_spec["spec"]["initContainers"][1]
    assert git_sync_container["image"] == GIT_SYNC_IMAGE
    assert "--one-time" in git_sync_container["args"]
    assert "--repo=ssh://git@git.example.test/platform/example-workloads.git" in git_sync_container["args"]
    assert "--depth=1" in git_sync_container["args"]
    assert "--sparse-checkout-file=/workspace/.dpone/git-sync/sparse-checkout" in git_sync_container["args"]
    assert not any(str(arg).startswith("--filter=") for arg in git_sync_container["args"])
    assert "--ssh-key-file=/etc/git-secret/ssh" in git_sync_container["args"]
    assert "--ssh-known-hosts-file=/etc/git-secret/known_hosts" in git_sync_container["args"]
    assert kpo_kwargs["pod_template_file"] == ".dpone/gitops/airflow/pod-spec.yaml"
    assert "build_dpone_gitops_task_from_artifacts" in dag_factory
    assert "load_dpone_kpo_kwargs" in dag_factory
    assert "pod-spec.yaml" in dag_factory
    assert str(tmp_path) not in PyYamlCodec().dump(pod_spec)


def test_pod_contract_materializes_https_secret_git_sync_without_secret_values(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    _workload(tmp_path)
    _git_baseline(tmp_path)
    GitOpsBundleService(ctx=_ctx(tmp_path)).build_view(_bundle_args())
    GitOpsAirflowRunSpecService(ctx=_ctx(tmp_path)).build_view(_run_spec_args())
    GitOpsAirflowRuntimeProfileService(ctx=_ctx(tmp_path)).build_view(
        _runtime_profile_args(
            git_sync_repo="https://git.example.test/platform/example-workloads.git",
            git_sync_ref="main",
            git_sync_image=GIT_SYNC_IMAGE,
            git_sync_auth_mode="https_secret",
            git_sync_https_secret="example-workloads-token",
            git_sync_https_username_key="username",
            git_sync_https_password_key="token",
        )
    )

    view = GitOpsAirflowPodContractService(ctx=_ctx(tmp_path)).build_view(_pod_contract_args())
    pod_spec = PyYamlCodec().load((tmp_path / ".dpone/gitops/airflow/pod-spec.yaml").read_text(encoding="utf-8"))
    rendered = json.dumps(view.report.to_jsonable())
    git_sync_container = pod_spec["spec"]["initContainers"][1]

    assert view.exit_code == 0
    assert {"name": "dpone-git-sync-https", "secret": {"secretName": "example-workloads-token"}} in (
        pod_spec["spec"]["volumes"]
    )
    assert {
        "name": "GITSYNC_USERNAME",
        "valueFrom": {"secretKeyRef": {"name": "example-workloads-token", "key": "username"}},
    } in git_sync_container["env"]
    assert {
        "name": "GITSYNC_PASSWORD",
        "valueFrom": {"secretKeyRef": {"name": "example-workloads-token", "key": "token"}},
    } in git_sync_container["env"]
    assert "actual-token" not in rendered
    assert "actual-password" not in rendered


def test_pod_contract_materializes_supported_blobless_partial_clone(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    _build_runtime_profile_with_git_sync(
        tmp_path,
        git_sync_image=GIT_SYNC_FILTER_IMAGE,
        git_sync_filter="blob:none",
    )

    view = GitOpsAirflowPodContractService(ctx=_ctx(tmp_path)).build_view(_pod_contract_args())
    pod_spec = PyYamlCodec().load((tmp_path / ".dpone/gitops/airflow/pod-spec.yaml").read_text(encoding="utf-8"))
    payload = view.report.to_jsonable()
    git_sync_container = pod_spec["spec"]["initContainers"][1]

    assert view.exit_code == 0
    assert payload["git_sync"]["clone"] == {"depth": 1, "filter": "blob:none"}
    assert "--filter=blob:none" in git_sync_container["args"]
    assert "--depth=1" in git_sync_container["args"]

    doctor = GitOpsAirflowPodDoctorService(ctx=_ctx(tmp_path)).build_view(_pod_doctor_args())
    checks = {check["name"]: check["passed"] for check in doctor.report.to_jsonable()["checks"]}
    assert doctor.exit_code == 0
    assert checks["pod_contract_git_sync_sparse_checkout"] is True
    assert checks["pod_contract_git_sync_partial_clone"] is True


def test_pod_contract_materializes_tree_filter_and_custom_depth(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    _build_runtime_profile_with_git_sync(
        tmp_path,
        git_sync_image=GIT_SYNC_FILTER_IMAGE,
        git_sync_depth=2,
        git_sync_filter="tree:0",
    )

    GitOpsAirflowPodContractService(ctx=_ctx(tmp_path)).build_view(_pod_contract_args())
    pod_spec = PyYamlCodec().load((tmp_path / ".dpone/gitops/airflow/pod-spec.yaml").read_text(encoding="utf-8"))
    git_sync_container = pod_spec["spec"]["initContainers"][1]

    assert "--depth=2" in git_sync_container["args"]
    assert "--filter=tree:0" in git_sync_container["args"]


def test_runtime_profile_blocks_partial_clone_for_git_sync_v44(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    _workload(tmp_path)
    _git_baseline(tmp_path)
    GitOpsBundleService(ctx=_ctx(tmp_path)).build_view(_bundle_args())
    GitOpsAirflowRunSpecService(ctx=_ctx(tmp_path)).build_view(_run_spec_args())

    view = GitOpsAirflowRuntimeProfileService(ctx=_ctx(tmp_path)).build_view(
        _runtime_profile_args(
            git_sync_repo=GIT_SYNC_REPO,
            git_sync_ref="main",
            git_sync_image=GIT_SYNC_IMAGE,
            git_sync_filter="blob:none",
        )
    )
    payload = view.report.to_jsonable()

    assert view.exit_code == 2
    assert any(blocker["code"] == "git_sync_filter_unsupported" for blocker in payload["blockers"])


def test_runtime_profile_warns_for_unknown_git_sync_filter_in_advisory(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    _workload(tmp_path)
    _git_baseline(tmp_path)
    GitOpsBundleService(ctx=_ctx(tmp_path)).build_view(_bundle_args())
    GitOpsAirflowRunSpecService(ctx=_ctx(tmp_path)).build_view(_run_spec_args())

    view = GitOpsAirflowRuntimeProfileService(ctx=_ctx(tmp_path)).build_view(
        _runtime_profile_args(
            runner_policy="advisory",
            git_sync_repo=GIT_SYNC_REPO,
            git_sync_ref="main",
            git_sync_image="registry.example.com/git-sync/custom",
            git_sync_filter="blob:none",
        )
    )
    payload = view.report.to_jsonable()

    assert view.exit_code == 0
    assert any(warning["code"] == "git_sync_filter_support_unverified" for warning in payload["warnings"])
    assert payload["git_sync"]["clone"] == {"depth": 1, "filter": "blob:none"}


def test_runtime_profile_blocks_unknown_git_sync_filter_in_release(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    _workload(tmp_path)
    _git_baseline(tmp_path)
    GitOpsBundleService(ctx=_ctx(tmp_path)).build_view(_bundle_args())
    GitOpsAirflowRunSpecService(ctx=_ctx(tmp_path)).build_view(_run_spec_args())

    view = GitOpsAirflowRuntimeProfileService(ctx=_ctx(tmp_path)).build_view(
        _runtime_profile_args(
            git_sync_repo=GIT_SYNC_REPO,
            git_sync_ref="main",
            git_sync_image="registry.example.com/git-sync/custom",
            git_sync_filter="blob:none",
        )
    )
    payload = view.report.to_jsonable()

    assert view.exit_code == 2
    assert any(blocker["code"] == "git_sync_filter_support_unverified" for blocker in payload["blockers"])


def test_pod_doctor_blocks_missing_requested_partial_clone_arg(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    _build_runtime_profile_with_git_sync(
        tmp_path,
        git_sync_image=GIT_SYNC_FILTER_IMAGE,
        git_sync_filter="blob:none",
    )
    GitOpsAirflowPodContractService(ctx=_ctx(tmp_path)).build_view(_pod_contract_args())

    pod_spec_path = tmp_path / ".dpone/gitops/airflow/pod-spec.yaml"
    pod_spec = PyYamlCodec().load(pod_spec_path.read_text(encoding="utf-8"))
    git_sync_args = pod_spec["spec"]["initContainers"][1]["args"]
    pod_spec["spec"]["initContainers"][1]["args"] = [arg for arg in git_sync_args if arg != "--filter=blob:none"]
    pod_spec_path.write_text(PyYamlCodec().dump(pod_spec), encoding="utf-8")

    drifted = GitOpsAirflowPodDoctorService(ctx=_ctx(tmp_path)).build_view(_pod_doctor_args())
    blocker_codes = {blocker["code"] for blocker in drifted.report.to_jsonable()["blockers"]}

    assert drifted.exit_code == 2
    assert "pod_contract_git_sync_filter_missing" in blocker_codes


def test_pod_contract_blocks_reserved_git_sync_volume_names(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    _build_runtime_profile_with_git_sync(tmp_path)

    view = GitOpsAirflowPodContractService(ctx=_ctx(tmp_path)).build_view(
        _pod_contract_args(volume=["dpone-worktree=.dpone/gitops/airflow"])
    )
    payload = view.report.to_jsonable()

    assert view.exit_code == 2
    assert any(blocker["code"] == "git_sync_reserved_name" for blocker in payload["blockers"])


def test_pod_doctor_blocks_git_sync_drift_for_release_policy(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    _build_runtime_profile_with_git_sync(tmp_path)
    GitOpsAirflowPodContractService(ctx=_ctx(tmp_path)).build_view(_pod_contract_args())

    valid = GitOpsAirflowPodDoctorService(ctx=_ctx(tmp_path)).build_view(_pod_doctor_args())
    assert valid.exit_code == 0

    pod_spec_path = tmp_path / ".dpone/gitops/airflow/pod-spec.yaml"
    pod_spec = PyYamlCodec().load(pod_spec_path.read_text(encoding="utf-8"))
    pod_spec["spec"]["initContainers"] = []
    pod_spec["spec"]["containers"][0]["workingDir"] = "/workspace"
    pod_spec_path.write_text(PyYamlCodec().dump(pod_spec), encoding="utf-8")

    drifted = GitOpsAirflowPodDoctorService(ctx=_ctx(tmp_path)).build_view(_pod_doctor_args())
    blocker_codes = {blocker["code"] for blocker in drifted.report.to_jsonable()["blockers"]}

    assert drifted.exit_code == 2
    assert "pod_contract_git_sync_init_containers_required" in blocker_codes
    assert "pod_contract_git_sync_working_dir_mismatch" in blocker_codes


def test_runtime_profile_blocks_invalid_git_sync_auth(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    _workload(tmp_path)
    _git_baseline(tmp_path)
    GitOpsBundleService(ctx=_ctx(tmp_path)).build_view(_bundle_args())
    GitOpsAirflowRunSpecService(ctx=_ctx(tmp_path)).build_view(_run_spec_args())

    view = GitOpsAirflowRuntimeProfileService(ctx=_ctx(tmp_path)).build_view(
        _runtime_profile_args(
            git_sync_repo=GIT_SYNC_REPO,
            git_sync_ref="main",
            git_sync_image=GIT_SYNC_IMAGE,
            git_sync_auth_mode="ssh_secret",
            git_sync_ssh_secret=None,
        )
    )
    payload = view.report.to_jsonable()

    assert view.exit_code == 2
    assert any(blocker["code"] == "git_sync_auth_invalid" for blocker in payload["blockers"])
