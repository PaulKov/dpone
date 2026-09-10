"""Offline author-to-Pod resource delivery; no scheduler or live cluster is used."""

from __future__ import annotations

import base64
import hashlib
import json
from copy import deepcopy
from pathlib import Path
from typing import Any

import dpone_airflow_pack.pack_tasks as pack_tasks
import pytest
import yaml
from dpone_airflow_pack.dag_loader import load_dpone_dags
from dpone_airflow_pack.launch_pin_store import InMemoryLaunchPinStore, get_launch_pin_store, set_launch_pin_store
from dpone_airflow_pack.pack_identity import compute_pack_fingerprint

from tests.airflow_dag_spec_repo import dag_declaration
from tests.test_airflow_deployment_index_provider import _install_fake_airflow
from tests.test_gitops_airflow_compact_pack_release import XCOM, _projection, _run_cli, _strict_airflow_build_args

DAG_ID = "DAG__sales__orders__sync"
PACK_ROOT = ".dpone/gitops/airflow"
RESOURCES = {
    "requests": {"cpu": "250m", "memory": "256Mi", "ephemeral-storage": "2Gi"},
    "limits": {"cpu": "1", "memory": "1Gi", "ephemeral-storage": "8Gi"},
}


def _write_authoring(root: Path, resources: dict[str, dict[str, str]]) -> None:
    """Use the real manifest-local resource surface and one separate SQL hook."""

    documents = {
        "pipeline.yaml": {
            "name": "orders",
            "execution": {"visibility": "task"},
            "source": {
                "type": "postgres",
                "connection_ref": "warehouse",
                "table": {"schema": "src", "name": "orders"},
                "options": {
                    "hooks": {
                        "pre_hook": [
                            {
                                "id": "refresh_orders",
                                "kind": "source_refresh",
                                "type": "sql",
                                "sql": "SELECT 1",
                                "mutates_source": True,
                                "execution": {"airflow": "separate_task"},
                            }
                        ],
                    },
                },
            },
            "sink": {
                "type": "postgres",
                "connection_ref": "warehouse",
                "table": {"schema": "dst", "name": "orders"},
                "mode": "append",
            },
            "gitops": {"airflow": {"resources": resources}},
        },
        "catalog.yaml": {
            "domain": "sales",
            "workloads": {"orders": {"manifest": "pipeline.yaml"}},
            "dags": {
                DAG_ID: dag_declaration(
                    workloads=["orders"],
                    wiring={"mode": "explicit", "dependencies": {}},
                ),
            },
        },
        "workloads.yaml": {
            "gitops": {
                "includes": [{"path": "catalog.yaml"}],
                "defaults": {
                    "image": "registry.example/dpone:dev",
                    "airflow": {"connection_projection": _projection()},
                },
            }
        },
    }
    for name, payload in documents.items():
        (root / name).write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")


def _reconcile(capsys: pytest.CaptureFixture[str]) -> tuple[int, dict[str, object], str]:
    return _run_cli(
        [
            "gitops",
            "airflow",
            "reconcile",
            "--workload-set",
            "workloads.yaml",
            "--all-workloads",
            "--env",
            "dev",
            "--format",
            "json",
        ],
        capsys,
    )


def _release_and_activate(root: Path, capsys: pytest.CaptureFixture[str]) -> dict[str, Any]:
    """Exercise the same immutable handoff and cache promotion as user commands."""

    code, materialized, stderr = _run_cli(
        [
            "gitops",
            "airflow",
            "release-materialize",
            "--pack-root",
            PACK_ROOT,
            "--cache-root",
            ".dpone-cache",
            "--xcom-sidecar-image",
            XCOM,
            "--format",
            "json",
        ],
        capsys,
    )
    assert code == 0, (materialized, stderr)
    code, built, stderr = _run_cli(
        [
            "airflow",
            "build",
            "--release-id",
            str(materialized["release_id"]),
            "--environment",
            "dev",
            *_strict_airflow_build_args(),
            "--format",
            "json",
        ],
        capsys,
    )
    assert code == 0, (built, stderr)
    code, activated, stderr = _run_cli(
        [
            "airflow",
            "cache-sync",
            "--cache-root",
            ".dpone-cache",
            "--deployment-dir",
            str(built["deployment_dir"]),
            "--environment",
            "dev",
            "--promoted-by",
            "ci://tests",
            "--allowed-promoter",
            "ci://tests",
            "--expect-current-absent",
            "--confirm-promote",
            "--format",
            "json",
        ],
        capsys,
    )
    assert code == 0, (activated, stderr)
    index_path = root / ".dpone-cache/current/airflow-index.json"
    index = json.loads(index_path.read_text(encoding="utf-8"))
    assert index["schema"] == "dpone.airflow-deployment-index.v2"
    assert index["runtime_artifact_delivery"]["mode"] == "init_fetch"
    assert index["release_id"] == materialized["release_id"]
    assert index["deployment_id"] == activated["deployment_id"]
    return index


def _record_operators(monkeypatch: pytest.MonkeyPatch) -> list[Any]:
    _install_fake_airflow(monkeypatch)
    operators: list[Any] = []

    class RecordingOperator:
        def __init__(self, **kwargs: Any) -> None:
            self.kwargs = kwargs
            self.task_id = str(kwargs["task_id"])
            self.upstream_task_ids: set[str] = set()
            self.downstream_task_ids: set[str] = set()
            operators.append(self)

        def __rshift__(self, other: Any) -> Any:
            self.downstream_task_ids.add(other.task_id)
            if isinstance(other, RecordingOperator):
                other.upstream_task_ids.add(self.task_id)
            return other

    monkeypatch.setattr(pack_tasks, "_operator_class", lambda *_args, **_kwargs: RecordingOperator)
    return operators


def _pod_payload(operator: Any) -> dict[str, Any]:
    pod = operator.kwargs["full_pod_spec"]
    if isinstance(pod, dict):
        return pod
    from kubernetes.client import ApiClient

    return ApiClient().sanitize_for_serialization(pod)


@pytest.fixture
def local_launch_pin_store():
    """Inject the supported in-memory store; no Kubernetes credentials are used."""

    previous = get_launch_pin_store()
    set_launch_pin_store(InMemoryLaunchPinStore())
    try:
        yield
    finally:
        set_launch_pin_store(previous)


@pytest.mark.usefixtures("local_launch_pin_store")
def test_resources_survive_full_strict_delivery_and_bind_downstream_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Authored quantities reach both Pods and resource changes require new delivery IDs."""

    identities = []
    for variant, limit in (("original", "8Gi"), ("changed", "16Gi")):
        root = tmp_path / variant
        root.mkdir()
        monkeypatch.chdir(root)
        code, initialized, stderr = _run_cli(["init", "project", "--airflow", "--format", "json"], capsys)
        assert code == 0, (initialized, stderr)
        resources = deepcopy(RESOURCES)
        resources["limits"]["ephemeral-storage"] = limit
        _write_authoring(root, resources)
        code, reconciled, stderr = _reconcile(capsys)
        assert code == 0, (reconciled, stderr)
        authored_pack_path = root / PACK_ROOT / "orders/airflow-pack.json"
        compact_pack = json.loads(authored_pack_path.read_text(encoding="utf-8"))
        assert compact_pack["provider_execution"]["pod_spec"]["spec"]["containers"][0]["resources"] == resources
        index = _release_and_activate(root, capsys)
        descriptor = index["workload_packs"][0]
        released_bytes = (root / ".dpone-cache" / descriptor["artifact_ref"].removeprefix("cache://")).read_bytes()
        released_pack = json.loads(released_bytes)
        assert descriptor["sha256"] == "sha256:" + hashlib.sha256(released_bytes).hexdigest()
        assert descriptor["pack_fingerprint"] == compute_pack_fingerprint(released_pack)
        assert released_pack["provider_execution"]["pod_spec"]["spec"]["containers"][0]["resources"] == resources
        operators = _record_operators(monkeypatch)
        namespace: dict[str, object] = {}
        report = load_dpone_dags(namespace, index_path=root / ".dpone-cache/current/airflow-index.json")
        assert report.errors == ()
        assert report.loaded == (DAG_ID,)
        assert not report.fatal
        assert report.skipped == ()
        assert DAG_ID in namespace
        by_kind = {}
        for operator in operators:
            plan = json.loads(base64.b64decode(operator.kwargs["env_vars"]["DPONE_INIT_FETCH_PLAN_B64"], validate=True))
            by_kind[plan["execution"]["kind"]] = operator
            assert plan["execution"]["hook_execution"] == "externalized"
            pod = _pod_payload(operator)
            base = next(container for container in pod["spec"]["containers"] if container["name"] == "base")
            assert base["resources"] == resources
            assert base["command"] == ["dpone", "airflow", "runtime-pack-exec"]
            mounts = {mount["name"]: mount for mount in base["volumeMounts"]}
            assert mounts["dpone-worktree"]["readOnly"] is True
            assert mounts["dpone-run-output"] == {
                "name": "dpone-run-output",
                "mountPath": "/var/lib/dpone/run",
                "readOnly": False,
            }
            volumes = {volume["name"]: volume for volume in pod["spec"]["volumes"]}
            assert volumes["dpone-run-output"]["emptyDir"] == {}
            assert all("resources" not in container for container in pod["spec"]["initContainers"])
        assert set(by_kind) == {"runtime", "pre_hook"}
        runtime, hook = by_kind["runtime"], by_kind["pre_hook"]
        assert hook.task_id in runtime.upstream_task_ids
        assert runtime.task_id in hook.downstream_task_ids
        assert runtime.kwargs.get("trigger_rule", "all_success") == "all_success"
        assert runtime.kwargs["do_xcom_push"] is True
        assert hook.kwargs["do_xcom_push"] is False
        identities.append(
            (descriptor["pack_fingerprint"], descriptor["sha256"], index["release_id"], index["deployment_id"])
        )
    assert all(first != second for first, second in zip(*identities, strict=True))


@pytest.mark.parametrize(
    "field", ["pod_template_dict", "pod_template_file", "full_pod_spec", "container_resources", "resources"]
)
def test_unsupported_resource_override_fails_before_artifact_write(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    field: str,
) -> None:
    monkeypatch.chdir(tmp_path)
    _write_authoring(tmp_path, RESOURCES)
    manifest_path = tmp_path / "pipeline.yaml"
    source = yaml.safe_load(manifest_path.read_text(encoding="utf-8"))
    source["gitops"]["airflow"]["operator_overrides"] = {field: {"requests": {"cpu": "2"}}}
    manifest_path.write_text(yaml.safe_dump(source, sort_keys=False), encoding="utf-8")

    code, payload, _stderr = _reconcile(capsys)

    assert code == 2
    assert payload["packs"] == []
    assert payload["dag_specs"] == []
    messages = "\n".join(blocker["message"] for blocker in payload["blockers"])
    assert f"airflow.operator_overrides.{field}" in messages
    assert "airflow.resources" in messages
    assert not (tmp_path / PACK_ROOT).exists()
    assert not (tmp_path / ".dpone-cache").exists()
