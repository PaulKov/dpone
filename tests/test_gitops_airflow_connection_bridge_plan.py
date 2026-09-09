from __future__ import annotations

import json
import logging
from argparse import Namespace
from pathlib import Path

from dpone.adapters.fs_local import LocalFileSystem
from dpone.adapters.yaml_pyyaml import PyYamlCodec
from dpone.app.context import AppContext
from dpone.app.settings import Settings
from dpone.commands.gitops.airflow_connection_bridge_plan_cmd import cmd_gitops_airflow_connection_bridge_plan
from dpone.commands.gitops.airflow_preflight_cmd import (
    cmd_gitops_airflow_artifact_index,
    cmd_gitops_airflow_preflight,
)


def _ctx(tmp_path: Path) -> AppContext:
    settings = Settings(repo_root=tmp_path, project_dir=tmp_path, manifest_dir=tmp_path, sources_registry_paths=())
    return AppContext(settings=settings, logger=logging.getLogger("test"), fs=LocalFileSystem(), yaml=PyYamlCodec())


def _plan_args(**overrides: object) -> Namespace:
    data = {
        "artifact_dir": ".dpone/gitops/airflow",
        "output_path": None,
        "runtime_profile_path": None,
        "pod_contract_path": None,
        "secret_manifest_path": None,
        "external_secret_path": None,
        "env_example_path": None,
        "external_secret_store": "airflow-connections",
        "external_secret_store_kind": "SecretStore",
        "external_secret_remote_prefix": "airflow/connections",
        "format": "json",
        "output": None,
    }
    data.update(overrides)
    return Namespace(**data)


def _artifact_index_args(**overrides: object) -> Namespace:
    data = {"artifact_dir": ".dpone/gitops/airflow", "output_path": None, "format": "json", "output": None}
    data.update(overrides)
    return Namespace(**data)


def _preflight_args(**overrides: object) -> Namespace:
    data = {
        "artifact_dir": ".dpone/gitops/airflow",
        "artifact_index_path": None,
        "runner_policy": "release",
        "format": "json",
        "output": None,
    }
    data.update(overrides)
    return Namespace(**data)


def _write_airflow_artifacts(tmp_path: Path) -> Path:
    artifact_dir = tmp_path / ".dpone/gitops/airflow"
    artifact_dir.mkdir(parents=True)
    image = "ghcr.io/acme/dpone:2026.06.17"
    bridge = {
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
    pod_spec = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "dpone-runtime", "namespace": "dpone-runners"},
        "spec": {
            "serviceAccountName": "dpone-runner",
            "containers": [
                {
                    "name": "base",
                    "image": image,
                    "env": [
                        {
                            "name": "AIRFLOW_CONN_MSSQL_DWH",
                            "valueFrom": {
                                "secretKeyRef": {
                                    "name": "dpone-airflow-connections",
                                    "key": "AIRFLOW_CONN_MSSQL_DWH",
                                }
                            },
                        }
                    ],
                    "resources": {"requests": {"cpu": "250m"}, "limits": {"cpu": "1"}},
                }
            ],
        },
    }
    kpo_kwargs = {
        "task_id": "dpone_gitops_runtime",
        "name": "dpone-runtime",
        "namespace": "dpone-runners",
        "pod_template_file": ".dpone/gitops/airflow/pod-spec.yaml",
        "do_xcom_push": True,
        "cmds": ["/bin/sh", "-ec"],
        "arguments": ["dpone gitops airflow run-spec-exec .dpone/gitops/airflow/run-spec.json"],
    }
    _write_json(
        artifact_dir / "run-spec.json",
        {
            "kind": "gitops.airflow_run_spec",
            "schema_version": "1",
            "producer": "dpone gitops airflow run-spec",
            "bundle_path": ".dpone/gitops/bundle/bundle.json",
            "image": image,
            "worktree": ".",
            "evidence_output": ".dpone/gitops/airflow/runtime-evidence.json",
            "entries": [],
            "steps": [],
        },
    )
    _write_json(
        artifact_dir / "runtime-profile.json",
        {
            "kind": "gitops.airflow_runtime_profile",
            "schema_version": "1",
            "producer": "dpone gitops airflow runtime-profile",
            "bundle_path": ".dpone/gitops/bundle/bundle.json",
            "run_spec_path": ".dpone/gitops/airflow/run-spec.json",
            "runtime_evidence_path": ".dpone/gitops/airflow/runtime-evidence.json",
            "xcom_summary_path": ".dpone/gitops/airflow/xcom-summary.json",
            "dag_factory_path": ".dpone/gitops/airflow/airflow_dag_factory.py",
            "outcome_gate_path": ".dpone/gitops/airflow/outcome_gate.py",
            "image": image,
            "namespace": "dpone-runners",
            "service_account": "dpone-runner",
            "resources": {"requests": {"cpu": "250m"}, "limits": {"cpu": "1"}},
            "artifact_sink": {"kind": "local", "path": ".dpone/gitops/airflow"},
            "runner_policy": "release",
            "outcome_mode": "strict_fail",
            "connection_bridge": bridge,
        },
    )
    (artifact_dir / "pod-spec.yaml").write_text(PyYamlCodec().dump(pod_spec), encoding="utf-8")
    _write_json(artifact_dir / "kpo-kwargs.json", kpo_kwargs)
    _write_json(
        artifact_dir / "pod-contract.json",
        {
            "kind": "gitops.airflow_pod_contract",
            "schema_version": "1",
            "producer": "dpone gitops airflow pod-contract",
            "bundle_path": ".dpone/gitops/bundle/bundle.json",
            "run_spec_path": ".dpone/gitops/airflow/run-spec.json",
            "runtime_profile_path": ".dpone/gitops/airflow/runtime-profile.json",
            "pod_spec_path": ".dpone/gitops/airflow/pod-spec.yaml",
            "kpo_kwargs_path": ".dpone/gitops/airflow/kpo-kwargs.json",
            "image": image,
            "namespace": "dpone-runners",
            "service_account": "dpone-runner",
            "xcom": {
                "enabled": True,
                "return_path": "/airflow/xcom/return.json",
                "summary_path": ".dpone/gitops/airflow/xcom-summary.json",
                "mode": "final_outcome",
                "outcome_mode": "strict_fail",
            },
            "pod_spec": pod_spec,
            "kpo_kwargs": kpo_kwargs,
            "connection_bridge": bridge,
        },
    )
    _write_json(
        artifact_dir / "xcom-summary.json",
        {
            "kind": "gitops.airflow_xcom_summary",
            "schema_version": "1",
            "producer": "dpone gitops airflow runtime-profile",
            "status": "planned",
            "run_spec_path": ".dpone/gitops/airflow/run-spec.json",
            "runtime_evidence_path": ".dpone/gitops/airflow/runtime-evidence.json",
        },
    )
    return artifact_dir


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def test_connection_bridge_plan_writes_secret_skeletons_without_secret_values(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("AIRFLOW_CONN_MSSQL_DWH", "mssql://real:secret@mssql.example/DWH")
    artifact_dir = _write_airflow_artifacts(tmp_path)

    code = cmd_gitops_airflow_connection_bridge_plan(
        _plan_args(),
        ctx=_ctx(tmp_path),
        logger=logging.getLogger("test"),
    )
    payload = json.loads(capsys.readouterr().out)
    secret_text = (artifact_dir / "airflow-connections-secret.yaml").read_text("utf-8")
    external_secret_text = (artifact_dir / "airflow-connections-externalsecret.yaml").read_text("utf-8")
    env_example_text = (artifact_dir / "airflow-connections.env.example").read_text("utf-8")
    combined = json.dumps(payload) + secret_text + external_secret_text + env_example_text

    assert code == 0
    assert payload["kind"] == "gitops.airflow_connection_bridge_plan"
    assert payload["required_connection_ids"] == ["mssql_dwh"]
    assert payload["env"][0]["secret_ref"] == {
        "name": "dpone-airflow-connections",
        "key": "AIRFLOW_CONN_MSSQL_DWH",
    }
    assert "kind: Secret" in secret_text
    assert "kind: ExternalSecret" in external_secret_text
    assert "AIRFLOW_CONN_MSSQL_DWH=REPLACE_WITH_AIRFLOW_CONNECTION_URI" in env_example_text
    assert "real:secret" not in combined
    assert str(tmp_path) not in combined


def test_connection_bridge_plan_rejects_uri_connection_ids_without_leaking_payload(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    monkeypatch.chdir(tmp_path)
    artifact_dir = _write_airflow_artifacts(tmp_path)
    bad_connection_id = "postgres://etl:secret@pg.internal:5432/dwh"
    for name in ("runtime-profile.json", "pod-contract.json"):
        path = artifact_dir / name
        payload = json.loads(path.read_text(encoding="utf-8"))
        bridge = payload["connection_bridge"]
        bridge["required_connection_ids"] = [bad_connection_id]
        bridge["env"][0]["connection_id"] = bad_connection_id
        path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    code = cmd_gitops_airflow_connection_bridge_plan(
        _plan_args(),
        ctx=_ctx(tmp_path),
        logger=logging.getLogger("test"),
    )
    payload = json.loads(capsys.readouterr().out)
    combined = json.dumps(payload)

    assert code == 2
    assert [blocker["code"] for blocker in payload["blockers"]] == [
        "airflow_connection_bridge_plan_invalid_connection_id",
        "airflow_connection_bridge_plan_invalid_connection_id",
    ]
    assert bad_connection_id not in combined
    assert "etl:secret" not in combined
    assert not (artifact_dir / "airflow-connections-secret.yaml").exists()


def test_connection_bridge_plan_rejects_invalid_secret_keys_without_leaking_payload(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    monkeypatch.chdir(tmp_path)
    artifact_dir = _write_airflow_artifacts(tmp_path)
    bad_secret_key = "AIRFLOW_CONN_MSSQL_DWH\nPASSWORD=secret"
    for name in ("runtime-profile.json", "pod-contract.json"):
        path = artifact_dir / name
        payload = json.loads(path.read_text(encoding="utf-8"))
        payload["connection_bridge"]["env"][0]["secret_ref"]["key"] = bad_secret_key
        path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    code = cmd_gitops_airflow_connection_bridge_plan(
        _plan_args(),
        ctx=_ctx(tmp_path),
        logger=logging.getLogger("test"),
    )
    payload = json.loads(capsys.readouterr().out)
    combined = json.dumps(payload)

    assert code == 2
    assert [blocker["code"] for blocker in payload["blockers"]] == [
        "airflow_connection_bridge_plan_invalid_secret_key",
    ]
    assert bad_secret_key not in combined
    assert "PASSWORD=secret" not in combined
    assert not (artifact_dir / "airflow-connections-secret.yaml").exists()


def test_connection_bridge_plan_rejects_invalid_secret_names_without_leaking_payload(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    monkeypatch.chdir(tmp_path)
    artifact_dir = _write_airflow_artifacts(tmp_path)
    bad_secret_name = "dpone-airflow-connections\nPASSWORD=secret"
    for name in ("runtime-profile.json", "pod-contract.json"):
        path = artifact_dir / name
        payload = json.loads(path.read_text(encoding="utf-8"))
        payload["connection_bridge"]["secret_name"] = bad_secret_name
        payload["connection_bridge"]["env"][0]["secret_ref"]["name"] = bad_secret_name
        path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    code = cmd_gitops_airflow_connection_bridge_plan(
        _plan_args(),
        ctx=_ctx(tmp_path),
        logger=logging.getLogger("test"),
    )
    payload = json.loads(capsys.readouterr().out)
    combined = json.dumps(payload)

    assert code == 2
    assert [blocker["code"] for blocker in payload["blockers"]] == [
        "airflow_connection_bridge_plan_invalid_secret_name",
        "airflow_connection_bridge_plan_invalid_secret_name",
    ]
    assert bad_secret_name not in combined
    assert "PASSWORD=secret" not in combined
    assert not (artifact_dir / "airflow-connections-secret.yaml").exists()


def test_connection_bridge_plan_is_indexed_and_validated_by_preflight(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    monkeypatch.chdir(tmp_path)
    _write_airflow_artifacts(tmp_path)
    ctx = _ctx(tmp_path)

    cmd_gitops_airflow_artifact_index(_artifact_index_args(), ctx=ctx, logger=logging.getLogger("test"))
    capsys.readouterr()
    blocked_code = cmd_gitops_airflow_preflight(_preflight_args(), ctx=ctx, logger=logging.getLogger("test"))
    blocked_payload = json.loads(capsys.readouterr().out)

    assert blocked_code == 2
    assert any(blocker["code"] == "airflow_connection_bridge_plan_missing" for blocker in blocked_payload["blockers"])
    assert "connection-bridge-plan" in " ".join(blocked_payload["next_actions"])

    cmd_gitops_airflow_connection_bridge_plan(_plan_args(), ctx=ctx, logger=logging.getLogger("test"))
    capsys.readouterr()
    cmd_gitops_airflow_artifact_index(_artifact_index_args(), ctx=ctx, logger=logging.getLogger("test"))
    indexed_payload = json.loads(capsys.readouterr().out)
    code = cmd_gitops_airflow_preflight(_preflight_args(), ctx=ctx, logger=logging.getLogger("test"))
    payload = json.loads(capsys.readouterr().out)

    assert code == 0
    assert {entry["name"] for entry in indexed_payload["entries"]} >= {"connection_bridge_plan"}
    assert any(check["name"] == "airflow_connection_bridge_plan" and check["passed"] for check in payload["checks"])
    assert payload["blockers"] == []
