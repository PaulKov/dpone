from __future__ import annotations

import json
import logging
from argparse import Namespace
from pathlib import Path

from dpone.adapters.fs_local import LocalFileSystem
from dpone.adapters.yaml_pyyaml import PyYamlCodec
from dpone.app.context import AppContext
from dpone.app.settings import Settings
from dpone.commands.gitops.airflow_preflight_cmd import (
    cmd_gitops_airflow_artifact_index,
    cmd_gitops_airflow_preflight,
)


def _ctx(tmp_path: Path) -> AppContext:
    settings = Settings(repo_root=tmp_path, project_dir=tmp_path, manifest_dir=tmp_path, sources_registry_paths=())
    return AppContext(settings=settings, logger=logging.getLogger("test"), fs=LocalFileSystem(), yaml=PyYamlCodec())


def _artifact_index_args(**overrides: object) -> Namespace:
    data = {
        "artifact_dir": ".dpone/gitops/airflow",
        "output_path": None,
        "format": "json",
        "output": None,
    }
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
    image = "ghcr.io/acme/dpone:2026.06.16"
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
            "resources": {"requests": {"cpu": "250m", "memory": "512Mi"}, "limits": {"cpu": "1", "memory": "1Gi"}},
            "artifact_sink": {"kind": "local", "path": ".dpone/gitops/airflow"},
            "runner_policy": "release",
            "outcome_mode": "strict_fail",
        },
    )
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
                    "resources": {"requests": {"cpu": "250m", "memory": "512Mi"}, "limits": {"cpu": "1"}},
                }
            ],
        },
    }
    (artifact_dir / "pod-spec.yaml").write_text(PyYamlCodec().dump(pod_spec), encoding="utf-8")
    kpo_kwargs = {
        "task_id": "dpone_gitops_runtime",
        "name": "dpone-runtime",
        "namespace": "dpone-runners",
        "pod_template_file": ".dpone/gitops/airflow/pod-spec.yaml",
        "do_xcom_push": True,
        "cmds": ["/bin/sh", "-ec"],
        "arguments": ["dpone gitops airflow run-spec-exec .dpone/gitops/airflow/run-spec.json"],
    }
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


def test_airflow_artifact_index_cli_writes_inventory(tmp_path: Path, monkeypatch, capsys) -> None:
    monkeypatch.chdir(tmp_path)
    _write_airflow_artifacts(tmp_path)

    code = cmd_gitops_airflow_artifact_index(
        _artifact_index_args(),
        ctx=_ctx(tmp_path),
        logger=logging.getLogger("test"),
    )
    payload = json.loads(capsys.readouterr().out)
    file_payload = json.loads((tmp_path / ".dpone/gitops/airflow/artifact-index.json").read_text("utf-8"))

    assert code == 0
    assert payload["kind"] == "gitops.airflow_artifact_index"
    assert payload["artifact_dir"] == ".dpone/gitops/airflow"
    assert payload["output_path"] == ".dpone/gitops/airflow/artifact-index.json"
    assert file_payload["entries"] == payload["entries"]
    assert {entry["name"] for entry in payload["entries"]} >= {"run_spec", "pod_contract", "pod_spec", "kpo_kwargs"}
    assert all(entry["sha256"] for entry in payload["entries"] if entry["exists"])
    assert str(tmp_path) not in json.dumps(payload)


def test_airflow_preflight_release_requires_artifact_index(tmp_path: Path, monkeypatch, capsys) -> None:
    monkeypatch.chdir(tmp_path)
    _write_airflow_artifacts(tmp_path)

    code = cmd_gitops_airflow_preflight(_preflight_args(), ctx=_ctx(tmp_path), logger=logging.getLogger("test"))
    payload = json.loads(capsys.readouterr().out)

    assert code == 2
    assert payload["kind"] == "gitops.airflow_preflight"
    assert any(blocker["code"] == "airflow_artifact_index_missing" for blocker in payload["blockers"])
    assert "dpone gitops airflow artifact-index" in payload["next_actions"][0]


def test_airflow_preflight_passes_with_fresh_index_and_blocks_drift(tmp_path: Path, monkeypatch, capsys) -> None:
    monkeypatch.chdir(tmp_path)
    artifact_dir = _write_airflow_artifacts(tmp_path)
    cmd_gitops_airflow_artifact_index(_artifact_index_args(), ctx=_ctx(tmp_path), logger=logging.getLogger("test"))
    capsys.readouterr()

    code = cmd_gitops_airflow_preflight(_preflight_args(), ctx=_ctx(tmp_path), logger=logging.getLogger("test"))
    payload = json.loads(capsys.readouterr().out)

    assert code == 0
    assert payload["blockers"] == []
    assert any(check["name"] == "airflow_artifact_index_fresh" and check["passed"] for check in payload["checks"])
    assert any(check["name"] == "airflow_pod_doctor" and check["passed"] for check in payload["checks"])

    run_spec = json.loads((artifact_dir / "run-spec.json").read_text("utf-8"))
    run_spec["image"] = "ghcr.io/acme/dpone:drifted"
    _write_json(artifact_dir / "run-spec.json", run_spec)

    drift_code = cmd_gitops_airflow_preflight(_preflight_args(), ctx=_ctx(tmp_path), logger=logging.getLogger("test"))
    drift_payload = json.loads(capsys.readouterr().out)

    assert drift_code == 2
    assert any(blocker["code"] == "airflow_artifact_index_digest_mismatch" for blocker in drift_payload["blockers"])
    assert "Regenerate artifact-index.json" in " ".join(drift_payload["next_actions"])
