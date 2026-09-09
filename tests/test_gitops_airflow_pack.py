from __future__ import annotations

import json
import logging
from argparse import Namespace
from pathlib import Path

from dpone.adapters.fs_local import LocalFileSystem
from dpone.adapters.yaml_pyyaml import PyYamlCodec
from dpone.app.context import AppContext
from dpone.app.settings import Settings
from dpone.commands.gitops.airflow_pack_cmd import cmd_gitops_airflow_pack


def _ctx(tmp_path: Path) -> AppContext:
    settings = Settings(repo_root=tmp_path, project_dir=tmp_path, manifest_dir=tmp_path, sources_registry_paths=())
    return AppContext(settings=settings, logger=logging.getLogger("test"), fs=LocalFileSystem(), yaml=PyYamlCodec())


def _pack_args(**overrides: object) -> Namespace:
    data = {
        "artifact_dir": ".dpone/gitops/airflow",
        "bundle_path": ".dpone/gitops/bundle/bundle.json",
        "image": None,
        "image_digest": None,
        "mode": "plan",
        "runner_policy": "release",
        "include_live_gates": False,
        "output_path": None,
        "format": "json",
        "output": None,
    }
    data.update(overrides)
    return Namespace(**data)


def test_airflow_pack_plan_writes_golden_path_report_without_absolute_paths(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    monkeypatch.chdir(tmp_path)

    code = cmd_gitops_airflow_pack(
        _pack_args(image="ghcr.io/acme/dpone:2026.06.17", image_digest="sha256:" + "a" * 64),
        ctx=_ctx(tmp_path),
        logger=logging.getLogger("test"),
    )
    payload = json.loads(capsys.readouterr().out)
    report = json.loads((tmp_path / ".dpone/gitops/airflow/airflow-runtime-pack.json").read_text("utf-8"))

    assert code == 0
    assert payload["kind"] == "gitops.airflow_pack"
    assert payload["mode"] == "plan"
    assert payload["runner_policy"] == "release"
    assert payload["artifact_dir"] == ".dpone/gitops/airflow"
    assert payload["output_path"] == ".dpone/gitops/airflow/airflow-runtime-pack.json"
    assert report["kind"] == "gitops.airflow_pack"
    assert [step["name"] for step in payload["steps"]] == [
        "render",
        "run-spec",
        "runtime-profile",
        "pod-contract",
        "connection-bridge-plan",
        "k8s-manifests",
        "artifact-index",
        "preflight",
        "cluster-doctor",
        "admission-check",
        "k8s-smoke",
        "pod-watch",
        "evidence-bundle",
    ]
    assert any(step["command"].startswith("dpone gitops airflow runtime-profile") for step in payload["steps"])
    assert any(
        artifact["name"] == "pod_spec" and artifact["path"].endswith("pod-spec.yaml")
        for artifact in payload["artifacts"]
    )
    assert any(warning["code"] == "airflow_pack_artifact_missing" for warning in payload["warnings"])
    assert any("artifact-index" in action for action in payload["next_actions"])
    assert str(tmp_path) not in json.dumps(payload)


def test_airflow_pack_verify_blocks_missing_required_artifacts(tmp_path: Path, monkeypatch, capsys) -> None:
    monkeypatch.chdir(tmp_path)

    code = cmd_gitops_airflow_pack(_pack_args(mode="verify"), ctx=_ctx(tmp_path), logger=logging.getLogger("test"))
    payload = json.loads(capsys.readouterr().out)

    assert code == 2
    assert payload["mode"] == "verify"
    assert any(blocker["code"] == "airflow_pack_artifact_missing" for blocker in payload["blockers"])
    assert "Run dpone gitops airflow render" in " ".join(payload["next_actions"])


def test_airflow_pack_verify_passes_with_minimal_required_artifacts(tmp_path: Path, monkeypatch, capsys) -> None:
    monkeypatch.chdir(tmp_path)
    _write_required_airflow_artifacts(tmp_path)

    code = cmd_gitops_airflow_pack(_pack_args(mode="verify"), ctx=_ctx(tmp_path), logger=logging.getLogger("test"))
    payload = json.loads(capsys.readouterr().out)

    assert code == 0
    assert payload["blockers"] == []
    assert all(artifact["passed"] for artifact in payload["artifacts"] if artifact["required"])
    assert any(artifact["sha256"] for artifact in payload["artifacts"] if artifact["name"] == "run_spec")


def test_airflow_pack_includes_opt_in_live_gate_steps(tmp_path: Path, monkeypatch, capsys) -> None:
    monkeypatch.chdir(tmp_path)

    code = cmd_gitops_airflow_pack(
        _pack_args(include_live_gates=True),
        ctx=_ctx(tmp_path),
        logger=logging.getLogger("test"),
    )
    payload = json.loads(capsys.readouterr().out)

    assert code == 0
    live_steps = [step for step in payload["steps"] if step["credential_required"]]
    assert {step["name"] for step in live_steps} == {"cluster-doctor-live", "k8s-smoke-live", "pod-watch-live"}
    assert all("--mode live" in step["command"] for step in live_steps)


def test_airflow_pack_markdown_is_human_readable(tmp_path: Path, monkeypatch, capsys) -> None:
    monkeypatch.chdir(tmp_path)

    code = cmd_gitops_airflow_pack(
        _pack_args(format="markdown"),
        ctx=_ctx(tmp_path),
        logger=logging.getLogger("test"),
    )
    output = capsys.readouterr().out

    assert code == 0
    assert "# GitOps Airflow runtime pack" in output
    assert "airflow-runtime-pack.json" in output
    assert "dpone gitops airflow artifact-index" in output


def _write_required_airflow_artifacts(tmp_path: Path) -> None:
    artifact_dir = tmp_path / ".dpone/gitops/airflow"
    artifact_dir.mkdir(parents=True)
    _write_json(
        artifact_dir / "run-spec.json",
        {
            "kind": "gitops.airflow_run_spec",
            "schema_version": "1",
            "producer": "dpone gitops airflow run-spec",
        },
    )
    _write_json(
        artifact_dir / "runtime-profile.json",
        {
            "kind": "gitops.airflow_runtime_profile",
            "schema_version": "1",
            "producer": "dpone gitops airflow runtime-profile",
        },
    )
    _write_json(
        artifact_dir / "pod-contract.json",
        {
            "kind": "gitops.airflow_pod_contract",
            "schema_version": "1",
            "producer": "dpone gitops airflow pod-contract",
        },
    )
    _write_json(
        artifact_dir / "xcom-summary.json",
        {
            "kind": "gitops.airflow_xcom_summary",
            "schema_version": "1",
            "producer": "dpone gitops airflow runtime-profile",
        },
    )
    _write_json(
        artifact_dir / "kpo-kwargs.json",
        {
            "task_id": "dpone_gitops_runtime",
            "pod_template_file": ".dpone/gitops/airflow/pod-spec.yaml",
            "do_xcom_push": True,
        },
    )
    (artifact_dir / "pod-spec.yaml").write_text(
        PyYamlCodec().dump({"apiVersion": "v1", "kind": "Pod", "metadata": {"name": "dpone-runtime"}}),
        encoding="utf-8",
    )


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
