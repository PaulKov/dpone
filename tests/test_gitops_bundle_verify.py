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
from dpone.services.gitops.bundle_service import GitOpsBundleService
from dpone.services.gitops.bundle_verify_service import GitOpsBundleVerifyService


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
        "runner": "generic",
        "worktree": ".",
        "verify_lock": False,
        "fail_on_empty_impact": False,
        "fail_on_warnings": False,
        "require_lock": False,
        "policy_profile": "release",
        "attest": True,
        "output_dir": ".dpone/gitops/bundle",
        "output": None,
        "format": "json",
    }
    data.update(overrides)
    return Namespace(**data)


def _verify_args(**overrides: object) -> Namespace:
    data = {
        "bundle_path": ".dpone/gitops/bundle/bundle.json",
        "require_attestation": False,
        "output": None,
        "format": "json",
    }
    data.update(overrides)
    return Namespace(**data)


def _write(path: Path, text: str = "source: {}\nsink: {}\n") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


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


def _build_bundle(tmp_path: Path, *, attest: bool = True) -> dict[str, object]:
    view = GitOpsBundleService(ctx=_ctx(tmp_path)).build_view(_bundle_args(attest=attest))
    assert view.exit_code == 0
    return view.report.to_jsonable()


def test_gitops_bundle_verify_passes_fresh_attested_bundle(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    _workload(tmp_path)
    _git_baseline(tmp_path)
    _build_bundle(tmp_path)

    view = GitOpsBundleVerifyService(ctx=_ctx(tmp_path)).build_view(_verify_args(require_attestation=True))
    payload = view.report.to_jsonable()

    assert view.exit_code == 0
    assert payload["kind"] == "gitops.bundle_verify"
    assert payload["bundle_path"] == ".dpone/gitops/bundle/bundle.json"
    assert payload["schema_check"]["passed"] is True
    assert payload["attestation_check"]["present"] is True
    assert payload["attestation_check"]["passed"] is True
    assert len(payload["artifact_checks"]) == 4
    assert all(check["passed"] for check in payload["artifact_checks"])
    assert payload["warnings"] == []
    assert payload["blockers"] == []
    assert str(tmp_path) not in json.dumps(payload)


def test_gitops_bundle_verify_blocks_artifact_digest_drift(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    _workload(tmp_path)
    _git_baseline(tmp_path)
    _build_bundle(tmp_path)
    (tmp_path / ".dpone/gitops/bundle/affected.json").write_text('{"kind": "gitops.affected", "drifted": true}\n')

    view = GitOpsBundleVerifyService(ctx=_ctx(tmp_path)).build_view(_verify_args(require_attestation=True))
    payload = view.report.to_jsonable()

    assert view.exit_code == 2
    assert any(blocker["code"] == "artifact_digest_mismatch" for blocker in payload["blockers"])
    assert any(
        check["path"] == ".dpone/gitops/bundle/affected.json" and not check["passed"]
        for check in payload["artifact_checks"]
    )


def test_gitops_bundle_verify_blocks_missing_artifact(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    _workload(tmp_path)
    _git_baseline(tmp_path)
    _build_bundle(tmp_path)
    (tmp_path / ".dpone/gitops/bundle/summary.md").unlink()

    view = GitOpsBundleVerifyService(ctx=_ctx(tmp_path)).build_view(_verify_args(require_attestation=True))
    payload = view.report.to_jsonable()

    assert view.exit_code == 2
    assert any(blocker["code"] == "artifact_missing" for blocker in payload["blockers"])
    assert any(
        check["path"] == ".dpone/gitops/bundle/summary.md" and not check["exists"]
        for check in payload["artifact_checks"]
    )


def test_gitops_bundle_verify_warns_or_blocks_missing_attestation(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    _workload(tmp_path)
    _git_baseline(tmp_path)
    _build_bundle(tmp_path, attest=False)

    advisory_view = GitOpsBundleVerifyService(ctx=_ctx(tmp_path)).build_view(_verify_args(require_attestation=False))
    advisory_payload = advisory_view.report.to_jsonable()
    strict_view = GitOpsBundleVerifyService(ctx=_ctx(tmp_path)).build_view(_verify_args(require_attestation=True))
    strict_payload = strict_view.report.to_jsonable()

    assert advisory_view.exit_code == 0
    assert advisory_payload["attestation_check"]["present"] is False
    assert advisory_payload["warnings"][0]["code"] == "attestation_missing"
    assert strict_view.exit_code == 2
    assert strict_payload["blockers"][0]["code"] == "attestation_required"


def test_gitops_bundle_verify_blocks_invalid_bundle_schema(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    bundle = tmp_path / ".dpone/gitops/bundle/bundle.json"
    bundle.parent.mkdir(parents=True, exist_ok=True)
    bundle.write_text('{"kind": "gitops.bundle", "entries": []}\n', encoding="utf-8")

    view = GitOpsBundleVerifyService(ctx=_ctx(tmp_path)).build_view(_verify_args(require_attestation=True))
    payload = view.report.to_jsonable()

    assert view.exit_code == 2
    assert payload["schema_check"]["passed"] is False
    assert any(blocker["code"] == "schema_required_field_missing" for blocker in payload["blockers"])
