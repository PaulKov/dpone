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


def _ctx(tmp_path: Path) -> AppContext:
    settings = Settings(repo_root=tmp_path, project_dir=tmp_path, manifest_dir=tmp_path, sources_registry_paths=())
    return AppContext(settings=settings, logger=logging.getLogger("test"), fs=LocalFileSystem(), yaml=PyYamlCodec())


def _args(**overrides: object) -> Namespace:
    data = {
        "changed_files": [],
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
        "policy_profile": "custom",
        "attest": False,
        "output_dir": ".dpone/gitops/bundle",
        "output": None,
        "format": "json",
    }
    data.update(overrides)
    return Namespace(**data)


def _write(path: Path, text: str = "source: {}\nsink: {}\n") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


def _workload(tmp_path: Path, *, with_group_warning: bool = False) -> Path:
    workload = tmp_path / "dpone_workloads"
    depends_on = "depends_on:\n  - path: seed.yaml\n"
    if with_group_warning:
        depends_on += "  - group: shared\n"
    _write(workload / "manifests" / "orders.yaml", depends_on + "source: {}\nsink: {}\n")
    _write(workload / "manifests" / "seed.yaml")
    _write(workload / "manifests" / "customers.yaml")
    return workload


def _git_baseline(tmp_path: Path) -> None:
    subprocess.run(["git", "init"], cwd=tmp_path, check=True, capture_output=True, text=True)
    subprocess.run(["git", "config", "user.email", "test@example.com"], cwd=tmp_path, check=True)
    subprocess.run(["git", "config", "user.name", "Test User"], cwd=tmp_path, check=True)
    subprocess.run(["git", "add", "."], cwd=tmp_path, check=True, capture_output=True, text=True)
    subprocess.run(["git", "commit", "-m", "baseline"], cwd=tmp_path, check=True, capture_output=True, text=True)
    subprocess.run(
        ["git", "update-ref", "refs/remotes/origin/master", "HEAD"],
        cwd=tmp_path,
        check=True,
        capture_output=True,
        text=True,
    )


def test_gitops_bundle_service_writes_handoff_artifacts(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    _workload(tmp_path)
    _git_baseline(tmp_path)

    view = GitOpsBundleService(ctx=_ctx(tmp_path)).build_view(
        _args(
            changed_files=["dpone_workloads/manifests/seed.yaml"],
            verify_lock=True,
        )
    )
    payload = view.report.to_jsonable()

    assert view.exit_code == 0
    assert payload["kind"] == "gitops.bundle"
    assert payload["output_dir"] == ".dpone/gitops/bundle"
    assert payload["affected_path"] == ".dpone/gitops/bundle/affected.json"
    assert payload["summary_path"] == ".dpone/gitops/bundle/summary.md"
    assert payload["policy"]["verify_lock"] is True
    assert payload["policy"]["profile"] == "custom"
    assert payload["entries"] == [
        {
            "manifest": "dpone_workloads/manifests/orders.yaml",
            "plan_path": ".dpone/gitops/bundle/manifests/dpone_workloads__manifests__orders.yaml/gitops_plan.json",
            "verify_path": ".dpone/gitops/bundle/manifests/dpone_workloads__manifests__orders.yaml/gitops_verify.json",
            "passed": True,
        }
    ]
    bundle_file = json.loads((tmp_path / ".dpone/gitops/bundle/bundle.json").read_text(encoding="utf-8"))
    affected_file = json.loads((tmp_path / payload["affected_path"]).read_text(encoding="utf-8"))
    plan_file = json.loads((tmp_path / payload["entries"][0]["plan_path"]).read_text(encoding="utf-8"))
    verify_file = json.loads((tmp_path / payload["entries"][0]["verify_path"]).read_text(encoding="utf-8"))

    assert bundle_file == payload
    assert affected_file["kind"] == "gitops.affected"
    assert plan_file["kind"] == "gitops.plan"
    assert verify_file["kind"] == "gitops.verify"
    assert verify_file["lock_checks"][0]["passed"] is True
    assert "# GitOps bundle" in (tmp_path / payload["summary_path"]).read_text(encoding="utf-8")
    assert str(tmp_path) not in json.dumps(payload)


def test_gitops_bundle_service_blocks_empty_impact_when_policy_requires_it(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    _workload(tmp_path)

    view = GitOpsBundleService(ctx=_ctx(tmp_path)).build_view(
        _args(
            changed_files=["README.md"],
            fail_on_empty_impact=True,
        )
    )
    payload = view.report.to_jsonable()

    assert view.exit_code == 2
    assert payload["entries"] == []
    assert payload["blockers"][0]["code"] == "empty_impact"


def test_gitops_bundle_service_blocks_warnings_when_policy_requires_it(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    _workload(tmp_path, with_group_warning=True)

    view = GitOpsBundleService(ctx=_ctx(tmp_path)).build_view(
        _args(
            changed_files=["dpone_workloads/manifests/seed.yaml"],
            fail_on_warnings=True,
        )
    )
    payload = view.report.to_jsonable()

    assert view.exit_code == 2
    assert any(warning["code"] == "group_dependency_unresolved" for warning in payload["warnings"])
    assert payload["blockers"][0]["code"] == "warnings_present"


def test_gitops_bundle_service_requires_lock_verification_when_policy_requires_lock(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    _workload(tmp_path)

    view = GitOpsBundleService(ctx=_ctx(tmp_path)).build_view(
        _args(
            changed_files=["dpone_workloads/manifests/seed.yaml"],
            require_lock=True,
            verify_lock=False,
        )
    )
    payload = view.report.to_jsonable()

    assert view.exit_code == 2
    assert payload["blockers"][0]["code"] == "lock_verification_required"


def test_gitops_bundle_service_blocks_unknown_policy_profile(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    _workload(tmp_path)

    view = GitOpsBundleService(ctx=_ctx(tmp_path)).build_view(
        _args(
            changed_files=["dpone_workloads/manifests/seed.yaml"],
            policy_profile="strictest",
        )
    )
    payload = view.report.to_jsonable()

    assert view.exit_code == 2
    assert payload["policy"]["profile"] == "strictest"
    assert payload["blockers"][0]["code"] == "invalid_policy_profile"


def test_gitops_bundle_service_release_profile_enables_strict_policy(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    _workload(tmp_path)

    view = GitOpsBundleService(ctx=_ctx(tmp_path)).build_view(
        _args(
            changed_files=["dpone_workloads/manifests/seed.yaml"],
            policy_profile="release",
        )
    )
    payload = view.report.to_jsonable()

    assert view.exit_code == 0
    assert payload["policy"] == {
        "profile": "release",
        "verify_lock": True,
        "fail_on_empty_impact": True,
        "fail_on_warnings": True,
        "require_lock": True,
    }
    assert payload["entries"][0]["passed"] is True


def test_gitops_bundle_service_writes_attestation_with_artifact_digests(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    _workload(tmp_path)
    _git_baseline(tmp_path)

    view = GitOpsBundleService(ctx=_ctx(tmp_path)).build_view(
        _args(
            changed_files=["dpone_workloads/manifests/seed.yaml"],
            policy_profile="release",
            attest=True,
            from_ref="origin/master",
            to_ref="HEAD",
        )
    )
    payload = view.report.to_jsonable()
    attestation = payload["attestation"]

    assert view.exit_code == 0
    assert attestation["producer"] == "dpone gitops bundle"
    assert attestation["schema_version"] == "1"
    assert attestation["hash_algorithm"] == "sha256"
    assert len(attestation["bundle_digest"]) == 64
    assert attestation["provenance"]["from_ref"] == "origin/master"
    assert attestation["provenance"]["to_ref"] == "HEAD"
    assert attestation["provenance"]["policy_profile"] == "release"
    assert attestation["provenance"]["output_dir"] == ".dpone/gitops/bundle"
    artifact_paths = [artifact["path"] for artifact in attestation["artifacts"]]
    assert artifact_paths == [
        ".dpone/gitops/bundle/affected.json",
        ".dpone/gitops/bundle/manifests/dpone_workloads__manifests__orders.yaml/gitops_plan.json",
        ".dpone/gitops/bundle/manifests/dpone_workloads__manifests__orders.yaml/gitops_verify.json",
        ".dpone/gitops/bundle/summary.md",
    ]
    assert all(len(artifact["sha256"]) == 64 for artifact in attestation["artifacts"])
    assert all(artifact["bytes"] > 0 for artifact in attestation["artifacts"])
    bundle_file = json.loads((tmp_path / ".dpone/gitops/bundle/bundle.json").read_text(encoding="utf-8"))
    assert bundle_file == payload
    assert str(tmp_path) not in json.dumps(payload)
