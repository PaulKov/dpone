from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import sys
import textwrap
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
SPEC = ROOT / "docs/feature-design-ci-shadow-pr3b-semantic-privilege-boundary.md"
TASK_PATH = "test_artifacts/agent-policy/dpone-ci-shadow-pr3b-public-output-amendment.yml"
SPEC_PATH = "docs/feature-design-ci-shadow-pr3b-semantic-privilege-boundary.md"
RECEIPT_SCHEMA_PATH = "evals/agent/pr-merge-receipt.schema.json"
RECEIPT_SCHEMA = ROOT / RECEIPT_SCHEMA_PATH
BINDING_FIELDS = (
    "repository",
    "protected_base_ref",
    "pr_number",
    "merged_at",
    "integration_method",
    "reviewed_head_sha",
    "reviewed_head_tree",
    "base_parent_sha",
    "integration_commit_sha",
    "integration_tree",
    "changed_paths",
    "pr_body_sha256",
    "source_receipt",
)


def git(root: Path, *args: str) -> str:
    completed = subprocess.run(
        ["git", *args],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    )
    return completed.stdout.strip()


def commit(root: Path, message: str, files: dict[str, str]) -> str:
    for relative, content in files.items():
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
    git(root, "add", "--all")
    git(root, "commit", "-m", message)
    return git(root, "rev-parse", "HEAD")


def binding_id(receipt: dict[str, Any]) -> str:
    binding = {field: receipt.get(field) for field in BINDING_FIELDS}
    encoded = json.dumps(binding, sort_keys=True, separators=(",", ":")).encode()
    return f"sha256:{hashlib.sha256(encoded).hexdigest()}"


def complete_receipt(
    *,
    reviewed: str,
    reviewed_tree: str,
    base: str,
    integration: str,
    integration_tree: str,
    changed_paths: list[str],
) -> dict[str, Any]:
    digest = "sha256:" + "a" * 64
    receipt: dict[str, Any] = {
        "schema_version": 1,
        "status": "PASS",
        "binding_id": None,
        "repository": "PaulKov/dpone",
        "protected_base_ref": "master",
        "pr_number": 539,
        "merged_at": "2026-08-11T12:00:00Z",
        "integration_method": "squash",
        "reviewed_head_sha": reviewed,
        "reviewed_head_tree": reviewed_tree,
        "base_parent_sha": base,
        "integration_commit_sha": integration,
        "integration_tree": integration_tree,
        "changed_paths": changed_paths,
        "pr_body_sha256": digest,
        "source_receipt": {
            "check_run_id": 1,
            "workflow_run_id": 2,
            "workflow_run_attempt": 1,
            "artifact_id": 3,
            "artifact_digest": digest,
            "artifact_size_bytes": 1,
            "archive_sha256": digest,
            "archive_size_bytes": 1,
            "created_at": "2026-08-11T11:55:00Z",
            "completed_at": "2026-08-11T11:56:00Z",
            "receipt_sha256": digest,
            "audit_manifest_sha256": digest,
            "body_sha256": digest,
            "status": "PASS",
        },
        "producer": {
            "workflow": "Agent PR receipt",
            "run_id": "4",
            "run_attempt": "1",
        },
        "errors": [],
        "warnings": [],
    }
    receipt["binding_id"] = binding_id(receipt)
    return receipt


def receipt_script() -> str:
    rollout = SPEC.read_text(encoding="utf-8").split("## Rollout and rollback", maxsplit=1)[1]
    return textwrap.dedent(rollout.split("```bash", maxsplit=1)[1].split("```", maxsplit=1)[0])


def squash_repository(
    tmp_path: Path,
    *,
    approved: bool = True,
    include_spec: bool = True,
    prior_task_history: bool = False,
) -> tuple[Path, str, str, dict[str, Any]]:
    root = tmp_path / "repository"
    root.mkdir(parents=True)
    git(root, "init", "--quiet")
    git(root, "config", "user.name", "PR3B Receipt Test")
    git(root, "config", "user.email", "pr3b@example.invalid")
    base_files = {
        "base.txt": "base\n",
        RECEIPT_SCHEMA_PATH: RECEIPT_SCHEMA.read_text(encoding="utf-8"),
    }
    if prior_task_history:
        commit(root, "historical task introduction", {**base_files, TASK_PATH: "task: historical\n"})
        (root / TASK_PATH).unlink()
        git(root, "add", "--all")
        git(root, "commit", "-m", "delete historical task")
        base = commit(root, "base", {"base-marker.txt": "approved base\n"})
    else:
        base = commit(root, "base", base_files)
    lifecycle = "APPROVED" if approved else "RESEARCHED"
    checklist = "x" if approved else " "

    git(root, "checkout", "--quiet", "-b", "reviewed", base)
    reviewed_files = {TASK_PATH: "task: approved\n"}
    if include_spec:
        reviewed_files[SPEC_PATH] = (
            f"- Public-output amendment status: {lifecycle}\n"
            f"- [{checklist}] Maintainer changed public-output amendment status to `APPROVED` after\n"
            "  reviewing the exact amendment head.\n"
        )
    reviewed = commit(root, "approved clarification", reviewed_files)
    git(root, "checkout", "--quiet", "-b", "mainline", base)
    git(root, "merge", "--quiet", "--squash", "reviewed")
    git(root, "commit", "-m", "squash approved clarification")
    integration = git(root, "rev-parse", "HEAD")
    reviewed_tree = git(root, "rev-parse", f"{reviewed}^{{tree}}")
    integration_tree = git(root, "rev-parse", f"{integration}^{{tree}}")
    assert reviewed_tree == integration_tree

    changed_paths = [TASK_PATH]
    if include_spec:
        changed_paths.insert(0, SPEC_PATH)
    receipt = complete_receipt(
        reviewed=reviewed,
        reviewed_tree=reviewed_tree,
        base=base,
        integration=integration,
        integration_tree=integration_tree,
        changed_paths=changed_paths,
    )
    return root, reviewed, integration, receipt


def run_receipt_check(
    root: Path,
    reviewed: str,
    integration: str,
    receipt: dict[str, Any] | str | bytes | None,
    *,
    receipt_name: str = "receipt.json",
    extra_env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    receipt_path = root / receipt_name
    if isinstance(receipt, bytes):
        receipt_path.write_bytes(receipt)
    elif isinstance(receipt, str):
        receipt_path.write_text(receipt, encoding="utf-8")
    elif receipt is not None:
        receipt_path.write_text(json.dumps(receipt), encoding="utf-8")
    fake_bin = root / ".receipt-test-bin"
    fake_bin.mkdir(exist_ok=True)
    fake_uv = fake_bin / "uv"
    fake_uv.write_text(
        "#!/bin/sh\n"
        'test "$1" = run || exit 64\n'
        "shift\n"
        'test "$1" = --frozen || exit 64\n'
        "shift\n"
        'test "$1" = --offline || exit 64\n'
        "shift\n"
        'test "$1" = python || exit 64\n'
        "shift\n"
        'exec "$RECEIPT_TEST_PYTHON" "$@"\n',
        encoding="utf-8",
    )
    fake_uv.chmod(0o755)
    env = {
        **os.environ,
        "REVIEWED_HEAD": reviewed,
        "INTEGRATION_COMMIT": integration,
        "RECEIPT": str(receipt_path),
        "RECEIPT_TEST_PYTHON": sys.executable,
        "RECEIPT_TEST_REAL_GIT": shutil.which("git") or "git",
        "PATH": f"{fake_bin}{os.pathsep}{os.environ['PATH']}",
    }
    if extra_env:
        env.update(extra_env)
    return subprocess.run(
        ["bash"],
        cwd=root,
        env=env,
        input=receipt_script(),
        capture_output=True,
        text=True,
        check=False,
    )
