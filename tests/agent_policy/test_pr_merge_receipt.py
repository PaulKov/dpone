from __future__ import annotations

import hashlib
import importlib.util
import io
import json
import subprocess
import sys
import zipfile
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest
from jsonschema import Draft202012Validator

ROOT = Path(__file__).resolve().parents[2]
REPOSITORY = "PaulKov/dpone"
BASE_REF = "master"
REVIEWED_HEAD = "a" * 40
BASE_PARENT = "b" * 40
INTEGRATION_COMMIT = "c" * 40
TREE = "d" * 40
BODY = "Immutable reviewed body\n"
MERGED_AT = "2026-07-29T00:03:00Z"
CONTROL_PATHS = (".agents/policy/required.yml", "docs/required.yml")


def _load(name: str, relative: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, ROOT / relative)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


merge_receipt = _load(
    "dpone_agent_pr_merge_receipt_test",
    "tools/agent_policy/pr_merge_receipt.py",
)


def _digest(content: bytes) -> str:
    return "sha256:" + hashlib.sha256(content).hexdigest()


def _validate_schema(payload: dict[str, Any]) -> None:
    schema = json.loads((ROOT / "evals/agent/pr-merge-receipt.schema.json").read_text(encoding="utf-8"))
    Draft202012Validator(schema).validate(payload)


def _event(*, body: str = BODY) -> dict[str, Any]:
    return {
        "action": "closed",
        "repository": {"full_name": REPOSITORY},
        "pull_request": {
            "number": 455,
            "merged": True,
            "state": "closed",
            "merged_at": MERGED_AT,
            "merge_commit_sha": INTEGRATION_COMMIT,
            "body": body,
            "head": {"sha": REVIEWED_HEAD},
            "base": {"ref": BASE_REF, "sha": BASE_PARENT, "repo": {"full_name": REPOSITORY}},
        },
    }


def _raw_governance_artifact(paths: tuple[str, ...], digest: str) -> dict[str, Any]:
    return {
        "name": "agent-governance-gate",
        "workflow_run_head_sha": REVIEWED_HEAD,
        "workflow_run_id": 77,
        "expired": False,
        "url": "https://api.github.test/artifacts/301",
        "artifact_id": 301,
        "digest": digest,
        "size_in_bytes": 10,
        "archive_sha256": digest,
        "archive_size_bytes": 10,
        "created_at": "2026-07-29T00:01:00Z",
        "expires_at": "2026-10-27T00:01:00Z",
        "content": None,
        "attestation": None,
    }


def _chain_governance_artifact(paths: tuple[str, ...], digest: str) -> dict[str, Any]:
    return {
        "name": "agent-governance-gate",
        "artifact_id": 301,
        "workflow_run_id": 77,
        "workflow_run_head_sha": REVIEWED_HEAD,
        "digest": digest,
        "archive_sha256": digest,
        "archive_size_bytes": 10,
        "expired": False,
        "url": "https://api.github.test/artifacts/301",
        "content_status": "PASS",
        "content_control_surface_changed": True,
        "content_changed_paths": list(paths),
        "content_checks": [],
        "attestation": None,
    }


def _source_archive(
    *,
    body: str = BODY,
    paths: tuple[str, ...] = CONTROL_PATHS,
    status: str = "PASS",
    audit_overrides: dict[str, Any] | None = None,
    evidence_head: str = REVIEWED_HEAD,
    governance_artifact_id: int = 301,
    include_github_evidence: bool = True,
) -> bytes:
    governance_digest = _digest(b"governance")
    raw_governance = _raw_governance_artifact(paths, governance_digest)
    chain_governance = _chain_governance_artifact(paths, governance_digest)
    receipt = {
        "schema_version": 2,
        "status": status,
        "control_surface_changed": status == "PASS",
        "changed_paths": list(paths),
        "errors": [],
        "warnings": [],
        "traceability": None,
        "github_evidence": (
            {
                "head_sha": REVIEWED_HEAD,
                "required_checks": [],
                "check_runs": [],
                "statuses": [],
                "artifacts": [raw_governance] if status == "PASS" else [],
                "errors": [],
            }
            if include_github_evidence
            else None
        ),
        "evidence_chain": (
            {
                "head_sha": evidence_head,
                "required_checks": [],
                "governance_artifact": {
                    **chain_governance,
                    "artifact_id": governance_artifact_id,
                },
            }
            if status == "PASS"
            else None
        ),
    }
    audit = {
        "schema_version": 1,
        "kind": "agent_pr_receipt",
        "repository": REPOSITORY,
        "workflow": "Agent PR receipt",
        "run_id": "101",
        "run_attempt": "1",
        "artifact_name": "agent-pr-receipt",
        "artifact_retention_days": 90,
        "generated_at": "2026-07-29T00:02:00Z",
        "pr_number": 455,
        "base_ref": BASE_REF,
        "base_sha": BASE_PARENT,
        "head_ref": "feature",
        "head_sha": REVIEWED_HEAD,
        "merge_sha": None,
        "receipt_status": status,
        "receipt_errors": [],
        "receipt_warnings": [],
        "traceability_source": None,
        "traceability_source_kind": None,
        "traceability_statuses": [],
        "traceability_non_pass_reasons": [],
        "evidence_chain_head_sha": REVIEWED_HEAD if status == "PASS" else None,
        "evidence_chain_required_checks": [],
        "evidence_chain_governance_artifact": chain_governance if status == "PASS" else None,
        "required_checks": [],
        "evidence_artifacts": [raw_governance] if status == "PASS" else [],
    }
    audit.update(audit_overrides or {})
    members = {
        "agent_pr_receipt.json": json.dumps(receipt).encode(),
        "agent_audit_manifest.json": json.dumps(audit).encode(),
        "pr-body.md": body.encode(),
        "pr-head-sha.txt": f"{REVIEWED_HEAD}\n".encode(),
        "pr-changed-paths.txt": ("\n".join(paths) + "\n").encode(),
        "pr-receipt-exit-code.txt": b"0\n",
    }
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        for name, content in members.items():
            archive.writestr(zipfile.ZipInfo(name, date_time=(2026, 7, 29, 0, 0, 0)), content)
    return buffer.getvalue()


@dataclass
class FakeSourceAdapter:
    archive: bytes

    def list_check_runs(self, repository: str, reviewed_head_sha: str) -> list[dict[str, Any]]:
        assert (repository, reviewed_head_sha) == (REPOSITORY, REVIEWED_HEAD)
        return [
            {
                "id": 11,
                "name": "Agent PR receipt",
                "status": "completed",
                "conclusion": "success",
                "head_sha": REVIEWED_HEAD,
                "app": {"slug": "github-actions"},
                "completed_at": "2026-07-29T00:02:00Z",
                "details_url": f"https://github.com/{REPOSITORY}/actions/runs/101/job/1",
            }
        ]

    def get_workflow_run(self, repository: str, run_id: int) -> dict[str, Any]:
        assert (repository, run_id) == (REPOSITORY, 101)
        return {
            "id": 101,
            "name": "Agent PR receipt",
            "event": "pull_request",
            "status": "completed",
            "conclusion": "success",
            "head_sha": REVIEWED_HEAD,
            "repository": {"full_name": REPOSITORY},
            "path": ".github/workflows/agent-pr-receipt.yml@refs/pull/455/merge",
            "updated_at": "2026-07-29T00:02:30Z",
            "run_attempt": 1,
        }

    def list_run_artifacts(self, repository: str, run_id: int) -> list[dict[str, Any]]:
        assert (repository, run_id) == (REPOSITORY, 101)
        return [
            {
                "id": 201,
                "name": "agent-pr-receipt",
                "expired": False,
                "created_at": "2026-07-29T00:02:30Z",
                "digest": _digest(self.archive),
                "size_in_bytes": len(self.archive),
                "archive_download_url": "https://api.github.test/artifacts/201/zip",
                "workflow_run": {"id": 101, "head_sha": REVIEWED_HEAD},
            }
        ]

    def download_artifact(self, url: str) -> bytes:
        assert url == "https://api.github.test/artifacts/201/zip"
        return self.archive


@dataclass
class FakeGitRunner:
    paths: tuple[str, ...] = CONTROL_PATHS
    parents: tuple[str, ...] = (BASE_PARENT, REVIEWED_HEAD)
    integration_tree: str = TREE
    reviewed_tree: str = TREE
    ancestor_returncode: int = 0

    def __call__(self, command: list[str], **kwargs: Any) -> subprocess.CompletedProcess[Any]:
        args = command[3:]
        if args == ["cat-file", "-e", f"{REVIEWED_HEAD}^{{commit}}"]:
            output, returncode = "", 0
        elif args == ["rev-parse", "HEAD"]:
            output, returncode = INTEGRATION_COMMIT, 0
        elif args[:3] == ["show", "-s", "--format=%P"]:
            output, returncode = " ".join(self.parents), 0
        elif args[:2] == ["merge-base", "--is-ancestor"]:
            output, returncode = "", self.ancestor_returncode
        elif args == ["rev-parse", f"{INTEGRATION_COMMIT}^{{tree}}"]:
            output, returncode = self.integration_tree, 0
        elif args == ["rev-parse", f"{REVIEWED_HEAD}^{{tree}}"]:
            output, returncode = self.reviewed_tree, 0
        elif args[:4] == ["diff", "--no-renames", "--name-only", "--diff-filter=ACDMRT"]:
            output, returncode = "\0".join(self.paths) + "\0", 0
        else:
            raise AssertionError(command)
        if kwargs.get("text", False):
            stdout: str | bytes = f"{output}\n"
            stderr: str | bytes = ""
        else:
            stdout = output.encode()
            stderr = b""
        return subprocess.CompletedProcess(command, returncode, stdout=stdout, stderr=stderr)


def _derive(
    *,
    event: dict[str, Any] | None = None,
    archive: bytes | None = None,
    paths: tuple[str, ...] = CONTROL_PATHS,
    run_id: str = "900",
    run_attempt: str = "1",
    runner: FakeGitRunner | None = None,
) -> Any:
    return merge_receipt.derive_merge_receipt(
        event=event or _event(),
        root=Path("/injected/repository"),
        repository=REPOSITORY,
        integration_commit_sha=INTEGRATION_COMMIT,
        protected_base_refs=[BASE_REF],
        adapter=FakeSourceAdapter(archive or _source_archive(paths=paths)),
        producer=merge_receipt.Producer(
            workflow="Agent PR receipt",
            run_id=run_id,
            run_attempt=run_attempt,
        ),
        runner=runner or FakeGitRunner(paths=paths),
    )


def test_merge_receipt_cross_binds_event_git_source_and_producer() -> None:
    result = _derive()
    receipt = result.payload

    assert receipt["status"] == "PASS"
    assert receipt["reviewed_head_sha"] == REVIEWED_HEAD
    assert receipt["integration_commit_sha"] == INTEGRATION_COMMIT
    assert receipt["changed_paths"] == list(CONTROL_PATHS)
    assert receipt["pr_body_sha256"] == _digest(BODY.encode())
    assert receipt["source_receipt"]["artifact_id"] == 201
    assert receipt["errors"] == []
    assert result.source_archive == _source_archive()
    _validate_schema(receipt)


def test_binding_is_retry_stable_but_changes_with_immutable_semantics() -> None:
    first = _derive(run_id="900", run_attempt="1").payload
    retry = _derive(run_id="901", run_attempt="2").payload
    changed_body = "Different immutable reviewed body\n"
    changed = _derive(
        event=_event(body=changed_body),
        archive=_source_archive(body=changed_body),
    ).payload

    assert first["binding_id"] == retry["binding_id"]
    assert first["producer"] != retry["producer"]
    assert changed["binding_id"] != first["binding_id"]


def test_post_merge_body_mutation_cannot_replace_archived_body() -> None:
    with pytest.raises(ValueError, match="body"):
        _derive(event=_event(body="Retrospectively edited body\n"))


def test_rename_out_path_omission_cannot_close_the_merge() -> None:
    with pytest.raises(ValueError, match="changed_paths|changed paths"):
        _derive(archive=_source_archive(paths=("docs/required.yml",)))


@pytest.mark.parametrize(
    ("runner", "message"),
    [
        (FakeGitRunner(parents=(BASE_PARENT,), integration_tree="e" * 40), "tree"),
        (FakeGitRunner(parents=("e" * 40,), ancestor_returncode=1), "ancestor"),
    ],
)
def test_squash_rejects_tree_or_first_parent_mismatch(
    runner: FakeGitRunner,
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        _derive(runner=runner)


@pytest.mark.parametrize(
    ("archive_overrides", "message"),
    [
        ({"audit_overrides": {"head_sha": "e" * 40}}, "head_sha"),
        ({"audit_overrides": {"repository": "attacker/fork"}}, "repository"),
        ({"audit_overrides": {"base_ref": "develop"}}, "base_ref"),
        ({"audit_overrides": {"pr_number": 999}}, "pr_number"),
        ({"audit_overrides": {"receipt_errors": ["contradiction"]}}, "receipt_errors"),
        ({"evidence_head": "e" * 40}, "evidence chain"),
        ({"include_github_evidence": False}, "GitHub evidence"),
        ({"governance_artifact_id": 999}, "governance artifact"),
        ({"status": "FAIL"}, "PASS or N/A"),
        ({"status": "N/A", "paths": ("tools/agent_policy/pr_receipt.py",)}, "N/A|control-surface"),
    ],
)
def test_source_cross_binding_mismatch_never_produces_pass(
    archive_overrides: dict[str, Any],
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        _derive(
            archive=_source_archive(**archive_overrides),
            paths=archive_overrides.get("paths", CONTROL_PATHS),
        )


def test_source_na_is_reconfirmed_against_non_control_integration_paths() -> None:
    result = _derive(archive=_source_archive(paths=("README.md",), status="N/A"), paths=("README.md",))

    assert (result.payload["status"], result.payload["source_receipt"]["status"]) == ("PASS", "N/A")
