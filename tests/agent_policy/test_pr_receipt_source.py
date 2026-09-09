from __future__ import annotations

import hashlib
import importlib.util
import io
import json
import stat
import sys
import warnings
import zipfile
from dataclasses import dataclass, field
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[2]
REPOSITORY = "PaulKov/dpone"
HEAD_SHA = "a" * 40
MERGED_AT = "2026-07-29T00:03:00Z"
WORKFLOW = "Agent PR receipt"
RAW_ARCHIVE = b"downloaded source archive"


def _load(name: str, relative: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, ROOT / relative)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


receipt_source = _load(
    "dpone_agent_pr_receipt_source_test",
    "tools/agent_policy/pr_receipt_source.py",
)


def _digest(content: bytes) -> str:
    return "sha256:" + hashlib.sha256(content).hexdigest()


def _check_run(
    check_id: int,
    workflow_run_id: int,
    completed_at: str,
    *,
    conclusion: str = "success",
) -> dict[str, Any]:
    return {
        "id": check_id,
        "name": WORKFLOW,
        "status": "completed",
        "conclusion": conclusion,
        "head_sha": HEAD_SHA,
        "app": {"slug": "github-actions"},
        "completed_at": completed_at,
        "details_url": f"https://github.com/{REPOSITORY}/actions/runs/{workflow_run_id}/job/1",
    }


def _workflow_run(
    run_id: int,
    *,
    updated_at: str = "2026-07-29T00:02:30Z",
) -> dict[str, Any]:
    return {
        "id": run_id,
        "name": WORKFLOW,
        "event": "pull_request",
        "status": "completed",
        "conclusion": "success",
        "head_sha": HEAD_SHA,
        "repository": {"full_name": REPOSITORY},
        "path": ".github/workflows/agent-pr-receipt.yml@refs/pull/455/merge",
        "updated_at": updated_at,
        "run_attempt": 1,
    }


def _artifact(
    artifact_id: int,
    *,
    raw: bytes = RAW_ARCHIVE,
    created_at: str = "2026-07-29T00:02:30Z",
    expired: bool = False,
    digest: Any = "valid",
    size_in_bytes: Any = "valid",
    workflow_run: Any = "valid",
) -> dict[str, Any]:
    return {
        "id": artifact_id,
        "name": "agent-pr-receipt",
        "expired": expired,
        "created_at": created_at,
        "digest": _digest(raw) if digest == "valid" else digest,
        "size_in_bytes": len(raw) if size_in_bytes == "valid" else size_in_bytes,
        "archive_download_url": f"https://api.github.test/artifacts/{artifact_id}/zip",
        "workflow_run": ({"id": 101, "head_sha": HEAD_SHA} if workflow_run == "valid" else workflow_run),
    }


@dataclass
class FakeGitHubSourceAdapter:
    check_runs: list[dict[str, Any]]
    runs: dict[int, dict[str, Any]]
    artifacts: dict[int, list[dict[str, Any]]]
    downloads: dict[str, bytes] = field(default_factory=dict)
    requested_runs: list[int] = field(default_factory=list)

    def list_check_runs(self, repository: str, head_sha: str) -> list[dict[str, Any]]:
        assert (repository, head_sha) == (REPOSITORY, HEAD_SHA)
        return self.check_runs

    def get_workflow_run(self, repository: str, run_id: int) -> dict[str, Any]:
        assert repository == REPOSITORY
        return self.runs[run_id]

    def list_run_artifacts(self, repository: str, run_id: int) -> list[dict[str, Any]]:
        assert repository == REPOSITORY
        self.requested_runs.append(run_id)
        return self.artifacts.get(run_id, [])

    def download_artifact(self, url: str) -> bytes:
        return self.downloads.get(url, RAW_ARCHIVE)


def _select(adapter: FakeGitHubSourceAdapter) -> Any:
    return receipt_source.select_source_receipt(
        repository=REPOSITORY,
        reviewed_head_sha=HEAD_SHA,
        merged_at=MERGED_AT,
        adapter=adapter,
    )


def _source_receipt(
    *,
    status: str = "N/A",
    paths: list[str] | None = None,
) -> dict[str, Any]:
    return {
        "schema_version": 2,
        "status": status,
        "control_surface_changed": False,
        "changed_paths": paths or ["README.md"],
        "errors": [],
        "warnings": [],
        "traceability": None,
        "github_evidence": {
            "head_sha": HEAD_SHA,
            "required_checks": [],
            "check_runs": [],
            "statuses": [],
            "artifacts": [],
            "errors": [],
        },
        "evidence_chain": None,
    }


def _audit_manifest(*, status: str = "N/A") -> dict[str, Any]:
    return {
        "schema_version": 1,
        "kind": "agent_pr_receipt",
        "repository": REPOSITORY,
        "workflow": WORKFLOW,
        "run_id": "101",
        "run_attempt": "1",
        "artifact_name": "agent-pr-receipt",
        "artifact_retention_days": 90,
        "generated_at": "2026-07-29T00:02:00Z",
        "pr_number": 455,
        "base_ref": "master",
        "base_sha": "b" * 40,
        "head_ref": "feature",
        "head_sha": HEAD_SHA,
        "merge_sha": None,
        "receipt_status": status,
        "receipt_errors": [],
        "receipt_warnings": [],
        "traceability_source": None,
        "traceability_source_kind": None,
        "traceability_statuses": [],
        "traceability_non_pass_reasons": [],
        "evidence_chain_head_sha": None,
        "evidence_chain_required_checks": [],
        "evidence_chain_governance_artifact": None,
        "required_checks": [],
        "evidence_artifacts": [],
    }


def _required_members() -> list[tuple[str, bytes]]:
    return [
        ("agent_pr_receipt.json", json.dumps(_source_receipt()).encode()),
        ("agent_audit_manifest.json", json.dumps(_audit_manifest()).encode()),
        ("pr-body.md", b"Immutable reviewed body\n"),
        ("pr-head-sha.txt", f"{HEAD_SHA}\n".encode()),
        ("pr-changed-paths.txt", b"README.md\0"),
        ("pr-receipt-exit-code.txt", b"0\n"),
    ]


def _archive(members: list[tuple[str | zipfile.ZipInfo, bytes]] | None = None) -> bytes:
    buffer = io.BytesIO()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        with zipfile.ZipFile(buffer, "w") as archive:
            for name, content in members or _required_members():
                archive.writestr(name, content)
    return buffer.getvalue()


def _source_artifact(raw: bytes) -> Any:
    return receipt_source.SourceArtifact(
        check_run_id=11,
        workflow_run_id=101,
        workflow_run_attempt=1,
        artifact_id=201,
        artifact_digest=_digest(raw),
        artifact_size_bytes=len(raw),
        archive_sha256=_digest(raw),
        archive_size_bytes=len(raw),
        created_at="2026-07-29T00:02:30Z",
        completed_at="2026-07-29T00:02:00Z",
        archive_bytes=raw,
    )


def _validate_archive(raw: bytes) -> Any:
    return receipt_source.validate_source_archive(
        _source_artifact(raw),
        repository=REPOSITORY,
        reviewed_head_sha=HEAD_SHA,
        pr_number=455,
        base_ref="master",
        changed_paths=["README.md"],
        body="Immutable reviewed body\n",
    )


def test_selects_latest_eligible_run_before_fetching_its_exact_artifact() -> None:
    adapter = FakeGitHubSourceAdapter(
        check_runs=[
            _check_run(10, 100, "2026-07-29T00:01:00Z"),
            _check_run(11, 101, "2026-07-29T00:02:00Z"),
            _check_run(12, 102, "2026-07-29T00:04:00Z"),
            _check_run(13, 103, "2026-07-29T00:02:30Z", conclusion="failure"),
        ],
        runs={run_id: _workflow_run(run_id) for run_id in range(100, 104)},
        artifacts={101: [_artifact(201)]},
    )

    selection = _select(adapter)

    assert (selection.check_run_id, selection.workflow_run_id, selection.artifact_id) == (11, 101, 201)
    assert adapter.requested_runs == [101]


def test_equal_completion_times_have_a_stable_provider_id_tiebreaker() -> None:
    adapter = FakeGitHubSourceAdapter(
        check_runs=[
            _check_run(11, 101, "2026-07-29T00:02:00Z"),
            _check_run(10, 100, "2026-07-29T00:02:00Z"),
        ],
        runs={100: _workflow_run(100), 101: _workflow_run(101)},
        artifacts={101: [_artifact(201)]},
    )

    assert _select(adapter).workflow_run_id == 101


def test_post_merge_success_is_not_eligible_source_evidence() -> None:
    adapter = FakeGitHubSourceAdapter(
        check_runs=[_check_run(12, 102, "2026-07-29T00:04:00Z")],
        runs={102: _workflow_run(102, updated_at="2026-07-29T00:04:00Z")},
        artifacts={102: [_artifact(202)]},
    )

    with pytest.raises(ValueError, match="pre-merge"):
        _select(adapter)
    assert adapter.requested_runs == []


@pytest.mark.parametrize(
    "artifacts",
    [
        [],
        [_artifact(201), _artifact(202)],
        [_artifact(201, expired=True)],
        [_artifact(201, created_at="2026-07-29T00:04:00Z")],
        [{**_artifact(201), "name": "different-artifact"}],
        [{**_artifact(201), "id": 0}],
    ],
)
def test_source_artifact_must_be_unique_unexpired_and_created_before_merge(
    artifacts: list[dict[str, Any]],
) -> None:
    adapter = FakeGitHubSourceAdapter(
        check_runs=[_check_run(11, 101, "2026-07-29T00:02:00Z")],
        runs={101: _workflow_run(101)},
        artifacts={101: artifacts},
    )

    with pytest.raises(ValueError, match="artifact|expired|created"):
        _select(adapter)


@pytest.mark.parametrize(
    ("artifact", "message"),
    [
        (_artifact(201, digest="sha256:" + "f" * 64), "digest"),
        (_artifact(201, size_in_bytes=999_999), "size"),
        (_artifact(201, digest=None), "digest"),
        (_artifact(201, size_in_bytes=None), "size"),
    ],
)
def test_downloaded_archive_must_match_provider_digest_and_size(
    artifact: dict[str, Any],
    message: str,
) -> None:
    adapter = FakeGitHubSourceAdapter(
        check_runs=[_check_run(11, 101, "2026-07-29T00:02:00Z")],
        runs={101: _workflow_run(101)},
        artifacts={101: [artifact]},
    )

    with pytest.raises(ValueError, match=message):
        _select(adapter)


def test_archive_validation_records_body_and_inner_json_digests() -> None:
    raw = _archive()

    validated = _validate_archive(raw)

    members = dict(_required_members())
    assert validated.body_sha256 == _digest(b"Immutable reviewed body\n")
    assert validated.receipt_sha256 == _digest(members["agent_pr_receipt.json"])
    assert validated.audit_manifest_sha256 == _digest(members["agent_audit_manifest.json"])
    assert validated.artifact.archive_bytes == raw


@pytest.mark.parametrize(
    ("members", "message"),
    [
        (_required_members() + [("pr-body.md", b"retrospective\n")], "duplicate"),
        (
            [(name, content) for name, content in _required_members() if name != "pr-head-sha.txt"],
            "missing",
        ),
        (_required_members() + [("../current-pr-body.md", b"mutable\n")], "unsafe"),
        (_required_members() + [("current-pr-body.md", b"mutable\n")], "unexpected"),
    ],
)
def test_archive_rejects_ambiguous_or_unsafe_authority_files(
    members: list[tuple[str | zipfile.ZipInfo, bytes]],
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        _validate_archive(_archive(members))


def test_archive_rejects_symlink_members() -> None:
    symlink = zipfile.ZipInfo("pr-body.md")
    symlink.create_system = 3
    symlink.external_attr = (stat.S_IFLNK | 0o777) << 16
    members = [(name, content) for name, content in _required_members() if name != "pr-body.md"]

    with pytest.raises(ValueError, match="unsafe|symlink"):
        _validate_archive(_archive(members + [(symlink, b"pr-body.md")]))


@pytest.mark.parametrize(
    ("filename", "content", "message"),
    [
        ("agent_pr_receipt.json", b"\xff", "UTF-8 JSON"),
        ("agent_audit_manifest.json", b"[]", "JSON object"),
    ],
)
def test_archive_rejects_invalid_json_authority_files(
    filename: str,
    content: bytes,
    message: str,
) -> None:
    members = [(name, content if name == filename else original) for name, original in _required_members()]

    with pytest.raises(ValueError, match=message):
        _validate_archive(_archive(members))
