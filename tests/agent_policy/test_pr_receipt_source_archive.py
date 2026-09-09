from __future__ import annotations

import hashlib
import importlib.util
import io
import sys
import zipfile
from pathlib import Path
from types import ModuleType

import pytest

ROOT = Path(__file__).resolve().parents[2]


def _load(name: str, relative: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, ROOT / relative)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


source_archive = _load(
    "dpone_agent_pr_receipt_source_archive_resource_test",
    "tools/agent_policy/pr_receipt_source_archive.py",
)
receipt_source = _load(
    "dpone_agent_pr_receipt_source_resource_test",
    "tools/agent_policy/pr_receipt_source.py",
)
REPOSITORY = "PaulKov/dpone"
HEAD_SHA = "a" * 40


def _archive(members: list[tuple[str, bytes]]) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        for name, content in members:
            archive.writestr(name, content)
    return buffer.getvalue()


def _required_members() -> list[tuple[str, bytes]]:
    return [(name, b"content") for name in sorted(source_archive.REQUIRED_ARCHIVE_FILES)]


def test_source_archive_keeps_small_existing_artifact_shape_compatible() -> None:
    files = source_archive._archive_files(_archive(_required_members() + [("diagnostics/notes.txt", b"ignored")]))

    assert set(files) == source_archive.REQUIRED_ARCHIVE_FILES


def test_source_archive_applies_member_count_before_read(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(source_archive.resource_limits, "MAX_ZIP_MEMBERS", 1)

    with pytest.raises(ValueError, match="member count"):
        source_archive._archive_files(_archive([("a.txt", b"a"), ("b.txt", b"b")]))


class _SourceAdapter:
    def __init__(self, *, artifact_size: int, raw: bytes) -> None:
        self.artifact_size = artifact_size
        self.raw = raw
        self.downloads = 0

    def list_check_runs(self, _repository: str, _head_sha: str) -> list[dict[str, object]]:
        return [
            {
                "id": 11,
                "name": "Agent PR receipt",
                "status": "completed",
                "conclusion": "success",
                "head_sha": HEAD_SHA,
                "app": {"slug": "github-actions"},
                "completed_at": "2026-07-29T00:02:00Z",
                "workflow_run_id": 101,
            }
        ]

    def get_workflow_run(self, _repository: str, _run_id: int) -> dict[str, object]:
        return {
            "id": 101,
            "name": "Agent PR receipt",
            "event": "pull_request",
            "status": "completed",
            "conclusion": "success",
            "head_sha": HEAD_SHA,
            "repository": {"full_name": REPOSITORY},
            "path": ".github/workflows/agent-pr-receipt.yml",
            "updated_at": "2026-07-29T00:02:00Z",
            "run_attempt": 1,
        }

    def list_run_artifacts(self, _repository: str, _run_id: int) -> list[dict[str, object]]:
        return [
            {
                "id": 201,
                "name": "agent-pr-receipt",
                "expired": False,
                "created_at": "2026-07-29T00:02:00Z",
                "digest": f"sha256:{hashlib.sha256(self.raw).hexdigest()}",
                "size_in_bytes": self.artifact_size,
                "archive_download_url": "https://api.github.test/artifacts/201/zip",
                "workflow_run": {"id": 101, "head_sha": HEAD_SHA},
            }
        ]

    def download_artifact(self, _url: str) -> bytes:
        self.downloads += 1
        return self.raw


def test_source_selection_rejects_oversized_provider_metadata_before_download() -> None:
    adapter = _SourceAdapter(
        artifact_size=receipt_source.resource_limits.MAX_COMPRESSED_ARTIFACT_BYTES + 1,
        raw=b"artifact",
    )

    with pytest.raises(ValueError, match="provider size.*exceeds"):
        receipt_source.select_source_receipt(
            repository=REPOSITORY,
            reviewed_head_sha=HEAD_SHA,
            merged_at="2026-07-29T00:03:00Z",
            adapter=adapter,
        )

    assert adapter.downloads == 0


def test_source_download_limit_failure_is_terminal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(receipt_source.resource_limits, "MAX_COMPRESSED_ARTIFACT_BYTES", 5)
    adapter = _SourceAdapter(artifact_size=5, raw=b"123456")

    with pytest.raises(ValueError, match="download exceeds"):
        receipt_source._download_verified_archive(
            adapter,
            url="https://api.github.test/artifacts/201/zip",
            expected_digest=f"sha256:{'0' * 64}",
            expected_size=5,
        )

    assert adapter.downloads == 1


def test_source_integrity_mismatch_cannot_be_retried_into_success() -> None:
    good = b"expected"
    adapter = _SourceAdapter(artifact_size=len(good), raw=b"tampered")
    responses = iter((b"tampered", good))

    def changing_download(_url: str) -> bytes:
        adapter.downloads += 1
        return next(responses)

    adapter.download_artifact = changing_download  # type: ignore[method-assign]

    with pytest.raises(ValueError, match="digest mismatch"):
        receipt_source._download_verified_archive(
            adapter,
            url="https://api.github.test/artifacts/201/zip",
            expected_digest=f"sha256:{hashlib.sha256(good).hexdigest()}",
            expected_size=len(good),
        )

    assert adapter.downloads == 1
