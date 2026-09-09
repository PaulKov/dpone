from __future__ import annotations

import importlib.util
import io
import sys
import zipfile
from pathlib import Path
from types import ModuleType
from typing import Any

ROOT = Path(__file__).resolve().parents[2]


def _load(name: str, relative: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, ROOT / relative)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


github_receipt = _load(
    "dpone_agent_pr_receipt_github_resource_limit_test",
    "tools/agent_policy/pr_receipt_github.py",
)
artifact_resource_limits = _load(
    "dpone_agent_artifact_resource_limits",
    "tools/agent_policy/artifact_resource_limits.py",
)


def _artifact(*, size_in_bytes: int, url: str = "https://example.test/archive.zip") -> dict[str, Any]:
    return {
        "id": 9,
        "name": "agent-governance-gate",
        "archive_download_url": url,
        "size_in_bytes": size_in_bytes,
        "expired": False,
        "workflow_run": {"id": 11, "head_sha": "abc123"},
    }


def _fetch_artifacts(monkeypatch: Any, artifact: dict[str, Any]) -> list[Any]:
    monkeypatch.setattr(
        github_receipt,
        "_github_paginated_items",
        lambda *_args, **_kwargs: [artifact],
    )
    return github_receipt._fetch_artifacts(
        repo="PaulKov/dpone",
        head_sha="abc123",
        token="secret",
        artifact_name="agent-governance-gate",
        require_attestation=False,
        signer_workflow="PaulKov/dpone/.github/workflows/agent-governance-gate.yml",
    )


def test_oversized_provider_metadata_is_rejected_before_download(monkeypatch: Any) -> None:
    downloads = 0

    def unexpected_download(*_args: Any, **_kwargs: Any) -> bytes:
        nonlocal downloads
        downloads += 1
        return b""

    monkeypatch.setattr(github_receipt, "_github_bytes", unexpected_download)

    artifacts = _fetch_artifacts(
        monkeypatch,
        _artifact(size_in_bytes=artifact_resource_limits.MAX_COMPRESSED_ARTIFACT_BYTES + 1),
    )

    assert downloads == 0
    assert len(artifacts) == 1
    assert artifacts[0].archive_size_bytes is None
    assert any("provider size" in error for error in artifacts[0].content.errors)


def test_compressed_governance_subject_is_rejected_end_to_end(monkeypatch: Any) -> None:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(
            "agent_governance_gate.json",
            b"x" * (artifact_resource_limits.MAX_ZIP_MEMBER_BYTES + 1),
        )
    archive_bytes = buffer.getvalue()
    monkeypatch.setattr(github_receipt, "_github_bytes", lambda *_args, **_kwargs: archive_bytes)

    artifacts = _fetch_artifacts(monkeypatch, _artifact(size_in_bytes=len(archive_bytes)))

    assert len(artifacts) == 1
    assert artifacts[0].archive_size_bytes == len(archive_bytes)
    assert any("member size limit" in error for error in artifacts[0].content.errors)
