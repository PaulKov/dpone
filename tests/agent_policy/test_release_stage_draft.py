"""Live draft staging under an active lease (mocked GitHub API)."""

from __future__ import annotations

import hashlib
import importlib.util
import json
import sys
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


canonical = _load("dpone_agent_release_canonical", "tools/agent_policy/release_canonical.py")
store_mod = _load("dpone_agent_release_evidence_store_b2", "tools/agent_policy/release_evidence_store_b2.py")
lease = _load("dpone_agent_release_lease_service", "tools/agent_policy/release_lease_service.py")
github = _load("dpone_agent_release_github_api", "tools/agent_policy/release_github_api.py")
stage = _load("dpone_agent_release_stage_draft", "tools/agent_policy/release_stage_draft.py")


def _producer() -> dict[str, Any]:
    return {
        "kind": "github_actions_job",
        "repository_id": "1305993853",
        "workflow_id": "316322127",
        "workflow_path": ".github/workflows/release-controller.yml",
        "workflow_sha": "a" * 40,
        "run_id": "100",
        "run_attempt": 1,
        "job_name": "attest-and-draft",
        "environment": "release-attest",
    }


class _FakeHttp:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

    def __call__(
        self,
        method: str,
        url: str,
        headers: dict[str, str] | None,
        body: bytes | None,
    ) -> tuple[int, dict[str, Any] | list[Any] | None, str]:
        self.calls.append((method, url))
        if method == "GET" and url.endswith("/repos/PaulKov/dpone"):
            return 200, {"default_branch": "master"}, "{}"
        if method == "GET" and url.endswith("/git/ref/heads/master"):
            return 200, {"object": {"sha": "b" * 40}}, "{}"
        if method == "GET" and "/git/ref/tags/" in url:
            return 404, None, "Not Found"
        if method == "POST" and url.endswith("/git/refs"):
            return 201, {"ref": "refs/tags/v9.9.9", "object": {"sha": "b" * 40}}, "{}"
        if method == "GET" and url.endswith("/releases?per_page=100"):
            return 200, [], "[]"
        if method == "POST" and url.endswith("/releases"):
            payload = {
                "id": 4242,
                "html_url": "https://github.com/PaulKov/dpone/releases/tag/untagged-bootstrap",
                "upload_url": "https://uploads.github.com/repos/PaulKov/dpone/releases/4242/assets{?name,label}",
                "tag_name": "v9.9.9",
                "draft": True,
            }
            return 201, payload, json.dumps(payload)
        if method == "POST" and url.startswith("https://uploads.github.com/"):
            return 201, {"id": 7, "name": "bootstrap.bin"}, "{}"
        raise AssertionError(f"unexpected call {method} {url}")


def test_stage_draft_live_appends_live_receipts_and_creates_draft() -> None:
    mem = store_mod.InMemoryEvidenceStore()
    release_id = canonical.sha256_id("dpone.release.identity.v2", {"tag": "v9.9.9"})
    authority_id = canonical.sha256_id("dpone.release.authority.v2", {"release_id": release_id})
    lease.acquire_publication_lease(
        mem,
        release_identity_id=release_id,
        release_authority_id=authority_id,
        repository_id=1255975556,
        tag_ref="refs/tags/v9.9.9",
        attempt_seed={"run_id": 1, "run_attempt": 1},
        producer=_producer(),
        ttl_seconds=300,
        now_utc="2026-07-19T00:00:00Z",
    )
    subject = b"bootstrap-subject-bytes"
    fake = _FakeHttp()
    api = github.GitHubApi(token="test-token", http=fake)
    result = stage.run_stage_draft_live(
        mem,
        api,
        owner="PaulKov",
        repo="dpone",
        release_identity_id=release_id,
        release_authority_id=authority_id,
        repository_id=1255975556,
        tag_ref="refs/tags/v9.9.9",
        producer=_producer(),
        now_utc="2026-07-19T00:01:00Z",
        subject_filename="dpone-v9.9.9-bootstrap.bin",
        subject_bytes=subject,
        attestation={"digest": "sha256:" + ("e" * 64), "predicate_type": "https://slsa.dev/provenance/v1"},
    )
    assert result["status"] == "DRAFT_STAGED"
    assert result["draft"]["draft_release_id"] == "4242"
    kinds = [item["payload"]["kind"] for item in result["receipts"]]
    assert kinds == [
        "MUTATION_INTENT",
        "ATTESTATION_VERIFIED",
        "PUBLIC_BUNDLE_VERIFIED",
        "DRAFT_TRANSITION",
    ]
    assert all(item["payload"]["mode"] == "LIVE" for item in result["receipts"])
    assert result["receipts"][1]["payload"]["subjects"][0]["sha256"] == hashlib.sha256(subject).hexdigest()
    assert any(url.endswith("/releases") for method, url in fake.calls if method == "POST")


def test_stage_draft_requires_active_lease() -> None:
    mem = store_mod.InMemoryEvidenceStore()
    release_id = canonical.sha256_id("dpone.release.identity.v2", {"tag": "v9.9.8"})
    authority_id = canonical.sha256_id("dpone.release.authority.v2", {"release_id": release_id})
    api = github.GitHubApi(token="test-token", http=_FakeHttp())
    try:
        stage.run_stage_draft_live(
            mem,
            api,
            owner="PaulKov",
            repo="dpone",
            release_identity_id=release_id,
            release_authority_id=authority_id,
            repository_id=1255975556,
            tag_ref="refs/tags/v9.9.8",
            producer=_producer(),
            now_utc="2026-07-19T00:01:00Z",
            subject_filename="x.bin",
            subject_bytes=b"x",
            attestation={"digest": "sha256:" + ("f" * 64)},
        )
    except stage.StreamPrerequisiteError as exc:
        assert "ACTIVE_LEASE_REQUIRED" in str(exc)
    else:
        raise AssertionError("expected StreamPrerequisiteError")
