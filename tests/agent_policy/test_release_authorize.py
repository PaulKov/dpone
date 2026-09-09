"""Bootstrap Snapshot B + AUTHORIZED receipt (mocked GitHub, no PyPI)."""

from __future__ import annotations

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
snapshots = _load("dpone_agent_release_governance_snapshot", "tools/agent_policy/release_governance_snapshot.py")
stage = _load("dpone_agent_release_stage_draft", "tools/agent_policy/release_stage_draft.py")
authorize = _load("dpone_agent_release_authorize", "tools/agent_policy/release_authorize.py")


def _producer() -> dict[str, Any]:
    return {
        "kind": "github_actions_job",
        "repository_id": "1305993853",
        "workflow_id": "316322127",
        "workflow_path": ".github/workflows/release-controller.yml",
        "workflow_sha": "a" * 40,
        "run_id": "101",
        "run_attempt": 1,
        "job_name": "authorize-publication",
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
            return 200, {"object": {"sha": "c" * 40}}, "{}"
        if method == "GET" and "/git/ref/tags/" in url:
            return 404, None, "Not Found"
        if method == "POST" and url.endswith("/git/refs"):
            return 201, {"ref": "refs/tags/v9.8.8", "object": {"sha": "c" * 40}}, "{}"
        if method == "GET" and url.endswith("/releases?per_page=100"):
            return 200, [], "[]"
        if method == "POST" and url.endswith("/releases"):
            payload = {
                "id": 555,
                "html_url": "https://github.com/PaulKov/dpone/releases/tag/untagged-x",
                "upload_url": "https://uploads.github.com/repos/PaulKov/dpone/releases/555/assets{?name,label}",
                "tag_name": "v9.8.8",
                "draft": True,
                "assets": [],
            }
            return 201, payload, json.dumps(payload)
        if method == "POST" and url.startswith("https://uploads.github.com/"):
            return 201, {"id": 9, "name": "bootstrap.bin"}, "{}"
        if method == "GET" and url.endswith("/releases/555"):
            payload = {
                "id": 555,
                "draft": True,
                "tag_name": "v9.8.8",
                "assets": [{"id": 9, "name": "dpone-v9.8.8-bootstrap.bin", "state": "uploaded"}],
            }
            return 200, payload, json.dumps(payload)
        raise AssertionError(f"unexpected call {method} {url}")


def _seed_live_draft(
    mem: Any,
    release_id: str,
    authority_id: str,
    api: Any,
    *,
    tag: str = "v9.8.8",
    include_snapshot_a: bool = True,
) -> None:
    tag_ref = f"refs/tags/{tag}"
    lease.acquire_publication_lease(
        mem,
        release_identity_id=release_id,
        release_authority_id=authority_id,
        repository_id=1255975556,
        tag_ref=tag_ref,
        attempt_seed={"run_id": 1, "run_attempt": 1},
        producer=_producer(),
        ttl_seconds=300,
        now_utc="2026-07-20T00:00:00Z",
    )
    if include_snapshot_a:
        snapshots.append_governance_snapshot(
            mem,
            api,
            label="A",
            owner="PaulKov",
            repo="dpone",
            release_identity_id=release_id,
            release_authority_id=authority_id,
            repository_id=1255975556,
            tag_ref=tag_ref,
            producer=_producer(),
            now_utc="2026-07-20T00:00:30Z",
            gap_seconds=0,
            sleeper=lambda _seconds: None,
        )
    stage.run_stage_draft_live(
        mem,
        api,
        owner="PaulKov",
        repo="dpone",
        release_identity_id=release_id,
        release_authority_id=authority_id,
        repository_id=1255975556,
        tag_ref=tag_ref,
        producer=_producer(),
        now_utc="2026-07-20T00:01:00Z",
        subject_filename=f"dpone-{tag}-bootstrap.bin",
        subject_bytes=b"bootstrap-subject",
        attestation={"digest": "sha256:" + ("a" * 64)},
    )


def test_authorize_appends_snapshot_b_and_authorized_without_pass_go() -> None:
    mem = store_mod.InMemoryEvidenceStore()
    release_id = canonical.sha256_id("dpone.release.identity.v2", {"tag": "v9.8.8"})
    authority_id = canonical.sha256_id("dpone.release.authority.v2", {"release_id": release_id})
    fake = _FakeHttp()
    api = github.GitHubApi(token="test-token", http=fake)
    _seed_live_draft(mem, release_id, authority_id, api)
    result = authorize.run_authorize_publication(
        mem,
        api,
        owner="PaulKov",
        repo="dpone",
        release_identity_id=release_id,
        release_authority_id=authority_id,
        repository_id=1255975556,
        tag_ref="refs/tags/v9.8.8",
        producer=_producer(),
        now_utc="2026-07-20T00:02:00Z",
        snapshot_gap_seconds=0,
        sleeper=lambda _seconds: None,
    )
    assert result["status"] == "AUTHORIZED"
    assert result["mode"] == "BOOTSTRAP"
    auth_payload = result["receipts"][-1]["payload"]
    assert "status" not in auth_payload
    assert "decision" not in auth_payload
    kinds = [item["payload"]["kind"] for item in result["receipts"]]
    assert kinds == ["GOVERNANCE_SNAPSHOT", "AUTHORIZED"]
    assert auth_payload["authorization_state"] == "AUTHORIZED"
    assert result["draft_release_id"] == "555"
    assert "snapshot_a_sha256" in auth_payload


def test_authorize_requires_snapshot_a() -> None:
    mem = store_mod.InMemoryEvidenceStore()
    release_id = canonical.sha256_id("dpone.release.identity.v2", {"tag": "v9.8.6"})
    authority_id = canonical.sha256_id("dpone.release.authority.v2", {"release_id": release_id})
    api = github.GitHubApi(token="test-token", http=_FakeHttp())
    _seed_live_draft(mem, release_id, authority_id, api, tag="v9.8.6", include_snapshot_a=False)
    try:
        authorize.run_authorize_publication(
            mem,
            api,
            owner="PaulKov",
            repo="dpone",
            release_identity_id=release_id,
            release_authority_id=authority_id,
            repository_id=1255975556,
            tag_ref="refs/tags/v9.8.6",
            producer=_producer(),
            now_utc="2026-07-20T00:02:00Z",
            snapshot_gap_seconds=0,
            sleeper=lambda _seconds: None,
        )
    except authorize.AuthorizationError as exc:
        assert "SNAPSHOT_A_REQUIRED" in str(exc)
    else:
        raise AssertionError("expected AuthorizationError")


def test_authorize_rejects_non_draft_release() -> None:
    mem = store_mod.InMemoryEvidenceStore()
    release_id = canonical.sha256_id("dpone.release.identity.v2", {"tag": "v9.8.7"})
    authority_id = canonical.sha256_id("dpone.release.authority.v2", {"release_id": release_id})

    class PublishedHttp(_FakeHttp):
        def __call__(self, method: str, url: str, headers: dict[str, str] | None, body: bytes | None):
            if method == "GET" and url.endswith("/releases/555"):
                payload = {"id": 555, "draft": False, "assets": [{"id": 9}]}
                return 200, payload, json.dumps(payload)
            return super().__call__(method, url, headers, body)

    api = github.GitHubApi(token="test-token", http=PublishedHttp())
    _seed_live_draft(mem, release_id, authority_id, api, tag="v9.8.7")
    try:
        authorize.run_authorize_publication(
            mem,
            api,
            owner="PaulKov",
            repo="dpone",
            release_identity_id=release_id,
            release_authority_id=authority_id,
            repository_id=1255975556,
            tag_ref="refs/tags/v9.8.7",
            producer=_producer(),
            now_utc="2026-07-20T00:02:00Z",
            snapshot_gap_seconds=0,
            sleeper=lambda _seconds: None,
        )
    except authorize.AuthorizationError as exc:
        assert "DRAFT_NOT_DRAFT" in str(exc)
    else:
        raise AssertionError("expected AuthorizationError")


def test_release_lease_clears_active_lease() -> None:
    mem = store_mod.InMemoryEvidenceStore()
    release_id = canonical.sha256_id("dpone.release.identity.v2", {"tag": "v9.8.5"})
    authority_id = canonical.sha256_id("dpone.release.authority.v2", {"release_id": release_id})
    lease.acquire_publication_lease(
        mem,
        release_identity_id=release_id,
        release_authority_id=authority_id,
        repository_id=1255975556,
        tag_ref="refs/tags/v9.8.5",
        attempt_seed={"run_id": 2, "run_attempt": 1},
        producer=_producer(),
        ttl_seconds=300,
        now_utc="2026-07-20T00:00:00Z",
    )
    released = lease.release_publication_lease(
        mem,
        release_identity_id=release_id,
        release_authority_id=authority_id,
        repository_id=1255975556,
        tag_ref="refs/tags/v9.8.5",
        producer=_producer(),
        now_utc="2026-07-20T00:03:00Z",
        reason="BOOTSTRAP_COMPLETE",
    )
    assert released["payload"]["kind"] == "LEASE_RELEASED"
    assert lease.active_lease(mem.list_receipts(release_id), now=lease.parse_utc("2026-07-20T00:03:01Z")) is None
