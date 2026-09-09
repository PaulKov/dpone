"""Observe-only draft inventory classification tests."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace
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
stream = _load("dpone_agent_release_stream_service", "tools/agent_policy/release_stream_service.py")
draft_inv = _load("dpone_agent_release_draft_inventory", "tools/agent_policy/release_draft_inventory.py")


def _producer() -> dict[str, Any]:
    return {
        "kind": "github_actions_job",
        "repository_id": "1305993853",
        "workflow_id": "316322127",
        "workflow_path": ".github/workflows/release-controller.yml",
        "workflow_sha": "a" * 40,
        "run_id": "130",
        "run_attempt": 1,
        "job_name": "observe-draft",
        "environment": "release-attest",
    }


def _seed(mem: Any, release_id: str, authority_id: str) -> None:
    lease.acquire_publication_lease(
        mem,
        release_identity_id=release_id,
        release_authority_id=authority_id,
        repository_id=1255975556,
        tag_ref="refs/tags/v9.9.0",
        attempt_seed={"run_id": 1, "run_attempt": 1},
        producer=_producer(),
        ttl_seconds=300,
        now_utc="2026-07-20T00:00:00Z",
    )
    stream.append_stream_receipt(
        mem,
        release_identity_id=release_id,
        release_authority_id=authority_id,
        repository_id=1255975556,
        tag_ref="refs/tags/v9.9.0",
        producer=_producer(),
        receipt_type="draft_transition",
        payload={
            "kind": "DRAFT_TRANSITION",
            "mode": "LIVE",
            "draft_release_id": "356540001",
            "asset_id": "99",
        },
        now_utc="2026-07-20T00:01:00Z",
    )
    stream.append_stream_receipt(
        mem,
        release_identity_id=release_id,
        release_authority_id=authority_id,
        repository_id=1255975556,
        tag_ref="refs/tags/v9.9.0",
        producer=_producer(),
        receipt_type="authorized",
        payload={
            "kind": "AUTHORIZED",
            "authorization_state": "AUTHORIZED",
            "mode": "BOOTSTRAP",
            "authorization_id": "sha256:" + ("e" * 64),
        },
        now_utc="2026-07-20T00:02:00Z",
    )


def test_classify_still_draft_published_and_missing() -> None:
    still = draft_inv.classify_draft_observation(
        expected_draft_release_id="1",
        expected_asset_id="9",
        release={"id": 1, "draft": True, "prerelease": True, "assets": [{"id": 9}]},
    )
    published = draft_inv.classify_draft_observation(
        expected_draft_release_id="1",
        expected_asset_id="9",
        release={"id": 1, "draft": False, "prerelease": False, "assets": [{"id": 9}]},
    )
    missing = draft_inv.classify_draft_observation(
        expected_draft_release_id="1",
        expected_asset_id="9",
        release=None,
        api_status=404,
    )
    assert still["classification"] == "STILL_DRAFT"
    assert published["classification"] == "ALREADY_PUBLISHED"
    assert missing["classification"] == "DRAFT_MISSING"


def test_observe_appends_still_draft_without_publish() -> None:
    mem = store_mod.InMemoryEvidenceStore()
    release_id = canonical.sha256_id("dpone.release.identity.v2", {"tag": "v9.9.0"})
    authority_id = canonical.sha256_id("dpone.release.authority.v2", {"release_id": release_id})
    _seed(mem, release_id, authority_id)

    api = SimpleNamespace(
        request=lambda method, path, **kwargs: {
            "id": 356540001,
            "draft": True,
            "prerelease": True,
            "tag_name": "v9.9.0",
            "html_url": "https://example.test/draft",
            "assets": [{"id": 99}],
            "immutable": False,
        }
    )
    # Bypass get_release helper path by patching module attribute used by run.
    draft_inv.github = SimpleNamespace(
        get_release=lambda api_obj, owner, repo, release_id: {
            "id": int(release_id),
            "draft": True,
            "prerelease": True,
            "tag_name": "v9.9.0",
            "html_url": "https://example.test/draft",
            "assets": [{"id": 99}],
            "immutable": False,
        }
    )

    result = draft_inv.run_draft_inventory(
        mem,
        api,
        owner="PaulKov",
        repo="dpone",
        release_identity_id=release_id,
        release_authority_id=authority_id,
        repository_id=1255975556,
        tag_ref="refs/tags/v9.9.0",
        producer=_producer(),
        now_utc="2026-07-20T00:03:00Z",
    )
    assert result["status"] == "DRAFT_INVENTORY"
    assert result["classification"] == "STILL_DRAFT"
    assert result["publish_attempted"] is False
    assert result["receipt"]["payload"]["publish_attempted"] is False
