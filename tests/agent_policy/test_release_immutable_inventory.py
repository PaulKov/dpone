"""Observe-only immutable-release inventory tests."""

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
immutable = _load("dpone_agent_release_immutable_inventory", "tools/agent_policy/release_immutable_inventory.py")
github = _load("dpone_agent_release_github_api", "tools/agent_policy/release_github_api.py")


def _producer() -> dict[str, Any]:
    return {
        "kind": "github_actions_job",
        "repository_id": "1305993853",
        "workflow_id": "316322127",
        "workflow_path": ".github/workflows/release-controller.yml",
        "workflow_sha": "a" * 40,
        "run_id": "111",
        "run_attempt": 1,
        "job_name": "observe-immutable",
        "environment": "release-attest",
    }


def _seed_authorized(mem: Any, release_id: str, authority_id: str) -> None:
    lease.acquire_publication_lease(
        mem,
        release_identity_id=release_id,
        release_authority_id=authority_id,
        repository_id=1255975556,
        tag_ref="refs/tags/v9.7.8",
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
        tag_ref="refs/tags/v9.7.8",
        producer=_producer(),
        receipt_type="authorized",
        payload={
            "kind": "AUTHORIZED",
            "authorization_state": "AUTHORIZED",
            "mode": "BOOTSTRAP",
            "authorization_id": "sha256:" + ("c" * 64),
        },
        now_utc="2026-07-20T00:01:00Z",
    )


def test_observe_prefers_repo_endpoint_disabled() -> None:
    mem = store_mod.InMemoryEvidenceStore()
    release_id = canonical.sha256_id("dpone.release.identity.v2", {"tag": "v9.7.8"})
    authority_id = canonical.sha256_id("dpone.release.authority.v2", {"release_id": release_id})
    _seed_authorized(mem, release_id, authority_id)

    class Api:
        def request(self, method: str, path: str) -> dict[str, Any]:
            assert method == "GET"
            assert path == "/repos/PaulKov/dpone/immutable-releases"
            return {"enabled": False, "enforced_by_owner": False}

    result = immutable.run_immutable_inventory(
        mem,
        Api(),
        owner="PaulKov",
        repo="dpone",
        release_identity_id=release_id,
        release_authority_id=authority_id,
        repository_id=1255975556,
        tag_ref="refs/tags/v9.7.8",
        producer=_producer(),
        now_utc="2026-07-20T00:02:00Z",
    )
    assert result["status"] == "IMMUTABLE_RELEASE_INVENTORY"
    assert result["observed"] == "DISABLED"
    assert result["receipt"]["payload"]["mutated"] is False
    assert result["receipt"]["payload"]["source"] == "repos/PaulKov/dpone/immutable-releases"


def test_observe_records_unverified_when_repo_and_org_404() -> None:
    mem = store_mod.InMemoryEvidenceStore()
    release_id = canonical.sha256_id("dpone.release.identity.v2", {"tag": "v9.7.8-b"})
    authority_id = canonical.sha256_id("dpone.release.authority.v2", {"release_id": release_id})
    _seed_authorized(mem, release_id, authority_id)

    class Api:
        def request(self, method: str, path: str) -> dict[str, Any]:
            raise github.GitHubApiError(status=404, body=f"{method} {path} not found")

    result = immutable.run_immutable_inventory(
        mem,
        Api(),
        owner="PaulKov",
        repo="dpone",
        release_identity_id=release_id,
        release_authority_id=authority_id,
        repository_id=1255975556,
        tag_ref="refs/tags/v9.7.8",
        producer=_producer(),
        now_utc="2026-07-20T00:02:00Z",
    )
    assert result["observed"] == "UNVERIFIED"
    assert result["receipt"]["payload"]["reason"] == "HTTP_404"
    assert result["receipt"]["payload"]["fallback_org_reason"] == "HTTP_404"


def test_observe_falls_back_to_org_when_repo_404() -> None:
    class Api:
        def request(self, method: str, path: str) -> dict[str, Any]:
            del method
            if path.startswith("/repos/"):
                raise github.GitHubApiError(status=404, body="missing")
            return {"enabled_for_new_repos": True}

    observation = immutable.observe_immutable_releases(
        Api(),
        owner="example-org",
        repo="dpone",
    )
    assert observation["observed"] == "ENABLED"
    assert observation["source"] == "orgs/example-org/settings/immutable-releases"


def test_observe_enabled_when_repo_setting_true() -> None:
    observation = immutable.observe_immutable_releases(
        SimpleNamespace(request=lambda method, path: {"enabled": True, "enforced_by_owner": False}),
        owner="PaulKov",
        repo="dpone",
    )
    assert observation["observed"] == "ENABLED"
    assert observation["enforced_by_owner"] is False
