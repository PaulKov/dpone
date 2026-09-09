"""Observe-only Trusted Publisher / Integrity claim inventory tests."""

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
stream = _load("dpone_agent_release_stream_service", "tools/agent_policy/release_stream_service.py")
tp = _load(
    "dpone_agent_release_trusted_publisher_inventory",
    "tools/agent_policy/release_trusted_publisher_inventory.py",
)


def _producer() -> dict[str, Any]:
    return {
        "kind": "github_actions_job",
        "repository_id": "1305993853",
        "workflow_id": "316322127",
        "workflow_path": ".github/workflows/release-controller.yml",
        "workflow_sha": "a" * 40,
        "run_id": "120",
        "run_attempt": 1,
        "job_name": "observe-tp",
        "environment": "pypi",
    }


def _seed_authorized(mem: Any, release_id: str, authority_id: str) -> None:
    lease.acquire_publication_lease(
        mem,
        release_identity_id=release_id,
        release_authority_id=authority_id,
        repository_id=1255975556,
        tag_ref="refs/tags/v9.8.0",
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
        tag_ref="refs/tags/v9.8.0",
        producer=_producer(),
        receipt_type="authorized",
        payload={
            "kind": "AUTHORIZED",
            "authorization_state": "AUTHORIZED",
            "mode": "BOOTSTRAP",
            "authorization_id": "sha256:" + ("d" * 64),
        },
        now_utc="2026-07-20T00:01:00Z",
    )


def test_classify_controller_candidate_other_and_missing() -> None:
    assert (
        tp.classify_publisher_claims(
            [
                {
                    "kind": "GitHub",
                    "repository": "PaulKov/dpone-release-controller",
                    "workflow": "release-controller.yml",
                    "environment": "pypi",
                }
            ]
        )
        == "PUBLISHER_CONTROLLER"
    )
    assert (
        tp.classify_publisher_claims(
            [
                {
                    "kind": "GitHub",
                    "repository": "PaulKov/dpone",
                    "workflow": "release.yml",
                    "environment": "",
                }
            ]
        )
        == "PUBLISHER_CANDIDATE_REPO"
    )
    assert (
        tp.classify_publisher_claims(
            [{"kind": "GitHub", "repository": "other/repo", "workflow": "x.yml", "environment": ""}]
        )
        == "PUBLISHER_OTHER"
    )
    assert tp.classify_publisher_claims([]) == "PROVENANCE_MISSING"


def test_observe_appends_inventory_without_rebind() -> None:
    mem = store_mod.InMemoryEvidenceStore()
    release_id = canonical.sha256_id("dpone.release.identity.v2", {"tag": "v9.8.0"})
    authority_id = canonical.sha256_id("dpone.release.authority.v2", {"release_id": release_id})
    _seed_authorized(mem, release_id, authority_id)

    def http_json(url: str) -> tuple[int, dict[str, Any] | None, str]:
        payload = {
            "info": {"version": "0.73.2"},
            "releases": {
                "0.73.2": [
                    {
                        "filename": "dpone-0.73.2-py3-none-any.whl",
                        "size": 10,
                        "yanked": False,
                        "digests": {"sha256": "1" * 64},
                    }
                ]
            },
        }
        return 200, payload, json.dumps(payload)

    def http_integrity(url: str) -> tuple[int, dict[str, Any] | None, str]:
        assert "/integrity/" in url
        return 404, None, '{"message":"No provenance available"}'

    result = tp.run_trusted_publisher_inventory(
        mem,
        release_identity_id=release_id,
        release_authority_id=authority_id,
        repository_id=1255975556,
        tag_ref="refs/tags/v9.8.0",
        producer=_producer(),
        now_utc="2026-07-20T00:02:00Z",
        projects=("dpone",),
        http_get_json=http_json,
        http_get_integrity=http_integrity,
    )
    assert result["status"] == "TRUSTED_PUBLISHER_INVENTORY"
    assert result["rebind_attempted"] is False
    assert result["projects"][0]["classification"] == "PROVENANCE_MISSING"
    assert result["receipt"]["payload"]["config_binding"] == "UNVERIFIED"
    assert result["receipt"]["payload"]["rebind_attempted"] is False
