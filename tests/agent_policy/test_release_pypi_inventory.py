"""Read-only PyPI inventory classification tests."""

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
pypi = _load("dpone_agent_release_pypi_inventory", "tools/agent_policy/release_pypi_inventory.py")


def _producer() -> dict[str, Any]:
    return {
        "kind": "github_actions_job",
        "repository_id": "1305993853",
        "workflow_id": "316322127",
        "workflow_path": ".github/workflows/release-controller.yml",
        "workflow_sha": "a" * 40,
        "run_id": "110",
        "run_attempt": 1,
        "job_name": "observe-pypi",
        "environment": "pypi",
    }


def _seed_authorized(mem: Any, release_id: str, authority_id: str) -> None:
    lease.acquire_publication_lease(
        mem,
        release_identity_id=release_id,
        release_authority_id=authority_id,
        repository_id=1255975556,
        tag_ref="refs/tags/v9.7.7",
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
        tag_ref="refs/tags/v9.7.7",
        producer=_producer(),
        receipt_type="authorized",
        payload={
            "kind": "AUTHORIZED",
            "authorization_state": "AUTHORIZED",
            "mode": "BOOTSTRAP",
            "authorization_id": "sha256:" + ("b" * 64),
        },
        now_utc="2026-07-20T00:01:00Z",
    )


def test_classify_pending_exact_conflict_and_non_pypi() -> None:
    files = [
        {
            "filename": "dpone-1.0.0-py3-none-any.whl",
            "size": 10,
            "sha256": "a" * 64,
            "yanked": False,
            "version": "1.0.0",
        },
        {
            "filename": "dpone-1.0.1-py3-none-any.whl",
            "size": 11,
            "sha256": "c" * 64,
            "yanked": False,
            "version": "1.0.1",
        },
    ]
    pending = pypi.classify_expected_file(
        {"project": "dpone", "filename": "dpone-9.9.9-py3-none-any.whl", "size": 1, "sha256": "d" * 64},
        files,
    )
    exact = pypi.classify_expected_file(
        {"project": "dpone", "filename": "dpone-1.0.0-py3-none-any.whl", "size": 10, "sha256": "a" * 64},
        files,
    )
    conflict = pypi.classify_expected_file(
        {"project": "dpone", "filename": "dpone-1.0.1-py3-none-any.whl", "size": 99, "sha256": "e" * 64},
        files,
    )
    not_pypi = pypi.classify_expected_file(
        {"project": "dpone", "filename": "bootstrap.bin", "size": 3, "sha256": "f" * 64},
        files,
    )
    assert pending["classification"] == "PENDING_UPLOAD"
    assert exact["classification"] == "ALREADY_PUBLISHED_EXACT"
    assert conflict["classification"] == "CONFLICT"
    assert not_pypi["classification"] == "NOT_PYPI_ARTIFACT"


def test_observe_appends_inventory_without_upload() -> None:
    mem = store_mod.InMemoryEvidenceStore()
    release_id = canonical.sha256_id("dpone.release.identity.v2", {"tag": "v9.7.7"})
    authority_id = canonical.sha256_id("dpone.release.authority.v2", {"release_id": release_id})
    _seed_authorized(mem, release_id, authority_id)

    def http_get(url: str) -> tuple[int, dict[str, Any] | None, str]:
        project = url.rstrip("/").split("/")[-2]
        payload = {
            "info": {"version": "0.1.0"},
            "releases": {
                "0.1.0": [
                    {
                        "filename": f"{project}-0.1.0-py3-none-any.whl",
                        "size": 12,
                        "yanked": False,
                        "digests": {"sha256": "1" * 64},
                    }
                ]
            },
        }
        return 200, payload, json.dumps(payload)

    result = pypi.run_pypi_inventory_observe(
        mem,
        release_identity_id=release_id,
        release_authority_id=authority_id,
        repository_id=1255975556,
        tag_ref="refs/tags/v9.7.7",
        producer=_producer(),
        now_utc="2026-07-20T00:02:00Z",
        expected_distributions=[
            {
                "project": "dpone",
                "filename": "dpone-9.9.9-py3-none-any.whl",
                "size": 1,
                "sha256": "2" * 64,
            }
        ],
        projects=("dpone",),
        http_get=http_get,
    )
    assert result["status"] == "PYPI_INVENTORY"
    assert result["upload_subset"][0]["classification"] == "PENDING_UPLOAD"
    assert result["receipt"]["payload"]["upload_attempted"] is False
    assert result["conflicts"] == []
