"""Attest → public-bundle → draft-transition bootstrap (no live GitHub/PyPI)."""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from types import ModuleType
from typing import Any

from jsonschema import Draft202012Validator

ROOT = Path(__file__).resolve().parents[2]
BUNDLE_SCHEMA = ROOT / "docs/schemas/release/release-public-bundle-v2.schema.json"


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
bundle = _load("dpone_agent_release_public_bundle", "tools/agent_policy/release_public_bundle.py")
flow = _load("dpone_agent_release_attest_draft", "tools/agent_policy/release_attest_draft.py")


def _producer() -> dict[str, Any]:
    return {
        "kind": "github_actions_job",
        "repository_id": "1305993853",
        "workflow_id": "316322127",
        "workflow_path": ".github/workflows/release-controller.yml",
        "workflow_sha": "a" * 40,
        "run_id": "99",
        "run_attempt": 1,
        "job_name": "attest-and-draft",
        "environment": "release-attest",
    }


def test_public_bundle_matches_schema_and_digest() -> None:
    candidate_id = canonical.sha256_id("dpone.release.candidate.v2", {"n": 1})
    built = bundle.build_public_bundle(
        candidate_id=candidate_id,
        distributions=[
            {
                "project": "dpone",
                "filename": "dpone-0.0.0-py3-none-any.whl",
                "size": 12,
                "sha256": "a" * 64,
            }
        ],
        asset_names=["dpone-0.0.0-py3-none-any.whl"],
    )
    Draft202012Validator(json.loads(BUNDLE_SCHEMA.read_text(encoding="utf-8"))).validate(built)
    assert built["manifest_sha256"].startswith("sha256:")


def test_attest_draft_dry_run_appends_ordered_receipts() -> None:
    mem = store_mod.InMemoryEvidenceStore()
    release_id = canonical.sha256_id("dpone.release.identity.v2", {"tag": "v9.0.0"})
    authority_id = canonical.sha256_id("dpone.release.authority.v2", {"release_id": release_id})
    lease.acquire_publication_lease(
        mem,
        release_identity_id=release_id,
        release_authority_id=authority_id,
        repository_id=1,
        tag_ref="refs/tags/v9.0.0",
        attempt_seed={"run_id": 1, "run_attempt": 1},
        producer=_producer(),
        ttl_seconds=300,
        now_utc="2026-07-19T00:00:00Z",
    )
    result = flow.run_attest_and_draft_dry_run(
        mem,
        release_identity_id=release_id,
        release_authority_id=authority_id,
        repository_id=1,
        tag_ref="refs/tags/v9.0.0",
        producer=_producer(),
        now_utc="2026-07-19T00:01:00Z",
        distributions=[
            {
                "project": "dpone",
                "filename": "dpone-0.0.0-py3-none-any.whl",
                "size": 12,
                "sha256": "b" * 64,
            }
        ],
    )
    kinds = [item["payload"]["kind"] for item in result["receipts"]]
    assert kinds == [
        "MUTATION_INTENT",
        "ATTESTATION_VERIFIED",
        "PUBLIC_BUNDLE_VERIFIED",
        "DRAFT_TRANSITION",
    ]
    assert result["draft"]["mode"] == "DRY_RUN"
    assert result["draft"]["draft_release_id"].startswith("dry-run:")
    stream = mem.list_receipts(release_id)
    assert [row["stream"]["sequence"] for row in stream] == [0, 1, 2, 3, 4]
    assert stream[-1]["stream"]["previous"] == stream[-2]["receipt_id"]


def test_attest_draft_requires_active_lease() -> None:
    mem = store_mod.InMemoryEvidenceStore()
    release_id = canonical.sha256_id("dpone.release.identity.v2", {"tag": "v9.0.1"})
    authority_id = canonical.sha256_id("dpone.release.authority.v2", {"release_id": release_id})
    try:
        flow.run_attest_and_draft_dry_run(
            mem,
            release_identity_id=release_id,
            release_authority_id=authority_id,
            repository_id=1,
            tag_ref="refs/tags/v9.0.1",
            producer=_producer(),
            now_utc="2026-07-19T00:01:00Z",
            distributions=[
                {
                    "project": "dpone",
                    "filename": "dpone-0.0.0-py3-none-any.whl",
                    "size": 12,
                    "sha256": "c" * 64,
                }
            ],
        )
    except flow.StreamPrerequisiteError as exc:
        assert "ACTIVE_LEASE_REQUIRED" in str(exc)
    else:
        raise AssertionError("expected StreamPrerequisiteError")
