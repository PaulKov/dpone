"""Release evidence store + lease acquire (shape B bootstrap, non-live)."""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest
from jsonschema import Draft202012Validator

ROOT = Path(__file__).resolve().parents[2]
SCHEMA = ROOT / "docs/schemas/release/release-receipt-envelope-v2.schema.json"


def _load(name: str, relative: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, ROOT / relative)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


canonical = _load("dpone_agent_release_canonical", "tools/agent_policy/release_canonical.py")
envelope = _load("dpone_agent_release_receipt_envelope", "tools/agent_policy/release_receipt_envelope.py")
lease = _load("dpone_agent_release_lease_service", "tools/agent_policy/release_lease_service.py")
store_mod = _load("dpone_agent_release_evidence_store_b2", "tools/agent_policy/release_evidence_store_b2.py")


def _sha(domain: str, payload: dict[str, Any]) -> str:
    return canonical.sha256_id(domain, payload)


def test_canonical_id_is_stable_and_domain_separated() -> None:
    body = {"repository_id": 1, "tag": "v1.0.0"}
    first = _sha("dpone.release.identity.v2", body)
    second = _sha("dpone.release.identity.v2", body)
    other = _sha("dpone.release.authority.v2", body)
    assert first == second
    assert first.startswith("sha256:")
    assert first != other


def test_build_envelope_matches_schema_and_self_digest() -> None:
    release_id = _sha("dpone.release.identity.v2", {"repository_id": 1, "tag": "v0.0.0"})
    authority_id = _sha(
        "dpone.release.authority.v2",
        {
            "release_id": release_id,
            "tag_object_sha": "a" * 40,
            "peeled_commit_sha": "b" * 40,
            "policy_sha256": "sha256:" + "c" * 64,
            "protected_base_ref": "refs/heads/master",
        },
    )
    attempt_id = _sha(
        "dpone.release.attempt.v2",
        {"authority_id": authority_id, "run_id": 1, "run_attempt": 1},
    )
    queue_id = _sha(
        "dpone.release.queue-entry.v2",
        {"authority_id": authority_id, "attempt_id": attempt_id},
    )
    lease_id = _sha(
        "dpone.release.publication-lease.v2",
        {"repository_id": 1, "tag_ref": "refs/tags/v0.0.0"},
    )
    built = envelope.build_receipt_envelope(
        receipt_type="lease_acquired",
        stream={
            "release_identity_id": release_id,
            "release_authority_id": authority_id,
            "sequence": 0,
            "previous": "GENESIS",
        },
        scope={"kind": "release", "release_identity_id": release_id},
        attempt={"attempt_id": attempt_id, "queue_entry_id": queue_id},
        lease={"lease_id": lease_id, "fencing_token": 1},
        producer={
            "kind": "github_actions_job",
            "repository_id": "1305993853",
            "workflow_id": "316322127",
            "workflow_path": ".github/workflows/release-controller.yml",
            "workflow_sha": "a" * 40,
            "run_id": "1",
            "run_attempt": 1,
            "job_name": "admit-and-lease",
            "environment": "none",
        },
        payload={"kind": "LEASE_ACQUIRED", "ttl_seconds": 300, "expires_at": "2026-07-19T00:05:00Z"},
        observed_at="2026-07-19T00:00:00Z",
        committed_at="2026-07-19T00:00:01Z",
    )
    Draft202012Validator(json.loads(SCHEMA.read_text(encoding="utf-8"))).validate(built)
    assert built["receipt_id"] == envelope.receipt_id_for(built)
    assert built["payload_sha256"] == canonical.sha256_id("dpone.release.payload.v2", built["payload"])


def test_memory_store_acquire_lease_is_cas_ordered() -> None:
    mem = store_mod.InMemoryEvidenceStore()
    release_id = _sha("dpone.release.identity.v2", {"repository_id": 9, "tag": "v9.9.9"})
    authority_id = _sha(
        "dpone.release.authority.v2",
        {
            "release_id": release_id,
            "tag_object_sha": "d" * 40,
            "peeled_commit_sha": "e" * 40,
            "policy_sha256": "sha256:" + "f" * 64,
            "protected_base_ref": "refs/heads/master",
        },
    )
    first = lease.acquire_publication_lease(
        mem,
        release_identity_id=release_id,
        release_authority_id=authority_id,
        repository_id=9,
        tag_ref="refs/tags/v9.9.9",
        attempt_seed={"run_id": 11, "run_attempt": 1},
        producer={
            "kind": "github_actions_job",
            "repository_id": "1305993853",
            "workflow_id": "316322127",
            "workflow_path": ".github/workflows/release-controller.yml",
            "workflow_sha": "a" * 40,
            "run_id": "11",
            "run_attempt": 1,
            "job_name": "admit-and-lease",
            "environment": "none",
        },
        ttl_seconds=300,
        now_utc="2026-07-19T00:00:00Z",
    )
    assert first["stream"]["sequence"] == 0
    assert first["stream"]["previous"] == "GENESIS"
    assert first["lease"]["fencing_token"] == 1
    assert first["payload"]["kind"] == "LEASE_ACQUIRED"

    with pytest.raises(lease.LeaseConflictError, match="ACTIVE_LEASE"):
        lease.acquire_publication_lease(
            mem,
            release_identity_id=release_id,
            release_authority_id=authority_id,
            repository_id=9,
            tag_ref="refs/tags/v9.9.9",
            attempt_seed={"run_id": 12, "run_attempt": 1},
            producer={
                "kind": "github_actions_job",
                "repository_id": "1305993853",
                "workflow_id": "316322127",
                "workflow_path": ".github/workflows/release-controller.yml",
                "workflow_sha": "a" * 40,
                "run_id": "12",
                "run_attempt": 1,
                "job_name": "admit-and-lease",
                "environment": "none",
            },
            ttl_seconds=300,
            now_utc="2026-07-19T00:01:00Z",
        )

    # After expiry, a higher fencing token may be issued.
    second = lease.acquire_publication_lease(
        mem,
        release_identity_id=release_id,
        release_authority_id=authority_id,
        repository_id=9,
        tag_ref="refs/tags/v9.9.9",
        attempt_seed={"run_id": 13, "run_attempt": 1},
        producer={
            "kind": "github_actions_job",
            "repository_id": "1305993853",
            "workflow_id": "316322127",
            "workflow_path": ".github/workflows/release-controller.yml",
            "workflow_sha": "a" * 40,
            "run_id": "13",
            "run_attempt": 1,
            "job_name": "admit-and-lease",
            "environment": "none",
        },
        ttl_seconds=300,
        now_utc="2026-07-19T00:10:00Z",
    )
    assert second["stream"]["sequence"] == 1
    assert second["stream"]["previous"] == first["receipt_id"]
    assert second["lease"]["fencing_token"] == 2


def test_b2_object_key_layout_is_deterministic() -> None:
    key = store_mod.object_key_for(
        release_identity_id="sha256:" + "a" * 64,
        sequence=7,
        receipt_id="sha256:" + "b" * 64,
    )
    assert key.startswith("streams/sha256:")
    assert "/0000000007-" in key
    assert key.endswith(".json")
