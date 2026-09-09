"""Bootstrap attest → public-bundle → draft-transition under an active lease."""

from __future__ import annotations

import importlib.util
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import Any


def _load_sibling(module_name: str, filename: str) -> Any:
    if module_name in sys.modules:
        return sys.modules[module_name]
    path = Path(__file__).with_name(filename)
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {filename}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


canonical = _load_sibling("dpone_agent_release_canonical", "release_canonical.py")
bundle_mod = _load_sibling("dpone_agent_release_public_bundle", "release_public_bundle.py")
stream = _load_sibling("dpone_agent_release_stream_service", "release_stream_service.py")

StreamPrerequisiteError = stream.StreamPrerequisiteError


def run_attest_and_draft_dry_run(
    store: Any,
    *,
    release_identity_id: str,
    release_authority_id: str,
    repository_id: int,
    tag_ref: str,
    producer: Mapping[str, Any],
    now_utc: str,
    distributions: list[dict[str, Any]],
    retention_days: int = 365,
) -> dict[str, Any]:
    """Append the attest/bundle/draft dry-run receipt chain.

    Does **not** call GitHub attestations or create a real draft release. The
    draft id is a deterministic dry-run marker bound to the public-bundle digest.
    """

    candidate_id = canonical.sha256_id(
        "dpone.release.candidate.v2",
        {
            "authority_id": release_authority_id,
            "distributions": sorted(
                [
                    {
                        "project": str(row["project"]),
                        "filename": str(row["filename"]),
                        "sha256": str(row["sha256"]),
                    }
                    for row in distributions
                ],
                key=lambda row: (row["project"], row["filename"]),
            ),
        },
    )
    public_bundle = bundle_mod.build_public_bundle(
        candidate_id=candidate_id,
        distributions=distributions,
    )
    intent = stream.append_stream_receipt(
        store,
        release_identity_id=release_identity_id,
        release_authority_id=release_authority_id,
        repository_id=repository_id,
        tag_ref=tag_ref,
        producer=producer,
        receipt_type="mutation_intent",
        payload={
            "kind": "MUTATION_INTENT",
            "intent": "ATTEST_AND_STAGE_DRAFT",
            "mode": "DRY_RUN",
            "candidate_id": candidate_id,
        },
        now_utc=now_utc,
        retention_days=retention_days,
        scope={"kind": "candidate", "candidate_id": candidate_id},
    )
    attestation = stream.append_stream_receipt(
        store,
        release_identity_id=release_identity_id,
        release_authority_id=release_authority_id,
        repository_id=repository_id,
        tag_ref=tag_ref,
        producer=producer,
        receipt_type="attestation_verified",
        payload={
            "kind": "ATTESTATION_VERIFIED",
            "mode": "DRY_RUN",
            "candidate_id": candidate_id,
            "subject_count": len(distributions),
            "subjects": [{"filename": row["filename"], "sha256": row["sha256"]} for row in distributions],
        },
        now_utc=now_utc,
        retention_days=retention_days,
        scope={"kind": "candidate", "candidate_id": candidate_id},
    )
    verified = stream.append_stream_receipt(
        store,
        release_identity_id=release_identity_id,
        release_authority_id=release_authority_id,
        repository_id=repository_id,
        tag_ref=tag_ref,
        producer=producer,
        receipt_type="public_bundle_verified",
        payload={
            "kind": "PUBLIC_BUNDLE_VERIFIED",
            "mode": "DRY_RUN",
            "candidate_id": candidate_id,
            "public_bundle_sha256": public_bundle["manifest_sha256"],
        },
        now_utc=now_utc,
        retention_days=retention_days,
        scope={"kind": "candidate", "candidate_id": candidate_id},
    )
    draft_id = f"dry-run:{public_bundle['manifest_sha256'].removeprefix('sha256:')[:16]}"
    draft = stream.append_stream_receipt(
        store,
        release_identity_id=release_identity_id,
        release_authority_id=release_authority_id,
        repository_id=repository_id,
        tag_ref=tag_ref,
        producer=producer,
        receipt_type="draft_transition",
        payload={
            "kind": "DRAFT_TRANSITION",
            "mode": "DRY_RUN",
            "state": "STAGED",
            "draft_release_id": draft_id,
            "public_bundle_sha256": public_bundle["manifest_sha256"],
            "tag_ref": tag_ref,
        },
        now_utc=now_utc,
        retention_days=retention_days,
        scope={"kind": "candidate", "candidate_id": candidate_id},
    )
    return {
        "status": "ATTEST_DRAFT_DRY_RUN",
        "candidate_id": candidate_id,
        "public_bundle": public_bundle,
        "draft": {"mode": "DRY_RUN", "draft_release_id": draft_id},
        "receipts": [intent, attestation, verified, draft],
    }
