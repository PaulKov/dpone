"""Live Shape B attest → public-bundle → draft staging (no PyPI)."""

from __future__ import annotations

import hashlib
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
github = _load_sibling("dpone_agent_release_github_api", "release_github_api.py")

StreamPrerequisiteError = stream.StreamPrerequisiteError
GitHubApiError = github.GitHubApiError


def run_stage_draft_live(
    store: Any,
    api: Any,
    *,
    owner: str,
    repo: str,
    release_identity_id: str,
    release_authority_id: str,
    repository_id: int,
    tag_ref: str,
    producer: Mapping[str, Any],
    now_utc: str,
    subject_filename: str,
    subject_bytes: bytes,
    attestation: Mapping[str, Any],
    retention_days: int = 365,
) -> dict[str, Any]:
    """Create/resume a real draft release and append LIVE receipt chain.

    Does **not** publish the draft and does **not** upload to PyPI. Requires an
    active publication lease and a non-empty subject plus attestation metadata
    produced by the controller attestation step.
    """

    if not subject_bytes:
        raise ValueError("subject_bytes must be non-empty")
    attestation_digest = str(attestation.get("digest") or "").strip()
    if not attestation_digest.startswith("sha256:"):
        raise ValueError("attestation.digest must be sha256:<hex>")

    tag_name = tag_ref.removeprefix("refs/tags/")
    subject_sha256 = hashlib.sha256(subject_bytes).hexdigest()
    distributions = [
        {
            "project": "dpone",
            "filename": subject_filename,
            "size": len(subject_bytes),
            "sha256": subject_sha256,
        }
    ]
    candidate_id = canonical.sha256_id(
        "dpone.release.candidate.v2",
        {
            "authority_id": release_authority_id,
            "distributions": [
                {
                    "project": "dpone",
                    "filename": subject_filename,
                    "sha256": subject_sha256,
                }
            ],
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
            "mode": "LIVE",
            "candidate_id": candidate_id,
        },
        now_utc=now_utc,
        retention_days=retention_days,
        scope={"kind": "candidate", "candidate_id": candidate_id},
    )
    attestation_receipt = stream.append_stream_receipt(
        store,
        release_identity_id=release_identity_id,
        release_authority_id=release_authority_id,
        repository_id=repository_id,
        tag_ref=tag_ref,
        producer=producer,
        receipt_type="attestation_verified",
        payload={
            "kind": "ATTESTATION_VERIFIED",
            "mode": "LIVE",
            "candidate_id": candidate_id,
            "subject_count": 1,
            "subjects": [{"filename": subject_filename, "sha256": subject_sha256}],
            "attestation": dict(attestation),
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
            "mode": "LIVE",
            "candidate_id": candidate_id,
            "public_bundle_sha256": public_bundle["manifest_sha256"],
        },
        now_utc=now_utc,
        retention_days=retention_days,
        scope={"kind": "candidate", "candidate_id": candidate_id},
    )

    _branch, tip_sha = github.resolve_default_branch_sha(api, owner=owner, repo=repo)
    tag_result = github.ensure_lightweight_tag(
        api,
        owner=owner,
        repo=repo,
        tag_ref=tag_ref if tag_ref.startswith("refs/") else f"refs/tags/{tag_name}",
        commit_sha=tip_sha,
    )
    draft_result = github.create_or_get_draft_release(
        api,
        owner=owner,
        repo=repo,
        tag_name=tag_name,
        name=f"Shape B bootstrap draft {tag_name}",
        body=(
            "Shape B controller bootstrap draft. NOT a production release. "
            f"public_bundle={public_bundle['manifest_sha256']} "
            f"candidate_id={candidate_id}"
        ),
    )
    release = draft_result["release"]
    upload_url = str(release["upload_url"])
    asset = github.upload_release_asset(
        api,
        upload_url_template=upload_url,
        filename=subject_filename,
        content=subject_bytes,
    )
    draft_id = str(release["id"])
    draft_receipt = stream.append_stream_receipt(
        store,
        release_identity_id=release_identity_id,
        release_authority_id=release_authority_id,
        repository_id=repository_id,
        tag_ref=tag_ref,
        producer=producer,
        receipt_type="draft_transition",
        payload={
            "kind": "DRAFT_TRANSITION",
            "mode": "LIVE",
            "state": "STAGED",
            "draft_release_id": draft_id,
            "draft_html_url": release.get("html_url"),
            "public_bundle_sha256": public_bundle["manifest_sha256"],
            "tag_ref": tag_ref if tag_ref.startswith("refs/") else f"refs/tags/{tag_name}",
            "tag_object_sha": tip_sha,
            "asset_id": asset.get("id"),
            "tag_created": tag_result["created"],
            "draft_created": draft_result["created"],
        },
        now_utc=now_utc,
        retention_days=retention_days,
        scope={"kind": "candidate", "candidate_id": candidate_id},
    )
    return {
        "status": "DRAFT_STAGED",
        "mode": "LIVE",
        "candidate_id": candidate_id,
        "public_bundle": public_bundle,
        "draft": {
            "mode": "LIVE",
            "draft_release_id": draft_id,
            "html_url": release.get("html_url"),
            "tag_name": tag_name,
        },
        "receipts": [intent, attestation_receipt, verified, draft_receipt],
    }
