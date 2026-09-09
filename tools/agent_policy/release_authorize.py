"""Bootstrap Snapshot B verification and pre-publication ``AUTHORIZED`` receipt."""

from __future__ import annotations

import importlib.util
import sys
from collections.abc import Callable, Mapping
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
stream = _load_sibling("dpone_agent_release_stream_service", "release_stream_service.py")
github = _load_sibling("dpone_agent_release_github_api", "release_github_api.py")
snapshots = _load_sibling("dpone_agent_release_governance_snapshot", "release_governance_snapshot.py")

StreamPrerequisiteError = stream.StreamPrerequisiteError
GitHubApiError = github.GitHubApiError
SnapshotError = snapshots.SnapshotError


class AuthorizationError(RuntimeError):
    """Draft or snapshot prerequisites failed."""


def run_authorize_publication(
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
    retention_days: int = 365,
    snapshot_gap_seconds: float = 5.0,
    sleeper: Callable[[float], None] | None = None,
) -> dict[str, Any]:
    """Verify the staged draft, capture bootstrap Snapshot B, append AUTHORIZED.

    Requires a prior Snapshot A receipt. Bootstrap mode binds A→B by equal
    protected-base tip (fast-forward under tip-only inventory). Does **not**
    claim full governance-policy v2 projection equality. Does **not** publish
    the draft or touch PyPI. Never writes ``status: PASS`` or ``decision: GO``.
    """

    receipts = store.list_receipts(release_identity_id)
    snapshot_a = snapshots.latest_snapshot(receipts, label="A")
    if snapshot_a is None:
        raise AuthorizationError("SNAPSHOT_A_REQUIRED")
    draft = _require_latest_kind(receipts, "DRAFT_TRANSITION")
    if str(draft["payload"].get("mode")) != "LIVE":
        raise AuthorizationError("DRAFT_TRANSITION_NOT_LIVE")
    bundle = _require_latest_kind(receipts, "PUBLIC_BUNDLE_VERIFIED")
    candidate_id = str(bundle["payload"]["candidate_id"])
    public_bundle_sha256 = str(bundle["payload"]["public_bundle_sha256"])
    draft_release_id = str(draft["payload"]["draft_release_id"])
    expected_asset_id = draft["payload"].get("asset_id")

    release = github.get_release(api, owner=owner, repo=repo, release_id=draft_release_id)
    if not bool(release.get("draft")):
        raise AuthorizationError("DRAFT_NOT_DRAFT")
    if str(release.get("id")) != draft_release_id:
        raise AuthorizationError("DRAFT_ID_MISMATCH")
    assets = release.get("assets") or []
    if not isinstance(assets, list) or not assets:
        raise AuthorizationError("DRAFT_ASSETS_MISSING")
    if expected_asset_id is not None:
        asset_ids = {str(item.get("id")) for item in assets if isinstance(item, dict)}
        if str(expected_asset_id) not in asset_ids:
            raise AuthorizationError("DRAFT_ASSET_ID_MISMATCH")

    try:
        snapshot_b_result = snapshots.append_governance_snapshot(
            store,
            api,
            label="B",
            owner=owner,
            repo=repo,
            release_identity_id=release_identity_id,
            release_authority_id=release_authority_id,
            repository_id=repository_id,
            tag_ref=tag_ref,
            producer=producer,
            now_utc=now_utc,
            retention_days=retention_days,
            gap_seconds=snapshot_gap_seconds,
            sleeper=sleeper,
        )
    except SnapshotError as exc:
        raise AuthorizationError(str(exc)) from exc
    snapshot_b = snapshot_b_result["snapshot"]
    try:
        snapshots.require_bootstrap_fast_forward(
            api,
            owner=owner,
            repo=repo,
            snapshot_a=snapshot_a,
            snapshot_b=snapshot_b,
        )
    except SnapshotError as exc:
        raise AuthorizationError(str(exc)) from exc
    except GitHubApiError as exc:
        raise AuthorizationError(f"SNAPSHOT_COMPARE_FAILED:{exc}") from exc

    active = _require_active_lease_fields(store, release_identity_id=release_identity_id, now_utc=now_utc)
    authorization_id = canonical.sha256_id(
        "dpone.release.authorization.v2",
        {
            "release_identity_id": release_identity_id,
            "candidate_id": candidate_id,
            "public_bundle_sha256": public_bundle_sha256,
            "draft_release_id": draft_release_id,
            "snapshot_a_sha256": snapshot_a["snapshot_sha256"],
            "snapshot_b_sha256": snapshot_b["snapshot_sha256"],
            "lease_id": active["lease_id"],
            "fencing_token": active["fencing_token"],
        },
    )
    authorized = stream.append_stream_receipt(
        store,
        release_identity_id=release_identity_id,
        release_authority_id=release_authority_id,
        repository_id=repository_id,
        tag_ref=tag_ref,
        producer=producer,
        receipt_type="authorized",
        payload={
            "kind": "AUTHORIZED",
            "authorization_state": "AUTHORIZED",
            "mode": "BOOTSTRAP",
            "authorization_id": authorization_id,
            "candidate_id": candidate_id,
            "public_bundle_sha256": public_bundle_sha256,
            "draft_release_id": draft_release_id,
            "lease_id": active["lease_id"],
            "fencing_token": active["fencing_token"],
            "snapshot_a_sha256": snapshot_a["snapshot_sha256"],
            "snapshot_b_sha256": snapshot_b["snapshot_sha256"],
            "governance_projection": "BOOTSTRAP_BASE_TIP_ONLY",
        },
        now_utc=now_utc,
        retention_days=retention_days,
        scope={"kind": "candidate", "candidate_id": candidate_id},
    )
    payload = authorized["payload"]
    if "status" in payload or "decision" in payload:
        raise AuthorizationError("FORBIDDEN_PASS_GO_VOCABULARY")
    if payload.get("authorization_state") != "AUTHORIZED":
        raise AuthorizationError("AUTHORIZATION_STATE_INVALID")
    return {
        "status": "AUTHORIZED",
        "mode": "BOOTSTRAP",
        "authorization_id": authorization_id,
        "draft_release_id": draft_release_id,
        "candidate_id": candidate_id,
        "snapshot_a": snapshot_a,
        "snapshot_b": snapshot_b,
        "receipts": [snapshot_b_result["receipt"], authorized],
    }


def _require_latest_kind(receipts: list[dict[str, Any]], kind: str) -> dict[str, Any]:
    for receipt in reversed(receipts):
        if str((receipt.get("payload") or {}).get("kind")) == kind:
            return receipt
    raise AuthorizationError(f"{kind}_REQUIRED")


def _require_active_lease_fields(
    store: Any,
    *,
    release_identity_id: str,
    now_utc: str,
) -> dict[str, Any]:
    lease_mod = _load_sibling("dpone_agent_release_lease_service", "release_lease_service.py")
    now = lease_mod.parse_utc(now_utc)
    active = lease_mod.active_lease(store.list_receipts(release_identity_id), now=now)
    if active is None:
        raise StreamPrerequisiteError("ACTIVE_LEASE_REQUIRED")
    return {
        "lease_id": str(active["lease"]["lease_id"]),
        "fencing_token": int(active["lease"]["fencing_token"]),
    }
