"""Observe staged GitHub draft by exact release ID (never publish)."""

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
stream = _load_sibling("dpone_agent_release_stream_service", "release_stream_service.py")
github = _load_sibling("dpone_agent_release_github_api", "release_github_api.py")

StreamPrerequisiteError = stream.StreamPrerequisiteError


class InventoryError(RuntimeError):
    """Draft inventory prerequisites failed."""


def classify_draft_observation(
    *,
    expected_draft_release_id: str,
    expected_asset_id: str | None,
    release: Mapping[str, Any] | None,
    api_status: int | None = None,
) -> dict[str, Any]:
    """Classify one exact-ID draft re-read without mutating publication state."""

    if release is None:
        return {
            "classification": "DRAFT_MISSING",
            "reason": f"HTTP_{api_status or 'UNKNOWN'}",
            "draft_release_id": expected_draft_release_id,
        }
    observed_id = str(release.get("id") or "")
    if observed_id != str(expected_draft_release_id):
        return {
            "classification": "DRAFT_ID_MISMATCH",
            "draft_release_id": expected_draft_release_id,
            "observed_release_id": observed_id,
        }
    assets = release.get("assets") or []
    asset_ids = [str(item.get("id")) for item in assets if isinstance(item, dict)]
    if expected_asset_id is not None and str(expected_asset_id) not in asset_ids:
        return {
            "classification": "DRAFT_ASSET_MISMATCH",
            "draft_release_id": observed_id,
            "expected_asset_id": str(expected_asset_id),
            "asset_ids": asset_ids,
            "draft": bool(release.get("draft")),
            "prerelease": bool(release.get("prerelease")),
        }
    if bool(release.get("draft")):
        classification = "STILL_DRAFT"
    else:
        classification = "ALREADY_PUBLISHED"
    return {
        "classification": classification,
        "draft_release_id": observed_id,
        "tag_name": release.get("tag_name"),
        "html_url": release.get("html_url"),
        "draft": bool(release.get("draft")),
        "prerelease": bool(release.get("prerelease")),
        "immutable": release.get("immutable"),
        "asset_ids": asset_ids,
        "asset_count": len(asset_ids),
        "expected_asset_id": str(expected_asset_id) if expected_asset_id is not None else None,
    }


def run_draft_inventory(
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
) -> dict[str, Any]:
    """Append DRAFT_INVENTORY under AUTHORIZED; never publish or edit the draft."""

    receipts = store.list_receipts(release_identity_id)
    authorized = _latest_kind(receipts, "AUTHORIZED")
    if authorized is None:
        raise InventoryError("AUTHORIZED_REQUIRED")
    draft = _latest_kind(receipts, "DRAFT_TRANSITION")
    if draft is None:
        raise InventoryError("DRAFT_TRANSITION_REQUIRED")
    if str(draft["payload"].get("mode")) != "LIVE":
        raise InventoryError("DRAFT_TRANSITION_NOT_LIVE")
    draft_release_id = str(draft["payload"]["draft_release_id"])
    expected_asset_id = draft["payload"].get("asset_id")
    authorization_id = str(authorized["payload"]["authorization_id"])

    release: dict[str, Any] | None
    api_status: int | None = None
    try:
        release = github.get_release(api, owner=owner, repo=repo, release_id=draft_release_id)
    except Exception as exc:
        status = getattr(exc, "status", None)
        if not isinstance(status, int):
            raise
        release = None
        api_status = status

    observation = classify_draft_observation(
        expected_draft_release_id=draft_release_id,
        expected_asset_id=str(expected_asset_id) if expected_asset_id is not None else None,
        release=release,
        api_status=api_status,
    )
    inventory_id = canonical.sha256_id(
        "dpone.release.draft-inventory.v2",
        {
            "authorization_id": authorization_id,
            "draft_release_id": draft_release_id,
            "observation": observation,
        },
    )
    receipt = stream.append_stream_receipt(
        store,
        release_identity_id=release_identity_id,
        release_authority_id=release_authority_id,
        repository_id=repository_id,
        tag_ref=tag_ref,
        producer=producer,
        receipt_type="draft_inventory",
        payload={
            "kind": "DRAFT_INVENTORY",
            "mode": "OBSERVE",
            "authorization_id": authorization_id,
            "inventory_id": inventory_id,
            "publish_attempted": False,
            **observation,
        },
        now_utc=now_utc,
        retention_days=retention_days,
        scope={"kind": "release", "release_identity_id": release_identity_id},
    )
    return {
        "status": "DRAFT_INVENTORY",
        "mode": "OBSERVE",
        "inventory_id": inventory_id,
        "classification": observation["classification"],
        "publish_attempted": False,
        "receipt": receipt,
    }


def _latest_kind(receipts: list[dict[str, Any]], kind: str) -> dict[str, Any] | None:
    for receipt in reversed(receipts):
        if str((receipt.get("payload") or {}).get("kind")) == kind:
            return receipt
    return None
