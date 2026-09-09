"""Inventory GitHub immutable-release setting (observe-only; no mutation)."""

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

StreamPrerequisiteError = stream.StreamPrerequisiteError


def _http_status(exc: BaseException) -> int | None:
    status = getattr(exc, "status", None)
    return status if isinstance(status, int) else None


def _observe_repo_immutable(api: Any, *, owner: str, repo: str) -> dict[str, Any]:
    """Prefer repository setting (works for user-owned accounts)."""

    path = f"/repos/{owner}/{repo}/immutable-releases"
    payload = api.request("GET", path)
    assert isinstance(payload, dict)
    enabled = bool(payload.get("enabled"))
    return {
        "required": True,
        "observed": "ENABLED" if enabled else "DISABLED",
        "source": path.lstrip("/"),
        "enforced_by_owner": bool(payload.get("enforced_by_owner")),
        "raw_keys": sorted(payload.keys()),
    }


def _observe_org_immutable(api: Any, *, owner: str) -> dict[str, Any]:
    """Fallback org policy path when repo endpoint is unavailable."""

    path = f"/orgs/{owner}/settings/immutable-releases"
    payload = api.request("GET", path)
    assert isinstance(payload, dict)
    enabled = bool(payload.get("enabled_for_new_repos") or payload.get("enabled") or False)
    return {
        "required": True,
        "observed": "ENABLED" if enabled else "DISABLED",
        "source": path.lstrip("/"),
        "raw_keys": sorted(payload.keys()),
    }


def observe_immutable_releases(
    api: Any,
    *,
    owner: str,
    repo: str,
) -> dict[str, Any]:
    """Return observed immutable-release inventory without changing settings.

    Prefer ``GET /repos/{owner}/{repo}/immutable-releases`` (GA for repositories).
    Fall back to the org settings endpoint. Never enable or mutate.
    """

    try:
        return _observe_repo_immutable(api, owner=owner, repo=repo)
    except Exception as repo_exc:
        repo_status = _http_status(repo_exc)
        if repo_status is None:
            raise
        try:
            return _observe_org_immutable(api, owner=owner)
        except Exception as org_exc:
            org_status = _http_status(org_exc)
            if org_status is None:
                raise
            return {
                "required": True,
                "observed": "UNVERIFIED",
                "source": f"repos/{owner}/{repo}/immutable-releases",
                "reason": f"HTTP_{repo_status}",
                "fallback_org_reason": f"HTTP_{org_status}",
                "detail": str(repo_exc)[:300],
            }


class InventoryError(RuntimeError):
    """Immutable-release inventory prerequisites failed."""


def run_immutable_inventory(
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
    """Append IMMUTABLE_RELEASE_INVENTORY under an active AUTHORIZED lease."""

    receipts = store.list_receipts(release_identity_id)
    authorized = None
    for receipt in reversed(receipts):
        if str((receipt.get("payload") or {}).get("kind")) == "AUTHORIZED":
            authorized = receipt
            break
    if authorized is None:
        raise InventoryError("AUTHORIZED_REQUIRED")
    authorization_id = str(authorized["payload"]["authorization_id"])
    observation = observe_immutable_releases(api, owner=owner, repo=repo)
    inventory_id = canonical.sha256_id(
        "dpone.release.immutable-inventory.v2",
        {
            "authorization_id": authorization_id,
            "owner": owner,
            "repo": repo,
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
        receipt_type="immutable_release_inventory",
        payload={
            "kind": "IMMUTABLE_RELEASE_INVENTORY",
            "mode": "OBSERVE",
            "authorization_id": authorization_id,
            "inventory_id": inventory_id,
            "owner": owner,
            "repo": repo,
            "mutated": False,
            **observation,
        },
        now_utc=now_utc,
        retention_days=retention_days,
        scope={"kind": "release", "release_identity_id": release_identity_id},
    )
    return {
        "status": "IMMUTABLE_RELEASE_INVENTORY",
        "mode": "OBSERVE",
        "inventory_id": inventory_id,
        "observed": observation["observed"],
        "receipt": receipt,
    }
