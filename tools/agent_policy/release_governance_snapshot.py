"""Bootstrap governance snapshots (protected-base tip, two equal reads)."""

from __future__ import annotations

import importlib.util
import sys
import time
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

StreamPrerequisiteError = stream.StreamPrerequisiteError
GitHubApiError = github.GitHubApiError


class SnapshotError(RuntimeError):
    """Unstable or missing governance snapshot."""


def capture_bootstrap_snapshot(
    api: Any,
    *,
    label: str,
    owner: str,
    repo: str,
    now_utc: str,
    gap_seconds: float = 5.0,
    sleeper: Callable[[float], None] | None = None,
) -> dict[str, Any]:
    """Capture a two-read protected-base tip snapshot for label ``A``/``B``/``C``."""

    if label not in {"A", "B", "C"}:
        raise ValueError("label must be A, B, or C")
    sleep = sleeper or time.sleep
    branch, first = github.resolve_default_branch_sha(api, owner=owner, repo=repo)
    if gap_seconds > 0:
        sleep(gap_seconds)
    branch2, second = github.resolve_default_branch_sha(api, owner=owner, repo=repo)
    if branch != branch2 or first != second:
        raise SnapshotError(f"SNAPSHOT_{label}_UNSTABLE")
    body = {
        "label": label,
        "mode": "BOOTSTRAP",
        "protected_base_ref": f"refs/heads/{branch}",
        "protected_base_sha": first,
        "read_count": 2,
        "started_at": now_utc,
        "completed_at": now_utc,
        "gap_seconds": gap_seconds,
    }
    body["snapshot_sha256"] = canonical.sha256_id("dpone.release.governance-snapshot.v2", body)
    return body


def append_governance_snapshot(
    store: Any,
    api: Any,
    *,
    label: str,
    owner: str,
    repo: str,
    release_identity_id: str,
    release_authority_id: str,
    repository_id: int,
    tag_ref: str,
    producer: Mapping[str, Any],
    now_utc: str,
    retention_days: int = 365,
    gap_seconds: float = 5.0,
    sleeper: Callable[[float], None] | None = None,
) -> dict[str, Any]:
    """Capture and append one ``GOVERNANCE_SNAPSHOT`` receipt under an active lease."""

    snapshot = capture_bootstrap_snapshot(
        api,
        label=label,
        owner=owner,
        repo=repo,
        now_utc=now_utc,
        gap_seconds=gap_seconds,
        sleeper=sleeper,
    )
    receipt = stream.append_stream_receipt(
        store,
        release_identity_id=release_identity_id,
        release_authority_id=release_authority_id,
        repository_id=repository_id,
        tag_ref=tag_ref,
        producer=producer,
        receipt_type="governance_snapshot",
        payload={
            "kind": "GOVERNANCE_SNAPSHOT",
            "label": label,
            "mode": "BOOTSTRAP",
            "snapshot": snapshot,
        },
        now_utc=now_utc,
        retention_days=retention_days,
        scope={"kind": "release", "release_identity_id": release_identity_id},
    )
    return {"status": "GOVERNANCE_SNAPSHOT", "label": label, "snapshot": snapshot, "receipt": receipt}


def latest_snapshot(receipts: list[dict[str, Any]], *, label: str) -> dict[str, Any] | None:
    """Return the latest bootstrap snapshot payload for ``label``, if present."""

    for receipt in reversed(receipts):
        payload = receipt.get("payload") or {}
        if str(payload.get("kind")) == "GOVERNANCE_SNAPSHOT" and str(payload.get("label")) == label:
            snapshot = payload.get("snapshot")
            if isinstance(snapshot, dict):
                return snapshot
    return None


def require_bootstrap_fast_forward(
    api: Any,
    *,
    owner: str,
    repo: str,
    snapshot_a: Mapping[str, Any],
    snapshot_b: Mapping[str, Any],
) -> None:
    """Bootstrap ancestry: B tip is identical to or ahead of A (fast-forward)."""

    if str(snapshot_a.get("protected_base_ref")) != str(snapshot_b.get("protected_base_ref")):
        raise SnapshotError("SNAPSHOT_BASE_REF_MISMATCH")
    sha_a = str(snapshot_a.get("protected_base_sha") or "")
    sha_b = str(snapshot_b.get("protected_base_sha") or "")
    if not sha_a or not sha_b:
        raise SnapshotError("SNAPSHOT_BASE_SHA_MISSING")
    if sha_a == sha_b:
        return
    compare = api.request("GET", f"/repos/{owner}/{repo}/compare/{sha_a}...{sha_b}")
    assert isinstance(compare, dict)
    status = str(compare.get("status") or "")
    if status not in {"identical", "ahead"}:
        raise SnapshotError(f"SNAPSHOT_BASE_NOT_FAST_FORWARD:{status}")
