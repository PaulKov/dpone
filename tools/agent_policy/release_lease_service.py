"""Publication-lease acquire against an append-only evidence store."""

from __future__ import annotations

import importlib.util
import sys
from collections.abc import Mapping
from datetime import datetime, timedelta, timezone
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
envelope_mod = _load_sibling("dpone_agent_release_receipt_envelope", "release_receipt_envelope.py")


class LeaseConflictError(RuntimeError):
    """Active lease still covers ``now_utc``."""


def acquire_publication_lease(
    store: Any,
    *,
    release_identity_id: str,
    release_authority_id: str,
    repository_id: int,
    tag_ref: str,
    attempt_seed: Mapping[str, Any],
    producer: Mapping[str, Any],
    ttl_seconds: int,
    now_utc: str,
    retention_days: int = 365,
) -> dict[str, Any]:
    """Append ``LEASE_ACQUIRED`` when no unexpired lease is active.

    CAS rules:
    - ``sequence`` must equal current stream length
    - ``previous`` is ``GENESIS`` at 0 else the prior ``receipt_id``
    - ``fencing_token`` is monotonic (max prior lease token + 1)
    """

    if ttl_seconds <= 0:
        raise ValueError("ttl_seconds must be positive")
    now = parse_utc(now_utc)
    receipts = store.list_receipts(release_identity_id)
    active = active_lease(receipts, now=now)
    if active is not None:
        raise LeaseConflictError(
            f"ACTIVE_LEASE fencing_token={active['lease']['fencing_token']} "
            f"expires_at={active['payload'].get('expires_at')}"
        )

    sequence = len(receipts)
    previous = "GENESIS" if sequence == 0 else str(receipts[-1]["receipt_id"])
    fencing_token = _next_fencing_token(receipts)
    lease_id = canonical.sha256_id(
        "dpone.release.publication-lease.v2",
        {"repository_id": repository_id, "tag_ref": tag_ref},
    )
    attempt_id = canonical.sha256_id(
        "dpone.release.attempt.v2",
        {"authority_id": release_authority_id, **dict(attempt_seed)},
    )
    queue_entry_id = canonical.sha256_id(
        "dpone.release.queue-entry.v2",
        {"authority_id": release_authority_id, "attempt_id": attempt_id},
    )
    expires_at = (now + timedelta(seconds=ttl_seconds)).strftime("%Y-%m-%dT%H:%M:%SZ")
    built = envelope_mod.build_receipt_envelope(
        receipt_type="lease_acquired",
        stream={
            "release_identity_id": release_identity_id,
            "release_authority_id": release_authority_id,
            "sequence": sequence,
            "previous": previous,
        },
        scope={"kind": "release", "release_identity_id": release_identity_id},
        attempt={"attempt_id": attempt_id, "queue_entry_id": queue_entry_id},
        lease={"lease_id": lease_id, "fencing_token": fencing_token},
        producer=dict(producer),
        payload={
            "kind": "LEASE_ACQUIRED",
            "ttl_seconds": ttl_seconds,
            "expires_at": expires_at,
        },
        observed_at=now_utc,
        committed_at=now_utc,
    )
    store.append_receipt(built, retention_days=retention_days)
    return built


def release_publication_lease(
    store: Any,
    *,
    release_identity_id: str,
    release_authority_id: str,
    repository_id: int,
    tag_ref: str,
    producer: Mapping[str, Any],
    now_utc: str,
    reason: str = "BOOTSTRAP_COMPLETE",
    retention_days: int = 365,
) -> dict[str, Any]:
    """Append ``LEASE_RELEASED`` for the current active lease (no public delete)."""

    now = parse_utc(now_utc)
    receipts = store.list_receipts(release_identity_id)
    active = active_lease(receipts, now=now)
    if active is None:
        raise LeaseConflictError("ACTIVE_LEASE_REQUIRED")
    expected_lease_id = canonical.sha256_id(
        "dpone.release.publication-lease.v2",
        {"repository_id": repository_id, "tag_ref": tag_ref},
    )
    lease_id = str(active["lease"]["lease_id"])
    if lease_id != expected_lease_id:
        raise LeaseConflictError("LEASE_IDENTITY_MISMATCH")
    fencing_token = int(active["lease"]["fencing_token"])
    sequence = len(receipts)
    previous = str(receipts[-1]["receipt_id"])
    built = envelope_mod.build_receipt_envelope(
        receipt_type="lease_released",
        stream={
            "release_identity_id": release_identity_id,
            "release_authority_id": release_authority_id,
            "sequence": sequence,
            "previous": previous,
        },
        scope={"kind": "release", "release_identity_id": release_identity_id},
        attempt=dict(active["attempt"]),
        lease={"lease_id": lease_id, "fencing_token": fencing_token},
        producer=dict(producer),
        payload={
            "kind": "LEASE_RELEASED",
            "reason": reason,
            "released_at": now_utc,
        },
        observed_at=now_utc,
        committed_at=now_utc,
    )
    store.append_receipt(built, retention_days=retention_days)
    return built


def active_lease(receipts: list[dict[str, Any]], *, now: datetime) -> dict[str, Any] | None:
    """Return the unexpired ``LEASE_ACQUIRED`` receipt, if any."""

    active: dict[str, Any] | None = None
    for receipt in receipts:
        kind = str((receipt.get("payload") or {}).get("kind") or "")
        if kind == "LEASE_ACQUIRED":
            expires = parse_utc(str(receipt["payload"]["expires_at"]))
            if expires > now:
                active = receipt
            else:
                active = None
        elif kind in {"LEASE_RELEASED", "CLOSED", "CANCELLATION"}:
            active = None
    return active


def _next_fencing_token(receipts: list[dict[str, Any]]) -> int:
    highest = 0
    for receipt in receipts:
        token = receipt.get("lease", {}).get("fencing_token")
        if isinstance(token, int) and token > highest:
            highest = token
    return highest + 1


def parse_utc(value: str) -> datetime:
    """Parse an ISO-8601 UTC timestamp (``Z`` or offset)."""

    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)  # noqa: UP017
    return parsed.astimezone(timezone.utc)  # noqa: UP017
