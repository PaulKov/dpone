"""Append ordered stream receipts under an active publication lease."""

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


lease_mod = _load_sibling("dpone_agent_release_lease_service", "release_lease_service.py")
envelope_mod = _load_sibling("dpone_agent_release_receipt_envelope", "release_receipt_envelope.py")
canonical = _load_sibling("dpone_agent_release_canonical", "release_canonical.py")


class StreamPrerequisiteError(RuntimeError):
    """Missing active lease or fencing mismatch."""


def append_stream_receipt(
    store: Any,
    *,
    release_identity_id: str,
    release_authority_id: str,
    repository_id: int,
    tag_ref: str,
    producer: Mapping[str, Any],
    receipt_type: str,
    payload: Mapping[str, Any],
    now_utc: str,
    retention_days: int = 365,
    scope: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Append one receipt bound to the current active lease fencing token."""

    now = lease_mod.parse_utc(now_utc)
    receipts = store.list_receipts(release_identity_id)
    active = lease_mod.active_lease(receipts, now=now)
    if active is None:
        raise StreamPrerequisiteError("ACTIVE_LEASE_REQUIRED")
    fencing_token = int(active["lease"]["fencing_token"])
    lease_id = str(active["lease"]["lease_id"])
    expected_lease_id = canonical.sha256_id(
        "dpone.release.publication-lease.v2",
        {"repository_id": repository_id, "tag_ref": tag_ref},
    )
    if lease_id != expected_lease_id:
        raise StreamPrerequisiteError("LEASE_IDENTITY_MISMATCH")

    sequence = len(receipts)
    previous = "GENESIS" if sequence == 0 else str(receipts[-1]["receipt_id"])
    attempt = dict(active["attempt"])
    built = envelope_mod.build_receipt_envelope(
        receipt_type=receipt_type,
        stream={
            "release_identity_id": release_identity_id,
            "release_authority_id": release_authority_id,
            "sequence": sequence,
            "previous": previous,
        },
        scope=dict(scope or {"kind": "release", "release_identity_id": release_identity_id}),
        attempt=attempt,
        lease={"lease_id": lease_id, "fencing_token": fencing_token},
        producer=dict(producer),
        payload=dict(payload),
        observed_at=now_utc,
        committed_at=now_utc,
    )
    store.append_receipt(built, retention_days=retention_days)
    return built
