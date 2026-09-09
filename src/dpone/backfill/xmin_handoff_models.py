"""Durable models for the PostgreSQL XMin backfill handoff."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

_HEX_64 = re.compile(r"^[0-9a-f]{64}$")
_TOKEN = re.compile(r"^sha256:[0-9a-f]{64}$")
_STATUSES = frozenset({"anchored", "committing", "committed"})


@dataclass(slots=True)
class BackfillXminHandoffRecord:
    """Append-only campaign projection of one initial XMin anchor."""

    handoff_id: str
    status: str
    anchor_xmin: int
    snapshot_token: str
    state_key_sha256: str
    source_authority_sha256: str
    plan_hash: str
    seed_load_id: str
    receipt_id: str | None = None
    candidate_revision: int | None = None
    contract_version: str = "postgres_xmin_initial_handoff_v1"
    publication_receipt_id: str | None = None

    def __post_init__(self) -> None:
        if self.contract_version != "postgres_xmin_initial_handoff_v1":
            raise ValueError("postgres_xmin_handoff.contract_version_invalid")
        if not self.handoff_id or self.status not in _STATUSES:
            raise ValueError("postgres_xmin_handoff.record_invalid")
        if type(self.anchor_xmin) is not int or self.anchor_xmin < 1:
            raise ValueError("postgres_xmin_handoff.anchor_invalid")
        if not _TOKEN.fullmatch(self.snapshot_token):
            raise ValueError("postgres_xmin_handoff.snapshot_token_invalid")
        if not _HEX_64.fullmatch(self.state_key_sha256) or not _HEX_64.fullmatch(self.source_authority_sha256):
            raise ValueError("postgres_xmin_handoff.identity_invalid")
        if not self.plan_hash or not self.seed_load_id:
            raise ValueError("postgres_xmin_handoff.campaign_identity_invalid")
        if self.publication_receipt_id is not None and not self.publication_receipt_id.strip():
            raise ValueError("postgres_xmin_handoff.publication_receipt_invalid")
        committed = self.status == "committed"
        if committed != bool(self.receipt_id) or committed != (self.candidate_revision is not None):
            raise ValueError("postgres_xmin_handoff.receipt_state_invalid")
        if self.candidate_revision is not None and self.candidate_revision < 1:
            raise ValueError("postgres_xmin_handoff.receipt_state_invalid")

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "contract_version": self.contract_version,
            "handoff_id": self.handoff_id,
            "status": self.status,
            "anchor_xmin": self.anchor_xmin,
            "snapshot_token": self.snapshot_token,
            "state_key_sha256": self.state_key_sha256,
            "source_authority_sha256": self.source_authority_sha256,
            "plan_hash": self.plan_hash,
            "seed_load_id": self.seed_load_id,
            "publication_receipt_id": self.publication_receipt_id,
            "receipt_id": self.receipt_id,
            "candidate_revision": self.candidate_revision,
        }

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> BackfillXminHandoffRecord:
        if not isinstance(raw, dict):
            raise ValueError("postgres_xmin_handoff.record_invalid")
        return cls(
            contract_version=str(raw.get("contract_version") or ""),
            handoff_id=str(raw.get("handoff_id") or ""),
            status=str(raw.get("status") or ""),
            anchor_xmin=_exact_int(raw.get("anchor_xmin"), "anchor_xmin"),
            snapshot_token=str(raw.get("snapshot_token") or ""),
            state_key_sha256=str(raw.get("state_key_sha256") or ""),
            source_authority_sha256=str(raw.get("source_authority_sha256") or ""),
            plan_hash=str(raw.get("plan_hash") or ""),
            seed_load_id=str(raw.get("seed_load_id") or ""),
            publication_receipt_id=(str(raw["publication_receipt_id"]) if raw.get("publication_receipt_id") else None),
            receipt_id=(str(raw["receipt_id"]) if raw.get("receipt_id") else None),
            candidate_revision=(
                _exact_int(raw.get("candidate_revision"), "candidate_revision")
                if raw.get("candidate_revision") is not None
                else None
            ),
        )


def _exact_int(value: Any, field: str) -> int:
    if type(value) is not int:
        raise ValueError(f"postgres_xmin_handoff.{field}_invalid")
    return value


__all__ = ["BackfillXminHandoffRecord"]
