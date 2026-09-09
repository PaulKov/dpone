"""Approval gate for safe-window physical reconciliation."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any


@dataclass(frozen=True, slots=True)
class PhysicalReconciliationApproval:
    approved_by: str | None = None
    approved_risks: tuple[str, ...] = ()
    expires_at: str | None = None
    table: str | None = None

    @classmethod
    def from_config(cls, raw: Mapping[str, Any] | None) -> PhysicalReconciliationApproval | None:
        if raw is None:
            return None
        if not isinstance(raw, Mapping):
            raise ValueError("physical_design.reconciliation.approval must be an object")
        approved_risks = raw.get("approved_risks", ())
        if approved_risks is None:
            approved_risks = ()
        if not isinstance(approved_risks, (list, tuple)):
            raise ValueError("physical_design.reconciliation.approval.approved_risks must be a list")
        approved_by = str(raw.get("approved_by") or "").strip() or None
        expires_at = str(raw.get("expires_at") or "").strip() or None
        table = str(raw.get("table") or "").strip() or None
        return cls(
            approved_by=approved_by,
            approved_risks=tuple(str(item) for item in approved_risks),
            expires_at=expires_at,
            table=table,
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "approved_by": self.approved_by,
            "approved_risks": list(self.approved_risks),
            "expires_at": self.expires_at,
            "table": self.table,
        }


def approval_blockers(
    approval: PhysicalReconciliationApproval | None,
    *,
    risk: str,
    table: str,
) -> tuple[str, ...]:
    if approval is None:
        return (f"physical_design.approval_required:{risk}",)
    if approval.table and _normalize_table(approval.table) != _normalize_table(table):
        return (f"physical_design.approval_table_mismatch:{risk}",)
    if approval.expires_at and _is_expired(approval.expires_at):
        return (f"physical_design.approval_expired:{risk}",)
    approved = {str(item) for item in approval.approved_risks}
    if risk not in approved:
        return (f"physical_design.approval_required:{risk}",)
    return ()


def _normalize_table(value: str) -> str:
    return ".".join(part.strip('[]`" ') for part in str(value).split(".")).lower()


def _is_expired(value: str) -> bool:
    text = str(value).strip()
    if not text:
        return False
    normalized = text.replace("Z", "+00:00")
    try:
        expires_at = datetime.fromisoformat(normalized)
    except ValueError:
        return True
    if expires_at.tzinfo is None:
        expires_at = expires_at.replace(tzinfo=UTC)
    return expires_at <= datetime.now(UTC)


__all__ = ["PhysicalReconciliationApproval", "approval_blockers"]
