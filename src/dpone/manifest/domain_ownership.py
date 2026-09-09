"""Canonical non-secret ownership values for one self-service domain."""

from __future__ import annotations


def normalize_domain_ownership(
    owner_team: str,
    owner_contact: str,
    approver_team: str,
) -> tuple[str, str, str] | None:
    """Normalize explicit bounded ownership values without inventing defaults."""

    values = (
        owner_team.strip() if isinstance(owner_team, str) else "",
        owner_contact.strip() if isinstance(owner_contact, str) else "",
        approver_team.strip() if isinstance(approver_team, str) else "",
    )
    return None if any(not value or value == "TODO" or len(value) > 256 for value in values) else values


__all__ = ["normalize_domain_ownership"]
