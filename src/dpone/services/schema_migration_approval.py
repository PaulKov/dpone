"""Approval helper predicates for schema migration packs."""

from __future__ import annotations

from dpone.readiness.migration_control import MigrationPack


def approval_required(pack: MigrationPack) -> bool:
    return any(
        str(change.get("risk", "")) in {"manual_sql", "destructive", "shadow_required", "unsafe_direct_rename"}
        for change in pack.changes
    )


def phased_approval_required(pack: MigrationPack) -> bool:
    return any(
        str(change.get("risk", "")) in {"manual_sql", "destructive", "unsafe_direct_rename"} for change in pack.changes
    )


__all__ = ["approval_required", "phased_approval_required"]
