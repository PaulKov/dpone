"""Quality evidence helpers for migration rehearsal certificates."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any


def quality_evidence(
    *,
    run: Mapping[str, Any],
    fixture_build: Mapping[str, Any] | None,
    before_profile: Mapping[str, Any] | None,
    after_profile: Mapping[str, Any] | None,
) -> dict[str, Any]:
    blockers: list[str] = []
    warnings: list[str] = []
    checks: list[dict[str, Any]] = []
    pack_id = run.get("pack_id")
    if fixture_build is None and before_profile is None and after_profile is None:
        return _evidence(None, None, None, blockers, warnings, checks)
    blockers.extend(_pack_mismatch_blockers(pack_id, fixture_build, "fixture_build"))
    blockers.extend(_pack_mismatch_blockers(pack_id, before_profile, "before_profile"))
    blockers.extend(_pack_mismatch_blockers(pack_id, after_profile, "after_profile"))
    blockers.extend(_artifact_blockers(fixture_build, "fixture_build"))
    blockers.extend(_artifact_blockers(before_profile, "before_profile"))
    blockers.extend(_artifact_blockers(after_profile, "after_profile"))
    blockers.extend(_profile_compare_blockers(before_profile, after_profile))
    if fixture_build is None:
        warnings.append("schema_migration_rehearsal.fixture_build_missing")
    if before_profile is None or after_profile is None:
        warnings.append("schema_migration_rehearsal.quality_profile_missing")
    checks.append(
        _check("fixture_profile", "blocked" if blockers else "warning" if warnings else "passed", blockers + warnings)
    )
    return _evidence(
        _id(fixture_build, "fixture_build_id"),
        _id(before_profile, "profile_id"),
        _id(after_profile, "profile_id"),
        blockers,
        warnings,
        checks,
    )


def _evidence(
    fixture_build_id: str | None,
    before_profile_id: str | None,
    after_profile_id: str | None,
    blockers: list[str],
    warnings: list[str],
    checks: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "fixture_build_id": fixture_build_id,
        "before_profile_id": before_profile_id,
        "after_profile_id": after_profile_id,
        "blockers": blockers,
        "warnings": warnings,
        "checks": checks,
    }


def _pack_mismatch_blockers(pack_id: object, payload: Mapping[str, Any] | None, label: str) -> list[str]:
    if payload is not None and payload.get("pack_id") != pack_id:
        return [f"schema_migration_rehearsal.{label}_pack_id_mismatch"]
    return []


def _artifact_blockers(payload: Mapping[str, Any] | None, label: str) -> list[str]:
    if payload is None:
        return []
    blockers = [str(item) for item in payload.get("blockers", []) if str(item)]
    status = str(payload.get("status", ""))
    if status == "blocked":
        blockers.insert(0, f"schema_migration_rehearsal.{label}_blocked")
    return blockers


def _profile_compare_blockers(
    before_profile: Mapping[str, Any] | None,
    after_profile: Mapping[str, Any] | None,
) -> list[str]:
    if before_profile is None or after_profile is None:
        return []
    before_metrics = before_profile.get("metrics", {})
    after_metrics = after_profile.get("metrics", {})
    if not isinstance(before_metrics, Mapping) or not isinstance(after_metrics, Mapping):
        return []
    blockers: list[str] = []
    if before_metrics.get("row_count") != after_metrics.get("row_count"):
        blockers.append("schema_migration_rehearsal.row_count_drift")
    before_hash = before_metrics.get("typed_hash")
    after_hash = after_metrics.get("typed_hash")
    if before_hash and after_hash and before_hash != after_hash:
        blockers.append("schema_migration_rehearsal.typed_hash_drift")
    return blockers


def _id(payload: Mapping[str, Any] | None, key: str) -> str | None:
    if payload is None:
        return None
    value = payload.get(key)
    return str(value) if value else None


def _check(name: str, status: str, details: list[str]) -> dict[str, Any]:
    return {"name": name, "status": status, "details": list(details)}


__all__ = ["quality_evidence"]
