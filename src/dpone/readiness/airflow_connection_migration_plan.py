"""Read-only migration plans for legacy Airflow connection registry entries."""

from __future__ import annotations

import difflib
from pathlib import Path
from typing import Any

import yaml

from dpone.contracts.credential_env import connection_env_name
from dpone.readiness.airflow_secret_redaction import redacted_mapping, without_secret_keys


def connection_registry_migration_plan(
    *,
    registry: dict[str, Any],
    registry_path: Path,
    root: Path,
    environment: str,
) -> dict[str, Any] | None:
    """Build a secret-free plan for legacy ``connection_type`` / ``vault_path`` entries."""

    connections = registry.get("connections")
    if not isinstance(connections, dict):
        return None
    changes = [
        _legacy_entry_change(
            connection_ref=str(connection_ref),
            entry=entry,
            registry_path=registry_path,
            root=root,
        )
        for connection_ref, entry in sorted(connections.items())
        if isinstance(entry, dict) and _is_legacy_entry(entry)
    ]
    if not changes:
        return None
    return {
        "kind": "dpone.connection-registry-migration-plan.v1",
        "schema": "dpone.connection-registry-migration-plan.v1",
        "mode": "plan",
        "apply": False,
        "environment": environment,
        "registry_path": _relative(registry_path, root),
        "summary": {
            "legacy_entries": len(changes),
            "manual_review_required": True,
            "apply_supported": False,
        },
        "changes": changes,
    }


def _legacy_entry_change(
    *,
    connection_ref: str,
    entry: dict[str, Any],
    registry_path: Path,
    root: Path,
) -> dict[str, Any]:
    proposed_entry = _proposed_entry(connection_ref, entry)
    relative_path = _relative(registry_path, root)
    return {
        "action": "replace_legacy_connection_entry",
        "connection_ref": connection_ref,
        "path": f"connections.{connection_ref}",
        "detected": _detected_legacy_fields(entry),
        "proposed_entry": proposed_entry,
        "manual_review_required": True,
        "unified_diff": _entry_diff(
            before=redacted_mapping(entry),
            after=proposed_entry,
            before_label=f"before/{relative_path}#connections.{connection_ref}",
            after_label=f"after/{relative_path}#connections.{connection_ref}",
        ),
    }


def _proposed_entry(connection_ref: str, entry: dict[str, Any]) -> dict[str, Any]:
    proposed: dict[str, Any] = {
        "type": str(entry.get("type") or "unknown"),
        "connection": without_secret_keys(entry.get("connection")) if isinstance(entry.get("connection"), dict) else {},
        "credentials": _proposed_credentials(connection_ref, entry),
    }
    return proposed


def _proposed_credentials(connection_ref: str, entry: dict[str, Any]) -> dict[str, Any]:
    if entry.get("connection_type") == "vault" or entry.get("vault_path"):
        return {
            "resolver": "vault_kv",
            "mount": str(entry.get("vault_mount") or "kv"),
            "kv_version": _kv_version(entry.get("kv_version")),
            "path": str(entry.get("vault_path") or f"dpone/credentials/{connection_ref}"),
            "fields": _default_secret_field_mapping(),
            "version_policy": "latest",
            "resolution_scope": "workload_start",
        }
    return {
        "resolver": "env_var",
        "support": "development_only",
        "fields": {
            "username": connection_env_name(connection_ref, "username"),
            "password": connection_env_name(connection_ref, "password"),
        },
    }


def _kv_version(value: object) -> int:
    return value if value in {1, 2} else 2


def _default_secret_field_mapping() -> dict[str, str]:
    return {"username": "username", "password": "password"}


def _detected_legacy_fields(entry: dict[str, Any]) -> dict[str, str]:
    detected: dict[str, str] = {}
    for key in ("connection_type", "vault_path"):
        if entry.get(key) is not None:
            detected[key] = str(entry[key])
    return detected


def _is_legacy_entry(entry: dict[str, Any]) -> bool:
    return "connection_type" in entry or "vault_path" in entry


def _entry_diff(*, before: dict[str, Any], after: dict[str, Any], before_label: str, after_label: str) -> str:
    before_lines = _yaml_lines(before)
    after_lines = _yaml_lines(after)
    return "".join(
        difflib.unified_diff(
            before_lines,
            after_lines,
            fromfile=before_label,
            tofile=after_label,
            lineterm="",
        )
    )


def _yaml_lines(payload: dict[str, Any]) -> list[str]:
    return yaml.safe_dump(payload, sort_keys=True).splitlines(keepends=True)


def _relative(path: Path, root: Path) -> str:
    try:
        return path.relative_to(root).as_posix()
    except ValueError:
        return path.as_posix()


__all__ = ["connection_registry_migration_plan"]
