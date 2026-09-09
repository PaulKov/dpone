"""Append and inspect local Airflow deployment promotion audit events."""

from __future__ import annotations

import json
import os
import stat
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dpone.runtime.deployment_cache_common import DeploymentCacheError, fsync_directory, open_regular_file

_POINTER_FIELDS = (
    "schema",
    "activation_id",
    "environment",
    "deployment_id",
    "release_id",
    "promoted_by",
    "promoted_at",
    "previous_deployment_id",
    "source_commit",
    "attestation_ref",
    "workspace_authority_connection_ref",
)


@dataclass(frozen=True, slots=True)
class PromotionAuditIssue:
    code: str
    severity: str
    message: str
    path: str


def append_promotion_audit(path: Path, payload: Mapping[str, Any]) -> None:
    """Append one durable JSONL event, separating it from a partial prior line."""

    encoded = json.dumps(dict(payload), ensure_ascii=False, sort_keys=True).encode("utf-8") + b"\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    flags = os.O_CREAT | os.O_RDWR | os.O_APPEND | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags, 0o600)
    try:
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise OSError("promotion audit must be a regular file")
        with os.fdopen(descriptor, "rb+") as handle:
            descriptor = -1
            _append_event(handle, encoded)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    fsync_directory(path.parent)


def _append_event(handle: Any, encoded: bytes) -> None:
    handle.seek(0, os.SEEK_END)
    if handle.tell() > 0:
        handle.seek(-1, os.SEEK_END)
        if handle.read(1) != b"\n":
            handle.seek(0, os.SEEK_END)
            handle.write(b"\n")
        handle.seek(0, os.SEEK_END)
    handle.write(encoded)
    handle.flush()
    os.fsync(handle.fileno())


def inspect_promotion_audit(
    path: Path,
    *,
    expected_pointer: Mapping[str, Any],
    environment: str,
) -> tuple[PromotionAuditIssue, ...]:
    """Compare the latest valid environment event with current-pointer metadata."""

    latest: Mapping[str, Any] | None = None
    invalid_lines = 0
    descriptor = -1
    try:
        descriptor = open_regular_file(
            path,
            missing_code="DPONE_CURRENT_POINTER_AUDIT_MISSING",
            invalid_code="DPONE_CURRENT_POINTER_AUDIT_UNREADABLE",
            label="promotion audit",
            root=path.parent,
        )
        with os.fdopen(descriptor, "r", encoding="utf-8") as handle:
            descriptor = -1
            for line in handle:
                if not line.strip():
                    continue
                try:
                    event = json.loads(line)
                except json.JSONDecodeError:
                    invalid_lines += 1
                    continue
                if not isinstance(event, Mapping):
                    invalid_lines += 1
                    continue
                if event.get("environment") == environment:
                    latest = event
    except DeploymentCacheError as exc:
        return (_issue(exc.code, str(exc), path),)
    except (OSError, UnicodeError):
        return (_issue("DPONE_CURRENT_POINTER_AUDIT_UNREADABLE", "promotion audit cannot be read", path),)
    finally:
        if descriptor >= 0:
            os.close(descriptor)

    issues: list[PromotionAuditIssue] = []
    if invalid_lines:
        issues.append(
            _issue(
                "DPONE_CURRENT_POINTER_AUDIT_INVALID_LINE",
                "promotion audit contains malformed historical events",
                path,
                severity="warning",
            )
        )
    if latest is None:
        issues.append(_issue("DPONE_CURRENT_POINTER_AUDIT_MISSING", "promotion audit event is missing", path))
    elif not _same_pointer(latest, expected_pointer):
        issues.append(
            _issue(
                "DPONE_CURRENT_POINTER_AUDIT_MISMATCH",
                "latest promotion audit event does not match current pointer",
                path,
            )
        )
    return tuple(issues)


def _same_pointer(event: Mapping[str, Any], pointer: Mapping[str, Any]) -> bool:
    return all(event.get(field) == pointer.get(field) for field in _POINTER_FIELDS)


def _issue(code: str, message: str, path: Path, *, severity: str = "error") -> PromotionAuditIssue:
    return PromotionAuditIssue(code=code, severity=severity, message=message, path=path.as_posix())


__all__ = ["PromotionAuditIssue", "append_promotion_audit", "inspect_promotion_audit"]
