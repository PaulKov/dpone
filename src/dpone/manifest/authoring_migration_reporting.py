"""Redacted deterministic reports for authoring-mode migration."""

from __future__ import annotations

import copy
import difflib
import hashlib
from collections.abc import Mapping
from dataclasses import replace
from typing import Any, Literal, cast

import yaml

from dpone.manifest.authoring import AuthoringCompilation, canonical_authoring_fingerprint
from dpone.manifest.authoring_migration_io import AuthoringSourceSnapshot
from dpone.manifest.authoring_migration_models import (
    AuthoringMigrationChange,
    AuthoringMigrationIdentity,
    AuthoringMigrationResult,
    AuthoringMode,
)
from dpone.manifest.authoring_migration_rendering import AuthoringMigrationCandidate
from dpone.readiness.authoring_migration_support import authoring_migration_error, redacted_authoring_mapping

_UNKNOWN_FINGERPRINT = "sha256:" + "0" * 64


def migration_changes(
    *,
    snapshot: AuthoringSourceSnapshot,
    candidate: AuthoringMigrationCandidate,
    desired_root: bytes,
    fragment_relative: str | None,
    desired_fragment: bytes | None,
    fragment_existing: bytes | None,
) -> tuple[AuthoringMigrationChange, ...]:
    changes = [
        AuthoringMigrationChange(
            action="modify",
            path=snapshot.relative_path,
            before_sha256=snapshot.sha256,
            after_sha256=_sha256(desired_root),
            unified_diff=_safe_yaml_diff(
                snapshot.payload,
                candidate.root_payload,
                before_path=snapshot.relative_path,
                after_path=snapshot.relative_path,
            ),
        )
    ]
    if fragment_relative and desired_fragment is not None and candidate.fragment_payload is not None:
        action: Literal["create", "no_op"] = "no_op" if fragment_existing == desired_fragment else "create"
        changes.append(
            AuthoringMigrationChange(
                action=action,
                path=fragment_relative,
                before_sha256=_sha256(fragment_existing) if fragment_existing is not None else None,
                after_sha256=_sha256(desired_fragment),
                unified_diff=(
                    ""
                    if action == "no_op"
                    else _safe_yaml_diff(
                        {},
                        candidate.fragment_payload,
                        before_path="/dev/null",
                        after_path=fragment_relative,
                    )
                ),
            )
        )
    return tuple(changes)


def migration_plan_id(
    *,
    snapshot: AuthoringSourceSnapshot,
    source_mode: AuthoringMode,
    target_mode: AuthoringMode,
    semantic_fingerprint: str,
    changes: tuple[AuthoringMigrationChange, ...],
    retained_files: tuple[str, ...],
) -> str:
    return canonical_authoring_fingerprint(
        {
            "schema": "dpone.authoring-migration.v1",
            "source": {
                "path": snapshot.relative_path,
                "mode": source_mode,
                "sha256": snapshot.sha256,
                "semantic_fingerprint": semantic_fingerprint,
            },
            "target_mode": target_mode,
            "changes": [
                {
                    "action": item.action,
                    "path": item.path,
                    "before_sha256": item.before_sha256,
                    "after_sha256": item.after_sha256,
                }
                for item in changes
            ],
            "retained_files": list(retained_files),
        }
    )


def no_op_result(
    snapshot: AuthoringSourceSnapshot,
    compilation: AuthoringCompilation,
    *,
    apply: bool,
) -> AuthoringMigrationResult:
    mode = _mode(compilation.authoring_mode)
    identity = AuthoringMigrationIdentity(
        mode=mode,
        path=snapshot.relative_path,
        sha256=snapshot.sha256,
        semantic_fingerprint=compilation.semantic_fingerprint,
    )
    plan_id = canonical_authoring_fingerprint(
        {
            "schema": "dpone.authoring-migration.v1",
            "source": identity.to_jsonable(),
            "target_mode": mode,
            "changes": [],
        }
    )
    return AuthoringMigrationResult(
        mode="apply" if apply else "plan",
        status="no_op",
        plan_id=plan_id,
        source=identity,
        target=identity,
    )


def blocked_without_source_result(
    *,
    mode: str,
    target_mode: AuthoringMode,
    code: str,
    message: str,
    exit_code: int,
) -> AuthoringMigrationResult:
    identity = AuthoringMigrationIdentity(mode=target_mode, semantic_fingerprint=_UNKNOWN_FINGERPRINT)
    return AuthoringMigrationResult(
        mode=cast(Literal["plan", "apply"], mode),
        status="blocked",
        plan_id=_UNKNOWN_FINGERPRINT,
        source=identity,
        target=identity,
        errors=(_error(code, message),),
        exit_code=exit_code,
    )


def blocked_result(
    *,
    snapshot: AuthoringSourceSnapshot,
    source_mode: AuthoringMode,
    target_mode: AuthoringMode,
    mode: str,
    code: str,
    message: str,
    exit_code: int,
    semantic_fingerprint: str = _UNKNOWN_FINGERPRINT,
) -> AuthoringMigrationResult:
    source = AuthoringMigrationIdentity(
        mode=source_mode,
        path=snapshot.relative_path,
        sha256=snapshot.sha256,
        semantic_fingerprint=semantic_fingerprint,
    )
    target = AuthoringMigrationIdentity(mode=target_mode, semantic_fingerprint=semantic_fingerprint)
    return AuthoringMigrationResult(
        mode=cast(Literal["plan", "apply"], mode),
        status="blocked",
        plan_id=_UNKNOWN_FINGERPRINT,
        source=source,
        target=target,
        errors=(_error(code, message, path=snapshot.relative_path),),
        exit_code=exit_code,
    )


def blocked_from_compilation_result(
    snapshot: AuthoringSourceSnapshot,
    before: AuthoringCompilation,
    *,
    target_mode: AuthoringMode,
    mode: str,
    code: str,
    message: str,
    exit_code: int,
    target_fingerprint: str | None = None,
) -> AuthoringMigrationResult:
    result = blocked_result(
        snapshot=snapshot,
        source_mode=_mode(before.authoring_mode),
        target_mode=target_mode,
        mode=mode,
        code=code,
        message=message,
        exit_code=exit_code,
        semantic_fingerprint=before.semantic_fingerprint,
    )
    if target_fingerprint is None:
        return result
    return replace(
        result,
        target=AuthoringMigrationIdentity(mode=target_mode, semantic_fingerprint=target_fingerprint),
    )


def with_apply_error(
    result: AuthoringMigrationResult,
    *,
    code: str,
    message: str,
    exit_code: int,
) -> AuthoringMigrationResult:
    return AuthoringMigrationResult(
        mode="apply",
        status="blocked",
        plan_id=result.plan_id,
        source=result.source,
        target=result.target,
        changes=result.changes,
        retained_files=result.retained_files,
        warnings=result.warnings,
        errors=(_error(code, message, path=result.source.path),),
        exit_code=exit_code,
    )


def yaml_bytes(payload: Mapping[str, Any] | None) -> bytes:
    if payload is None:
        return b""
    return yaml.safe_dump(dict(payload), sort_keys=False, allow_unicode=False).encode("utf-8")


def _safe_yaml_diff(
    before: Mapping[str, Any],
    after: Mapping[str, Any],
    *,
    before_path: str,
    after_path: str,
) -> str:
    before_text = yaml.safe_dump(
        redacted_authoring_mapping(copy.deepcopy(dict(before))),
        sort_keys=False,
        allow_unicode=False,
    )
    after_text = yaml.safe_dump(
        redacted_authoring_mapping(copy.deepcopy(dict(after))),
        sort_keys=False,
        allow_unicode=False,
    )
    return "".join(
        difflib.unified_diff(
            before_text.splitlines(keepends=True),
            after_text.splitlines(keepends=True),
            fromfile=before_path,
            tofile=after_path,
        )
    )


def _error(code: str, message: str, *, path: str = "") -> dict[str, Any]:
    return authoring_migration_error(code, message, path=path)


def _mode(value: str | None) -> AuthoringMode:
    normalized = str(value or "").strip().lower()
    if normalized not in {"classic", "flow", "folder"}:
        raise ValueError("Authoring mode must be classic, flow, or folder.")
    return cast(AuthoringMode, normalized)


def _sha256(content: bytes | None) -> str:
    return "sha256:" + hashlib.sha256(content or b"").hexdigest()


__all__ = [
    "blocked_from_compilation_result",
    "blocked_result",
    "blocked_without_source_result",
    "migration_changes",
    "migration_plan_id",
    "no_op_result",
    "with_apply_error",
    "yaml_bytes",
]
