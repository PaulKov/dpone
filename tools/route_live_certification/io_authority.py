"""Strict JSON and filesystem authority for route-live evidence."""

from __future__ import annotations

import json
import os
import stat
import tempfile
from pathlib import Path
from typing import Any

from .contract import CertificationValidationError, fail, safe_relative_path


def json_object(payload: bytes, *, code: str) -> dict[str, Any]:
    """Decode one object while rejecting duplicate keys at every depth."""

    def reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        output: dict[str, Any] = {}
        for key, value in pairs:
            if key in output:
                raise CertificationValidationError(f"{code}:duplicate_key:{key}")
            output[key] = value
        return output

    try:
        value = json.loads(payload, object_pairs_hook=reject_duplicate_keys)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CertificationValidationError(code) from exc
    if not isinstance(value, dict):
        fail(code)
    return value


def require_exact_relative_files(
    root: Path,
    *,
    expected: tuple[str, ...],
    code: str,
) -> None:
    """Require an exact regular-file inventory and reject symlink aliases."""

    try:
        paths = tuple(root.rglob("*"))
    except OSError as exc:
        raise CertificationValidationError(code) from exc
    expected_directories = {str(parent) for item in expected for parent in Path(item).parents if str(parent) != "."}
    regular_files: list[Path] = []
    for path in paths:
        try:
            mode = path.lstat().st_mode
        except OSError as exc:
            raise CertificationValidationError(code) from exc
        relative = str(path.relative_to(root))
        if stat.S_ISDIR(mode):
            if relative not in expected_directories:
                fail(code)
            continue
        if not stat.S_ISREG(mode):
            fail(code)
        regular_files.append(path)
    if root.is_symlink():
        fail(code)
    actual = tuple(sorted(str(path.relative_to(root)) for path in regular_files))
    if actual != tuple(sorted(expected)):
        fail(code)


def resolve_under(root: Path, relative: str, *, code: str) -> Path:
    """Resolve one reviewed relative path without permitting root escape."""

    safe = safe_relative_path(relative, code=code)
    base = root.resolve()
    candidate = (base / safe).resolve()
    if candidate != base and base not in candidate.parents:
        fail(code)
    return candidate


def require_output_path_disjoint(
    output: Path,
    *,
    inventory_path: Path,
    evidence_root: Path,
    junit_root: Path,
) -> None:
    """Reject output aliases that could overwrite or contaminate inputs."""

    candidate = output.resolve()
    inventory = inventory_path.resolve()
    evidence = evidence_root.resolve()
    junit = junit_root.resolve()
    if output.is_symlink() or candidate == inventory:
        fail("output.path_aliases_input")
    if candidate in {evidence, junit} or evidence in candidate.parents or junit in candidate.parents:
        fail("output.path_inside_input_root")
    if output.exists():
        input_paths = [inventory]
        for root in (evidence, junit):
            input_paths.extend(path for path in root.rglob("*") if path.exists())
        try:
            output_identity = (output.stat().st_dev, output.stat().st_ino)
            if any((path.stat().st_dev, path.stat().st_ino) == output_identity for path in input_paths):
                fail("output.path_aliases_input")
        except OSError as exc:
            raise CertificationValidationError("output.path_unavailable") from exc


def write_create_only_exact(path: Path, payload: bytes, *, code: str) -> None:
    """Create one regular file, permitting only an exact idempotent replay."""

    if path.is_symlink():
        fail(f"{code}.path_alias")
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.parent.is_symlink():
        fail(f"{code}.parent_alias")
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.link(temporary_name, path, follow_symlinks=False)
        except FileExistsError:
            try:
                existing = path.read_bytes()
                mode = path.lstat().st_mode
            except OSError as exc:
                raise CertificationValidationError(f"{code}.existing_unavailable") from exc
            if not stat.S_ISREG(mode) or existing != payload:
                fail(f"{code}.conflict")
        except OSError as exc:
            raise CertificationValidationError(f"{code}.create_failed") from exc
    except OSError as exc:
        raise CertificationValidationError(f"{code}.write_failed") from exc
    finally:
        try:
            os.unlink(temporary_name)
        except FileNotFoundError:
            pass
