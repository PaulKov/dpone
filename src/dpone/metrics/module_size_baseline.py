"""Closed JSON codec and durable writer for module-size debt baselines."""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path, PurePosixPath
from typing import Any

from .module_size_confined_write import (
    ModuleSizeConfinedWriteError,
    ModuleSizeRootIdentity,
    write_confined_baseline_bytes,
)

_FULL_SHA = re.compile(r"[0-9a-f]{40}\Z")
_ROOT_FIELDS = frozenset({"schema_version", "debt"})
_ENTRY_FIELDS = frozenset(
    {
        "max_lines",
        "max_sloc",
        "owner",
        "reason",
        "target_sloc",
        "target_date",
        "accepted_adr",
        "baseline_commit",
    }
)
_MAX_BASELINE_BYTES = 1 << 20


class ModuleSizeBaselineError(ValueError):
    """The module-size baseline or its Git identity is not trustworthy."""


@dataclass(frozen=True)
class ModuleSizeDebtEntry:
    path: str
    max_lines: int
    max_sloc: int
    owner: str
    reason: str
    target_sloc: int
    target_date: date
    accepted_adr: str | None
    baseline_commit: str


@dataclass(frozen=True)
class ModuleSizeBaseline:
    entries: tuple[ModuleSizeDebtEntry, ...]


@dataclass(frozen=True)
class ModuleSizePolicyIssue:
    path: str
    message: str


def load_module_size_baseline(path: Path) -> ModuleSizeBaseline:
    """Load one bounded, duplicate-free v2 baseline with a closed schema."""

    if _path_has_symlink_component(path):
        raise ModuleSizeBaselineError(f"Module-size baseline path must not contain symlinks: {path}")
    try:
        raw = path.read_bytes()
    except OSError as exc:
        raise ModuleSizeBaselineError(f"Cannot read module-size baseline {path}: {exc}") from exc
    return decode_module_size_baseline(raw, source=str(path))


def decode_module_size_baseline(raw: bytes, *, source: str) -> ModuleSizeBaseline:
    payload = decode_json_object(raw, source=source)
    root = _closed_object(payload, fields=_ROOT_FIELDS, label="baseline root")
    if root.get("schema_version") != 2:
        raise ModuleSizeBaselineError("Module-size baseline schema_version must equal 2")
    debt = _closed_object(root.get("debt"), fields=None, label="baseline debt")
    entries = tuple(_decode_entry(path, value) for path, value in sorted(debt.items()))
    return ModuleSizeBaseline(entries=entries)


def is_legacy_empty_baseline(raw: bytes, *, source: str) -> bool:
    """Recognize only the exact audited v1 empty-baseline migration source."""

    return decode_json_object(raw, source=source) == {"entries": [], "version": 1}


def write_module_size_baseline(path: Path, baseline: ModuleSizeBaseline) -> None:
    """Atomically persist canonical v2 JSON and fsync both file and directory."""

    content = encode_module_size_baseline(baseline)
    write_module_size_baseline_bytes(path, content)


def encode_module_size_baseline(baseline: ModuleSizeBaseline) -> bytes:
    """Return the canonical, round-trip-verified v2 representation."""

    payload = {
        "schema_version": 2,
        "debt": {
            entry.path: {
                "accepted_adr": entry.accepted_adr,
                "baseline_commit": entry.baseline_commit,
                "max_lines": entry.max_lines,
                "max_sloc": entry.max_sloc,
                "owner": entry.owner,
                "reason": entry.reason,
                "target_date": entry.target_date.isoformat(),
                "target_sloc": entry.target_sloc,
            }
            for entry in sorted(baseline.entries, key=lambda item: item.path)
        },
    }
    content = (json.dumps(payload, indent=2, sort_keys=True) + "\n").encode("utf-8")
    decoded = decode_module_size_baseline(content, source="generated baseline")
    expected = ModuleSizeBaseline(entries=tuple(sorted(baseline.entries, key=lambda item: item.path)))
    if decoded != expected:
        raise ModuleSizeBaselineError("Generated baseline is not a lossless canonical representation")
    return content


def write_module_size_baseline_bytes(path: Path, content: bytes) -> None:
    """Atomically persist validated bytes through an anchored absolute path."""

    decode_module_size_baseline(content, source="baseline transaction bytes")
    absolute = Path(os.path.abspath(path))
    parent, missing = _existing_parent(absolute.parent)
    write_confined_module_size_baseline_bytes(
        anchor=parent,
        parts=(*missing, absolute.name),
        content=content,
        expected_bytes=None,
    )


def write_confined_module_size_baseline_bytes(
    *,
    anchor: Path,
    parts: tuple[str, ...],
    content: bytes,
    expected_bytes: bytes | None,
    root_identity: ModuleSizeRootIdentity | None = None,
) -> None:
    """Map the descriptor writer's internal failure into the public baseline error."""

    try:
        write_confined_baseline_bytes(
            anchor=anchor,
            parts=parts,
            content=content,
            expected_bytes=expected_bytes,
            root_identity=root_identity,
        )
    except ModuleSizeConfinedWriteError as exc:
        raise ModuleSizeBaselineError(str(exc)) from exc


def _existing_parent(path: Path) -> tuple[Path, tuple[str, ...]]:
    missing: list[str] = []
    current = path
    while True:
        try:
            return current.resolve(strict=True), tuple(reversed(missing))
        except FileNotFoundError:
            if current == current.parent:
                raise ModuleSizeBaselineError("Module-size baseline has no existing parent anchor")
            missing.append(current.name)
            current = current.parent


def exact_sha(value: Any, *, label: str) -> str:
    if not isinstance(value, str) or _FULL_SHA.fullmatch(value) is None:
        raise ModuleSizeBaselineError(f"{label} must be a full lowercase 40-character SHA")
    return value


def confined_repo_path(repo_root: Path, path: Path, *, label: str) -> Path:
    """Return one lexical repository path after rejecting every symlink component."""

    root = Path(os.path.abspath(repo_root))
    candidate = path if path.is_absolute() else root / path
    candidate = Path(os.path.abspath(candidate))
    try:
        candidate.relative_to(root)
    except ValueError as exc:
        raise ModuleSizeBaselineError(f"{label} must be confined to the repository") from exc
    if _path_has_symlink_component(candidate, stop=root):
        raise ModuleSizeBaselineError(f"{label} must not contain symlink components")
    return candidate


def decode_json_object(raw: bytes, *, source: str) -> dict[str, Any]:
    if len(raw) > _MAX_BASELINE_BYTES:
        raise ModuleSizeBaselineError(f"Module-size baseline exceeds {_MAX_BASELINE_BYTES} bytes: {source}")
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ModuleSizeBaselineError(f"Module-size baseline must be UTF-8: {source}") from exc
    try:
        payload = json.loads(text, object_pairs_hook=_unique_object, parse_constant=_reject_constant)
    except (json.JSONDecodeError, ValueError) as exc:
        raise ModuleSizeBaselineError(f"Invalid module-size baseline JSON in {source}: {exc}") from exc
    if not isinstance(payload, dict):
        raise ModuleSizeBaselineError(f"Module-size baseline root must be an object: {source}")
    return payload


def _decode_entry(path: str, value: Any) -> ModuleSizeDebtEntry:
    canonical = _canonical_python_path(path, label="debt path")
    item = _closed_object(value, fields=_ENTRY_FIELDS, label=f"debt entry {canonical}")
    entry = ModuleSizeDebtEntry(
        path=canonical,
        max_lines=_positive_int(item.get("max_lines"), label=f"{canonical}.max_lines"),
        max_sloc=_positive_int(item.get("max_sloc"), label=f"{canonical}.max_sloc"),
        owner=_nonempty(item.get("owner"), label=f"{canonical}.owner"),
        reason=_nonempty(item.get("reason"), label=f"{canonical}.reason"),
        target_sloc=_positive_int(item.get("target_sloc"), label=f"{canonical}.target_sloc"),
        target_date=_iso_date(item.get("target_date"), label=f"{canonical}.target_date"),
        accepted_adr=_optional_adr(item.get("accepted_adr"), label=f"{canonical}.accepted_adr"),
        baseline_commit=exact_sha(item.get("baseline_commit"), label=f"{canonical}.baseline_commit"),
    )
    return entry


def _closed_object(value: Any, *, fields: frozenset[str] | None, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ModuleSizeBaselineError(f"{label} must be an object")
    if fields is not None:
        unknown = sorted(set(value) - fields)
        missing = sorted(fields - set(value))
        if unknown:
            raise ModuleSizeBaselineError(f"{label} has unknown fields: {', '.join(unknown)}")
        if missing:
            raise ModuleSizeBaselineError(f"{label} is missing fields: {', '.join(missing)}")
    return value


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _reject_constant(value: str) -> None:
    raise ValueError(f"non-finite JSON number is forbidden: {value}")


def _canonical_python_path(value: Any, *, label: str) -> str:
    if not isinstance(value, str) or not value or "\\" in value:
        raise ModuleSizeBaselineError(f"{label} must be a canonical confined POSIX path")
    if _has_ascii_control(value):
        raise ModuleSizeBaselineError(f"{label} must not contain control characters")
    path = PurePosixPath(value)
    if path.is_absolute() or value != path.as_posix() or ".." in path.parts or path.suffix != ".py":
        raise ModuleSizeBaselineError(f"{label} must be a canonical confined Python path")
    return value


def _positive_int(value: Any, *, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ModuleSizeBaselineError(f"{label} must be a positive integer")
    return value


def _nonempty(value: Any, *, label: str) -> str:
    if not isinstance(value, str) or not value.strip() or value != value.strip():
        raise ModuleSizeBaselineError(f"{label} must be a non-empty trimmed string")
    if _has_ascii_control(value):
        raise ModuleSizeBaselineError(f"{label} must not contain control characters")
    return value


def _iso_date(value: Any, *, label: str) -> date:
    if not isinstance(value, str):
        raise ModuleSizeBaselineError(f"{label} must be an ISO date")
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise ModuleSizeBaselineError(f"{label} must be an ISO date") from exc
    if parsed.isoformat() != value:
        raise ModuleSizeBaselineError(f"{label} must be canonical YYYY-MM-DD")
    return parsed


def _optional_adr(value: Any, *, label: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value or "\\" in value:
        raise ModuleSizeBaselineError(f"{label} must be null or a confined docs/adr Markdown path")
    if _has_ascii_control(value):
        raise ModuleSizeBaselineError(f"{label} must not contain control characters")
    path = PurePosixPath(value)
    if (
        path.is_absolute()
        or path.as_posix() != value
        or ".." in path.parts
        or path.suffix != ".md"
        or tuple(path.parts[:2]) != ("docs", "adr")
    ):
        raise ModuleSizeBaselineError(f"{label} must be null or a confined docs/adr Markdown path")
    return value


def _has_ascii_control(value: str) -> bool:
    return any(ord(character) < 32 or ord(character) == 127 for character in value)


def _path_has_symlink_component(path: Path, *, stop: Path | None = None) -> bool:
    absolute = path.absolute()
    boundary = stop.absolute() if stop is not None else Path(absolute.anchor)
    try:
        relative = absolute.relative_to(boundary)
    except ValueError:
        return True
    current = boundary
    if current.is_symlink():
        return True
    for part in relative.parts:
        current = current / part
        if current.is_symlink():
            return True
    return False


__all__ = [
    "ModuleSizeBaseline",
    "ModuleSizeBaselineError",
    "ModuleSizeDebtEntry",
    "ModuleSizePolicyIssue",
    "decode_json_object",
    "decode_module_size_baseline",
    "encode_module_size_baseline",
    "confined_repo_path",
    "exact_sha",
    "is_legacy_empty_baseline",
    "load_module_size_baseline",
    "write_module_size_baseline",
    "write_module_size_baseline_bytes",
    "write_confined_module_size_baseline_bytes",
]
