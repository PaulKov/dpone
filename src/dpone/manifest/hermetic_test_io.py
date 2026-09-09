"""Bounded project-file gateway used by hermetic test discovery."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dpone.manifest.bounded_yaml import BoundedYamlError, BoundedYamlLimits, load_bounded_yaml
from dpone.manifest.confined_files import ConfinedFileError, read_confined_file

HERMETIC_YAML_MAX_BYTES = 1024 * 1024
_YAML_LIMITS = BoundedYamlLimits(max_bytes=HERMETIC_YAML_MAX_BYTES)


@dataclass(frozen=True, slots=True)
class HermeticInputReadError(ValueError):
    """Safe read failure that carries no operating-system exception text."""

    code: str
    message: str
    exit_code: int
    path: str

    def __str__(self) -> str:
        return self.message


def read_hermetic_project_file(
    root: Path,
    relative_path: str,
    *,
    max_bytes: int,
    fixture: bool,
) -> bytes:
    """Read one confined regular file and normalize adapter failures."""

    try:
        return read_confined_file(root, relative_path, max_bytes=max_bytes)
    except ConfinedFileError as exc:
        raise _normalize_read_error(exc, relative_path=relative_path, fixture=fixture) from exc


def parse_hermetic_project_yaml(content: bytes, *, path: str) -> dict[str, Any]:
    """Parse bounded YAML and return a string-keyed project object."""

    try:
        payload = load_bounded_yaml(content, limits=_YAML_LIMITS)
    except BoundedYamlError as exc:
        raise HermeticInputReadError(
            "DPONE_TEST_MANIFEST_INVALID",
            "Test or pipeline YAML is malformed or exceeds parser limits.",
            2,
            path,
        ) from exc
    if not isinstance(payload, Mapping) or not all(isinstance(key, str) for key in payload):
        raise HermeticInputReadError(
            "DPONE_TEST_MANIFEST_INVALID",
            "Test or pipeline YAML must be an object with string keys.",
            2,
            path,
        )
    return dict(payload)


def _normalize_read_error(
    error: ConfinedFileError,
    *,
    relative_path: str,
    fixture: bool,
) -> HermeticInputReadError:
    if error.code == "file_too_large":
        code = "DPONE_TEST_FIXTURE_LIMIT_EXCEEDED" if fixture else "DPONE_TEST_INPUT_LIMIT_EXCEEDED"
        return HermeticInputReadError(
            code,
            "Test input exceeds its configured byte limit.",
            4,
            relative_path,
        )
    if error.code in {"symlink_forbidden", "path_invalid", "not_regular_file"}:
        return HermeticInputReadError(
            "DPONE_TEST_PATH_UNSAFE",
            "Test input path is outside the confined regular-file policy.",
            4,
            relative_path,
        )
    if error.code == "file_unavailable":
        return HermeticInputReadError(
            "DPONE_TEST_INPUT_UNAVAILABLE",
            "Required test input could not be read safely.",
            4,
            relative_path,
        )
    return HermeticInputReadError(
        "DPONE_TEST_INPUT_NOT_FOUND",
        "Required test input was not found.",
        2,
        relative_path,
    )


__all__ = [
    "HERMETIC_YAML_MAX_BYTES",
    "HermeticInputReadError",
    "parse_hermetic_project_yaml",
    "read_hermetic_project_file",
]
