"""Package metadata, dependency pins, and changelog checks for release identity."""

from __future__ import annotations

import re
import tomllib
from collections.abc import Callable
from pathlib import Path
from typing import Any

REQUIREMENT_NAME = re.compile(r"^\s*(?P<name>[A-Za-z0-9_.-]+)(?:\[[^\]]+\])?")
EXACT_REQUIREMENT = re.compile(r"^\s*(?P<name>[A-Za-z0-9_.-]+)\s*==\s*(?P<version>[^,;\s@]+)\s*$")

PACKAGE_FILES = {
    "dpone": Path("pyproject.toml"),
    "dpone-native-accel": Path("packages/dpone-native-accel/pyproject.toml"),
    "dpone-airflow-pack": Path("packages/dpone-airflow-pack/pyproject.toml"),
    "apache-airflow-providers-dpone": Path("packages/apache-airflow-providers-dpone/pyproject.toml"),
}

ReadCommitText = Callable[[Path, str, Path], str | None]
BlockerFactory = Callable[..., Any]


def normalize_package_name(value: str) -> str:
    return re.sub(r"[-_.]+", "-", value).lower()


def load_package_projects(
    root: Path,
    commit_sha: str,
    *,
    read_commit_text: ReadCommitText,
    blocker: BlockerFactory,
) -> tuple[dict[str, dict[str, Any]], dict[str, str], list[Any]]:
    """Load the four public package projects from the frozen commit."""

    projects: dict[str, dict[str, Any]] = {}
    versions: dict[str, str] = {}
    blockers: list[Any] = []
    for expected_name, relative in PACKAGE_FILES.items():
        try:
            source = read_commit_text(root, commit_sha, relative)
            if source is None:
                raise OSError
            payload = tomllib.loads(source)
            project = payload["project"]
            name = str(project["name"])
            version = str(project["version"])
        except (OSError, KeyError, TypeError, tomllib.TOMLDecodeError):
            blockers.append(
                blocker(
                    "RELEASE_PACKAGE_METADATA_INVALID",
                    f"Package metadata is unavailable or invalid for {expected_name}.",
                    package=expected_name,
                )
            )
            continue
        if normalize_package_name(name) != normalize_package_name(expected_name):
            blockers.append(
                blocker(
                    "RELEASE_PACKAGE_NAME_MISMATCH",
                    f"Expected package metadata for {expected_name}.",
                    package=expected_name,
                )
            )
            continue
        projects[expected_name] = project
        versions[expected_name] = version
    return projects, versions, blockers


def _requirement_entries(project: dict[str, Any], *, group: str | None = None) -> tuple[str, ...]:
    raw: Any
    if group is None:
        raw = project.get("dependencies", [])
    else:
        optional = project.get("optional-dependencies", {})
        raw = optional.get(group, []) if isinstance(optional, dict) else []
    if not isinstance(raw, list):
        return ()
    return tuple(item for item in raw if isinstance(item, str))


def _exact_pin(entries: tuple[str, ...], package: str) -> str | None:
    expected = normalize_package_name(package)
    matches: list[str] = []
    for entry in entries:
        name_match = REQUIREMENT_NAME.match(entry)
        if name_match is None or normalize_package_name(name_match.group("name")) != expected:
            continue
        exact_match = EXACT_REQUIREMENT.fullmatch(entry)
        if exact_match is None:
            return None
        matches.append(exact_match.group("version"))
    return matches[0] if len(matches) == 1 else None


def dependency_blockers(
    projects: dict[str, dict[str, Any]],
    version: str,
    *,
    blocker: BlockerFactory,
) -> list[Any]:
    """Require exact internal dependency pins to the release version."""

    required_pins = (
        ("dpone", None, "dpone-airflow-pack"),
        ("dpone", "accel", "dpone-native-accel"),
        ("apache-airflow-providers-dpone", None, "dpone-airflow-pack"),
    )
    blockers: list[Any] = []
    for owner, group, dependency in required_pins:
        project = projects.get(owner)
        pin = _exact_pin(_requirement_entries(project or {}, group=group), dependency)
        if pin != version:
            blockers.append(
                blocker(
                    "RELEASE_DEPENDENCY_NOT_EXACT",
                    f"{owner} must pin {dependency} to the release version.",
                    package=owner,
                )
            )
    return blockers


def changelog_blockers(
    root: Path,
    commit_sha: str,
    version: str,
    *,
    read_commit_text: ReadCommitText,
    blocker: BlockerFactory,
) -> list[Any]:
    """Require a CHANGELOG.md section for the release version in the frozen commit."""

    changelog = read_commit_text(root, commit_sha, Path("CHANGELOG.md"))
    if changelog is None:
        return [
            blocker(
                "RELEASE_CHANGELOG_UNAVAILABLE",
                "CHANGELOG.md is unavailable or is not valid UTF-8 in the release commit.",
            )
        ]
    pattern = re.compile(rf"^##\s+{re.escape(version)}(?:\s|$)", re.MULTILINE)
    if pattern.search(changelog):
        return []
    return [
        blocker(
            "RELEASE_CHANGELOG_SECTION_MISSING",
            "CHANGELOG.md has no release section for the tag version.",
        )
    ]
