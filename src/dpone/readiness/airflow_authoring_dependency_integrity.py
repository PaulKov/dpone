"""Fail-closed parity check between compilation inputs and compact-pack pins."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any, Protocol


class _AuthoringSourceDependency(Protocol):
    @property
    def kind(self) -> str: ...

    @property
    def path(self) -> str: ...

    @property
    def sha256(self) -> str: ...


class AuthoringDependencyIntegrityError(ValueError):
    """The source graph changed between compilation and pack materialization."""


class PrimaryAuthoringSourceIntegrityError(AuthoringDependencyIntegrityError):
    """The primary authoring source no longer matches its captured pin."""


def verify_authoring_dependency_parity(
    *,
    expected: Iterable[_AuthoringSourceDependency],
    pack: Mapping[str, Any],
    expected_primary_path: str | None = None,
    expected_primary_sha256: str | None = None,
) -> None:
    """Require exact source kind/path/digest parity before publishing a release."""

    if (expected_primary_path is None) != (expected_primary_sha256 is None):
        raise ValueError("Primary source path and digest must be provided together.")
    expected_pins = tuple(sorted(_pin(item.kind, item.path, item.sha256) for item in expected))
    raw_dependencies = pack.get("workload_dependencies")
    if not isinstance(raw_dependencies, list):
        raise AuthoringDependencyIntegrityError("Compact pack has no workload dependency list.")
    actual_primary = tuple(
        sorted(
            _pin(
                str(item.get("kind") or ""),
                str(item.get("path") or ""),
                str(item.get("sha256") or ""),
            )
            for item in raw_dependencies
            if isinstance(item, Mapping) and item.get("kind") == "manifest"
        )
    )
    if expected_primary_path is not None and expected_primary_sha256 is not None:
        expected_primary = (_pin("manifest", expected_primary_path, expected_primary_sha256),)
        if expected_primary != actual_primary:
            raise PrimaryAuthoringSourceIntegrityError(
                "Primary authoring source changed while the release was being built."
            )
    actual_pins = tuple(
        sorted(
            _pin(
                str(item.get("kind") or ""),
                str(item.get("path") or ""),
                str(item.get("sha256") or ""),
            )
            for item in raw_dependencies
            if isinstance(item, Mapping)
            and item.get("kind") in {"authoring_fragment", "sql_file", "recipe", "profile", "component"}
        )
    )
    if expected_pins != actual_pins:
        raise AuthoringDependencyIntegrityError(
            "Authoring source dependencies changed while the release was being built."
        )


def source_file_provenance(pack: Mapping[str, Any]) -> list[dict[str, str]]:
    """Return safe root, fragment, and SQL pins for release provenance."""

    dependencies = pack.get("workload_dependencies")
    if not isinstance(dependencies, list):
        return []
    return [
        {"kind": str(item["kind"]), "path": str(item["path"]), "sha256": _digest(str(item["sha256"]))}
        for item in dependencies
        if isinstance(item, Mapping)
        and item.get("kind") in {"manifest", "authoring_fragment", "sql_file", "recipe", "profile", "component"}
        and item.get("path")
        and item.get("sha256")
    ]


def _pin(kind: str, path: str, digest: str) -> tuple[str, str, str]:
    return kind, path, _digest(digest)


def _digest(value: str) -> str:
    return "sha256:" + value.removeprefix("sha256:").lower()


__all__ = [
    "AuthoringDependencyIntegrityError",
    "PrimaryAuthoringSourceIntegrityError",
    "source_file_provenance",
    "verify_authoring_dependency_parity",
]
