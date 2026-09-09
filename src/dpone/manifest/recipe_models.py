"""Immutable public contracts for declarative recipe resolution."""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, Protocol

from dpone.manifest.authoring_folder import AuthoringSourceDependency

_REF_RE = re.compile(r"[a-z][a-z0-9-]{0,62}@[0-9]+\.[0-9]+\.[0-9]+\Z")
_DIGEST_RE = re.compile(r"sha256:[0-9a-f]{64}\Z")
_ID_RE = re.compile(r"[a-z][a-z0-9-]{0,62}\Z")


class RecipeResolutionError(ValueError):
    """A stable, data-safe recipe resolution failure."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True, slots=True)
class RecipeArtifactPin:
    """An exact project-confined recipe artifact identity."""

    ref: str
    artifact_ref: str
    sha256: str

    @classmethod
    def from_mapping(cls, raw: object, *, label: str) -> RecipeArtifactPin:
        if not isinstance(raw, Mapping) or set(raw) != {"ref", "artifact_ref", "sha256"}:
            raise RecipeResolutionError(
                "DPONE_RECIPE_ARTIFACT_INVALID",
                f"{label} must contain exactly ref, artifact_ref, and sha256.",
            )
        ref = validate_exact_ref(raw.get("ref"), label=label)
        artifact_ref = validate_artifact_ref(raw.get("artifact_ref"), label=label)
        digest = validate_digest(raw.get("sha256"), label=label)
        return cls(ref=ref, artifact_ref=artifact_ref, sha256=digest)

    def to_jsonable(self) -> dict[str, str]:
        return {"ref": self.ref, "artifact_ref": self.artifact_ref, "sha256": self.sha256}


@dataclass(frozen=True, slots=True)
class RecipeContext:
    """Non-secret context available to whole-value component placeholders."""

    pipeline_id: str
    domain: str


@dataclass(frozen=True, slots=True)
class RecipeResolutionProvenance:
    """Safe lineage for one exact declarative recipe closure."""

    catalog_id: str
    recipe_ref: str
    recipe_sha256: str
    domain: str
    profile_ref: str | None
    component_refs: tuple[str, ...]
    deprecated_refs: tuple[str, ...]
    status: str
    owner: str
    closure: tuple[AuthoringSourceDependency, ...]

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "catalog_id": self.catalog_id,
            "recipe_ref": self.recipe_ref,
            "recipe_sha256": self.recipe_sha256,
            "domain": self.domain,
            "profile_ref": self.profile_ref,
            "component_refs": list(self.component_refs),
            "deprecated_refs": list(self.deprecated_refs),
            "status": self.status,
            "owner": self.owner,
            "closure": [item.to_jsonable() for item in self.closure],
        }


@dataclass(frozen=True, slots=True)
class ResolvedRecipe:
    """Expanded data-only processes and exact source dependencies."""

    processes: tuple[Mapping[str, Any], ...]
    dependencies: tuple[AuthoringSourceDependency, ...]
    provenance: RecipeResolutionProvenance


class RecipeSourceResolver(Protocol):
    """Port injected into the canonical authoring compiler."""

    def resolve(
        self,
        recipe_block: Mapping[str, object],
        *,
        source_path: Path,
        project_root: Path | None,
        context: RecipeContext,
    ) -> ResolvedRecipe: ...


def validate_exact_ref(value: object, *, label: str = "recipe ref") -> str:
    if not isinstance(value, str) or _REF_RE.fullmatch(value) is None:
        raise RecipeResolutionError(
            "DPONE_RECIPE_REF_INVALID",
            f"{label} must use exact id@MAJOR.MINOR.PATCH syntax.",
        )
    return value


def split_exact_ref(value: str) -> tuple[str, str]:
    validate_exact_ref(value)
    return tuple(value.rsplit("@", 1))  # type: ignore[return-value]


def validate_artifact_ref(value: object, *, label: str) -> str:
    if not isinstance(value, str) or not value or "\\" in value or ":" in value:
        raise _artifact_error(label)
    path = PurePosixPath(value)
    if path.is_absolute() or ".." in path.parts or "." in path.parts or path.suffix not in {".yaml", ".yml"}:
        raise _artifact_error(label)
    return path.as_posix()


def validate_digest(value: object, *, label: str) -> str:
    if not isinstance(value, str) or _DIGEST_RE.fullmatch(value) is None:
        raise RecipeResolutionError(
            "DPONE_RECIPE_ARTIFACT_INVALID",
            f"{label} sha256 must be an exact lowercase SHA-256 digest.",
        )
    return value


def validate_logical_id(value: object, *, label: str) -> str:
    if not isinstance(value, str) or _ID_RE.fullmatch(value) is None:
        raise RecipeResolutionError("DPONE_RECIPE_ARTIFACT_INVALID", f"{label} is invalid.")
    return value


def _artifact_error(label: str) -> RecipeResolutionError:
    return RecipeResolutionError(
        "DPONE_RECIPE_ARTIFACT_INVALID",
        f"{label} artifact_ref must be a project-relative POSIX YAML path.",
    )


__all__ = [
    "RecipeArtifactPin",
    "RecipeContext",
    "RecipeResolutionError",
    "RecipeResolutionProvenance",
    "RecipeSourceResolver",
    "ResolvedRecipe",
    "split_exact_ref",
    "validate_artifact_ref",
    "validate_digest",
    "validate_exact_ref",
    "validate_logical_id",
]
