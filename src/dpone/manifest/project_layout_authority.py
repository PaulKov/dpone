"""Canonical no-follow classification of project authoring authority."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from dpone.manifest.confined_files import ConfinedFileError, sha256_confined_file
from dpone.manifest.project_discovery_namespace import observe_namespace, path_kind

AuthoringLayoutState = Literal["empty", "flat", "domain_first", "mixed", "unsafe"]
ProjectLayoutMode = Literal["flat", "domain_first"]
SUPPORTED_PROJECT_LAYOUTS = frozenset({"flat", "domain_first"})


@dataclass(frozen=True, slots=True)
class AuthoringAuthorityGuard:
    """CAS snapshot for bounded scaffold reads and final layout authority."""

    root: Path
    domain_first_root: str
    before_layout: AuthoringLayoutState
    before_config_sha256: str | None
    after_config_sha256: str | None
    allowed_after_layouts: frozenset[AuthoringLayoutState]

    def before_apply(self) -> bool:
        return (
            detect_authoring_layout(self.root, domain_first_root=self.domain_first_root) == self.before_layout
            and _project_config_digest(self.root) == self.before_config_sha256
        )

    def after_apply(self) -> bool:
        return (
            detect_authoring_layout(self.root, domain_first_root=self.domain_first_root) in self.allowed_after_layouts
            and _project_config_digest(self.root) == self.after_config_sha256
        )


def detect_authoring_layout(
    root: Path,
    *,
    domain_first_root: str = "workloads",
) -> AuthoringLayoutState:
    """Classify existing authoring authority without following filesystem links."""

    root_kind = path_kind(root)
    if root_kind == "missing":
        return "empty"
    if root_kind != "directory":
        return "unsafe"
    try:
        canonical_root = root.resolve(strict=True)
    except OSError:
        return "unsafe"
    flat = _authority_present(canonical_root, canonical_root / "pipelines")
    flat_domains = _authority_present(canonical_root, canonical_root / "domains")
    domain_first = _domain_first_authority(
        canonical_root,
        canonical_root.joinpath(*Path(domain_first_root).parts),
    )
    states = (flat, flat_domains, domain_first)
    if "unsafe" in states:
        return "unsafe"
    has_flat = flat == "present" or flat_domains == "present"
    has_domain_first = domain_first == "present"
    if has_flat and has_domain_first:
        return "mixed"
    if has_flat:
        return "flat"
    if has_domain_first:
        return "domain_first"
    return "empty"


def conflicting_authoring_layout(
    root: Path,
    *,
    expected_mode: str,
    domain_first_root: str,
) -> AuthoringLayoutState | None:
    """Return the observed authority when it conflicts with the configured layout."""

    detected = detect_authoring_layout(root, domain_first_root=domain_first_root)
    if detected in {"mixed", "unsafe"}:
        return detected
    if detected != "empty" and detected != expected_mode:
        return detected
    return None


def authoring_layout_is_compatible(
    root: Path,
    expected_mode: str,
    domain_first_root: str,
) -> bool:
    """Return whether existing authority can be read under the configured layout."""

    return (
        conflicting_authoring_layout(
            root,
            expected_mode=expected_mode,
            domain_first_root=domain_first_root,
        )
        is None
    )


def normalize_project_layout_option(value: str | None) -> ProjectLayoutMode | None:
    """Return the canonical layout name accepted by public authoring."""

    if value is None:
        return None
    normalized = value.strip().lower().replace("-", "_")
    if normalized == "flat":
        return "flat"
    if normalized == "domain_first":
        return "domain_first"
    return None


def _authority_present(root: Path, path: Path) -> Literal["empty", "present", "unsafe"]:
    observation = observe_namespace(root, path, limit=1_000)
    if observation is None:
        return "unsafe"
    if observation.kind == "missing":
        return "empty"
    if observation.kind != "directory":
        return "unsafe"
    return "present" if observation.children else "empty"


def _domain_first_authority(
    root: Path,
    path: Path,
) -> Literal["empty", "present", "unsafe"]:
    root_observation = observe_namespace(root, path, limit=500)
    if root_observation is None:
        return "unsafe"
    if root_observation.kind == "missing":
        return "empty"
    if root_observation.kind != "directory":
        return "unsafe"
    for domain_path in root_observation.children:
        domain = observe_namespace(root, domain_path, limit=128)
        if domain is None:
            return "unsafe"
        if domain.kind == "symlink":
            return "unsafe"
        if domain.kind != "directory":
            continue
        children = {child.name: child for child in domain.children}
        if "ownership.yaml" in children:
            return "present"
        pipelines_path = children.get("pipelines")
        if pipelines_path is None:
            continue
        pipelines = observe_namespace(root, pipelines_path, limit=1_000)
        if pipelines is None or pipelines.kind != "directory":
            return "unsafe"
        for pipeline_path in pipelines.children:
            pipeline = observe_namespace(root, pipeline_path, limit=128)
            if pipeline is None:
                return "unsafe"
            if pipeline.kind == "symlink":
                return "unsafe"
            if pipeline.kind == "directory" and any(child.name == "pipeline.yaml" for child in pipeline.children):
                return "present"
    return "empty"


def _project_config_digest(root: Path) -> str | None:
    try:
        return sha256_confined_file(root, "dpone.yaml")
    except ConfinedFileError as exc:
        return None if exc.code == "file_not_found" else "unsafe"
    except OSError:
        return "unsafe"


__all__ = [
    "AuthoringAuthorityGuard",
    "AuthoringLayoutState",
    "ProjectLayoutMode",
    "SUPPORTED_PROJECT_LAYOUTS",
    "authoring_layout_is_compatible",
    "conflicting_authoring_layout",
    "detect_authoring_layout",
    "normalize_project_layout_option",
]
