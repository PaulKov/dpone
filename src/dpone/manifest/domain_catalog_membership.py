"""Pure helpers for domain GitOps catalog membership paths and warning snippets."""

from __future__ import annotations

from pathlib import Path, PurePosixPath

from dpone.manifest.project_config import ProjectLayout

_DEFAULT_SYSTEM_ROOT = ".dpone"


def domain_catalog_path(layout: ProjectLayout, domain: str) -> Path:
    system_root = layout.system_root or _DEFAULT_SYSTEM_ROOT
    return Path(system_root) / "config" / "domains" / f"{domain}.yaml"


def catalog_manifest_ref(catalog_path: Path, pipeline_path: Path) -> str:
    catalog_dir = PurePosixPath(catalog_path.as_posix()).parent
    ascend = "/".join(".." for _ in catalog_dir.parts)
    return f"{ascend}/{pipeline_path.as_posix()}"


def catalog_membership_entry_text(
    pipeline_id: str,
    manifest_ref: str,
    *,
    key_indent: str = "  ",
    value_indent: str | None = None,
) -> str:
    manifest_indent = value_indent if value_indent is not None else f"{key_indent}  "
    return f"{key_indent}{pipeline_id}:\n{manifest_indent}manifest: {manifest_ref}\n"


def membership_catalog_snippet(
    layout: ProjectLayout,
    *,
    domain: str,
    pipeline_id: str,
    pipeline_path: Path,
) -> tuple[str, str]:
    """Return ``(catalog_rel_path, workloads_entry_text)`` for warning/fix hints."""

    catalog_path = domain_catalog_path(layout, domain)
    manifest_ref = catalog_manifest_ref(catalog_path, pipeline_path)
    return catalog_path.as_posix(), catalog_membership_entry_text(pipeline_id, manifest_ref).rstrip("\n")


__all__ = [
    "catalog_manifest_ref",
    "catalog_membership_entry_text",
    "domain_catalog_path",
    "membership_catalog_snippet",
]
