"""Public dependency-light API for deterministic dbt project bundles."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path, PurePosixPath

from dpone.contracts.dbt_contract_validation import DbtPublishingError
from dpone.contracts.dbt_project_bundle import (
    DEFAULT_DBT_PROJECT_BUNDLE_LIMITS,
    DbtProjectBundle,
    DbtProjectBundleArtifact,
    DbtProjectBundleLimits,
)
from dpone.runtime.dbt_project_bundle_archive import (
    archive_bytes,
    extract_archive,
    inspect_archive,
    observed_tree,
    require_empty_directory,
)
from dpone.runtime.dbt_project_bundle_source import build_archive


def build_dbt_project_bundle(
    project_root: Path,
    *,
    limits: DbtProjectBundleLimits = DEFAULT_DBT_PROJECT_BUNDLE_LIMITS,
    package_environment: Mapping[str, str] | None = None,
) -> DbtProjectBundleArtifact:
    """Capture one stable no-follow project snapshot as canonical tar+gzip bytes."""

    return build_archive(
        Path(project_root),
        _validated_limits(limits),
        package_environment={} if package_environment is None else package_environment,
    )


def extract_dbt_project_bundle(
    bundle_path_or_bytes: bytes | Path,
    destination: Path,
    *,
    limits: DbtProjectBundleLimits = DEFAULT_DBT_PROJECT_BUNDLE_LIMITS,
) -> DbtProjectBundle:
    """Validate the complete archive, then extract its root-relative file tree."""

    checked_limits = _validated_limits(limits)
    destination = Path(destination).absolute()
    destination.mkdir(parents=True, exist_ok=True, mode=0o700)
    require_empty_directory(destination)
    data = archive_bytes(bundle_path_or_bytes, checked_limits)
    bundle = inspect_archive(data, checked_limits)
    extract_archive(data, destination, checked_limits)
    verify_dbt_project_bundle_tree(data, destination, limits=checked_limits)
    return bundle


def verify_dbt_project_bundle_tree(
    bundle_path_or_bytes: bytes | Path,
    destination: Path,
    *,
    limits: DbtProjectBundleLimits = DEFAULT_DBT_PROJECT_BUNDLE_LIMITS,
) -> DbtProjectBundle:
    """Reconcile every extracted path, mode, byte count and digest with the archive."""

    checked_limits = _validated_limits(limits)
    data = archive_bytes(bundle_path_or_bytes, checked_limits)
    bundle = inspect_archive(data, checked_limits)
    files, directories = observed_tree(Path(destination).absolute(), checked_limits)
    expected_directories = {
        parent.as_posix()
        for item in bundle.files
        for parent in PurePosixPath(item.path).parents
        if parent != PurePosixPath(".")
    }
    if files != bundle.files or directories != expected_directories:
        raise DbtPublishingError(
            "DPONE_DBT_BUNDLE_TREE_MISMATCH",
            "extracted dbt project differs from the verified bundle inventory",
        )
    return bundle


def _validated_limits(value: DbtProjectBundleLimits) -> DbtProjectBundleLimits:
    if not isinstance(value, DbtProjectBundleLimits):
        raise TypeError("limits must be a DbtProjectBundleLimits")
    return value


__all__ = [
    "build_dbt_project_bundle",
    "extract_dbt_project_bundle",
    "verify_dbt_project_bundle_tree",
]
