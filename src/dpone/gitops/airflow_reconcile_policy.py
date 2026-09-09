"""Validate Airflow reconcile selection and confined artifact inputs."""

from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path
from typing import Protocol

from dpone.gitops.changed_files import resolve_changed_files
from dpone.gitops.paths import GitOpsPathValidationError, confined_repo_file_path, confined_repo_path
from dpone.gitops.workload_catalog_models import GitOpsWorkloadCatalogIssue, issue

RECONCILE_SOURCE = "dpone gitops airflow reconcile"
_PUBLIC_CONNECTION_PROJECTION_CODES = frozenset(
    {
        "DPONE_AIRFLOW_CONNECTION_PROJECTION_ENTRY_INVALID",
        "DPONE_AIRFLOW_CONNECTION_PROJECTION_OVERRIDE_CONFLICT",
        "DPONE_AIRFLOW_CONNECTION_PROJECTION_REF_AMBIGUOUS",
        "DPONE_AIRFLOW_CONNECTION_PROJECTION_REF_MISSING",
    }
)


class _ChangedFilesReader(Protocol):
    def read_text(self, path: Path, *, encoding: str = "utf-8") -> str: ...


def resolve_output_dir(
    args: object,
    *,
    repo_root: Path,
) -> tuple[Path, GitOpsWorkloadCatalogIssue | None]:
    raw_output_dir = getattr(args, "output_dir", ".dpone/gitops") or ".dpone/gitops"
    try:
        output_dir, _destination = confined_repo_path(repo_root, raw_output_dir, source="--output-dir")
        return output_dir, None
    except GitOpsPathValidationError as exc:
        return Path("."), _input_path_issue(raw_path=str(raw_output_dir), message=str(exc))


def resolve_output_mirror_blocker(
    args: object,
    *,
    repo_root: Path,
    reserved_paths: Iterable[str | Path] = (),
) -> GitOpsWorkloadCatalogIssue | None:
    raw_output = getattr(args, "output", None)
    if not raw_output:
        return None
    try:
        _relative, destination = confined_repo_file_path(repo_root, str(raw_output), source="--output")
        for reserved_path in reserved_paths:
            _reserved_relative, reserved_destination = confined_repo_path(
                repo_root,
                reserved_path,
                source="generated artifact",
            )
            if _paths_overlap(destination, reserved_destination):
                raise GitOpsPathValidationError(
                    f"--output conflicts with generated artifact path: {_reserved_relative.as_posix()}"
                )
    except GitOpsPathValidationError as exc:
        return _input_path_issue(raw_path=str(raw_output), message=str(exc))
    return None


def validate_artifact_path(
    *,
    repo_root: Path,
    raw_path: str | Path,
) -> GitOpsWorkloadCatalogIssue | None:
    try:
        confined_repo_file_path(repo_root, raw_path, source="--output-dir")
    except GitOpsPathValidationError as exc:
        return _input_path_issue(raw_path=str(raw_path), message=str(exc))
    return None


def has_changed_file_selection(args: object) -> bool:
    return bool(getattr(args, "changed_files", None)) or getattr(args, "changed_files_file", None) is not None


def build_failure_issue(
    *,
    workload_id: str,
    manifest_path: str,
    exc: Exception,
) -> GitOpsWorkloadCatalogIssue:
    code = str(getattr(exc, "code", "") or "")
    if code in _PUBLIC_CONNECTION_PROJECTION_CODES:
        return issue(
            code=code,
            message=str(exc),
            path=manifest_path or workload_id,
            source=RECONCILE_SOURCE,
        )
    return issue(
        code="reconcile_artifact_build_failed",
        message=(
            f"Artifact build failed ({type(exc).__name__}). "
            "Verify the manifest and workload configuration at the reported path."
        ),
        path=manifest_path or workload_id,
        source=RECONCILE_SOURCE,
    )


def changed_files(
    *,
    fs: _ChangedFilesReader,
    repo_root: Path,
    args: object,
) -> tuple[tuple[str, ...], tuple[GitOpsWorkloadCatalogIssue, ...]]:
    selected, _warnings, blockers = resolve_changed_files(
        fs=fs,
        args=args,
        repo_root=repo_root,
        require_input=False,
    )
    return selected, tuple(
        issue(
            code=item.code,
            message=item.message,
            path=item.path,
            source=RECONCILE_SOURCE,
        )
        for item in blockers
    )


def _input_path_issue(*, raw_path: str, message: str) -> GitOpsWorkloadCatalogIssue:
    return issue(
        code="invalid_path",
        message=message,
        path=raw_path,
        source=RECONCILE_SOURCE,
    )


def _paths_overlap(left: Path, right: Path) -> bool:
    return left == right or left in right.parents or right in left.parents


__all__ = [
    "RECONCILE_SOURCE",
    "build_failure_issue",
    "changed_files",
    "has_changed_file_selection",
    "resolve_output_dir",
    "resolve_output_mirror_blocker",
    "validate_artifact_path",
]
