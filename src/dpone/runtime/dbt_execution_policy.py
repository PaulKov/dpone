"""Pure command, identity and filesystem safety policy for dbt execution."""

from __future__ import annotations

import os
import stat
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING

from dpone.contracts.dbt_publishing import DbtPublishingError
from dpone.contracts.dbt_toolchain import DBT_SQLSERVER_1_12_CERTIFIED
from dpone.runtime.dbt_execution_failures import dbt_failure_code, runtime_failure_code

if TYPE_CHECKING:
    from dpone.contracts.dbt_runtime import DbtExecutionPack
    from dpone.ports.dbt_publishing import DbtInstalledToolchain

_DIRECTORY_FLAGS = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
DEFAULT_DBT_RUN_OUTPUT_ROOT = Path("/var/lib/dpone/run")


@dataclass(frozen=True, slots=True)
class DbtExecutionOutputPaths:
    """Absolute writable paths kept outside the extracted dbt project."""

    root: Path
    attempt: Path
    preflight_target: Path
    preflight_logs: Path
    target: Path
    logs: Path


def build_dbt_command(
    pack: DbtExecutionPack,
    *,
    project_dir: Path,
    profile_path: Path,
    target_path: Path,
    log_path: Path,
    interval_vars_json: str,
) -> tuple[str, ...]:
    """Build the fixed shell-free dbt invocation."""

    command = [
        "dbt",
    ]
    if pack.dbt_warning_policy == "fail":
        command.append("--warn-error")
    command.extend(
        (
            "build",
            "--project-dir",
            str(project_dir),
            "--profiles-dir",
            str(profile_path.parent),
            "--profile",
            pack.profile.profile_name,
            "--target",
            pack.profile.target_name,
            "--target-path",
            str(target_path),
            "--log-path",
            str(log_path),
            "--indirect-selection",
            pack.invocation_context.indirect_selection,
            "--select",
            *pack.selection_lock.selectors,
            "--vars",
            interval_vars_json,
        )
    )
    return tuple(command)


def build_dbt_parse_command(
    pack: DbtExecutionPack,
    *,
    project_dir: Path,
    profile_path: Path,
    target_path: Path,
    log_path: Path,
    interval_vars_json: str,
) -> tuple[str, ...]:
    """Build the non-mutating runtime parse command."""

    return (
        "dbt",
        "--quiet",
        "--no-use-colors",
        "parse",
        *_dbt_location(
            pack,
            project_dir=project_dir,
            profile_path=profile_path,
            target_path=target_path,
            log_path=log_path,
        ),
        "--vars",
        interval_vars_json,
    )


def build_dbt_ls_command(
    pack: DbtExecutionPack,
    *,
    project_dir: Path,
    profile_path: Path,
    target_path: Path,
    log_path: Path,
    interval_vars_json: str,
) -> tuple[str, ...]:
    """Build the exact non-mutating selected-graph probe."""

    return (
        "dbt",
        "--quiet",
        "--no-use-colors",
        "ls",
        *_dbt_location(
            pack,
            project_dir=project_dir,
            profile_path=profile_path,
            target_path=target_path,
            log_path=log_path,
        ),
        "--resource-type",
        "model",
        "seed",
        "snapshot",
        "test",
        "unit_test",
        "--indirect-selection",
        pack.invocation_context.indirect_selection,
        "--output",
        "json",
        "--output-keys",
        "unique_id",
        "--select",
        *pack.selection_lock.selectors,
        "--vars",
        interval_vars_json,
    )


def prepare_dbt_output_paths(
    run_output_root: Path,
    target_path: str,
    *,
    attempt_id: str,
) -> DbtExecutionOutputPaths:
    """Create only attempt-scoped preflight paths before identity is proven."""

    root = Path(run_output_root).absolute()
    target_parts = PurePosixPath(target_path).parts
    if (
        not isinstance(attempt_id, str)
        or len(attempt_id) != 32
        or any(character not in "0123456789abcdef" for character in attempt_id)
    ):
        raise DbtPublishingError(
            "DPONE_DBT_EXECUTION_FAILED",
            "dbt attempt identity is invalid",
        )
    attempt_parts = ("attempts", attempt_id)
    preflight_target_parts = (*attempt_parts, "preflight", "target")
    preflight_logs_parts = (*attempt_parts, "preflight", "logs")
    _ensure_confined_directory(root, preflight_target_parts)
    _ensure_confined_directory(root, preflight_logs_parts)
    return DbtExecutionOutputPaths(
        root=root,
        attempt=root.joinpath(*attempt_parts),
        preflight_target=root.joinpath(*preflight_target_parts),
        preflight_logs=root.joinpath(*preflight_logs_parts),
        target=root.joinpath(*attempt_parts, *target_parts),
        logs=root.joinpath(*attempt_parts, "logs"),
    )


def prepare_dbt_build_output_paths(paths: DbtExecutionOutputPaths) -> None:
    """Create final build paths only after preflight succeeds."""

    target_parts = paths.target.relative_to(paths.root).parts
    log_parts = paths.logs.relative_to(paths.root).parts
    _ensure_confined_directory(paths.root, target_parts)
    _ensure_confined_directory(paths.root, log_parts)


def _dbt_location(
    pack: DbtExecutionPack,
    *,
    project_dir: Path,
    profile_path: Path,
    target_path: Path,
    log_path: Path,
) -> tuple[str, ...]:
    return (
        "--project-dir",
        str(project_dir),
        "--profiles-dir",
        str(profile_path.parent),
        "--profile",
        pack.profile.profile_name,
        "--target",
        pack.profile.target_name,
        "--target-path",
        str(target_path),
        "--log-path",
        str(log_path),
    )


def validate_dbt_toolchain(
    pack: DbtExecutionPack,
    installed: DbtInstalledToolchain,
) -> None:
    """Reject selection or runtime toolchain drift."""

    expected_digest = DBT_SQLSERVER_1_12_CERTIFIED.sha256
    if pack.selection_lock.toolchain_sha256 != expected_digest:
        raise DbtPublishingError(
            "DPONE_DBT_EXECUTION_FAILED",
            "dbt selection toolchain does not match the pinned execution pack",
        )
    expected = (
        pack.dbt_core_version,
        pack.profile.adapter_type,
        pack.dbt_adapter_version,
    )
    actual = (
        installed.dbt_core_version,
        installed.adapter_name,
        installed.adapter_version,
    )
    if actual != expected:
        raise DbtPublishingError(
            "DPONE_DBT_EXECUTION_FAILED",
            "Installed dbt runtime does not match the pinned execution pack",
        )


def confined_dbt_project_directory(
    runtime_root: Path,
    project_subdir: str,
) -> Path:
    """Resolve a project directory without following symlinks."""

    root = runtime_root.absolute()
    try:
        descriptor = os.open(root, _DIRECTORY_FLAGS)
    except OSError as exc:
        raise DbtPublishingError(
            "DPONE_DBT_BUNDLE_INVALID",
            "dbt runtime root is unsafe",
        ) from exc
    current = descriptor
    try:
        for part in PurePosixPath(project_subdir).parts:
            child = os.open(part, _DIRECTORY_FLAGS, dir_fd=current)
            if current != descriptor:
                os.close(current)
            current = child
        if not stat.S_ISDIR(os.fstat(current).st_mode):
            raise OSError
    except OSError as exc:
        raise DbtPublishingError(
            "DPONE_DBT_BUNDLE_INVALID",
            "dbt project path is unsafe",
        ) from exc
    finally:
        if current != descriptor:
            os.close(current)
        os.close(descriptor)
    return root if project_subdir == "." else root.joinpath(*PurePosixPath(project_subdir).parts)


def _ensure_confined_directory(root: Path, parts: tuple[str, ...]) -> None:
    try:
        root_descriptor = os.open(root, _DIRECTORY_FLAGS)
    except OSError as exc:
        raise DbtPublishingError(
            "DPONE_DBT_EXECUTION_FAILED",
            "dbt run output root is unavailable or unsafe",
        ) from exc
    current = root_descriptor
    try:
        for part in parts:
            try:
                child = os.open(part, _DIRECTORY_FLAGS, dir_fd=current)
            except FileNotFoundError:
                try:
                    os.mkdir(part, mode=0o700, dir_fd=current)
                except FileExistsError:
                    pass
                child = os.open(part, _DIRECTORY_FLAGS, dir_fd=current)
            if current != root_descriptor:
                os.close(current)
            current = child
    except OSError as exc:
        raise DbtPublishingError(
            "DPONE_DBT_EXECUTION_FAILED",
            "dbt run output path is unavailable or unsafe",
        ) from exc
    finally:
        if current != root_descriptor:
            os.close(current)
        os.close(root_descriptor)


def discard_previous_dbt_run_results(
    output_root: Path,
    target_path: str,
) -> None:
    """Remove only a regular prior result without following target symlinks."""

    try:
        root_descriptor = os.open(output_root, _DIRECTORY_FLAGS)
    except OSError as exc:
        raise DbtPublishingError(
            "DPONE_DBT_RUN_RESULTS_INVALID",
            "dbt target path is unsafe",
        ) from exc
    current = root_descriptor
    try:
        for part in PurePosixPath(target_path).parts:
            try:
                child = os.open(part, _DIRECTORY_FLAGS, dir_fd=current)
            except FileNotFoundError:
                return
            if current != root_descriptor:
                os.close(current)
            current = child
        try:
            metadata = os.stat(
                "run_results.json",
                dir_fd=current,
                follow_symlinks=False,
            )
        except FileNotFoundError:
            return
        if not stat.S_ISREG(metadata.st_mode):
            raise DbtPublishingError(
                "DPONE_DBT_RUN_RESULTS_INVALID",
                "previous dbt run-results is unsafe",
            )
        os.unlink("run_results.json", dir_fd=current)
    except OSError as exc:
        raise DbtPublishingError(
            "DPONE_DBT_RUN_RESULTS_INVALID",
            "previous dbt run-results cannot be removed",
        ) from exc
    finally:
        if current != root_descriptor:
            os.close(current)
        os.close(root_descriptor)


def aware_timestamp(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise DbtPublishingError(
            "DPONE_DBT_EXECUTION_FAILED",
            "runtime clock must return an aware datetime",
        )
    return value.isoformat().replace("+00:00", "Z")


__all__ = [
    "DEFAULT_DBT_RUN_OUTPUT_ROOT",
    "DbtExecutionOutputPaths",
    "aware_timestamp",
    "build_dbt_command",
    "build_dbt_ls_command",
    "build_dbt_parse_command",
    "confined_dbt_project_directory",
    "dbt_failure_code",
    "discard_previous_dbt_run_results",
    "prepare_dbt_output_paths",
    "prepare_dbt_build_output_paths",
    "runtime_failure_code",
    "validate_dbt_toolchain",
]
