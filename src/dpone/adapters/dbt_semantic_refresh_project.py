"""Filesystem adapter for attempt-local dbt V2 project materialization."""

from __future__ import annotations

import hashlib
import json
import shutil
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory

import yaml

from dpone.contracts.dbt_publishing import DbtPublishingError
from dpone.contracts.dbt_semantic_refresh_project_overlay import dbt_project_models_config

_PACKAGE_FILES = frozenset(
    {
        "dbt_project.yml",
        "macros/dpone_publish.sql",
        "macros/semantic_refresh_restore.sql",
        "macros/semantic_refresh_scope_merge.sql",
    }
)
_MAX_PACKAGE_FILE_BYTES = 2 * 1024 * 1024
_MAX_PROJECT_FILES = 10_000
_DIGEST_PREFIX = "sha256:"


@dataclass(frozen=True, slots=True)
class SemanticRefreshDbtRuntimeAuthority:
    """Protected run and package authority captured by the Airflow callable."""

    workflow_execution_id: str
    workflow_execution_binding_sha256: str
    profile_sha256: str
    topology_sha256: str
    package_artifacts_sha256: str
    package_source_root: Path

    def __post_init__(self) -> None:
        if not isinstance(self.workflow_execution_id, str) or not self.workflow_execution_id.strip():
            raise _error("DPONE_DBT_V2_RUNTIME_AUTHORITY_INVALID", "workflow execution identity is absent")
        for field_name in (
            "workflow_execution_binding_sha256",
            "profile_sha256",
            "topology_sha256",
            "package_artifacts_sha256",
        ):
            _require_digest(getattr(self, field_name), field_name)
        if not isinstance(self.package_source_root, Path):
            raise _error("DPONE_DBT_V2_RUNTIME_AUTHORITY_INVALID", "package source root must be a path")


def semantic_refresh_package_sha256(package_root: Path) -> str:
    """Hash the closed executable dbt-dpone package inventory."""

    root = Path(package_root).absolute()
    observed = _regular_file_inventory(root, maximum_files=len(_PACKAGE_FILES))
    if observed != _PACKAGE_FILES:
        raise _error(
            "DPONE_DBT_V2_PACKAGE_INVALID",
            "platform dbt package inventory is incomplete or contains unexpected files",
        )
    files = []
    for relative in sorted(observed):
        content = _bounded_bytes(root / relative, maximum=_MAX_PACKAGE_FILE_BYTES)
        files.append(
            {
                "path": relative,
                "sha256": _DIGEST_PREFIX + hashlib.sha256(content).hexdigest(),
            }
        )
    return _canonical_sha256(
        {
            "files": files,
            "schema": "dpone.dbt-semantic-refresh-package-artifacts.v1",
        }
    )


@contextmanager
def materialize_semantic_refresh_project(
    *,
    runtime_root: Path,
    project_subdir: str,
    execution_pack_bytes: bytes,
    project_config_overlay: Mapping[str, object],
    authority: SemanticRefreshDbtRuntimeAuthority,
) -> Iterator[Path]:
    """Yield a temporary runtime root with verified overlay and package injection."""

    source_root = Path(runtime_root).absolute()
    source_project = _confined_project(source_root, project_subdir)
    if semantic_refresh_package_sha256(authority.package_source_root) != authority.package_artifacts_sha256:
        raise _error(
            "DPONE_DBT_V2_PACKAGE_INVALID",
            "platform dbt package authority differs from protected lifecycle policy",
        )
    try:
        overlay = dbt_project_models_config(project_config_overlay)
    except ValueError as exc:
        raise _error(
            "DPONE_DBT_V2_OVERLAY_INVALID",
            "semantic-refresh project overlay differs from activated topology authority",
        ) from exc
    _regular_file_inventory(source_project, maximum_files=_MAX_PROJECT_FILES)
    with TemporaryDirectory(prefix="dpone-dbt-semantic-refresh-") as temporary:
        attempt_root = Path(temporary).absolute()
        attempt_project = attempt_root / project_subdir
        attempt_project.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(source_project, attempt_project)
        _apply_project_overlay(attempt_project, overlay)
        _inject_package(attempt_project, authority)
        pack_path = attempt_root / "runtime/dbt-execution-pack.json"
        pack_path.parent.mkdir(parents=True, exist_ok=True)
        pack_path.write_bytes(execution_pack_bytes)
        yield attempt_root


def _apply_project_overlay(project: Path, overlay: Mapping[str, object]) -> None:
    project_file = project / "dbt_project.yml"
    try:
        raw = yaml.safe_load(_bounded_bytes(project_file, maximum=_MAX_PACKAGE_FILE_BYTES))
    except yaml.YAMLError as exc:
        raise _error("DPONE_DBT_V2_OVERLAY_INVALID", "dbt_project.yml is not valid YAML") from exc
    if not isinstance(raw, Mapping):
        raise _error("DPONE_DBT_V2_OVERLAY_INVALID", "dbt_project.yml must be an object")
    project_value = dict(raw)
    current_models = project_value.get("models", {})
    if not isinstance(current_models, Mapping):
        raise _error("DPONE_DBT_V2_OVERLAY_INVALID", "dbt project models config must be an object")
    project_value["models"] = _merge_mapping(current_models, _mapping(overlay.get("models")))
    project_file.write_text(
        yaml.safe_dump(project_value, allow_unicode=True, sort_keys=False),
        encoding="utf-8",
    )


def _inject_package(project: Path, authority: SemanticRefreshDbtRuntimeAuthority) -> None:
    raw = yaml.safe_load((project / "dbt_project.yml").read_text(encoding="utf-8"))
    if not isinstance(raw, Mapping):
        raise _error("DPONE_DBT_V2_OVERLAY_INVALID", "materialized dbt project is invalid")
    install_path = raw.get("packages-install-path", "dbt_packages")
    if not isinstance(install_path, str) or not install_path or Path(install_path).is_absolute():
        raise _error("DPONE_DBT_V2_PACKAGE_INVALID", "dbt packages-install-path is not confined")
    parts = Path(install_path).parts
    if any(part in {"", ".", ".."} for part in parts):
        raise _error("DPONE_DBT_V2_PACKAGE_INVALID", "dbt packages-install-path is not confined")
    target = project.joinpath(*parts, "dbt_dpone")
    if target.exists():
        shutil.rmtree(target)
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(authority.package_source_root, target)
    if semantic_refresh_package_sha256(target) != authority.package_artifacts_sha256:
        raise _error("DPONE_DBT_V2_PACKAGE_INVALID", "materialized dbt package digest differs")


def _merge_mapping(base: Mapping[str, object], overlay: Mapping[str, object]) -> dict[str, object]:
    result = dict(base)
    for key, value in overlay.items():
        current = result.get(key)
        if isinstance(value, Mapping):
            if current is not None and not isinstance(current, Mapping):
                raise _error("DPONE_DBT_V2_OVERLAY_INVALID", "project overlay collides with scalar config")
            result[key] = _merge_mapping({} if current is None else current, value)
        else:
            result[key] = value
    return result


def _confined_project(root: Path, project_subdir: str) -> Path:
    if not isinstance(project_subdir, str) or not project_subdir:
        raise _error("DPONE_DBT_V2_OVERLAY_INVALID", "project subdirectory is absent")
    relative = Path(project_subdir)
    if relative.is_absolute() or any(part in {"", ".", ".."} for part in relative.parts):
        raise _error("DPONE_DBT_V2_OVERLAY_INVALID", "project subdirectory is not confined")
    project = root.joinpath(*relative.parts)
    if not project.is_dir() or project.is_symlink():
        raise _error("DPONE_DBT_V2_OVERLAY_INVALID", "bundled dbt project is unavailable")
    return project


def _regular_file_inventory(root: Path, *, maximum_files: int) -> frozenset[str]:
    if not root.is_dir() or root.is_symlink():
        raise _error("DPONE_DBT_V2_PACKAGE_INVALID", "runtime source directory is unavailable")
    result: set[str] = set()
    for path in root.rglob("*"):
        if path.is_symlink():
            raise _error("DPONE_DBT_V2_PACKAGE_INVALID", "runtime source contains a symbolic link")
        if path.is_file():
            result.add(path.relative_to(root).as_posix())
            if len(result) > maximum_files:
                raise _error("DPONE_DBT_V2_PACKAGE_INVALID", "runtime source exceeds its file budget")
    return frozenset(result)


def _bounded_bytes(path: Path, *, maximum: int) -> bytes:
    try:
        if not path.is_file() or path.is_symlink() or path.stat().st_size > maximum:
            raise ValueError
        return path.read_bytes()
    except (OSError, ValueError) as exc:
        raise _error("DPONE_DBT_V2_PACKAGE_INVALID", "runtime source file is unavailable or oversized") from exc


def _mapping(value: object) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise _error("DPONE_DBT_V2_OVERLAY_INVALID", "project overlay models are invalid")
    return value


def _require_digest(value: object, field: str) -> str:
    if (
        not isinstance(value, str)
        or not value.startswith(_DIGEST_PREFIX)
        or len(value) != 71
        or any(character not in "0123456789abcdef" for character in value[7:])
    ):
        raise _error("DPONE_DBT_V2_RUNTIME_AUTHORITY_INVALID", f"{field} is not a canonical digest")
    return value


def _canonical_sha256(value: Mapping[str, object]) -> str:
    raw = json.dumps(value, allow_nan=False, ensure_ascii=True, separators=(",", ":"), sort_keys=True).encode()
    return _DIGEST_PREFIX + hashlib.sha256(raw).hexdigest()


def _error(code: str, message: str) -> DbtPublishingError:
    return DbtPublishingError(code, message)


__all__ = [
    "materialize_semantic_refresh_project",
    "semantic_refresh_package_sha256",
]
