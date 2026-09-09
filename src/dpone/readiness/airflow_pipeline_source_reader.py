"""Bounded, project-confined reads for beginner Airflow pipeline sources."""

from __future__ import annotations

from pathlib import Path, PurePosixPath
from typing import Any

from dpone.manifest.bounded_yaml import BoundedYamlError, BoundedYamlLimits, load_bounded_yaml
from dpone.manifest.confined_files import (
    ConfinedFileError,
    project_relative_path,
    read_confined_file,
)
from dpone.manifest.pipeline_identity import PipelineId, PipelineIdError
from dpone.manifest.pipeline_source_reference import (
    PipelineSourceReferenceError,
    ResolvedPipelineReference,
    resolve_pipeline_reference,
)
from dpone.readiness.airflow_self_service_models import dpone_error

_PIPELINE_SOURCE_LIMITS = BoundedYamlLimits()


class PipelineSourcePathError(ValueError):
    """A requested primary source cannot be addressed safely inside the project."""

    def __init__(
        self,
        message: str,
        *,
        code: str,
        exit_code: int,
        entity_id: str | None = None,
        domain: str | None = None,
        path: str | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.exit_code = exit_code
        self.entity_id = entity_id
        self.domain = domain
        self.path = path


def resolve_pipeline_source_reference(root: str | Path, target: str | Path) -> tuple[Path, ResolvedPipelineReference]:
    """Resolve a source path and retain any identity asserted by a bare ID."""

    root_path = Path(root).resolve(strict=True)
    try:
        reference = resolve_pipeline_reference(root_path, target)
    except PipelineSourceReferenceError as exc:
        raise PipelineSourcePathError(
            str(exc),
            code=exc.code,
            exit_code=exc.exit_code,
            entity_id=exc.entity_id,
            domain=exc.domain,
            path=exc.path,
        ) from exc
    return root_path.joinpath(*PurePosixPath(reference.relative_path).parts), reference


def resolve_pipeline_path(root: str | Path, target: str | Path) -> Path:
    """Resolve a pipeline id, directory, or explicit source without leaving ``root``."""

    source_path, _ = resolve_pipeline_source_reference(root, target)
    return source_path


def fallback_pipeline_identity(target: str | Path) -> str:
    """Return a safe entity id for failures before a source is resolved."""

    try:
        return str(PipelineId.parse(str(target).strip()))
    except PipelineIdError:
        return "pipeline"


def load_pipeline_mapping(
    path: Path,
    *,
    root: str | Path,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    """Load one bounded YAML mapping through a no-follow project root."""

    root_path = Path(root).resolve(strict=True)
    try:
        relative = project_relative_path(root_path, path)
        content = read_confined_file(
            root_path,
            relative,
            max_bytes=_PIPELINE_SOURCE_LIMITS.max_bytes,
        )
        payload = load_bounded_yaml(content, limits=_PIPELINE_SOURCE_LIMITS)
    except ConfinedFileError as exc:
        return None, _confined_read_error(exc, path=path, root=root_path)
    except BoundedYamlError as exc:
        return None, dpone_error(
            "DPONE_PIPELINE_YAML_INVALID",
            "Pipeline source must be bounded, unique-key UTF-8 YAML.",
            stage="self_service",
            path=_display_path(path, root_path),
            extra={"reason": exc.code},
        )
    if not isinstance(payload, dict):
        return None, dpone_error(
            "DPONE_PIPELINE_SOURCE_INVALID",
            "Pipeline source must be a YAML object.",
            stage="self_service",
            path=_display_path(path, root_path),
        )
    return payload, None


def _confined_read_error(
    error: ConfinedFileError,
    *,
    path: Path,
    root: Path,
) -> dict[str, Any]:
    if error.code == "file_not_found":
        code = "DPONE_PIPELINE_SOURCE_NOT_FOUND"
        message = "Pipeline source was not found."
    elif error.code in {"file_too_large", "not_regular_file"}:
        code = "DPONE_PIPELINE_SOURCE_INVALID"
        message = "Pipeline source must be a bounded regular file."
    else:
        code = "DPONE_PIPELINE_SOURCE_PATH_INVALID"
        message = "Pipeline source could not be read safely inside the project root."
    return dpone_error(
        code,
        message,
        stage="self_service",
        path=_display_path(path, root),
        extra={"reason": error.code},
    )


def _display_path(path: Path, root: Path) -> str:
    try:
        return path.relative_to(root).as_posix()
    except ValueError:
        return path.name or "pipeline.yaml"


__all__ = [
    "fallback_pipeline_identity",
    "PipelineSourcePathError",
    "load_pipeline_mapping",
    "resolve_pipeline_path",
    "resolve_pipeline_source_reference",
]
