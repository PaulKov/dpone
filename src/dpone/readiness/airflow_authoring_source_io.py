"""Confined single-snapshot reads for Airflow authoring sources."""

from __future__ import annotations

import hashlib
import shlex
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dpone.manifest.bounded_yaml import BoundedYamlError, BoundedYamlLimits, load_bounded_yaml
from dpone.manifest.confined_files import (
    ConfinedFileError,
    project_relative_path,
    read_confined_file,
)
from dpone.readiness.airflow_self_service_models import SelfServiceResult, dpone_error, manual_fix

_PIPELINE_SOURCE_LIMITS = BoundedYamlLimits()


@dataclass(frozen=True, slots=True)
class PinnedPipelineSource:
    """One confined byte snapshot used for parsing, hashing, and compilation."""

    payload: dict[str, Any]
    content: bytes
    sha256: str
    source_label: str


def load_pinned_pipeline_source(
    root: Path,
    source_path: Path,
) -> tuple[PinnedPipelineSource | None, dict[str, Any] | None]:
    """Read, hash, and parse a primary source from one confined byte snapshot."""

    try:
        source_label = project_relative_path(root, source_path)
        content = read_confined_file(
            root,
            source_label,
            max_bytes=_PIPELINE_SOURCE_LIMITS.max_bytes,
        )
        payload = load_bounded_yaml(content, limits=_PIPELINE_SOURCE_LIMITS)
    except ConfinedFileError as exc:
        return None, _confined_source_error(exc, source_path=source_path, root=root)
    except BoundedYamlError as exc:
        return None, dpone_error(
            "DPONE_PIPELINE_YAML_INVALID",
            "Pipeline source must be bounded, unique-key UTF-8 YAML.",
            stage="self_service",
            path=_display_path(source_path, root),
            extra={"reason": exc.code},
        )
    if not isinstance(payload, dict):
        return None, dpone_error(
            "DPONE_PIPELINE_SOURCE_INVALID",
            "Pipeline source must be a YAML object.",
            stage="self_service",
            path=source_label,
        )
    return (
        PinnedPipelineSource(
            payload=payload,
            content=content,
            sha256="sha256:" + hashlib.sha256(content).hexdigest(),
            source_label=source_label,
        ),
        None,
    )


def missing_source_result(source_path: Path, *, root: Path) -> SelfServiceResult:
    """Build the actionable result for one absent authoring source."""

    pipeline_id = source_path.parent.name if source_path.name == "pipeline.yaml" else source_path.stem
    display_path = _display_path(source_path, root)
    return SelfServiceResult(
        passed=False,
        errors=(
            dpone_error(
                "DPONE_PIPELINE_SOURCE_NOT_FOUND",
                "Pipeline source was not found",
                stage="self_service",
                entity={"kind": "pipeline", "id": display_path},
                path=display_path,
                fixes=[
                    manual_fix(
                        "init_pipeline",
                        command=(
                            f"dpone init pipeline {shlex.quote(pipeline_id)} "
                            "--recipe mssql-to-clickhouse-incremental --airflow"
                        ),
                    )
                ],
            ),
        ),
    )


def _confined_source_error(
    error: ConfinedFileError,
    *,
    source_path: Path,
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
        path=_display_path(source_path, root),
        extra={"reason": error.code},
    )


def _display_path(path: Path, root: Path) -> str:
    try:
        return path.relative_to(root).as_posix()
    except ValueError:
        return path.name or "pipeline.yaml"


__all__ = ["PinnedPipelineSource", "load_pinned_pipeline_source", "missing_source_result"]
