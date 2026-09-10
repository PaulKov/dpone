"""Static authoring-source validation for beginner Airflow self-service."""

from __future__ import annotations

import shlex
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from dpone.contracts.airflow_resources import manifest_airflow_resources
from dpone.manifest.authoring import (
    AuthoringCompilation,
    AuthoringCompilationError,
    AuthoringCompiler,
    default_authoring_compiler,
)
from dpone.manifest.batch_loader import BatchYamlManifestLoader
from dpone.manifest.errors import ManifestConfigurationError
from dpone.manifest.validation import Severity, validate_manifest
from dpone.readiness.airflow_authoring_migration_plan import suggest_connection_ref
from dpone.readiness.error_contract import dpone_error, error_docs_url, manual_fix, safe_fix


def validate_pipeline_source(
    payload: dict[str, Any] | None,
    path: Path,
    *,
    root: Path | None = None,
    compiler: AuthoringCompiler | None = None,
) -> list[dict[str, Any]]:
    _, errors = compile_pipeline_source(payload, path, root=root, compiler=compiler or default_authoring_compiler())
    return errors


def compile_pipeline_source(
    payload: dict[str, Any] | None,
    path: Path,
    *,
    root: Path | None = None,
    compiler: AuthoringCompiler,
) -> tuple[AuthoringCompilation | None, list[dict[str, Any]]]:
    """Compile one source and translate failures to the structured CLI contract."""

    if payload is None:
        return None, [_error("DPONE_PIPELINE_SOURCE_INVALID", "Pipeline source must be a YAML object", path.as_posix())]
    try:
        is_airflow_authoring_enabled(payload)
    except AirflowAuthoringPolicyError as exc:
        return None, [_error("DPONE_PIPELINE_SOURCE_INVALID", str(exc), path.as_posix())]
    errors: list[dict[str, Any]] = []
    try:
        compilation = compiler.compile(payload, source_path=path, project_root=root)
    except AuthoringCompilationError as exc:
        errors.append(_error(exc.code, str(exc), path.as_posix()))
    except ManifestConfigurationError as exc:
        errors.append(_error("DPONE_AUTHORING_COMPILATION_FAILED", str(exc), path.as_posix()))
    else:
        errors.extend(_legacy_connection_errors(list(compilation.processes), path, root=root))
        if errors:
            # The canonical materializer requires connection_ref authority. A
            # known legacy source already has one precise migration error, so
            # do not obscure it with a derivative compilation failure.
            return compilation, errors
        try:
            errors.extend(_universal_manifest_errors(compilation, path))
        except (ManifestConfigurationError, ValueError) as exc:
            errors.append(_error("DPONE_AUTHORING_COMPILATION_FAILED", str(exc), path.as_posix()))
        return compilation, errors
    return None, errors


def authoring_check_view(
    payload: dict[str, Any], compilation: AuthoringCompilation | None
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Return public check metadata and a process-oriented compatibility view."""

    if compilation is None:
        return {}, payload
    details: dict[str, Any] = {
        "authoring_mode": compilation.authoring_mode,
        "source_kind": compilation.source_kind,
        "canonical_kind": compilation.canonical_kind,
        "source_fingerprint": compilation.source_fingerprint,
        "semantic_fingerprint": compilation.semantic_fingerprint,
        "deprecated_aliases": list(compilation.deprecated_aliases),
        "source_files": _source_files(payload, compilation),
    }
    if compilation.recipe_provenance is not None:
        details["recipe_resolution"] = dict(compilation.recipe_provenance)
    resources = manifest_airflow_resources(payload)
    if resources is not None:
        details["airflow_resources"] = resources
    process_view = {**payload, "processes": [dict(process) for process in compilation.processes]}
    return details, process_view


class AirflowAuthoringPolicyError(ValueError):
    """The durable Airflow participation flag has an invalid public type."""


def is_airflow_authoring_enabled(payload: Mapping[str, Any]) -> bool:
    """Return the validated durable Airflow participation policy."""

    metadata = payload.get("metadata")
    if not isinstance(metadata, Mapping):
        return True
    enabled = metadata.get("airflow", True)
    if not isinstance(enabled, bool):
        raise AirflowAuthoringPolicyError("Pipeline metadata.airflow must be a boolean.")
    return enabled


def _source_files(payload: dict[str, Any], compilation: AuthoringCompilation) -> list[dict[str, str]]:
    authoring = payload.get("authoring")
    primary = str(authoring.get("source") or "") if isinstance(authoring, dict) else ""
    files = []
    if primary:
        files.append({"kind": "primary_source", "path": primary, "fingerprint": compilation.source_fingerprint})
    files.extend(dependency.to_jsonable() for dependency in compilation.dependencies)
    return files


def _legacy_connection_errors(processes: list[Any], path: Path, *, root: Path | None) -> list[dict[str, str]]:
    errors: list[dict[str, str]] = []
    fix_target = _fix_target(path, root=root)
    for index, process in enumerate(processes):
        if not isinstance(process, dict):
            continue
        for side in ("source", "sink"):
            section = process.get(side)
            if not isinstance(section, dict):
                continue
            if "connection_type" not in section and "vault_path" not in section:
                continue
            entity = f"processes[{index}].{side}"
            suggested_ref = suggest_connection_ref(section, side)
            errors.append(
                dpone_error(
                    "DPONE_LEGACY_CONNECTION_CONFIG_FOUND",
                    (
                        "Legacy connection_type/vault_path is not an Airflow self-service source of truth; "
                        "use connection_ref plus binding-set and connection-registry."
                    ),
                    stage="static_check",
                    path=path.as_posix(),
                    entity={"kind": "authoring_path", "id": entity},
                    docs_url=error_docs_url("DPONE_LEGACY_CONNECTION_CONFIG_FOUND"),
                    fixes=[
                        safe_fix(
                            "plan_authoring_connection_ref_migration",
                            command=f"dpone fix {fix_target} --plan",
                        ),
                        manual_fix(
                            "apply_authoring_connection_ref_migration",
                            command=f"dpone fix {fix_target} --apply",
                        ),
                    ],
                    extra={"suggested_connection_ref": suggested_ref},
                )
            )
    return errors


def _universal_manifest_errors(
    compilation: AuthoringCompilation,
    path: Path,
) -> list[dict[str, Any]]:
    """Validate the pinned canonical IR without rereading its authoring source."""

    manifest = BatchYamlManifestLoader().load_mapping(
        dict(compilation.canonical_manifest),
        path=path,
        metadata_only=True,
    )
    return [
        dpone_error(
            issue.code,
            issue.message,
            stage="static_check",
            path=path.as_posix(),
            entity=({"kind": "process", "id": issue.selector} if issue.selector is not None else None),
            docs_url=error_docs_url(issue.code),
        )
        for issue in validate_manifest(manifest)
        if issue.severity is Severity.ERROR
    ]


def _fix_target(path: Path, *, root: Path | None) -> str:
    target = path.parent if path.name == "pipeline.yaml" else path
    if root is not None:
        try:
            target = target.relative_to(root)
        except ValueError:
            pass
    return shlex.quote(target.as_posix())


def _error(code: str, message: str, path: str = "") -> dict[str, Any]:
    return dpone_error(code, message, stage="static_check", path=path, docs_url=error_docs_url(code))


__all__ = [
    "authoring_check_view",
    "compile_pipeline_source",
    "is_airflow_authoring_enabled",
    "validate_pipeline_source",
]
