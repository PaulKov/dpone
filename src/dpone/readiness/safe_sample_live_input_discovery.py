"""Discover platform-owned inputs for beginner live safe-sample execution.

Discovery is deliberately narrower than authorization. It derives one
deployment-scoped cache location, checks local path shape and completeness, and
returns paths to the existing live runtime assembly. It never parses registry
content, resolves credentials, verifies signatures, contacts external systems,
or resolves a mutable ``current`` pointer.
"""

from __future__ import annotations

import re
import stat
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from dpone.contracts.safe_sample_live_authorization import (
    AUTHORIZATION_FILE_NAMES,
    AUTHORIZATION_OVERLAY_PROFILE,
    SafeSampleLiveAuthorizationPathError,
    authorization_overlay_files,
    authorization_overlay_relative_path,
)

_PATH_COMPONENT_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$")
_AUTHORIZATION_FIELDS = frozenset(field for field, _ in AUTHORIZATION_FILE_NAMES)


class LiveSafeSamplePlanView(Protocol):
    """Structural plan view needed for local authorization discovery."""

    @property
    def deployment_context(self) -> Any: ...

    @property
    def temporary_target_plan(self) -> Any: ...

    @property
    def environment(self) -> str: ...


@dataclass(frozen=True, slots=True)
class LiveSafeSampleInputPaths:
    """Exact non-secret files consumed by the existing live assembly."""

    binding_set: Path
    connection_registry: Path
    credential_runtime: Path
    route_attestation: Path
    route_attestation_bundle: Path
    route_certification_bundle: Path
    route_attestation_policy: Path


@dataclass(frozen=True, slots=True)
class LiveSafeSampleInputDiscovery:
    """Fail-closed result of deployment-scoped live-input discovery."""

    status: str
    deployment_id: str | None
    pipeline_id: str | None
    paths: LiveSafeSampleInputPaths | None = None
    errors: tuple[dict[str, Any], ...] = ()

    @property
    def ready(self) -> bool:
        return self.status == "ready" and self.paths is not None and not self.errors

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "deployment_id": self.deployment_id,
            "pipeline_id": self.pipeline_id,
            "authorization_overlay_profile": AUTHORIZATION_OVERLAY_PROFILE,
            "errors": [dict(error) for error in self.errors],
        }


def discover_live_safe_sample_inputs(
    plan: LiveSafeSamplePlanView,
    *,
    project_root: str | Path,
    cache_root: str | Path,
) -> LiveSafeSampleInputDiscovery:
    """Find one complete overlay for the plan's already-pinned deployment."""

    context = plan.deployment_context
    target = plan.temporary_target_plan
    deployment_id = context.deployment_id if context is not None else None
    pipeline_id = target.pipeline_id if target is not None else None
    identity_error = _identity_error(deployment_id=deployment_id, pipeline_id=pipeline_id)
    if identity_error is not None:
        return _result(
            "invalid",
            deployment_id=deployment_id,
            pipeline_id=pipeline_id,
            error=identity_error,
        )
    assert context is not None
    assert deployment_id is not None
    assert pipeline_id is not None

    try:
        project = Path(project_root).resolve(strict=False)
        cache = _absolute_root(cache_root, relative_to=project)
    except (OSError, RuntimeError):
        return _result(
            "invalid",
            deployment_id=deployment_id,
            pipeline_id=pipeline_id,
            error=_path_error("configured project or cache root cannot be resolved safely"),
        )
    authorization_dir = cache / authorization_overlay_relative_path(deployment_id, pipeline_id).as_posix()
    path_error = _bounded_path_error(authorization_dir, root=cache, label="authorization directory")
    if path_error is not None:
        return _result(
            "invalid",
            deployment_id=deployment_id,
            pipeline_id=pipeline_id,
            error=path_error,
        )
    try:
        authorization_mode = authorization_dir.lstat().st_mode
    except FileNotFoundError:
        return LiveSafeSampleInputDiscovery(
            status="not_configured",
            deployment_id=deployment_id,
            pipeline_id=pipeline_id,
        )
    except OSError:
        return _result(
            "invalid",
            deployment_id=deployment_id,
            pipeline_id=pipeline_id,
            error=_path_error("authorization overlay cannot be inspected safely"),
        )
    if not stat.S_ISDIR(authorization_mode):
        return _result(
            "invalid",
            deployment_id=deployment_id,
            pipeline_id=pipeline_id,
            error=_path_error("authorization overlay must be a regular directory"),
        )

    environment = _environment_component(context.environment or plan.environment)
    if environment is None:
        return _result(
            "invalid",
            deployment_id=deployment_id,
            pipeline_id=pipeline_id,
            error=_path_error("deployment environment is not a safe path component"),
        )
    environment_paths = {
        "binding_set": project / "environments" / environment / "binding-set.yaml",
        "connection_registry": project / "platform" / "connection-registries" / f"{environment}.yaml",
        "credential_runtime": project / "environments" / environment / "credential-runtime.yaml",
    }
    authorization_paths = {
        field: cache / relative.as_posix()
        for field, relative in authorization_overlay_files(deployment_id, pipeline_id).items()
    }
    all_paths = {**environment_paths, **authorization_paths}
    invalid = _invalid_input_paths(all_paths, project_root=project, cache_root=cache)
    if invalid:
        return _result(
            "invalid",
            deployment_id=deployment_id,
            pipeline_id=pipeline_id,
            error=_path_error("live input path is unsafe or uses a symlink: " + ", ".join(invalid)),
        )
    missing = [path.name for path in all_paths.values() if not path.is_file()]
    if missing:
        return _result(
            "incomplete",
            deployment_id=deployment_id,
            pipeline_id=pipeline_id,
            error=_incomplete_error(missing),
        )
    return LiveSafeSampleInputDiscovery(
        status="ready",
        deployment_id=deployment_id,
        pipeline_id=pipeline_id,
        paths=LiveSafeSampleInputPaths(**all_paths),
    )


def _identity_error(*, deployment_id: str | None, pipeline_id: str | None) -> dict[str, Any] | None:
    try:
        authorization_overlay_relative_path(str(deployment_id or ""), str(pipeline_id or ""))
    except SafeSampleLiveAuthorizationPathError as exc:
        return _path_error(str(exc))
    return None


def _absolute_root(value: str | Path, *, relative_to: Path) -> Path:
    path = Path(value)
    if not path.is_absolute():
        path = relative_to / path
    return path.resolve(strict=False)


def _environment_component(value: str) -> str | None:
    normalized = str(value or "").strip().lower()
    if normalized in {"development", "local"}:
        normalized = "dev"
    elif normalized == "production":
        normalized = "prod"
    return normalized if _PATH_COMPONENT_RE.fullmatch(normalized) else None


def _invalid_input_paths(
    paths: dict[str, Path],
    *,
    project_root: Path,
    cache_root: Path,
) -> list[str]:
    invalid: list[str] = []
    for field, path in paths.items():
        root = cache_root if field in _AUTHORIZATION_FIELDS else project_root
        if _bounded_path_error(path, root=root, label=field) is not None:
            invalid.append(path.name)
    return sorted(invalid)


def _bounded_path_error(path: Path, *, root: Path, label: str) -> dict[str, Any] | None:
    try:
        relative = path.relative_to(root)
    except ValueError:
        return _path_error(f"{label} escapes its configured root")
    try:
        current = root
        for component in relative.parts:
            current = current / component
            if current.is_symlink():
                return _path_error(f"{label} uses a symlink")
        path.resolve(strict=False).relative_to(root.resolve(strict=False))
    except ValueError:
        return _path_error(f"{label} resolves outside its configured root")
    except (OSError, RuntimeError):
        return _path_error(f"{label} cannot be resolved safely")
    return None


def _result(
    status: str,
    *,
    deployment_id: str | None,
    pipeline_id: str | None,
    error: dict[str, Any],
) -> LiveSafeSampleInputDiscovery:
    return LiveSafeSampleInputDiscovery(
        status=status,
        deployment_id=deployment_id,
        pipeline_id=pipeline_id,
        errors=(error,),
    )


def _incomplete_error(missing: list[str]) -> dict[str, Any]:
    return _error(
        "DPONE_SAFE_SAMPLE_LIVE_INPUTS_INCOMPLETE",
        "Deployment-scoped live inputs are incomplete: " + ", ".join(sorted(missing)) + ".",
    )


def _path_error(message: str) -> dict[str, Any]:
    return _error("DPONE_SAFE_SAMPLE_LIVE_INPUT_PATH_INVALID", message + ".")


def _error(code: str, message: str) -> dict[str, Any]:
    return {
        "schema": "dpone.error.v1",
        "code": code,
        "stage": "safe_sample_live_input_discovery",
        "severity": "error",
        "message": message,
        "fixes": [],
    }


__all__ = [
    "LiveSafeSampleInputDiscovery",
    "LiveSafeSampleInputPaths",
    "LiveSafeSamplePlanView",
    "discover_live_safe_sample_inputs",
]
