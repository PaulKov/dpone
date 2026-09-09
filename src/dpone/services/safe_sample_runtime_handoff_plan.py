"""Self-service runtime handoff artifact for safe sample runs.

The beginner ``dpone run --sample --target temporary`` path prepares a
credential-free execution plan. This module persists that plan next to local
runtime evidence and returns platform diagnostic commands without resolving
credentials, touching Airflow, or contacting source/sink systems.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.services.safe_sample_execution_plan import SafeSampleExecutionPlan


import hashlib
import json
import shlex
from pathlib import Path
from typing import Any

from dpone.contracts.safe_sample_live_authorization import (
    SafeSampleLiveAuthorizationPathError,
    authorization_overlay_files,
)

_LIVE_AUTHORIZATION_OPTIONS = (
    ("--route-attestation", "route_attestation"),
    ("--route-attestation-bundle", "route_attestation_bundle"),
    ("--route-certification-bundle", "route_certification_bundle"),
    ("--route-attestation-policy", "route_attestation_policy"),
)


class SafeSampleRuntimeHandoffPathError(ValueError):
    """Reject an unsafe live-copy diagnostic path before writing a handoff."""

    code = "DPONE_SAFE_SAMPLE_LIVE_INPUT_PATH_INVALID"

    def __init__(self, code: str | None = None, message: str | None = None) -> None:
        super().__init__(message or "Pinned safe-sample identities cannot form a safe authorization path.")
        self.code = code or type(self).code

    def to_error(self) -> dict[str, Any]:
        return {
            "schema": "dpone.error.v1",
            "code": self.code,
            "stage": "safe_sample_runtime_handoff",
            "severity": "error",
            "message": str(self),
            "fixes": [],
        }


def write_safe_sample_runtime_handoff(
    plan: SafeSampleExecutionPlan,
    *,
    output_dir: str | Path,
    pipeline_source_path: str | Path,
) -> dict[str, Any]:
    """Write the execution plan and return a self-service runtime handoff."""

    if plan.source_snapshot is None:
        raise SafeSampleRuntimeHandoffPathError(
            "DPONE_SAFE_SAMPLE_PIPELINE_SOURCE_PIN_MISSING",
            "Safe-sample runtime handoff requires a canonical source snapshot; rerun the command.",
        )
    output_path = Path(output_dir)
    plan_path = output_path / "safe-sample-execution-plan.json"
    authorization_paths = _live_authorization_paths(plan)
    plan_bytes = _json_bytes(plan.to_dict())
    _write_atomic(plan_path, plan_bytes)
    relative_plan_path = _display_output_path(plan_path)
    pipeline_source = _display_input_path(Path(pipeline_source_path))
    environment = _normalized_environment(plan.environment)
    live_copy_arguments = _live_copy_arguments(authorization_paths)
    live_copy_requires = [
        f"environments/{environment}/binding-set.yaml",
        f"platform/connection-registries/{environment}.yaml",
        f"environments/{environment}/credential-runtime.yaml",
        *authorization_paths.values(),
    ]
    return {
        "schema": "dpone.safe-sample-runtime-handoff.v1",
        "plan_path": relative_plan_path,
        "plan_sha256": _sha256(plan_bytes),
        "plan_bytes": len(plan_bytes),
        "command": shlex.join(
            [
                "dpone",
                "ops",
                "safe-sample-runtime-run",
                "--plan-json",
                relative_plan_path,
            ]
        ),
        "live_copy_command": shlex.join(
            [
                "dpone",
                "ops",
                "safe-sample-runtime-run",
                "--plan-json",
                relative_plan_path,
                "--pipeline-source",
                pipeline_source,
                "--enable-live-copy",
                "--binding-set",
                f"environments/{environment}/binding-set.yaml",
                "--connection-registry",
                f"platform/connection-registries/{environment}.yaml",
                "--credential-runtime",
                f"environments/{environment}/credential-runtime.yaml",
                *live_copy_arguments,
            ]
        ),
        "live_copy_requires": live_copy_requires,
    }


def _write_atomic(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(path.name + ".tmp")
    tmp_path.write_bytes(payload)
    tmp_path.replace(path)


def _json_bytes(payload: dict[str, Any]) -> bytes:
    return (json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode("utf-8")


def _sha256(payload: bytes) -> str:
    return "sha256:" + hashlib.sha256(payload).hexdigest()


def _display_output_path(path: Path) -> str:
    try:
        return path.resolve(strict=False).relative_to(Path.cwd().resolve(strict=False)).as_posix()
    except (OSError, RuntimeError, ValueError):
        return f"$OUTPUT_ROOT/{path.name or 'safe-sample-execution-plan.json'}"


def _display_input_path(path: Path) -> str:
    try:
        return path.resolve(strict=False).relative_to(Path.cwd().resolve(strict=False)).as_posix()
    except (OSError, RuntimeError, ValueError):
        return path.name or "pipeline.yaml"


def _normalized_environment(value: str) -> str:
    text = str(value or "").strip().lower()
    if text in {"", "local"}:
        return "dev"
    if text == "development":
        return "dev"
    if text == "production":
        return "prod"
    return text


def _live_authorization_paths(plan: SafeSampleExecutionPlan) -> dict[str, str]:
    context = plan.deployment_context
    target = plan.temporary_target_plan
    if context is None or target is None or not context.deployment_id or not target.pipeline_id:
        return {}
    try:
        paths = authorization_overlay_files(context.deployment_id, target.pipeline_id)
    except SafeSampleLiveAuthorizationPathError as exc:
        raise SafeSampleRuntimeHandoffPathError from exc
    return {field: (Path(".dpone-cache") / relative.as_posix()).as_posix() for field, relative in paths.items()}


def _live_copy_arguments(paths: dict[str, str]) -> list[str]:
    arguments: list[str] = []
    for option, field in _LIVE_AUTHORIZATION_OPTIONS:
        if field in paths:
            arguments.extend((option, paths[field]))
    return arguments


__all__ = ["SafeSampleRuntimeHandoffPathError", "write_safe_sample_runtime_handoff"]
