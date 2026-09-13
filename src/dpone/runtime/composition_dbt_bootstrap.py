"""Runtime seams shared by ordinary and supervised dbt pack execution.

Keeping the official manifest-schema adaptation, the canonical preflight factory
and the supervised command derivation here lets the runtime bootstrap stay a thin
composition script, and lets a supervised parent root reuse exactly the same
command derivation the shared execution service uses instead of inventing argv.
"""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import TYPE_CHECKING, Any

from dpone.adapters.composition_dbt_command_runner import TrustedDbtCommand
from dpone.adapters.dbt_runtime import OfficialDbtManifestValidator
from dpone.runtime.dbt_execution_policy import (
    build_dbt_command,
    build_dbt_ls_command,
    build_dbt_parse_command,
)
from dpone.runtime.dbt_preflight import MAX_DBT_PREFLIGHT_SECONDS, DbtRuntimePreflight

if TYPE_CHECKING:
    from dpone.contracts.dbt_runtime import DbtExecutionPack


class RuntimeDbtManifestDiagnostic:
    """Severity projection consumed by the runtime preflight policy."""

    def __init__(self, severity: str) -> None:
        self.severity = severity


class RuntimeDbtManifestSchemaValidator:
    """Adapt the richer official diagnostic to the runtime's narrow port."""

    def __init__(self) -> None:
        self._delegate = OfficialDbtManifestValidator()

    def validate(self, payload: Mapping[str, Any], *, version: int) -> tuple[RuntimeDbtManifestDiagnostic, ...]:
        return tuple(
            RuntimeDbtManifestDiagnostic(item.severity) for item in self._delegate.validate(payload, version=version)
        )


def build_dbt_runtime_preflight(command_runner: Any, *, artifact_reader: Any) -> DbtRuntimePreflight:
    """Build the shared non-mutating preflight with the official schema check."""

    return DbtRuntimePreflight(
        command_runner=command_runner,
        artifact_reader=artifact_reader,
        manifest_validator=RuntimeDbtManifestSchemaValidator(),
    )


def composition_dbt_trusted_commands(
    pack: DbtExecutionPack,
    *,
    project_dir: Path,
    profile_path: Path,
    preflight_target: Path,
    preflight_logs: Path,
    target: Path,
    logs: Path,
    interval_vars_json: str,
) -> tuple[TrustedDbtCommand, ...]:
    """Derive the exact supervised invocations from the canonical dbt policy.

    A supervised parent may not invent argv: the preflight and build commands
    come from the same runtime policy the shared execution service uses, bound to
    the already reserved protected paths of exactly one attempt.
    """

    timeout = min(pack.timeout_seconds, MAX_DBT_PREFLIGHT_SECONDS)
    preflight = tuple(
        TrustedDbtCommand(
            "preflight",
            derive(
                pack,
                project_dir=project_dir,
                profile_path=profile_path,
                target_path=preflight_target,
                log_path=preflight_logs,
                interval_vars_json=interval_vars_json,
            ),
            project_dir,
            timeout,
        )
        for derive in (build_dbt_parse_command, build_dbt_ls_command)
    )
    return (
        *preflight,
        TrustedDbtCommand(
            "build",
            build_dbt_command(
                pack,
                project_dir=project_dir,
                profile_path=profile_path,
                target_path=target,
                log_path=logs,
                interval_vars_json=interval_vars_json,
            ),
            project_dir,
            pack.timeout_seconds,
        ),
    )


__all__ = [
    "RuntimeDbtManifestDiagnostic",
    "RuntimeDbtManifestSchemaValidator",
    "build_dbt_runtime_preflight",
    "composition_dbt_trusted_commands",
]
