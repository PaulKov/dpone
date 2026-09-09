"""Select existing live safe-sample ports for the beginner command.

This composition helper owns no execution engine. It combines deterministic
input discovery with the existing verified live assembly and returns either an
assembly, a backward-compatible local-handoff decision, or structured blockers.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

from dpone.readiness.safe_sample_live_errors import LiveSafeSampleRuntimeAssemblyError
from dpone.readiness.safe_sample_live_input_discovery import (
    LiveSafeSampleInputDiscovery,
    discover_live_safe_sample_inputs,
)

if TYPE_CHECKING:
    from dpone.ports.route_attestation import RouteAttestationSignatureVerifier
    from dpone.readiness.safe_sample_live_input_discovery import LiveSafeSamplePlanView
    from dpone.readiness.safe_sample_live_runtime import LiveSafeSampleRuntimeAssembly
Discovery = Callable[..., LiveSafeSampleInputDiscovery]
AssemblyBuilder = Callable[..., "LiveSafeSampleRuntimeAssembly"]
_SAFE_BLOCKED_MESSAGE = (
    "Live safe-sample authorization is blocked; inspect the structured code and platform configuration."
)


@dataclass(frozen=True, slots=True)
class AutoLiveSafeSampleRuntime:
    """Prepared live ports or an explicit non-live selection."""

    execution_mode: str
    discovery: LiveSafeSampleInputDiscovery
    assembly: LiveSafeSampleRuntimeAssembly | None = None
    errors: tuple[dict[str, Any], ...] = ()

    def __post_init__(self) -> None:
        if self.execution_mode == "live_copy" and (self.assembly is None or self.errors):
            raise ValueError("live_copy selection requires an assembly and no errors")
        if self.execution_mode == "local_handoff" and (self.assembly is not None or self.errors):
            raise ValueError("local_handoff selection cannot contain an assembly or errors")
        if self.execution_mode == "blocked" and (self.assembly is not None or not self.errors):
            raise ValueError("blocked selection requires errors and no assembly")
        if self.execution_mode not in {"live_copy", "local_handoff", "blocked"}:
            raise ValueError("execution_mode must be live_copy, local_handoff, or blocked")

    def to_dict(self) -> dict[str, Any]:
        payload = self.discovery.to_dict()
        payload["execution_mode"] = self.execution_mode
        if self.errors:
            payload["errors"] = [dict(error) for error in self.errors]
        return payload


def prepare_auto_live_safe_sample_runtime(
    plan: LiveSafeSamplePlanView,
    *,
    pipeline_source_path: str | Path,
    project_root: str | Path,
    cache_root: str | Path,
    artifact_cache_root: str | Path | None = None,
    process_name: str | None = None,
    signature_verifier: RouteAttestationSignatureVerifier | None = None,
    discovery: Discovery = discover_live_safe_sample_inputs,
    assembly_builder: AssemblyBuilder | None = None,
) -> AutoLiveSafeSampleRuntime:
    """Prepare verified live ports without resolving credentials or doing DB I/O."""

    project = Path(project_root).resolve(strict=False)
    cache = _absolute_path(cache_root, relative_to=project)
    artifact_cache = _absolute_path(
        artifact_cache_root if artifact_cache_root is not None else cache,
        relative_to=project,
    )
    discovered = discovery(plan, project_root=project, cache_root=cache)
    if discovered.status == "not_configured":
        return AutoLiveSafeSampleRuntime(execution_mode="local_handoff", discovery=discovered)
    if not discovered.ready:
        return AutoLiveSafeSampleRuntime(
            execution_mode="blocked",
            discovery=discovered,
            errors=discovered.errors
            or (_error("DPONE_SAFE_SAMPLE_LIVE_INPUTS_INCOMPLETE", "Live inputs are not ready."),),
        )

    paths = discovered.paths
    assert paths is not None
    builder = assembly_builder or _default_assembly_builder()
    try:
        assembly = builder(
            plan=plan,
            pipeline_source_path=pipeline_source_path,
            binding_set_path=paths.binding_set,
            connection_registry_path=paths.connection_registry,
            credential_runtime_path=paths.credential_runtime,
            route_attestation_path=paths.route_attestation,
            route_attestation_bundle_path=paths.route_attestation_bundle,
            route_certification_bundle_path=paths.route_certification_bundle,
            route_attestation_policy_path=paths.route_attestation_policy,
            cache_root=artifact_cache,
            source_root=project,
            process_name=process_name,
            route_attestation_signature_verifier=signature_verifier,
        )
    except LiveSafeSampleRuntimeAssemblyError as exc:
        return AutoLiveSafeSampleRuntime(
            execution_mode="blocked",
            discovery=discovered,
            errors=(_error(exc.code, _SAFE_BLOCKED_MESSAGE),),
        )
    except Exception:  # noqa: BLE001 - facade must return a redacted internal error, never a traceback.
        return AutoLiveSafeSampleRuntime(
            execution_mode="blocked",
            discovery=discovered,
            errors=(
                _error(
                    "DPONE_INTERNAL_SAFE_SAMPLE_LIVE_ASSEMBLY_FAILED",
                    _SAFE_BLOCKED_MESSAGE,
                ),
            ),
        )
    return AutoLiveSafeSampleRuntime(
        execution_mode="live_copy",
        discovery=discovered,
        assembly=assembly,
    )


def _default_assembly_builder() -> AssemblyBuilder:
    from dpone.readiness.safe_sample_live_runtime import build_live_safe_sample_runtime_assembly

    return build_live_safe_sample_runtime_assembly


def _absolute_path(path: str | Path, *, relative_to: Path) -> Path:
    candidate = Path(path)
    if not candidate.is_absolute():
        candidate = relative_to / candidate
    return candidate.resolve(strict=False)


def _error(code: str, message: str) -> dict[str, Any]:
    return {
        "schema": "dpone.error.v1",
        "code": code,
        "stage": "safe_sample_auto_live_runtime",
        "severity": "error",
        "message": message,
        "fixes": [],
    }


__all__ = ["AutoLiveSafeSampleRuntime", "prepare_auto_live_safe_sample_runtime"]
