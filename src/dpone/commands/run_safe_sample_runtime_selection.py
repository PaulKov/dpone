"""Runtime-mode orchestration for the beginner safe-sample command."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from dpone.readiness.safe_sample_runtime_handoff import (
    prepare_auto_live_safe_sample_runtime,
    run_local_safe_sample_runtime_handoff,
)

if TYPE_CHECKING:
    from dpone.ports.route_attestation import RouteAttestationSignatureVerifier
    from dpone.services.safe_sample_execution_plan import SafeSampleExecutionPlan
    from dpone.services.safe_sample_runtime_executor import SafeSampleDataCopier


@dataclass(frozen=True, slots=True)
class SafeSampleRuntimeSelection:
    """One selected runtime mode and its command-facing result."""

    payload: dict[str, object]
    runtime_run: dict[str, object] | None
    errors: tuple[dict[str, object], ...]


def select_safe_sample_runtime(
    plan: SafeSampleExecutionPlan,
    *,
    output_dir: Path,
    pipeline_source_path: str,
    project_root: Path,
    artifact_cache_root: Path | None = None,
    process_name: str | None,
    data_copier: SafeSampleDataCopier | None,
    signature_verifier: RouteAttestationSignatureVerifier | None = None,
) -> SafeSampleRuntimeSelection:
    """Select verified live execution or the network-free local handoff."""

    authorization_cache_root = project_root / ".dpone-cache"
    effective_artifact_cache_root = artifact_cache_root or authorization_cache_root
    auto_live = prepare_auto_live_safe_sample_runtime(
        plan,
        pipeline_source_path=pipeline_source_path,
        project_root=project_root,
        cache_root=authorization_cache_root,
        artifact_cache_root=effective_artifact_cache_root,
        process_name=process_name,
        signature_verifier=signature_verifier,
    )
    payload: dict[str, object] = {
        "execution_mode": auto_live.execution_mode,
        "live_selection": auto_live.to_dict(),
    }
    if auto_live.execution_mode == "blocked":
        return SafeSampleRuntimeSelection(payload=payload, runtime_run=None, errors=auto_live.errors)

    if auto_live.execution_mode == "live_copy":
        assembly = auto_live.assembly
        assert assembly is not None
        report = run_local_safe_sample_runtime_handoff(
            assembly.plan,
            output_dir=output_dir,
            cache_root=effective_artifact_cache_root,
            data_copier=assembly.data_copier,
            temporary_target_executor=assembly.temporary_target_executor,
            route_attestation_verification=assembly.route_attestation_verification.to_dict(),
        )
    else:
        report = run_local_safe_sample_runtime_handoff(
            plan,
            output_dir=output_dir,
            cache_root=effective_artifact_cache_root,
            data_copier=data_copier,
        )
    runtime_run = report.to_dict()
    payload["runtime_run"] = runtime_run
    return SafeSampleRuntimeSelection(payload=payload, runtime_run=runtime_run, errors=())


__all__ = ["SafeSampleRuntimeSelection", "select_safe_sample_runtime"]
