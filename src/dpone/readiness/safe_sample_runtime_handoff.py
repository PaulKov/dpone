"""Readiness-plane safe-sample runtime handoff.

This module is the thin composition boundary between the beginner CLI/service
contracts and runtime-plane artifact delivery. It keeps ``dpone.services`` free
of runtime imports while allowing local self-service runs to exercise pinned
``init_fetch`` when a runnable deployment projection is already current.
"""

from __future__ import annotations

import importlib
from collections.abc import Mapping
from pathlib import Path
from typing import TYPE_CHECKING, Any

from dpone.readiness.safe_sample_auto_live_runtime import prepare_auto_live_safe_sample_runtime
from dpone.readiness.safe_sample_pinned_source import verify_execution_plan_source_pin
from dpone.runtime.safe_sample_init_fetch import SafeSampleInitFetchArtifactFetcher
from dpone.services.safe_sample_cli_runtime import run_local_fail_closed_safe_sample_runtime

if TYPE_CHECKING:
    from dpone.services.safe_sample_execution_plan import SafeSampleExecutionPlan
    from dpone.services.safe_sample_runtime_executor import SafeSampleArtifactFetcher, SafeSampleDataCopier
    from dpone.services.safe_sample_runtime_runner import SafeSampleRuntimeRunReport
    from dpone.services.safe_sample_target_lifecycle import TemporaryTargetLifecycleExecutor


def run_local_safe_sample_runtime_handoff(
    plan: SafeSampleExecutionPlan,
    *,
    output_dir: str | Path,
    cache_root: str | Path = ".dpone-cache",
    data_copier: SafeSampleDataCopier | None = None,
    temporary_target_executor: TemporaryTargetLifecycleExecutor | None = None,
    route_attestation_verification: Mapping[str, Any] | None = None,
) -> SafeSampleRuntimeRunReport:
    """Run local safe-sample runtime with real pinned init-fetch when possible.

    Non-runnable preview plans still fail before artifact delivery. Runnable
    plans with ``runtime_artifact_delivery.mode=init_fetch`` fetch only pinned
    ``cache://`` workload packs from the local cache mirror into the run output
    directory. Source reads and physical target DDL remain fail-closed unless
    the explicit live composition root injects matching runtime ports.
    """

    output_path = Path(output_dir)
    artifact_fetcher = _artifact_fetcher(plan, cache_root=cache_root, output_dir=output_path)
    verify_execution_plan_source_pin(
        plan,
        pack_reader=artifact_fetcher.read_pinned_bytes if artifact_fetcher is not None else _missing_pack_reader,
    )
    return run_local_fail_closed_safe_sample_runtime(
        plan,
        output_dir=output_path,
        artifact_fetcher=artifact_fetcher,
        data_copier=data_copier,
        temporary_target_executor=temporary_target_executor,
        route_attestation_verification=dict(route_attestation_verification)
        if route_attestation_verification is not None
        else None,
    )


def build_safe_sample_runtime_data_copier(
    *,
    pipeline_source_path: str | Path | None = None,
    pipeline_source: Mapping[str, Any] | None = None,
    process_name: str | None = None,
    enable_live_copy: bool = False,
    binding_set_path: str | Path | None = None,
    connection_registry_path: str | Path | None = None,
    credential_runtime_path: str | Path | None = None,
    evidence_context: Mapping[str, Any] | None = None,
) -> SafeSampleDataCopier:
    """Build the route-certified safe-sample data copier for a runtime command."""

    del binding_set_path, connection_registry_path, credential_runtime_path, evidence_context
    if enable_live_copy:
        raise ValueError(
            "DPONE_SAFE_SAMPLE_LIVE_ASSEMBLY_REQUIRED: live copy must use build_live_safe_sample_runtime_assembly"
        )

    copier_module = importlib.import_module("dpone.services.mssql_clickhouse_safe_sample_copier")
    assembly_module = importlib.import_module("dpone.services.mssql_clickhouse_safe_sample_runtime_assembly")
    registry_module = importlib.import_module("dpone.services.safe_sample_data_copier_registry")
    policy_module = importlib.import_module("dpone.services.safe_sample_policy")

    if pipeline_source is None:
        if pipeline_source_path is None:
            raise ValueError("pipeline_source_path or pipeline_source is required")
        pipeline_source = policy_module.load_pipeline_source_from_file(pipeline_source_path)
    copier = copier_module.build_mssql_clickhouse_safe_sample_copier_from_pipeline_source(
        pipeline_source,
        process_name=process_name,
    )
    registry = registry_module.SafeSampleDataCopierRegistry.with_copiers(
        {assembly_module.MSSQL_CLICKHOUSE_SAFE_SAMPLE_CERTIFICATION_ID: copier}
    )
    return registry_module.RegistryBackedSafeSampleDataCopier(registry)


def _artifact_fetcher(
    plan: SafeSampleExecutionPlan,
    *,
    cache_root: str | Path,
    output_dir: Path,
) -> SafeSampleArtifactFetcher | None:
    if not _should_execute_local_init_fetch(plan):
        return None
    return SafeSampleInitFetchArtifactFetcher(
        registry_root=cache_root,
        destination_root=output_dir / "runtime-artifacts",
    )


def _should_execute_local_init_fetch(plan: SafeSampleExecutionPlan) -> bool:
    context = getattr(plan, "deployment_context", None)
    if context is None or not bool(getattr(plan, "runnable", False)):
        return False
    delivery = getattr(context, "runtime_artifact_delivery", {})
    if not isinstance(delivery, Mapping) or delivery.get("mode") != "init_fetch":
        return False
    packs = getattr(context, "workload_packs", ())
    return bool(packs)


def _missing_pack_reader(artifact_ref: str) -> bytes:
    del artifact_ref
    raise ValueError("Pinned workload pack is unavailable.")


__all__ = [
    "build_safe_sample_runtime_data_copier",
    "prepare_auto_live_safe_sample_runtime",
    "run_local_safe_sample_runtime_handoff",
]
