"""Local immutable-input checks for live safe-sample composition."""

from __future__ import annotations

import importlib
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Protocol

from dpone.manifest.confined_files import project_relative_path, sha256_confined_file
from dpone.readiness.airflow_deployment_projection import deployment_payload_fingerprint
from dpone.readiness.safe_sample_live_errors import LiveSafeSampleRuntimeAssemblyError
from dpone.readiness.safe_sample_pinned_source import (
    PinnedWorkloadSourceError,
    PinnedWorkloadSourceVerifier,
)


class _SafeSampleDeploymentContext(Protocol):
    @property
    def release_id(self) -> str: ...

    @property
    def workload_packs(self) -> tuple[dict[str, Any], ...]: ...

    @property
    def binding_set_ref(self) -> str | None: ...

    @property
    def connection_registry_ref(self) -> str | None: ...

    @property
    def credential_runtime_ref(self) -> str | None: ...


class _SafeSampleTargetPlan(Protocol):
    @property
    def process(self) -> str: ...

    @property
    def pipeline_id(self) -> str: ...


class _SafeSampleSourceSnapshot(Protocol):
    @property
    def pipeline_id(self) -> str: ...

    @property
    def path(self) -> str: ...

    @property
    def sha256(self) -> str: ...


class SafeSampleLiveIntegrityPlan(Protocol):
    @property
    def deployment_context(self) -> _SafeSampleDeploymentContext | None: ...

    @property
    def temporary_target_plan(self) -> _SafeSampleTargetPlan | None: ...

    @property
    def source_snapshot(self) -> _SafeSampleSourceSnapshot | None: ...


class SafeSampleLiveIntegrityInputs(Protocol):
    @property
    def pipeline_source_path(self) -> Path: ...

    @property
    def pipeline_source_sha256(self) -> str: ...

    @property
    def pipeline_source_dependencies(self) -> tuple[Any, ...]: ...

    @property
    def binding_set(self) -> Mapping[str, Any]: ...

    @property
    def connection_registry(self) -> Mapping[str, Any]: ...

    @property
    def credential_runtime(self) -> Mapping[str, Any]: ...


def verify_pinned_source(
    plan: SafeSampleLiveIntegrityPlan,
    inputs: SafeSampleLiveIntegrityInputs,
    *,
    cache_root: str | Path,
    source_root: str | Path | None,
) -> None:
    """Verify the selected source bytes against the immutable workload pack."""

    context = plan.deployment_context
    target = plan.temporary_target_plan
    if context is None or target is None:
        raise LiveSafeSampleRuntimeAssemblyError(
            "DPONE_SAFE_SAMPLE_PIPELINE_SOURCE_PIN_MISSING",
            "Live safe-sample execution requires a pinned workload pack.",
        )
    if source_root is None:
        raise LiveSafeSampleRuntimeAssemblyError(
            "DPONE_SAFE_SAMPLE_SOURCE_ROOT_REQUIRED",
            "Live safe-sample execution requires an explicit project source root.",
        )
    artifact_delivery = importlib.import_module("dpone.runtime.artifact_delivery")
    pack_reader = artifact_delivery.LocalArtifactRegistry(cache_root).read_bytes
    project_root = Path(source_root)
    _verify_plan_source_snapshot(plan, inputs, project_root=project_root)
    try:
        PinnedWorkloadSourceVerifier().verify(
            workload_id=target.pipeline_id,
            release_id=context.release_id,
            indexed_packs=context.workload_packs,
            source_path=inputs.pipeline_source_path,
            source_sha256=inputs.pipeline_source_sha256,
            source_dependencies=tuple(dependency.to_jsonable() for dependency in inputs.pipeline_source_dependencies),
            source_dependency_digester=lambda path: sha256_confined_file(
                project_root,
                path,
                follow_in_root_symlinks=True,
            ),
            source_root=source_root,
            pack_reader=pack_reader,
        )
    except PinnedWorkloadSourceError as exc:
        raise LiveSafeSampleRuntimeAssemblyError(exc.code, str(exc)) from exc


def _verify_plan_source_snapshot(
    plan: SafeSampleLiveIntegrityPlan,
    inputs: SafeSampleLiveIntegrityInputs,
    *,
    project_root: Path,
) -> None:
    snapshot = plan.source_snapshot
    target = plan.temporary_target_plan
    if snapshot is None:
        raise LiveSafeSampleRuntimeAssemblyError(
            "DPONE_SAFE_SAMPLE_PIPELINE_SOURCE_PIN_MISSING",
            "Live safe-sample execution requires a canonical source snapshot; regenerate the execution plan.",
        )
    if target is None:
        raise LiveSafeSampleRuntimeAssemblyError(
            "DPONE_SAFE_SAMPLE_PIPELINE_SOURCE_PIN_MISSING",
            "Live safe-sample execution requires a pinned temporary target.",
        )
    try:
        relative_path = project_relative_path(project_root.resolve(strict=True), inputs.pipeline_source_path)
    except (OSError, ValueError) as exc:
        raise LiveSafeSampleRuntimeAssemblyError(
            "DPONE_SAFE_SAMPLE_PIPELINE_SOURCE_FINGERPRINT_MISMATCH",
            "Pipeline source no longer matches the checked source snapshot.",
        ) from exc
    if (
        snapshot.pipeline_id != target.pipeline_id
        or snapshot.path != relative_path
        or snapshot.sha256 != inputs.pipeline_source_sha256
    ):
        raise LiveSafeSampleRuntimeAssemblyError(
            "DPONE_SAFE_SAMPLE_PIPELINE_SOURCE_FINGERPRINT_MISMATCH",
            "Pipeline source no longer matches the checked source snapshot.",
        )


def verify_pinned_environment_inputs(
    plan: SafeSampleLiveIntegrityPlan,
    inputs: SafeSampleLiveIntegrityInputs,
) -> None:
    """Verify all environment payloads against deployment fingerprints."""

    context = plan.deployment_context
    if context is None:
        raise LiveSafeSampleRuntimeAssemblyError(
            "DPONE_SAFE_SAMPLE_LIVE_POLICY_INVALID",
            "Live safe-sample execution requires pinned deployment context.",
        )
    for label, payload, expected, code in (
        (
            "binding-set",
            inputs.binding_set,
            context.binding_set_ref,
            "DPONE_SAFE_SAMPLE_BINDING_SET_FINGERPRINT_MISMATCH",
        ),
        (
            "connection-registry",
            inputs.connection_registry,
            context.connection_registry_ref,
            "DPONE_SAFE_SAMPLE_CONNECTION_REGISTRY_FINGERPRINT_MISMATCH",
        ),
        (
            "credential-runtime",
            inputs.credential_runtime,
            context.credential_runtime_ref,
            "DPONE_SAFE_SAMPLE_CREDENTIAL_RUNTIME_FINGERPRINT_MISMATCH",
        ),
    ):
        if not expected or deployment_payload_fingerprint(dict(payload)) != expected:
            raise LiveSafeSampleRuntimeAssemblyError(
                code,
                f"{label} content does not match the pinned deployment fingerprint; rebuild the deployment.",
            )


__all__ = [
    "SafeSampleLiveIntegrityInputs",
    "SafeSampleLiveIntegrityPlan",
    "verify_pinned_environment_inputs",
    "verify_pinned_source",
]
