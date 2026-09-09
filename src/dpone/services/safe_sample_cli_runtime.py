"""Local fail-closed runtime handoff for beginner safe sample CLI output."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from dpone._compat import UTC
from dpone.services.safe_sample_redaction import redact_safe_sample_text
from dpone.services.safe_sample_runtime_evidence import SafeSampleRuntimeEvidenceWriter
from dpone.services.safe_sample_runtime_executor import SafeSampleRuntimeExecutor
from dpone.services.safe_sample_runtime_handoff_plan import (
    SafeSampleRuntimeHandoffPathError,
    write_safe_sample_runtime_handoff,
)
from dpone.services.safe_sample_runtime_runner import SafeSampleRuntimeRunner
from dpone.services.safe_sample_target_lifecycle import TemporaryTargetLifecycleExecutor

if TYPE_CHECKING:
    from dpone.services.safe_sample_execution_plan import SafeSampleExecutionPlan
    from dpone.services.safe_sample_policy import TemporaryTargetPlan
    from dpone.services.safe_sample_runtime_executor import SafeSampleArtifactFetcher, SafeSampleDataCopier
    from dpone.services.safe_sample_runtime_runner import SafeSampleRuntimeRunReport


class LocalFailClosedSafeSampleArtifactFetcher:
    """Parse-safe artifact-fetch boundary for CLI previews.

    The real runtime uses init containers and pinned artifact registries. The
    beginner CLI must not fetch remote artifacts, so this port only records that
    a local fail-closed preview skipped artifact IO.
    """

    def fetch(self, plan: SafeSampleExecutionPlan) -> dict[str, Any]:
        context = plan.deployment_context
        return {
            "schema": "dpone.init-fetch-result.v1",
            "status": "skipped",
            "mode": "local_fail_closed_preview",
            "network": False,
            "release_id": context.release_id if context is not None else None,
            "deployment_id": context.deployment_id if context is not None else None,
            "reason": "Beginner CLI safe sample preview does not fetch runtime artifacts.",
        }


class LocalFailClosedTemporaryTargetAdapter:
    """Temporary-target adapter that never creates or drops physical tables."""

    def create(self, plan: TemporaryTargetPlan) -> dict[str, object]:
        return _metadata(plan, action="create", applied=False)

    def drop(self, plan: TemporaryTargetPlan) -> dict[str, object]:
        return _metadata(plan, action="drop", applied=False)


def run_local_fail_closed_safe_sample_runtime(
    plan: SafeSampleExecutionPlan,
    *,
    output_dir: str | Path,
    artifact_fetcher: SafeSampleArtifactFetcher | None = None,
    data_copier: SafeSampleDataCopier | None = None,
    temporary_target_executor: TemporaryTargetLifecycleExecutor | None = None,
    route_attestation_verification: dict[str, Any] | None = None,
) -> SafeSampleRuntimeRunReport:
    """Run with fail-closed defaults and explicitly injected live ports."""

    runner = SafeSampleRuntimeRunner(
        executor=SafeSampleRuntimeExecutor(
            artifact_fetcher=artifact_fetcher or LocalFailClosedSafeSampleArtifactFetcher(),
            temporary_target_executor=temporary_target_executor
            or TemporaryTargetLifecycleExecutor(adapter=LocalFailClosedTemporaryTargetAdapter()),
            data_copier=data_copier,
            route_attestation_verification=route_attestation_verification,
        ),
        evidence_writer=SafeSampleRuntimeEvidenceWriter(),
    )
    return runner.run(plan, output_dir=output_dir)


def default_safe_sample_runtime_output_dir(plan: SafeSampleExecutionPlan, *, run_id: str | None = None) -> Path:
    target = plan.temporary_target_plan
    pipeline_id = _safe_path_part(target.pipeline_id if target is not None else "pipeline")
    run_part = _safe_path_part(run_id or new_safe_sample_run_id())
    return Path(".dpone-cache") / "safe-sample-runs" / pipeline_id / run_part


def new_safe_sample_run_id() -> str:
    """Return a sortable, filesystem-safe identity for one sample invocation."""

    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
    return f"{timestamp}-{uuid4().hex[:8]}"


def build_safe_sample_cli_data_copier(
    *,
    pipeline_source_path: str | Path | None = None,
    pipeline_source: Mapping[str, Any] | None = None,
    process_name: str | None,
) -> tuple[SafeSampleDataCopier | None, dict[str, object] | None]:
    """Build the certified copier preview for the beginner CLI, failing closed."""

    try:
        from dpone.readiness.safe_sample_runtime_handoff import build_safe_sample_runtime_data_copier

        return (
            build_safe_sample_runtime_data_copier(
                pipeline_source_path=pipeline_source_path,
                pipeline_source=pipeline_source,
                process_name=process_name,
            ),
            None,
        )
    except Exception as exc:  # noqa: BLE001 - route-specific copier remains optional in the CLI.
        return None, {
            "schema": "dpone.error.v1",
            "code": "DPONE_SAFE_SAMPLE_CERTIFIED_COPIER_UNAVAILABLE",
            "stage": "safe_sample_data_copier",
            "severity": "warning",
            "message": _safe_sample_message(exc),
            "fixes": [],
        }


def _metadata(plan: TemporaryTargetPlan, *, action: str, applied: bool) -> dict[str, object]:
    return {
        "backend": plan.sink_type,
        "mode": "local_fail_closed_preview",
        "action": action,
        "applied": applied,
        "network": False,
        "temporary_table": dict(plan.temporary_table),
    }


def _safe_path_part(value: str) -> str:
    normalized = "".join(char if char.isalnum() or char in {"_", "-"} else "_" for char in str(value or ""))
    return normalized.strip("._-") or "sample"


def _safe_sample_message(exc: Exception) -> str:
    message = " ".join(str(exc).split()) or exc.__class__.__name__
    return redact_safe_sample_text(message)[:500]


__all__ = [
    "LocalFailClosedSafeSampleArtifactFetcher",
    "LocalFailClosedTemporaryTargetAdapter",
    "SafeSampleRuntimeHandoffPathError",
    "build_safe_sample_cli_data_copier",
    "default_safe_sample_runtime_output_dir",
    "new_safe_sample_run_id",
    "run_local_fail_closed_safe_sample_runtime",
    "write_safe_sample_runtime_handoff",
]
