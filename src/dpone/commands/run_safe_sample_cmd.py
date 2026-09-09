"""Fail-closed command handler for ``dpone run --sample``."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.readiness.airflow_authoring_check_service import CheckedPipelineSource
    from dpone.services.safe_sample_execution_plan import SafeSampleExecutionPlan
    from dpone.services.safe_sample_policy import SafeSamplePolicyResult, TemporaryTargetPlan


import argparse
from pathlib import Path

from dpone.commands.run_safe_sample_results import (
    SafeSampleCommandResult,
    argument_error_result,
    not_implemented_error,
    public_pipeline_source,
    safe_sample_result,
    source_error_result,
)
from dpone.commands.run_safe_sample_runtime_selection import select_safe_sample_runtime
from dpone.readiness.airflow_authoring_check_service import (
    AirflowAuthoringCheckService,
    CheckedPipelineSourceChangedError,
    verify_checked_pipeline_source,
)
from dpone.readiness.airflow_local_safe_sample_deployment import (
    LocalSafeSampleDeploymentResult,
    ensure_local_safe_sample_deployment,
    local_safe_sample_cache_root,
)
from dpone.readiness.airflow_verified_current_deployment import (
    resolve_verified_airflow_deployment_context,
)
from dpone.services.mssql_clickhouse_safe_sample_copier import (
    MssqlClickHouseSafeSampleCopyConfigError,
    build_mssql_clickhouse_safe_sample_copy_request_from_pipeline_source,
)
from dpone.services.safe_sample_cli_arguments import (
    safe_sample_cli_argument_error_lines,
    safe_sample_incompatible_options,
    selected_safe_sample_fix_command,
    validate_safe_sample_cli_arguments,
)
from dpone.services.safe_sample_cli_result import select_safe_sample_cli_result
from dpone.services.safe_sample_cli_runtime import (
    SafeSampleRuntimeHandoffPathError,
    build_safe_sample_cli_data_copier,
    default_safe_sample_runtime_output_dir,
    new_safe_sample_run_id,
    write_safe_sample_runtime_handoff,
)
from dpone.services.safe_sample_execution_plan import (
    SafeSampleExecutionPlanBuilder,
    SafeSampleSourceSnapshot,
)
from dpone.services.safe_sample_policy import (
    SafeSamplePlanError,
    SafeSamplePolicyEvaluator,
    SafeSamplePolicySet,
    SampleRunRequest,
    SampleTarget,
    TemporaryTargetPlanner,
    detect_source_sampling_capabilities,
)
from dpone.services.safe_sample_redaction import redact_safe_sample_text
from dpone.services.safe_sample_runtime_readiness import SafeSampleRuntimeReadinessEvaluator
from dpone.services.safe_sample_source_request import SafeSampleSourceRequestBuilder


def write_safe_sample(args: argparse.Namespace) -> int:
    """Validate arguments, prepare pinned artifacts, and run one safe sample."""

    return build_safe_sample_result(args).write(format=str(getattr(args, "format", "text")))


def build_safe_sample_result(args: argparse.Namespace) -> SafeSampleCommandResult:
    """Execute one safe sample without writing to the command output boundary."""

    argument_error = validate_safe_sample_cli_arguments(
        path=str(getattr(args, "path", "")),
        sample=getattr(args, "sample", None),
        target=getattr(args, "target", None),
        incompatible_options=safe_sample_incompatible_options(args),
        fix_command=selected_safe_sample_fix_command(args),
    )
    if argument_error is not None:
        return argument_error_result(
            args,
            argument_error,
            text_lines=safe_sample_cli_argument_error_lines(argument_error),
            markdown_lines=safe_sample_cli_argument_error_lines(argument_error, markdown=True),
        )
    checked = AirflowAuthoringCheckService(root=Path.cwd()).inspect(str(getattr(args, "path", "")))
    if not checked.result.passed:
        return source_error_result(args, checked)
    return _execute_safe_sample(args, checked)


def _execute_safe_sample(
    args: argparse.Namespace,
    checked: CheckedPipelineSource,
) -> SafeSampleCommandResult:
    assert checked.payload is not None
    assert checked.compilation is not None
    assert checked.source_label is not None
    assert checked.source_sha256 is not None
    source_path = checked.source_path
    pipeline_source = {
        **checked.payload,
        "processes": [dict(process) for process in checked.compilation.processes],
    }
    run_id = _effective_run_id(args)
    policy_result = _evaluate_policy(args, pipeline_source=pipeline_source)
    temporary_target_plan, plan_errors = _build_temporary_target_plan(
        args,
        pipeline_source=pipeline_source,
        run_id=run_id,
    )
    policy_environment = str(getattr(args, "environment", "") or "development")
    preparation_allowed = policy_result.passed and not plan_errors and temporary_target_plan is not None
    local_deployment = (
        ensure_local_safe_sample_deployment(
            root=".",
            pipeline_source_path=source_path,
            policy_environment=policy_environment,
            checked_source=checked,
        )
        if preparation_allowed
        else LocalSafeSampleDeploymentResult(status="skipped", environment=None)
    )
    preparation_error = local_deployment.error if local_deployment.status == "failed" else None
    deployment_context = None
    deployment_cache_root = local_safe_sample_cache_root(".") if local_deployment.usable else Path(".dpone-cache")
    if preparation_allowed and (local_deployment.usable or local_deployment.status == "skipped"):
        resolution = resolve_verified_airflow_deployment_context(deployment_cache_root)
        deployment_context = resolution.context
        if preparation_error is None and resolution.error is not None:
            preparation_error = resolution.error
    source_snapshot = (
        SafeSampleSourceSnapshot(
            pipeline_id=str(checked.compilation.pipeline_id),
            path=checked.source_label,
            sha256=checked.source_sha256,
        )
        if checked.compilation.pipeline_id is not None
        else None
    )
    execution_plan = SafeSampleExecutionPlanBuilder().build(
        sample_rows=int(getattr(args, "sample", 0) or 0),
        environment=str(getattr(args, "environment", "") or "development"),
        policy_result=policy_result,
        temporary_target_plan=temporary_target_plan,
        deployment_context=deployment_context,
        preparation_error=preparation_error,
        expected_release_id=local_deployment.release_id if local_deployment.usable else None,
        expected_deployment_id=local_deployment.deployment_id if local_deployment.usable else None,
        source_snapshot=source_snapshot,
    )
    process_name = execution_plan.temporary_target_plan.process if execution_plan.temporary_target_plan else None
    data_copier, data_copier_error = build_safe_sample_cli_data_copier(
        pipeline_source_path=source_path,
        pipeline_source=pipeline_source,
        process_name=process_name,
    )
    runtime_readiness = SafeSampleRuntimeReadinessEvaluator().evaluate(
        execution_plan,
        temporary_target_plan=temporary_target_plan,
        certified_data_copier_available=data_copier is not None,
    )
    runtime_readiness_payload = runtime_readiness.to_dict()
    safe_sample: dict[str, object] = {
        "run_id": run_id,
        "sample": getattr(args, "sample", None),
        "target": getattr(args, "target", None),
        "environment": getattr(args, "environment", "development"),
        "local_deployment": local_deployment.to_dict(),
        "policy": policy_result.policy.to_dict(),
        "policy_result": policy_result.to_dict(),
        "execution_plan": execution_plan.to_dict(),
        "runtime_readiness": runtime_readiness_payload,
        "available_contracts": runtime_readiness_payload["available_contracts"],
        "blockers": runtime_readiness_payload["blockers"],
        "source_snapshot": execution_plan.source_snapshot.to_dict() if execution_plan.source_snapshot else None,
    }
    if data_copier_error is not None:
        safe_sample["data_copier_error"] = data_copier_error
    runtime_run: dict[str, object] | None = None
    pre_runtime_errors: tuple[dict[str, object], ...] = ()
    if temporary_target_plan is not None:
        output_dir = default_safe_sample_runtime_output_dir(
            execution_plan,
            run_id=run_id,
        )
        if preparation_allowed:
            try:
                verify_checked_pipeline_source(Path.cwd(), checked)
                safe_sample["runtime_handoff"] = write_safe_sample_runtime_handoff(
                    execution_plan,
                    output_dir=output_dir,
                    pipeline_source_path=source_path.as_posix(),
                )
            except CheckedPipelineSourceChangedError as exc:
                pre_runtime_errors = (_source_changed_error(exc),)
                safe_sample["execution_mode"] = "blocked"
            except SafeSampleRuntimeHandoffPathError as exc:
                pre_runtime_errors = (exc.to_error(),)
                safe_sample["execution_mode"] = "blocked"
        else:
            safe_sample["execution_mode"] = "blocked"
        safe_sample["temporary_target_plan"] = temporary_target_plan.to_dict()
        safe_sample["source_request"] = (
            SafeSampleSourceRequestBuilder()
            .build(
                policy_result,
                temporary_target_plan,
            )
            .to_dict()
        )
        safe_sample.update(
            _build_certified_copy_request_preview(
                args,
                execution_plan,
                temporary_target_plan,
                pipeline_source=pipeline_source,
            )
        )
        if execution_plan.runnable and not pre_runtime_errors:
            runtime_selection = select_safe_sample_runtime(
                execution_plan,
                output_dir=output_dir,
                pipeline_source_path=_runtime_pipeline_source_ref(args, checked),
                project_root=Path.cwd(),
                artifact_cache_root=deployment_cache_root,
                process_name=process_name,
                data_copier=data_copier,
                signature_verifier=getattr(args, "route_attestation_signature_verifier", None),
            )
            safe_sample.update(runtime_selection.payload)
            pre_runtime_errors = runtime_selection.errors
            runtime_run = runtime_selection.runtime_run
        else:
            safe_sample["execution_mode"] = "blocked"
    cli_result = select_safe_sample_cli_result(
        policy_errors=policy_result.errors,
        target_plan_errors=plan_errors,
        execution_plan=execution_plan,
        pre_runtime_errors=pre_runtime_errors,
        runtime_run=runtime_run,
        fallback_error=not_implemented_error(),
    )
    return safe_sample_result(
        args,
        run_id=run_id,
        cli_result=cli_result,
        safe_sample=safe_sample,
        runtime_run=runtime_run,
    )


def _evaluate_policy(
    args: argparse.Namespace,
    *,
    pipeline_source: dict[str, object],
) -> SafeSamplePolicyResult:
    environment = str(getattr(args, "environment", "") or "development")
    policy = SafeSamplePolicySet.default().for_environment(environment)
    capabilities = detect_source_sampling_capabilities(
        pipeline_source,
        process_selector=getattr(args, "selector", None),
    )
    request = SampleRunRequest(
        sample_rows=int(getattr(args, "sample", 0) or 0),
        target=SampleTarget.TEMPORARY,
        environment=environment,
    )
    return SafeSamplePolicyEvaluator().evaluate(request, policy, capabilities)


def _build_certified_copy_request_preview(
    args: argparse.Namespace,
    execution_plan: SafeSampleExecutionPlan,
    temporary_target_plan: TemporaryTargetPlan,
    pipeline_source: dict[str, object],
) -> dict[str, object]:
    try:
        return {
            "certified_copy_request": build_mssql_clickhouse_safe_sample_copy_request_from_pipeline_source(
                pipeline_source,
                process_name=temporary_target_plan.process,
                plan=execution_plan,
                target_plan=temporary_target_plan,
            )
        }
    except MssqlClickHouseSafeSampleCopyConfigError as exc:
        return {"certified_copy_request_error": exc.to_error()}
    except Exception as exc:  # noqa: BLE001 - preview must stay diagnostic and fail-closed.
        return {
            "certified_copy_request_error": {
                "schema": "dpone.error.v1",
                "code": "DPONE_SAFE_SAMPLE_CERTIFIED_COPY_PREVIEW_FAILED",
                "stage": "safe_sample_certified_copy_preview",
                "severity": "error",
                "message": redact_safe_sample_text(str(exc)),
                "entity": {
                    "kind": "pipeline_source",
                    "id": public_pipeline_source(getattr(args, "path", "")),
                },
                "fixes": [],
            }
        }


def _build_temporary_target_plan(
    args: argparse.Namespace,
    *,
    pipeline_source: dict[str, object],
    run_id: str,
) -> tuple[TemporaryTargetPlan | None, list[dict[str, object]]]:
    try:
        return (
            TemporaryTargetPlanner().plan(
                pipeline_source=pipeline_source,
                environment=str(getattr(args, "environment", "") or "development"),
                run_id=run_id,
                process_selector=getattr(args, "selector", None),
            ),
            [],
        )
    except SafeSamplePlanError as exc:
        return (
            None,
            [
                {
                    "schema": "dpone.error.v1",
                    "code": exc.code,
                    "stage": "safe_sample_target_plan",
                    "severity": "error",
                    "message": redact_safe_sample_text(str(exc)),
                    "path": public_pipeline_source(exc.path or getattr(args, "path", "")),
                    "fixes": [],
                    **(
                        {"docs_url": "docs/errors/DPONE_PIPELINE_ID_INVALID.md"}
                        if exc.code == "DPONE_PIPELINE_ID_INVALID"
                        else {}
                    ),
                }
            ],
        )


def _effective_run_id(args: argparse.Namespace) -> str:
    configured = str(getattr(args, "run_id", "") or "").strip()
    return configured or new_safe_sample_run_id()


def _runtime_pipeline_source_ref(
    args: argparse.Namespace,
    checked: CheckedPipelineSource,
) -> str:
    raw = str(getattr(args, "path", "") or "")
    parsed = Path(raw)
    if parsed.is_absolute() or parsed.suffix in {".yaml", ".yml"} or "/" in raw:
        return raw
    return checked.source_label or checked.source_path.as_posix()


def _source_changed_error(exc: CheckedPipelineSourceChangedError) -> dict[str, object]:
    return {
        "schema": "dpone.error.v1",
        "code": exc.code,
        "stage": "safe_sample_runtime_handoff",
        "severity": "error",
        "message": "Pipeline source changed after validation; rerun the safe sample command.",
        "fixes": [],
    }


__all__ = [
    "SafeSampleCommandResult",
    "build_safe_sample_result",
    "new_safe_sample_run_id",
    "write_safe_sample",
]
