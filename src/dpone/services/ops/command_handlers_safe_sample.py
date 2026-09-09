from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import TYPE_CHECKING

from dpone.services.safe_sample_execution_plan_io import (
    SafeSampleExecutionPlanValidationError,
    load_safe_sample_execution_plan,
)
from dpone.services.safe_sample_redaction import contains_sensitive_assignment

from .command_helpers import _emit, _route_attestation_exit_code

if TYPE_CHECKING:
    from collections.abc import Mapping

    from dpone.services.safe_sample_execution_plan import SafeSampleExecutionPlan
    from dpone.services.safe_sample_runtime_executor import SafeSampleDataCopier
    from dpone.services.safe_sample_target_lifecycle import TemporaryTargetLifecycleExecutor


def cmd_safe_sample_runtime_run(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    """Execute a prepared safe-sample runtime plan with pinned local artifacts."""

    del logger
    usage_error = _live_copy_usage_error(args)
    if usage_error is not None:
        _emit(usage_error, _error_markdown(usage_error), args.format)
        return 2

    plan, plan_error = _plan_or_error(args)
    if plan_error is not None:
        _emit(plan_error, _error_markdown(plan_error), args.format)
        return 2
    assert plan is not None

    runtime_plan, data_copier, temporary_target_executor, route_verification, input_error = _runtime_ports_or_error(
        args, plan, ctx=ctx
    )
    if input_error is not None:
        _emit(input_error, _error_markdown(input_error), args.format)
        code = str(input_error.get("code") or "")
        return _route_attestation_exit_code(code) if code.startswith("DPONE_ROUTE_ATTESTATION_") else 2

    from dpone.readiness.safe_sample_runtime_handoff import run_local_safe_sample_runtime_handoff

    assert runtime_plan is not None
    try:
        report = run_local_safe_sample_runtime_handoff(
            runtime_plan,
            output_dir=args.output_dir,
            cache_root=args.cache_root,
            data_copier=data_copier,
            temporary_target_executor=temporary_target_executor,
            route_attestation_verification=route_verification,
        )
    except Exception as exc:  # noqa: BLE001 - pin verification must remain a structured CLI error.
        code = str(getattr(exc, "code", "") or "")
        if code.startswith("DPONE_"):
            error = _error(code, _safe_exception_message(exc))
            _emit(error, _error_markdown(error), args.format)
            return 2
        raise
    payload = report.to_dict()
    _emit(payload, _markdown(payload), args.format)
    return 0 if payload.get("execution_status") == "succeeded" and not payload.get("errors") else 1


def _plan_or_error(args: argparse.Namespace) -> tuple[SafeSampleExecutionPlan | None, dict[str, object] | None]:
    try:
        return load_safe_sample_execution_plan(
            args.plan_json,
            validator=getattr(args, "execution_plan_validator", None),
        ), None
    except SafeSampleExecutionPlanValidationError as exc:
        return None, _error(
            "DPONE_SAFE_SAMPLE_EXECUTION_PLAN_INVALID",
            _safe_exception_message(exc),
        )
    except Exception as exc:  # noqa: BLE001 - CLI must return stable structured config errors.
        return None, _error(
            "DPONE_SAFE_SAMPLE_EXECUTION_PLAN_INVALID",
            "Safe sample execution plan could not be loaded: " + _safe_exception_message(exc),
        )


def _markdown(payload: dict[str, object]) -> str:
    lines = [
        "# dpone Safe Sample Runtime Run",
        "",
        f"- release_id: `{payload.get('release_id')}`",
        f"- deployment_id: `{payload.get('deployment_id')}`",
        f"- execution_status: `{payload.get('execution_status')}`",
        f"- data_outcome: `{payload.get('data_outcome')}`",
        f"- errors: `{_error_count(payload)}`",
    ]
    evidence = payload.get("evidence_write")
    if isinstance(evidence, dict) and evidence.get("path"):
        lines.append(f"- evidence: `{evidence['path']}`")
    return "\n".join(lines) + "\n"


def _error_markdown(error: dict[str, object]) -> str:
    return f"# dpone Safe Sample Runtime Run\n\n- error: `{error.get('code')}`\n- message: {error.get('message')}\n"


def _error_count(payload: dict[str, object]) -> int:
    errors = payload.get("errors")
    return len(errors) if isinstance(errors, list) else 0


def _live_copy_usage_error(args: argparse.Namespace) -> dict[str, object] | None:
    route_inputs = (
        getattr(args, "route_attestation", None),
        getattr(args, "route_attestation_bundle", None),
        getattr(args, "route_certification_bundle", None),
        getattr(args, "route_attestation_policy", None),
    )
    if not bool(getattr(args, "enable_live_copy", False)):
        if any(route_inputs):
            return _error(
                "DPONE_ROUTE_ATTESTATION_LIVE_COPY_REQUIRED",
                "Route-attestation inputs are valid only with --enable-live-copy.",
            )
        return None
    if not getattr(args, "pipeline_source", None):
        return _error(
            "DPONE_SAFE_SAMPLE_LIVE_COPY_REQUIRES_PIPELINE_SOURCE",
            "--enable-live-copy requires --pipeline-source so the certified route copier can be assembled.",
        )
    missing = [
        name
        for name, value in (
            ("--binding-set", getattr(args, "binding_set", None)),
            ("--connection-registry", getattr(args, "connection_registry", None)),
            ("--credential-runtime", getattr(args, "credential_runtime", None)),
            ("--route-attestation", getattr(args, "route_attestation", None)),
            ("--route-attestation-bundle", getattr(args, "route_attestation_bundle", None)),
            ("--route-certification-bundle", getattr(args, "route_certification_bundle", None)),
            ("--route-attestation-policy", getattr(args, "route_attestation_policy", None)),
        )
        if not value
    ]
    if missing:
        return _error(
            "DPONE_SAFE_SAMPLE_LIVE_COPY_REQUIRES_RUNTIME_BINDINGS",
            "--enable-live-copy requires " + " and ".join(missing) + ".",
        )
    return None


def _data_copier_or_error(
    args: argparse.Namespace,
    plan: SafeSampleExecutionPlan,
) -> tuple[SafeSampleDataCopier | None, dict[str, object] | None]:
    try:
        return _data_copier(args, plan), None
    except Exception as exc:  # noqa: BLE001 - CLI must return stable structured config errors.
        code = (
            "DPONE_SAFE_SAMPLE_LIVE_COPY_INPUT_INVALID"
            if bool(getattr(args, "enable_live_copy", False))
            else "DPONE_SAFE_SAMPLE_COPIER_INPUT_INVALID"
        )
        return None, _error(
            code,
            "Safe sample runtime copier inputs are invalid: " + _safe_exception_message(exc),
        )


def _runtime_ports_or_error(
    args: argparse.Namespace,
    plan: SafeSampleExecutionPlan,
    *,
    ctx: object,
) -> tuple[
    SafeSampleExecutionPlan | None,
    SafeSampleDataCopier | None,
    TemporaryTargetLifecycleExecutor | None,
    dict[str, object] | None,
    dict[str, object] | None,
]:
    if not bool(getattr(args, "enable_live_copy", False)):
        data_copier, error = _data_copier_or_error(args, plan)
        return plan, data_copier, None, None, error

    try:
        from dpone.readiness.safe_sample_live_runtime import (
            LiveSafeSampleRuntimeAssemblyError,
            build_live_safe_sample_runtime_assembly,
        )
        from dpone.services.safe_sample_live_authorization import SafeSampleLiveAuthorizationError

        assembly = build_live_safe_sample_runtime_assembly(
            plan=plan,
            pipeline_source_path=args.pipeline_source,
            binding_set_path=args.binding_set,
            connection_registry_path=args.connection_registry,
            credential_runtime_path=args.credential_runtime,
            route_attestation_path=args.route_attestation,
            route_attestation_bundle_path=args.route_attestation_bundle,
            route_certification_bundle_path=args.route_certification_bundle,
            route_attestation_policy_path=args.route_attestation_policy,
            cache_root=args.cache_root,
            source_root=_project_root(ctx),
            process_name=getattr(args, "process_name", None),
            evidence_context=_credential_evidence_context(plan),
            route_attestation_signature_verifier=getattr(args, "route_attestation_signature_verifier", None),
        )
        return (
            assembly.plan,
            assembly.data_copier,
            assembly.temporary_target_executor,
            assembly.route_attestation_verification.to_dict(),
            None,
        )
    except (LiveSafeSampleRuntimeAssemblyError, SafeSampleLiveAuthorizationError) as exc:
        return None, None, None, None, _error(exc.code, _safe_exception_message(exc))
    except Exception as exc:  # noqa: BLE001 - CLI must return stable structured config errors.
        return (
            None,
            None,
            None,
            None,
            _error(
                "DPONE_SAFE_SAMPLE_LIVE_COPY_INPUT_INVALID",
                "Safe sample live runtime inputs are invalid: " + _safe_exception_message(exc),
            ),
        )


def _project_root(ctx: object) -> Path | None:
    settings = getattr(ctx, "settings", None)
    value = getattr(settings, "project_dir", None) or getattr(settings, "repo_root", None)
    return Path(value) if value is not None else None


def _data_copier(args: argparse.Namespace, plan: SafeSampleExecutionPlan) -> SafeSampleDataCopier | None:
    pipeline_source_path = getattr(args, "pipeline_source", None)
    if not pipeline_source_path:
        return None

    from dpone.readiness.safe_sample_runtime_handoff import build_safe_sample_runtime_data_copier

    return build_safe_sample_runtime_data_copier(
        pipeline_source_path=pipeline_source_path,
        process_name=getattr(args, "process_name", None),
        enable_live_copy=bool(getattr(args, "enable_live_copy", False)),
        binding_set_path=getattr(args, "binding_set", None),
        connection_registry_path=getattr(args, "connection_registry", None),
        credential_runtime_path=getattr(args, "credential_runtime", None),
        evidence_context=_credential_evidence_context(plan),
    )


def _credential_evidence_context(plan: SafeSampleExecutionPlan) -> Mapping[str, object]:
    context = getattr(plan, "deployment_context", None)
    if context is None or not hasattr(context, "to_dict"):
        return {}
    payload = context.to_dict()
    return {
        "release_id": payload.get("release_id"),
        "deployment_id": payload.get("deployment_id"),
        "binding_set_fingerprint": payload.get("binding_set_ref"),
        "connection_registry_fingerprint": payload.get("connection_registry_ref"),
        "credential_runtime_fingerprint": payload.get("credential_runtime_ref"),
        "runtime_image_digest": payload.get("runtime_image_digest"),
        "airflow_bundle_ref": payload.get("airflow_bundle_ref"),
    }


def _safe_exception_message(exc: Exception) -> str:
    message = " ".join(str(exc).split()) or exc.__class__.__name__
    if contains_sensitive_assignment(message):
        return "details redacted; check input files and resolver configuration."
    return message[:500]


def _error(code: str, message: str) -> dict[str, object]:
    return {
        "schema": "dpone.error.v1",
        "code": code,
        "stage": "safe_sample_runtime_run_cli",
        "severity": "error",
        "message": message,
        "fixes": [],
    }


__all__ = ["cmd_safe_sample_runtime_run"]
