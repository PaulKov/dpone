from __future__ import annotations

import argparse
import logging
import sys
import time
from contextlib import redirect_stdout, suppress
from dataclasses import replace
from io import StringIO
from uuid import uuid4

from dpone.commands.airflow_artifact_attestation_cmd import (
    artifact_attestation_group,
    cmd_airflow_artifact_attestation_verify,
    register_artifact_attestation_verify_parser,
)
from dpone.commands.airflow_artifact_delivery_cmd import (
    cmd_airflow_artifact_publish,
    cmd_airflow_cache_materialize,
    register_artifact_publish_parser,
    register_cache_materialize_parser,
)
from dpone.commands.airflow_cache_recovery_cmd import (
    cmd_airflow_cache_recovery_apply,
    cmd_airflow_cache_recovery_plan,
    register_cache_recovery_apply_parser,
    register_cache_recovery_plan_parser,
)
from dpone.commands.airflow_cache_retention_cmd import (
    cmd_airflow_cache_retention_apply,
    cmd_airflow_cache_retention_plan,
    register_cache_retention_apply_parser,
    register_cache_retention_plan_parser,
)
from dpone.commands.airflow_cache_status_publication_cmd import (
    cmd_airflow_cache_status_publish,
    register_cache_status_publish_parser,
)
from dpone.commands.airflow_cache_sync_cmd import (
    cmd_airflow_cache_sync,
    register_cache_sync_parser,
)
from dpone.commands.airflow_connection_secret_gc_cmd import (
    cmd_airflow_connection_secret_gc_apply,
    cmd_airflow_connection_secret_gc_plan,
    register_connection_secret_gc_apply_parser,
    register_connection_secret_gc_plan_parser,
)
from dpone.commands.airflow_deployment_build_cmd import cmd_airflow_build, register_build_parser
from dpone.commands.airflow_desired_state_cmd import desired_state_group
from dpone.commands.airflow_rerun_plan_cmd import cmd_airflow_rerun_plan, register_rerun_plan_parser
from dpone.commands.airflow_runtime_delivery_cmd import (
    cmd_airflow_runtime_init_fetch,
    cmd_airflow_runtime_pack_exec,
    register_runtime_init_fetch_parser,
    register_runtime_pack_exec_parser,
)
from dpone.commands.airflow_runtime_pod_retention_cmd import (
    cmd_airflow_runtime_pod_retention_apply,
    cmd_airflow_runtime_pod_retention_plan,
    cmd_airflow_runtime_pod_retention_render,
    register_runtime_pod_retention_apply_parser,
    register_runtime_pod_retention_plan_parser,
    register_runtime_pod_retention_render_parser,
)
from dpone.commands.airflow_self_service_output import (
    emit_internal_check_failure as _emit_internal_check_failure,
)
from dpone.commands.airflow_self_service_output import emit_self_service_result as _emit
from dpone.commands.selection_arguments import (
    add_project_selection_arguments,
    has_project_selection,
    selection_command,
)
from dpone.readiness.airflow_authoring_check_service import ProjectSelectionCheckService
from dpone.readiness.airflow_authoring_fix import AirflowAuthoringFixService
from dpone.readiness.airflow_self_service_composition import build_airflow_self_service_service
from dpone.readiness.airflow_self_service_models import SelfServiceResult, dpone_error
from dpone.readiness.project_selection_preview import ProjectSelectionPreviewService

_INTERNAL_CHECK_CODE = "DPONE_INTERNAL_CHECK_FAILED"
_INTERNAL_CHECK_MESSAGE = "Check failed unexpectedly. Use the trace id with platform support."
_CHECK_IO_CONTRACT = (
    "`0`: bounded check result on stdout; stderr is empty.",
    "`1`: validation failure with `dpone.error.v1` entries on stdout; stderr is empty.",
    "`2`: structured application config error on stdout, or argparse syntax error on stderr.",
    "`3`: live dependency unavailable with a structured result on stdout; stderr is empty.",
    "`4`: structured security or safety violation on stdout; stderr is empty.",
    "`5`: redacted internal error with trace id on stdout; stderr is empty.",
)
_PREVIEW_IO_CONTRACT = (
    "`0`: bounded non-runnable preview result on stdout; stderr is empty.",
    "`1`: validation or Airflow-authoring failure on stdout; stderr is empty.",
    "`2`: argparse syntax error on stderr.",
    "`4`: structured security or cache-integrity violation on stdout; stderr is empty.",
    "JSON failures contain `dpone.error.v1` entries.",
)


def register_check_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("check", help="Run fast credential-free checks for a dpone pipeline")
    parser._dpone_io_contract = _CHECK_IO_CONTRACT
    parser.add_argument(
        "target",
        help="Pipeline id, domain/pipeline shorthand, directory, or pipeline.yaml path",
    )
    live_group = parser.add_mutually_exclusive_group()
    live_group.add_argument("--connections", action="store_true", help="Phase 1B connection handshakes")
    live_group.add_argument("--live", action="store_true", help="Phase 1B bounded live preflight")
    parser.add_argument("--environment", default="dev", help="Environment name for binding/registry checks")
    parser.add_argument("--format", choices=["text", "json"], default="text")
    add_project_selection_arguments(parser, default_max_selected=1000)
    return parser


def cmd_check(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx
    mode = "live" if args.live else "connections" if args.connections else "static"
    try:
        selection_requested = has_project_selection(args)
        if selection_requested:
            result = ProjectSelectionCheckService().check(
                target=args.target,
                select=tuple(args.select),
                exclude=tuple(args.exclude),
                state_path=args.state,
                selectors_path=args.selectors,
                max_selected=args.max_selected,
                mode=mode,
                environment=args.environment,
            )
            if result.passed:
                details = dict(result.details or {})
                details["selection_scope"] = True
                details["next_command"] = selection_command(
                    ("dpone", "airflow", "preview"),
                    target=args.target,
                    args=args,
                )
                result = replace(result, details=details)
        else:
            result = build_airflow_self_service_service().check(args.target, mode=mode, environment=args.environment)
        exit_code = result.exit_code if result.exit_code is not None else 0 if result.passed else 1
        _emit_check_result(result, args.format, target=args.target)
    except Exception:  # noqa: BLE001 - public command boundary must return one safe contract.
        result, trace_id = _unexpected_check_failure(logger)
        try:
            _emit_check_result(result, args.format, target=args.target)
        except Exception:  # noqa: BLE001 - static emergency output cannot contain the triggering value.
            with suppress(Exception):
                _emit_internal_check_failure(args.format, trace_id=trace_id)
        return 5
    return exit_code


def _unexpected_check_failure(logger: logging.Logger) -> tuple[SelfServiceResult, str]:
    trace_id = _new_trace_id()
    with suppress(Exception):
        logger.error("%s trace_id=%s", _INTERNAL_CHECK_CODE, trace_id)
    return (
        SelfServiceResult(
            passed=False,
            errors=(
                dpone_error(
                    _INTERNAL_CHECK_CODE,
                    _INTERNAL_CHECK_MESSAGE,
                    stage="check",
                    trace_id=trace_id,
                ),
            ),
            details={"exit_code": 5},
            exit_code=5,
        ),
        trace_id,
    )


def _new_trace_id() -> str:
    try:
        return uuid4().hex
    except Exception:  # noqa: BLE001 - local entropy failure must not reopen the public exception boundary.
        with suppress(Exception):
            return f"{time.monotonic_ns():032x}"[-32:]
    return "0" * 32


def _emit_check_result(result: SelfServiceResult, fmt: str, *, target: str) -> None:
    """Commit check output only after the complete document renders successfully."""

    destination = sys.stdout
    buffer = StringIO()
    with redirect_stdout(buffer):
        _emit(result, fmt, command="check", target=target)
    destination.write(buffer.getvalue())


def check_command():
    from dpone.commands.func_command import FuncCommand

    return FuncCommand("check", register_check_parser, cmd_check)


def register_fix_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("fix", help="Plan or apply safe authoring-source fixes for a dpone pipeline")
    parser.add_argument("target", help="Pipeline directory or pipeline.yaml path")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--plan", action="store_true", help="Show the safe authoring-source fix plan")
    mode.add_argument("--apply", action="store_true", help="Apply the safe authoring-source fix plan")
    parser.add_argument("--format", choices=["text", "json"], default="text")
    return parser


def cmd_fix(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    result = AirflowAuthoringFixService().fix(args.target, apply=args.apply)
    _emit(result, args.format, command="fix", target=args.target)
    if result.exit_code is not None:
        return result.exit_code
    return 0 if result.passed else 1


def fix_command():
    from dpone.commands.func_command import FuncCommand

    return FuncCommand("fix", register_fix_parser, cmd_fix)


def airflow_group():
    from dpone.commands.func_command import CommandGroup, FuncCommand

    def build(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        return subparsers.add_parser("airflow", help="Airflow self-service and platform deployment")

    return CommandGroup(
        name="airflow",
        help="Airflow self-service and platform deployment",
        build_parser=build,
        subcommands=[
            FuncCommand("preview", register_preview_parser, cmd_airflow_preview),
            FuncCommand("explain", register_explain_parser, cmd_airflow_explain),
            FuncCommand("build", register_build_parser, cmd_airflow_build),
            FuncCommand("publish", register_artifact_publish_parser, cmd_airflow_artifact_publish),
            FuncCommand(
                "verify-attestation",
                register_artifact_attestation_verify_parser,
                cmd_airflow_artifact_attestation_verify,
            ),
            artifact_attestation_group(),
            FuncCommand("cache-materialize", register_cache_materialize_parser, cmd_airflow_cache_materialize),
            FuncCommand("cache-sync", register_cache_sync_parser, cmd_airflow_cache_sync),
            FuncCommand(
                "cache-status-publish",
                register_cache_status_publish_parser,
                cmd_airflow_cache_status_publish,
            ),
            desired_state_group(),
            FuncCommand("cache-retention-plan", register_cache_retention_plan_parser, cmd_airflow_cache_retention_plan),
            FuncCommand(
                "cache-retention-apply",
                register_cache_retention_apply_parser,
                cmd_airflow_cache_retention_apply,
            ),
            FuncCommand("cache-recovery-plan", register_cache_recovery_plan_parser, cmd_airflow_cache_recovery_plan),
            FuncCommand("cache-recovery-apply", register_cache_recovery_apply_parser, cmd_airflow_cache_recovery_apply),
            FuncCommand(
                "runtime-init-fetch",
                register_runtime_init_fetch_parser,
                cmd_airflow_runtime_init_fetch,
                _requires_app_context=False,
            ),
            FuncCommand(
                "runtime-pack-exec",
                register_runtime_pack_exec_parser,
                cmd_airflow_runtime_pack_exec,
                _requires_app_context=False,
            ),
            FuncCommand("rerun-plan", register_rerun_plan_parser, cmd_airflow_rerun_plan),
            FuncCommand(
                "connection-secret-gc-plan",
                register_connection_secret_gc_plan_parser,
                cmd_airflow_connection_secret_gc_plan,
            ),
            FuncCommand(
                "connection-secret-gc-apply",
                register_connection_secret_gc_apply_parser,
                cmd_airflow_connection_secret_gc_apply,
            ),
            FuncCommand(
                "runtime-pod-retention-plan",
                register_runtime_pod_retention_plan_parser,
                cmd_airflow_runtime_pod_retention_plan,
                _requires_app_context=False,
            ),
            FuncCommand(
                "runtime-pod-retention-apply",
                register_runtime_pod_retention_apply_parser,
                cmd_airflow_runtime_pod_retention_apply,
                _requires_app_context=False,
            ),
            FuncCommand(
                "runtime-pod-retention-render",
                register_runtime_pod_retention_render_parser,
                cmd_airflow_runtime_pod_retention_render,
                _requires_app_context=False,
            ),
        ],
        subdest="airflow_cmd",
    )


def register_preview_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("preview", help="Materialize a non-runnable Airflow preview deployment")
    parser._dpone_io_contract = _PREVIEW_IO_CONTRACT
    parser.add_argument("pipeline", help="Pipeline id, directory, or pipeline.yaml path")
    parser.add_argument("--format", choices=["text", "json"], default="text")
    add_project_selection_arguments(parser, default_max_selected=500)
    return parser


def cmd_airflow_preview(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    if has_project_selection(args):
        result = ProjectSelectionPreviewService().preview(
            target=args.pipeline,
            select=tuple(args.select),
            exclude=tuple(args.exclude),
            state_path=args.state,
            selectors_path=args.selectors,
            max_selected=args.max_selected,
        )
    else:
        result = build_airflow_self_service_service().preview(args.pipeline)
    _emit(result, args.format, command="airflow_preview", target=args.pipeline)
    if result.exit_code is not None:
        return result.exit_code
    return 0 if result.passed else 1


def register_explain_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("explain", help="Explain planned/materialized Airflow artifacts")
    parser.add_argument("pipeline", help="Pipeline id, directory, or pipeline.yaml path")
    parser.add_argument("--format", choices=["text", "json"], default="text")
    return parser


def cmd_airflow_explain(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    result = build_airflow_self_service_service().explain(args.pipeline)
    _emit(result, args.format, command="airflow_explain", target=args.pipeline)
    if result.exit_code is not None:
        return result.exit_code
    return 0 if result.passed else 1


__all__ = [
    "airflow_group",
    "check_command",
    "cmd_airflow_build",
    "cmd_airflow_artifact_publish",
    "cmd_airflow_cache_materialize",
    "cmd_airflow_cache_sync",
    "cmd_airflow_cache_status_publish",
    "cmd_airflow_connection_secret_gc_apply",
    "cmd_airflow_connection_secret_gc_plan",
    "cmd_airflow_cache_recovery_apply",
    "cmd_airflow_cache_recovery_plan",
    "cmd_airflow_cache_retention_apply",
    "cmd_airflow_cache_retention_plan",
    "cmd_airflow_explain",
    "cmd_airflow_preview",
    "cmd_airflow_rerun_plan",
    "cmd_airflow_runtime_pod_retention_apply",
    "cmd_airflow_runtime_pod_retention_plan",
    "cmd_airflow_runtime_pod_retention_render",
    "cmd_fix",
    "cmd_check",
    "fix_command",
    "register_build_parser",
    "register_artifact_publish_parser",
    "register_cache_materialize_parser",
    "register_cache_sync_parser",
    "register_cache_status_publish_parser",
    "register_cache_recovery_apply_parser",
    "register_cache_recovery_plan_parser",
    "register_cache_retention_apply_parser",
    "register_cache_retention_plan_parser",
    "register_check_parser",
    "register_explain_parser",
    "register_fix_parser",
    "register_preview_parser",
    "register_rerun_plan_parser",
    "register_runtime_pod_retention_apply_parser",
    "register_runtime_pod_retention_plan_parser",
    "register_runtime_pod_retention_render_parser",
]
