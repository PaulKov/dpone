"""CLI composition for Airflow runtime Pod retention."""

from __future__ import annotations

import argparse
import logging
import sys

import yaml

from dpone.adapters.airflow_runtime_pod_retention_events import (
    JsonLinesAirflowRuntimePodRetentionEvidencePublisher,
)
from dpone.commands.airflow_self_service_output import emit_self_service_result
from dpone.commands.output_json import write_json
from dpone.commands.output_text import write_text
from dpone.gitops.airflow_runtime_pod_retention_manifests import (
    AirflowRuntimePodRetentionRenderRequest,
    render_runtime_pod_retention,
)
from dpone.readiness.airflow_self_service_pod_retention import (
    runtime_pod_retention_apply_command_result,
    runtime_pod_retention_error_result,
    runtime_pod_retention_plan_command_result,
)


def register_runtime_pod_retention_plan_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("runtime-pod-retention-plan", help="Plan metadata-only runtime Pod cleanup")
    _add_live_arguments(parser, destructive=False)
    _attach_io_contract(
        parser,
        (
            "stdout contains text or dpone.airflow-runtime-pod-retention-plan.v1 JSON after service execution; "
            "pre-execution JSON failures use dpone.error.v1 and argparse errors use stderr.",
            "Exit 0 is clean or has bounded cleanup candidates, 1 is attention with evidence, 2 is invalid input, "
            "3 is dependency failure, "
            "4 is security/RBAC, and 5 is unexpected failure.",
            "The command is read-only and never deletes Pods.",
        ),
    )
    return parser


def register_runtime_pod_retention_apply_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("runtime-pod-retention-apply", help="Apply bounded runtime Pod cleanup")
    _add_live_arguments(parser, destructive=True)
    parser.add_argument(
        "--max-delete-count", type=int, default=100, help="Maximum conditional deletes (1..1000; default: 100)"
    )
    parser.add_argument("--actor", required=True, help="Declared operator identity recorded in evidence")
    parser.add_argument(
        "--allowed-actor",
        action="append",
        required=True,
        help="Repeatable exact actor acknowledgement; Kubernetes RBAC remains the authorization authority",
    )
    parser.add_argument("--confirm-delete", action="store_true", help="Required acknowledgement of Pod deletion")
    _attach_io_contract(
        parser,
        (
            "stdout contains text or dpone.airflow-runtime-pod-retention-apply.v1 JSON for bounded service "
            "outcomes; pre-execution JSON failures use dpone.error.v1 and argparse errors use stderr.",
            "Exit 0 means delete requests were accepted, not observed absent; every selected item must be "
            "delete-accepted or already absent with complete process evidence. Exits 1..5 retain the plan meanings.",
            "Mutation is bounded and UID/resourceVersion-conditional; stdout is the aggregate report and stderr "
            "is the flushed dpone.airflow-runtime-pod-retention-event.v1 JSONL stream; preserve stdout evidence "
            "and the stderr event stream before propagating the exit code.",
        ),
    )
    return parser


def register_runtime_pod_retention_render_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("runtime-pod-retention-render", help="Render a least-privilege retention CronJob")
    parser.add_argument("--namespace", required=True, help="Single Kubernetes namespace to inspect")
    parser.add_argument("--image", required=True, help="Immutable image@sha256 reference")
    parser.add_argument(
        "--schedule",
        default="17 * * * *",
        help=(
            "Kubernetes five-field cron (? equals *; default: 17 * * * *) or one of @yearly, @annually, @monthly, @weekly, "
            "@daily, @midnight, @hourly"
        ),
    )
    parser.add_argument(
        "--mode", choices=("plan", "apply"), default="plan", help="Plan is list-only; apply adds delete (default: plan)"
    )
    parser.add_argument(
        "--minimum-age-seconds", type=int, default=86_400, help="Creation-age floor (300..2592000; default: 86400)"
    )
    parser.add_argument("--page-size", type=int, default=500, help="Metadata page limit (1..1000; default: 500)")
    parser.add_argument(
        "--max-delete-count",
        type=int,
        default=100,
        help="Maximum deletes per apply cycle (1..1000; default: 100)",
    )
    parser.add_argument("--actor", default="", help="Declared actor evidence required by apply mode")
    parser.add_argument("--allowed-actor", action="append", default=[], help="Repeatable exact actor acknowledgement")
    parser.add_argument("--confirm-delete", action="store_true", help="Required acknowledgement in apply mode")
    parser.add_argument(
        "--alerts",
        choices=("off", "prometheus"),
        default="off",
        help="Optional rendered alert resources (default: off)",
    )
    parser.add_argument(
        "--stale-after-seconds",
        type=int,
        help="Required with Prometheus alerts; maximum expected interval between successful jobs",
    )
    parser.add_argument(
        "--format", choices=("yaml", "json"), default="yaml", help="Manifest or review evidence output (default: yaml)"
    )
    _attach_io_contract(
        parser,
        (
            "stdout contains rendered YAML or dpone.airflow-runtime-pod-retention-render.v1 JSON; validation "
            "failures preserve the requested YAML/JSON format, use dpone.error.v1, and exit 2.",
            "Render to a temporary file and atomically rename only after exit 0 so reviewed manifests cannot be "
            "clobbered.",
            "The command performs no Kubernetes API calls and writes no files itself.",
        ),
    )
    return parser


def cmd_airflow_runtime_pod_retention_plan(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    result = runtime_pod_retention_plan_command_result(
        namespace=args.namespace,
        minimum_age_seconds=args.minimum_age_seconds,
        page_size=args.page_size,
        auth_mode=args.kube_auth,
        kube_context=args.kube_context,
    )
    emit_self_service_result(result, args.format, command="airflow_runtime_pod_retention_plan")
    return result.exit_code if result.exit_code is not None else (0 if result.passed else 1)


def cmd_airflow_runtime_pod_retention_apply(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    result = runtime_pod_retention_apply_command_result(
        namespace=args.namespace,
        minimum_age_seconds=args.minimum_age_seconds,
        page_size=args.page_size,
        max_delete_count=args.max_delete_count,
        actor=args.actor,
        allowed_actors=tuple(args.allowed_actor),
        confirm_delete=args.confirm_delete,
        auth_mode=args.kube_auth,
        kube_context=args.kube_context,
        evidence=JsonLinesAirflowRuntimePodRetentionEvidencePublisher(sys.stderr),
    )
    emit_self_service_result(result, args.format, command="airflow_runtime_pod_retention_apply")
    return result.exit_code if result.exit_code is not None else (0 if result.passed else 1)


def cmd_airflow_runtime_pod_retention_render(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    try:
        report = render_runtime_pod_retention(
            AirflowRuntimePodRetentionRenderRequest(
                namespace=args.namespace,
                image=args.image,
                schedule=args.schedule,
                mode=args.mode,
                minimum_age_seconds=args.minimum_age_seconds,
                page_size=args.page_size,
                max_delete_count=args.max_delete_count,
                actor=args.actor,
                allowed_actors=tuple(args.allowed_actor),
                confirm_delete=args.confirm_delete,
                alerts=args.alerts,
                stale_after_seconds=args.stale_after_seconds,
            )
        )
    except ValueError as exc:
        result = runtime_pod_retention_error_result(
            exc,
            namespace=args.namespace,
            retry_operation="render",
        )
        if args.format == "json":
            emit_self_service_result(result, "json", command="airflow_runtime_pod_retention_render")
        else:
            write_text(yaml.safe_dump(result.to_dict(), sort_keys=False, allow_unicode=True))
        return result.exit_code or 2
    if args.format == "json":
        write_json(report)
    else:
        write_text(yaml.safe_dump_all(report["manifests"], sort_keys=False, allow_unicode=True))
    return 0


def _add_live_arguments(parser: argparse.ArgumentParser, *, destructive: bool) -> None:
    parser.add_argument("--namespace", required=True, help="Single Kubernetes namespace to inspect")
    parser.add_argument(
        "--minimum-age-seconds", type=int, default=86_400, help="Creation-age floor (300..2592000; default: 86400)"
    )
    parser.add_argument("--page-size", type=int, default=500, help="Metadata page limit (1..1000; default: 500)")
    if destructive:
        parser.add_argument(
            "--kube-auth",
            choices=("in-cluster", "kubeconfig"),
            required=True,
            help="Explicit credential source; destructive apply never resolves ambient credentials",
        )
        parser.add_argument(
            "--kube-context",
            help="Required explicit kubeconfig context with --kube-auth kubeconfig",
        )
    else:
        parser.add_argument(
            "--kube-auth",
            choices=("auto", "in-cluster", "kubeconfig"),
            default="auto",
            help="Credential source; auto prefers in-cluster then kubeconfig (default: auto)",
        )
        parser.add_argument("--kube-context", help="Optional kubeconfig context; invalid with in-cluster auth")
    parser.add_argument(
        "--format", choices=("text", "json"), default="text", help="Human or machine-readable evidence (default: text)"
    )


def _attach_io_contract(parser: argparse.ArgumentParser, contract: tuple[str, ...]) -> None:
    parser.formatter_class = argparse.RawDescriptionHelpFormatter
    parser.epilog = "I/O, side effects, and exit contract:\n\n" + "\n".join(f"- {line}" for line in contract)
    setattr(parser, "_dpone_io_contract", contract)


__all__ = [
    "cmd_airflow_runtime_pod_retention_apply",
    "cmd_airflow_runtime_pod_retention_plan",
    "cmd_airflow_runtime_pod_retention_render",
    "register_runtime_pod_retention_apply_parser",
    "register_runtime_pod_retention_plan_parser",
    "register_runtime_pod_retention_render_parser",
]
