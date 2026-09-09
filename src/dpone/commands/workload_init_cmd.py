from __future__ import annotations

import argparse
import logging
from pathlib import Path

from dpone.commands.airflow_self_service_rendering import self_service_init_text
from dpone.commands.output_json import write_json
from dpone.commands.output_text import write_text
from dpone.readiness.airflow_self_service_models import SelfServiceResult
from dpone.readiness.managed import ManagedRenderer
from dpone.readiness.self_service_error_catalog import invalid_reference_error
from dpone.readiness.workload_init_service import DEFAULT_WORKLOAD_SET, WorkloadInitRequest, WorkloadInitService
from dpone.readiness.workload_init_wizard import resolve_workload_init_inputs


def cmd_workload_init(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    title = "dpone workload init"
    repo_root = Path(args.repo_root or ".").resolve()
    try:
        wizard_inputs = resolve_workload_init_inputs(args)
        request = WorkloadInitRequest(
            workload_ref=args.workload_ref,
            source=wizard_inputs.source,
            sink=wizard_inputs.sink,
            strategy=wizard_inputs.strategy,
            layout=args.layout,
            domain=args.domain,
            dag_id=args.dag,
            schedule=wizard_inputs.schedule,
            owner=args.owner,
            timezone=args.timezone,
            workload_set=args.workload_set,
            apply=wizard_inputs.apply,
        )
        result = WorkloadInitService(repo_root=repo_root).init(request)
    except ValueError as exc:
        result = SelfServiceResult(passed=False, errors=(invalid_reference_error(str(exc)),))
        payload = result.to_dict()
        if args.format == "json":
            write_json(payload)
        elif args.format == "md":
            write_text(ManagedRenderer.render_markdown(title, payload))
        else:
            write_text(self_service_init_text(title, payload))
        return 1
    payload = result.to_dict()
    if args.format == "json":
        write_json(payload)
    elif args.format == "md":
        write_text(ManagedRenderer.render_markdown(title, payload))
    else:
        write_text(self_service_init_text(title, payload))
    return 0 if result.passed else 1


def register_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("init", help="Scaffold a GitOps workload with plan-first idempotency")
    parser.add_argument("workload_ref", help="DOMAIN/WORKLOAD_ID reference, for example marketing/sample_web_sync")
    parser.add_argument("--source", help="Source connector type, for example clickhouse")
    parser.add_argument("--sink", help="Sink connector type, for example mssql")
    parser.add_argument("--strategy", default="full_refresh", help="Load strategy mode")
    parser.add_argument("--wizard", action="store_true", help="Interactive route selection and apply confirmation")
    parser.add_argument("--layout", choices=("batch", "catalog"), default="batch", help="Manifest layout")
    parser.add_argument("--domain", help="Override domain when workload_ref is a bare workload id")
    parser.add_argument("--dag", dest="dag", help="Declared DAG id (default DAG__<domain>__<workload>__sync)")
    parser.add_argument("--schedule", default="0 6 * * *", help="Cron schedule for the declared DAG")
    parser.add_argument("--owner", default="data_platform", help="DAG and ownership owner")
    parser.add_argument("--timezone", default="Europe/Moscow", help="DAG timezone")
    parser.add_argument("--workload-set", default=DEFAULT_WORKLOAD_SET, help="GitOps workload-set root")
    parser.add_argument("--repo-root", default=".", help="Repository root for scaffold paths")
    parser.add_argument("--apply", action="store_true", help="Write files (default is plan-only)")
    parser.add_argument("--format", choices=("text", "json", "md"), default="text")
    return parser


__all__ = ["cmd_workload_init", "register_parser"]
