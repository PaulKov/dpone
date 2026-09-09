from __future__ import annotations

import argparse
import logging
from pathlib import Path

from dpone.commands.argument_types import non_negative_float, non_negative_int
from dpone.commands.output_json import write_json
from dpone.commands.output_text import write_text
from dpone.orchestration.handoff import SchedulerHandoffBuilder
from dpone.orchestration.locks import LocalRunLockManager
from dpone.orchestration.run import OrchestratedRunRequest, OrchestratedRunService
from dpone.orchestration.state import LocalJobStateStore
from dpone.services.manifest import build_manifest_context
from dpone.services.run_manifest import RunManifestService


def cmd_orchestrate_run(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del logger
    manifest_ctx = build_manifest_context(args, ctx=ctx)

    def execute(request: OrchestratedRunRequest):
        return RunManifestService().run(
            path=request.manifest_path,
            manifest_ctx=manifest_ctx,
            selector=request.selector,
            run_id=request.run_id,
            dag_id=request.dag_id,
            execution_date=request.execution_date,
            retry_attempts=request.retry_attempts,
            retry_backoff_seconds=request.retry_backoff_seconds,
        )

    report = OrchestratedRunService(
        lock_manager=LocalRunLockManager(args.lock_dir),
        handoff_builder=SchedulerHandoffBuilder(),
        run_executor=execute,
        job_state_store=LocalJobStateStore(args.state_dir),
    ).run(
        output_dir=args.output_dir,
        request=OrchestratedRunRequest(
            manifest_path=Path(args.manifest),
            selector=args.selector,
            run_id=args.run_id,
            dag_id=args.dag_id,
            execution_date=args.execution_date,
            retry_attempts=args.retry_attempts,
            retry_backoff_seconds=args.retry_backoff_seconds,
            concurrency_key=args.concurrency_key,
            lock_ttl_seconds=args.lock_ttl_seconds,
            resume_policy=args.resume_policy,
        ),
    )
    if args.format == "json":
        write_json(report.to_dict())
    else:
        write_text(report.to_markdown())
    return 0 if report.passed else 1


def register_run_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("run", help="Run one manifest process with orchestration locks and artifacts")
    parser.add_argument("--manifest", required=True, help="Path to a YAML manifest")
    parser.add_argument("--selector", help="Process selector inside a batch manifest")
    parser.add_argument("--run-id", help="Explicit run id")
    parser.add_argument("--dag-id")
    parser.add_argument("--execution-date")
    parser.add_argument("--retry-attempts", type=non_negative_int, default=0)
    parser.add_argument("--retry-backoff-seconds", type=non_negative_float, default=0.0)
    parser.add_argument("--concurrency-key")
    parser.add_argument("--lock-dir", default=".dpone/locks")
    parser.add_argument("--state-dir", default=".dpone/orchestration-state")
    parser.add_argument("--lock-ttl-seconds", type=int, default=3600)
    parser.add_argument("--resume-policy", choices=["fail", "resume", "restart"], default="fail")
    parser.add_argument("--output-dir", default=".dpone/orchestration/latest")
    parser.add_argument("--registry", action="append", default=[])
    parser.add_argument("--format", choices=["md", "json"], default="md")
    return parser
