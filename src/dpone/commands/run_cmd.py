from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.commands.run_safe_sample_cmd import SafeSampleCommandResult


import argparse
import logging
import os
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from dpone.commands.argument_types import non_negative_float, non_negative_int
from dpone.commands.run_failure_output import write_run_failure as _write_run_failure
from dpone.commands.run_output import write_json, write_text
from dpone.commands.run_safe_sample_cmd import build_safe_sample_result, new_safe_sample_run_id, write_safe_sample
from dpone.commands.run_safe_sample_rendering import safe_sample_recovery_lines
from dpone.commands.selection_arguments import add_project_selection_arguments, has_project_selection
from dpone.contracts import ETLConfigurationError
from dpone.readiness.error_contract import error_docs_url
from dpone.readiness.project_selection import ProjectSelectionService, SelectionError, selection_error_exit_code
from dpone.security_redaction import redact_absolute_paths, redact_text, redact_value
from dpone.services.airflow_mapping_context import AirflowMappingContextService
from dpone.services.interval_context import IntervalContextService
from dpone.services.manifest import build_manifest_context
from dpone.services.run_manifest import RunInvocationContextService, RunManifestService
from dpone.services.safe_sample_cli_arguments import (
    ExplicitAppendOptionAction,
    ExplicitOptionAction,
    build_safe_sample_incompatible_error,
    safe_sample_incompatible_options,
    selected_safe_sample_fix_command,
)


def cmd_run(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del logger
    selection_requested = has_project_selection(args)
    safe_sample_requested = args.sample is not None or args.target is not None
    if safe_sample_requested:
        incompatible = safe_sample_incompatible_options(args)
        if selection_requested and incompatible:
            error = build_safe_sample_incompatible_error(
                path=str(getattr(args, "path", "")),
                incompatible_options=incompatible,
                fix_command=selected_safe_sample_fix_command(args),
            )
            return _write_selected_failure(args, error=error, exit_code=2)
    if selection_requested:
        return write_selected_safe_samples(args)
    if safe_sample_requested:
        return write_safe_sample(args)
    execution_started = False

    def mark_execution_started() -> None:
        nonlocal execution_started
        execution_started = True

    try:
        invocation = RunInvocationContextService(
            mapping_context_service=AirflowMappingContextService(),
            interval_context_factory=IntervalContextService,
        ).resolve(
            environ=os.environ,
            dag_id=getattr(args, "dag_id", None),
            execution_date=getattr(args, "execution_date", None),
            interval_start=getattr(args, "interval_start", None),
            interval_end=getattr(args, "interval_end", None),
            repair_authority_ref=getattr(args, "repair_authority_ref", None),
            normalize_explicit_execution_date=True,
        )
        manifest_ctx = build_manifest_context(args, ctx=ctx)
        report = RunManifestService().run(
            path=Path(args.path),
            manifest_ctx=manifest_ctx,
            selector=args.selector,
            run_id=getattr(args, "run_id", None) or invocation.run_id,
            dag_id=invocation.dag_id,
            execution_date=invocation.execution_date,
            retry_attempts=args.retry_attempts,
            retry_backoff_seconds=args.retry_backoff_seconds,
            load_config_mutator=invocation.load_config_mutator,
            run_context_config=invocation.run_context_config,
            on_execution_started=mark_execution_started,
        )
        if args.format == "json":
            write_json(redact_value(report.to_dict()))
        elif args.format == "md":
            write_text(redact_absolute_paths(redact_text(report.to_markdown())))
        else:
            write_text(redact_absolute_paths(redact_text(report.to_text())))
        return 0 if report.passed else 1
    except ETLConfigurationError as exc:
        _write_run_failure(args, exc)
        return 1 if execution_started else 2
    except Exception as exc:  # noqa: BLE001 - Airflow runtime always needs machine-readable stdout.
        _write_run_failure(args, exc)
        return 1


def write_selected_safe_samples(args: argparse.Namespace) -> int:
    """Select workloads once, then execute safe samples in deterministic order."""

    safety_error = _safe_sample_requirement_error(args)
    if safety_error is not None:
        return _write_selected_failure(args, error=safety_error, exit_code=4)
    selection_service = ProjectSelectionService(root=".")
    try:
        outcome = selection_service.select(
            target=str(getattr(args, "path", "")),
            select=tuple(getattr(args, "select", ())),
            exclude=tuple(getattr(args, "exclude", ())),
            state_path=getattr(args, "state", None),
            selectors_path=str(getattr(args, "selectors", "selectors.yaml")),
            max_selected=int(getattr(args, "max_selected", 10)),
        )
    except SelectionError as exc:
        return _write_selected_failure(
            args,
            error=_selection_error(exc),
            exit_code=selection_error_exit_code(exc.code),
        )

    selected_ids = tuple(item.node.node_id for item in outcome.report.selected)
    results: list[dict[str, object]] = []
    base_run_id = str(getattr(args, "run_id", "") or new_safe_sample_run_id())
    for workload_id in selected_ids:
        try:
            selection_service.verify_consumed_files(dict(outcome.consumed_files))
        except SelectionError as exc:
            return _write_selected_failure(
                args,
                error=_selection_error(exc),
                exit_code=selection_error_exit_code(exc.code),
                selection=outcome.report.to_jsonable(),
                results=results,
                unscheduled=selected_ids[len(results) :],
            )
        checked = outcome.checked_sources[workload_id]
        child_args = argparse.Namespace(**vars(args))
        child_args.path = checked.source_label
        child_args.run_id = f"{base_run_id}-{workload_id}"
        child_result = build_safe_sample_result(child_args)
        results.append(_workload_result(workload_id, checked.source_label, child_result))
        if child_result.exit_code != 0:
            break

    unscheduled = selected_ids[len(results) :]
    passed = len(results) == len(selected_ids) and all(bool(item["passed"]) for item in results)
    exit_code = 0 if passed else _result_exit_code(results)
    payload: dict[str, object] = {
        "schema": "dpone.selected-safe-sample-report.v1",
        "passed": passed,
        "exit_code": exit_code,
        "sample": getattr(args, "sample", None),
        "target": getattr(args, "target", None),
        "environment": getattr(args, "environment", "development"),
        "selection": outcome.report.to_jsonable(),
        "results": results,
        "unscheduled": list(unscheduled),
        "errors": _result_errors(results),
    }
    _write_selected_report(args, payload)
    return exit_code


def _safe_sample_requirement_error(args: argparse.Namespace) -> dict[str, object] | None:
    sample = getattr(args, "sample", None)
    if isinstance(sample, int) and sample > 0 and getattr(args, "target", None) == "temporary":
        return None
    return {
        "schema": "dpone.error.v1",
        "code": "DPONE_SELECTION_RUN_REQUIRES_SAFE_SAMPLE",
        "stage": "selected_safe_sample",
        "severity": "error",
        "message": "Project workload selection can run only with --sample > 0 and --target temporary.",
        "entity": {"kind": "command", "id": "dpone run --select"},
        "fixes": [
            {
                "id": "use_bounded_temporary_sample",
                "safety": "safe",
                "command": selected_safe_sample_fix_command(args),
            }
        ],
        "docs_url": error_docs_url("DPONE_SELECTION_RUN_REQUIRES_SAFE_SAMPLE"),
    }


def _selection_error(error: SelectionError) -> dict[str, object]:
    return {
        "schema": "dpone.error.v1",
        "code": error.code,
        "stage": "selected_safe_sample_selection",
        "severity": "error",
        "message": str(error),
        "entity": {"kind": "project", "id": "selection"},
        "fixes": [],
        "docs_url": error_docs_url(error.code),
        **error.context,
    }


def _workload_result(
    workload_id: str,
    source: str,
    result: SafeSampleCommandResult,
) -> dict[str, object]:
    result_payload = result.payload.get("result")
    result_payload = result_payload if isinstance(result_payload, Mapping) else {}
    errors = result_payload.get("errors")
    error_items = [dict(item) for item in errors if isinstance(item, Mapping)] if isinstance(errors, list) else []
    return {
        "workload_id": workload_id,
        "source": source,
        "run_id": str(result.payload.get("run_id") or ""),
        "passed": bool(result.payload.get("passed")),
        "status": str(result_payload.get("status") or "unknown"),
        "exit_code": result.exit_code,
        "errors": error_items,
        "evidence_path": _evidence_path(result.payload),
    }


def _evidence_path(payload: Mapping[str, Any]) -> str | None:
    safe_sample = payload.get("safe_sample")
    runtime_run = safe_sample.get("runtime_run") if isinstance(safe_sample, Mapping) else None
    evidence = runtime_run.get("evidence_write") if isinstance(runtime_run, Mapping) else None
    path = evidence.get("path") if isinstance(evidence, Mapping) else None
    return str(path) if isinstance(path, str) and path else None


def _result_errors(results: list[dict[str, object]]) -> list[dict[str, object]]:
    for result in results:
        errors = result.get("errors")
        if isinstance(errors, list) and errors:
            return [dict(item) for item in errors if isinstance(item, Mapping)]
    return []


def _result_exit_code(results: list[dict[str, object]]) -> int:
    if not results:
        return 1
    exit_code = results[-1].get("exit_code")
    return exit_code if isinstance(exit_code, int) and exit_code > 0 else 1


def _write_selected_failure(
    args: argparse.Namespace,
    *,
    error: dict[str, object],
    exit_code: int,
    selection: dict[str, object] | None = None,
    results: list[dict[str, object]] | None = None,
    unscheduled: tuple[str, ...] = (),
) -> int:
    payload: dict[str, object] = {
        "schema": "dpone.selected-safe-sample-report.v1",
        "passed": False,
        "exit_code": exit_code,
        "selection": selection,
        "results": results or [],
        "unscheduled": list(unscheduled),
        "errors": [error],
    }
    _write_selected_report(args, payload)
    return exit_code


def _write_selected_report(args: argparse.Namespace, payload: dict[str, object]) -> None:
    public_payload = redact_value(payload)
    if not isinstance(public_payload, dict):
        raise TypeError("Selected safe-sample report payload must be a mapping.")
    if getattr(args, "format", "text") == "json":
        write_json(public_payload)
        return
    markdown = getattr(args, "format", "text") == "md"
    title = "# dpone selected safe sample" if markdown else "dpone selected safe sample"
    lines = [title, f"- status: {'passed' if public_payload.get('passed') else 'failed'}"]
    selection = public_payload.get("selection")
    if isinstance(selection, Mapping):
        lines.append(f"- selection fingerprint: {selection.get('selection_fingerprint')}")
    results = public_payload.get("results")
    if isinstance(results, list):
        for item in results:
            if isinstance(item, Mapping):
                lines.append(f"- {item.get('workload_id')}: {item.get('status')}")
    errors = public_payload.get("errors")
    if isinstance(errors, list):
        error_items = [error for error in errors if isinstance(error, Mapping)]
        for error in error_items:
            lines.append(f"- {error.get('code')}: {error.get('message')}")
        if error_items:
            lines.extend(safe_sample_recovery_lines(error_items, markdown=markdown))
    write_text("\n".join(lines) + "\n")


def register_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("run", help="Execute one manifest process")
    parser.add_argument(
        "path",
        help="YAML manifest path; safe-sample mode also accepts a canonical pipeline ID or directory",
    )
    parser.add_argument("--selector", help="Process selector inside a batch manifest")
    parser.add_argument("--run-id", help="Explicit run id; defaults to process name")
    parser.add_argument(
        "--dag-id",
        action=ExplicitOptionAction,
        help="Run-state identity; defaults to DPONE_DAG_ID",
    )
    parser.add_argument(
        "--execution-date",
        action=ExplicitOptionAction,
        help="Logical date (ISO-8601); defaults to DPONE_LOGICAL_DATE",
    )
    parser.add_argument(
        "--interval-start",
        dest="interval_start",
        action=ExplicitOptionAction,
        help="Data interval start (ISO-8601); defaults to DPONE_INTERVAL_START",
    )
    parser.add_argument(
        "--interval-end",
        dest="interval_end",
        action=ExplicitOptionAction,
        help="Data interval end (ISO-8601); defaults to DPONE_INTERVAL_END",
    )
    parser.add_argument(
        "--retry-attempts",
        action=ExplicitOptionAction,
        type=non_negative_int,
        default=0,
        help="Retry failed process results this many times",
    )
    parser.add_argument(
        "--repair-authority-ref",
        action=ExplicitOptionAction,
        help=(
            "One-shot environment-owned repair authority; defaults to "
            "DPONE_REPAIR_AUTHORITY_REF and is never read from the manifest"
        ),
    )
    parser.add_argument(
        "--retry-backoff-seconds",
        action=ExplicitOptionAction,
        type=non_negative_float,
        default=0.0,
        help="Sleep between failed attempts; defaults to no backoff",
    )
    parser.add_argument(
        "--registry",
        action=ExplicitAppendOptionAction,
        default=[],
        help="Path to a sources registry YAML (can be repeated)",
    )
    parser.add_argument("--sample", type=int, help="Phase 1B safe sample row budget")
    parser.add_argument("--target", choices=["temporary"], help="Phase 1B safe-run target policy")
    add_project_selection_arguments(parser, default_max_selected=10)
    parser.add_argument(
        "--environment",
        default="development",
        help="Safe sample policy environment; defaults to development",
    )
    parser.add_argument("--format", choices=["text", "json", "md"], default="text")
    return parser
