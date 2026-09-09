"""CLI for dbt-authored dpone publishing workflows."""

from __future__ import annotations

import argparse
import importlib
import json
import logging
import sys
from pathlib import Path
from typing import cast

from dpone.commands.dbt_publish_remediation import runtime_remediation as _runtime_remediation
from dpone.contracts.dbt_publish_models import DbtCompileReport, DbtPublishIssue
from dpone.ports.dbt_publishing import (
    DbtCliExecutionOutcome,
    DbtPublishComposition,
)
from dpone.readiness.dbt_airflow_execution_pack import (
    reject_non_executable_semantic_template,
)
from dpone.readiness.dbt_publish_release_materializer import DbtReleaseMaterializationError

from .dbt_publish_cli_support import (
    DbtCliUsageError,
    DbtProjectRootError,
    emit_failure,
    emit_internal_failure,
    emit_model,
    emit_report,
    explain_selector,
    manifest_path,
    project_argument_issue,
    project_root,
    project_root_issue,
    relative_pack_path,
    stale_default_manifest,
)


def register_check_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("check", help="Validate resolved dbt publish intents")
    _common(parser, project_positional=True)
    parser.add_argument("--allow-empty", action="store_true", help="Allow a report-only check with zero enabled models")
    parser.add_argument("--allow-missing-contracts", action="store_true", help="Dev-only contract relaxation")
    return parser


def register_explain_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "explain",
        help="Explain one resolved dbt publish model",
        usage=(
            "dpone dbt explain MODEL [--project-dir PROJECT] [options]\n"
            "       dpone dbt explain PROJECT MODEL [options]  (compatibility)\n"
            "       dpone dbt explain --model MODEL [options]  (deprecated)"
        ),
    )
    _common(parser)
    parser.add_argument(
        "model",
        nargs="?",
        metavar="MODEL_OR_PROJECT",
        help="Model selector; compatibility form treats this value as PROJECT when a second MODEL is present",
    )
    parser.add_argument(
        "project_model",
        nargs="?",
        metavar="MODEL",
        help="Model selector for the compatibility-only PROJECT MODEL form",
    )
    parser.add_argument(
        "--model",
        dest="legacy_model",
        metavar="MODEL",
        help="Deprecated compatibility form; pass MODEL positionally",
    )
    parser.add_argument("--allow-missing-contracts", action="store_true", help="Dev-only contract relaxation")
    return parser


def register_compile_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("compile", help="Compile immutable dpone manifests, packs, and DAG specs")
    _common(parser, project_positional=True, dbt_profiles=True)
    parser.add_argument("--output-dir", default=".dpone/gitops/airflow", help="Generated artifact root")
    parser.add_argument("--env", default="dev", help="Deprecated no-op")
    parser.add_argument("--cache-root", help="CI-only content-addressed release cache destination")
    parser.set_defaults(allow_missing_contracts=False)
    return parser


def register_execute_pack_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("execute-pack", help="Execute one verified dbt runtime pack")
    parser.add_argument(
        "relative_pack_path",
        type=relative_pack_path,
        help="Verified workload-relative dbt execution pack path",
    )
    parser.add_argument("--format", choices=["json"], default="json")
    return parser


def cmd_check(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del logger
    try:
        report = _build(args, context=cast(DbtPublishComposition, ctx))
    except DbtProjectRootError:
        emit_failure((project_root_issue(),), args.format, stage="dbt_check")
        return 2
    except DbtCliUsageError:
        emit_failure((project_argument_issue(),), args.format, stage="dbt_check")
        return 2
    except Exception:
        emit_internal_failure(args.format, stage="dbt_check")
        return 5
    if not report.passed:
        emit_failure(report.blockers, args.format, stage="dbt_check")
        return 1
    emit_report(report, args.format)
    return 0 if report.passed else 1


def cmd_explain(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del logger
    if args.legacy_model is not None:
        print(
            "warning: `dpone dbt explain --model MODEL` is deprecated; use "
            "`dpone dbt explain MODEL`. Compatibility starts with release 0.73.26; "
            "removal is not allowed before 0.75.0 and 2027-07-29; the later condition wins.",
            file=sys.stderr,
        )
    selector = explain_selector(args)
    if selector is None:
        emit_failure(
            (
                DbtPublishIssue(
                    code="DPONE_DBT_MODEL_REQUIRED",
                    message="Provide the dbt model to explain",
                    path="command",
                    remediation="Run `dpone dbt explain <model>`.",
                ),
            ),
            args.format,
            stage="dbt_explain",
        )
        return 2
    try:
        report = _build(
            args,
            context=cast(DbtPublishComposition, ctx),
            model_selector=selector,
        )
    except DbtProjectRootError:
        emit_failure((project_root_issue(),), args.format, stage="dbt_explain")
        return 2
    except DbtCliUsageError:
        emit_failure((project_argument_issue(),), args.format, stage="dbt_explain")
        return 2
    except Exception:
        emit_internal_failure(args.format, stage="dbt_explain")
        return 5
    if not report.passed:
        emit_failure(report.blockers, args.format, stage="dbt_explain")
        return 1
    matches = [
        item
        for item in report.models
        if selector
        in {
            item.model.name,
            item.model.alias,
            item.model.unique_id,
            item.model.fqn_selector,
            item.workload_id,
        }
    ]
    if len(matches) != 1:
        code = "DPONE_DBT_MODEL_NOT_FOUND" if not matches else "DPONE_DBT_MODEL_AMBIGUOUS"
        message = (
            f"dbt model is not publish-enabled or does not exist: {selector}"
            if not matches
            else f"dbt model selector is ambiguous: {selector}"
        )
        remediation = (
            "Run `dpone dbt check` to list publish-enabled models, then retry with the exact dbt unique_id."
            if not matches
            else "Retry with the exact dbt unique_id shown by `dpone dbt check`."
        )
        emit_failure(
            (
                DbtPublishIssue(
                    code=code,
                    message=message,
                    path="manifest.json",
                    remediation=remediation,
                ),
            ),
            args.format,
            stage="dbt_explain",
        )
        return 1
    if args.format == "json":
        schema = (
            "dpone.dbt-publish-explain.v2"
            if matches[0].profile.semantic_refresh is not None
            else "dpone.dbt-publish-explain.v1"
        )
        print(
            json.dumps(
                {
                    "schema": schema,
                    **matches[0].to_jsonable(),
                },
                allow_nan=False,
                ensure_ascii=False,
                indent=2,
            )
        )
    else:
        emit_model(matches[0].to_jsonable())
    return 0


def cmd_compile(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del logger
    context = cast(DbtPublishComposition, ctx)
    try:
        report = _build(args, context=context)
        if report.passed:
            writer = context.build_dbt_artifact_writer(
                dbt_profiles_dir=(Path(args.dbt_profiles_dir) if args.dbt_profiles_dir else None),
            )
            report = writer.write(
                report,
                Path(args.output_dir),
                project_root=project_root(args),
                environment=str(getattr(args, "env", "dev")),
            )
            if report.passed and args.cache_root:
                materialized = context.build_dbt_release_materializer().materialize(
                    compiled_root=Path(args.output_dir),
                    cache_root=Path(args.cache_root),
                )
                if materialized.release_id != report.release_id:
                    raise RuntimeError("materialized release identity drifted")
    except DbtProjectRootError:
        emit_failure((project_root_issue(),), args.format, stage="dbt_compile")
        return 2
    except DbtCliUsageError:
        emit_failure((project_argument_issue(),), args.format, stage="dbt_compile")
        return 2
    except DbtReleaseMaterializationError as exc:
        cache_lock_failed = exc.code == "DPONE_DBT_RELEASE_CACHE_LOCK_FAILED"
        emit_failure(
            (
                DbtPublishIssue(
                    code=exc.code,
                    message=(
                        "The local dpone cache writer lease could not be initialized"
                        if cache_lock_failed
                        else "The compiled dbt release failed immutable publication validation"
                    ),
                    path=str(args.cache_root) if cache_lock_failed else "release-set.json",
                    remediation=(
                        "Check cache-root permissions and require .promotion.lock to be a regular writable file."
                        if cache_lock_failed
                        else "Re-run the certified CI compile; do not edit generated release artifacts."
                    ),
                ),
            ),
            args.format,
            stage="dbt_compile",
        )
        return 1
    except Exception:
        emit_internal_failure(args.format, stage="dbt_compile")
        return 5
    if not report.passed:
        emit_failure(report.blockers, args.format, stage="dbt_compile")
        return 1
    emit_report(report, args.format)
    return 0


def cmd_execute_pack(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    """Delegate one confined pack path to the runtime-owned execution service."""

    del ctx, logger
    try:
        outcome = _execute_dbt_pack(args.relative_pack_path)
        exit_code = outcome.exit_code
        if isinstance(exit_code, bool) or not isinstance(exit_code, int):
            raise TypeError("dbt runtime returned an invalid exit code")
        payload = outcome.evidence.to_dict()
        if not isinstance(payload, dict):
            raise TypeError("dbt runtime returned invalid evidence")
    except Exception as exc:
        code = getattr(exc, "code", None)
        if isinstance(code, str) and (code.startswith("DPONE_DBT_") or code == "COMMIT_UNKNOWN"):
            emit_failure(
                (
                    DbtPublishIssue(
                        code=code,
                        message="The verified dbt execution pack was rejected safely",
                        path=args.relative_pack_path,
                        remediation=_runtime_remediation(code),
                    ),
                ),
                args.format,
                stage="dbt_execute_pack",
            )
            return 1
        emit_internal_failure(args.format, stage="dbt_execute_pack")
        return 5
    print(json.dumps(payload, allow_nan=False, ensure_ascii=False, indent=2))
    return exit_code


def _common(
    parser: argparse.ArgumentParser,
    *,
    project_positional: bool = False,
    dbt_profiles: bool = False,
) -> None:
    if project_positional:
        parser.add_argument(
            "project",
            nargs="?",
            help="dbt project root (default: current directory)",
        )
    parser.add_argument("--manifest", help="Path to dbt target/manifest.json")
    parser.add_argument(
        "--project-dir",
        help="Compatibility alias for the dbt project root",
    )
    if dbt_profiles:
        parser.add_argument(
            "--dbt-profiles-dir",
            help="CI-only dbt profiles directory for authoritative selection",
        )
    parser.add_argument("--profiles", help="Trusted dpone dbt publish profile registry")
    parser.add_argument("--format", choices=["text", "json"], default="text")


def _build(
    args: argparse.Namespace,
    *,
    context: DbtPublishComposition,
    model_selector: str | None = None,
) -> DbtCompileReport:
    selected_manifest = manifest_path(args)
    stale = stale_default_manifest(args, selected_manifest)
    if stale is not None:
        return stale
    return context.build_dbt_publish_compiler(
        root=project_root(args),
        require_certified_routes=(getattr(args, "dbt_cmd", None) == "compile"),
    ).build(
        selected_manifest,
        profiles_path=args.profiles,
        require_contracts=not bool(args.allow_missing_contracts),
        allow_empty=bool(getattr(args, "allow_empty", False)) or model_selector is not None,
        model_selector=model_selector,
    )


def _execute_dbt_pack(relative_pack_path: str) -> DbtCliExecutionOutcome:
    reject_non_executable_semantic_template(relative_pack_path)
    runtime_service = importlib.import_module("dpone.runtime.dbt_execution")
    execute = getattr(runtime_service, "execute_dbt_pack")
    return cast(DbtCliExecutionOutcome, execute(relative_pack_path))


__all__ = [
    "cmd_check",
    "cmd_compile",
    "cmd_execute_pack",
    "cmd_explain",
    "register_check_parser",
    "register_compile_parser",
    "register_execute_pack_parser",
    "register_explain_parser",
]
