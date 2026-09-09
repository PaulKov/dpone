from __future__ import annotations

import argparse
import logging

from dpone.commands.output_json import write_json
from dpone.commands.output_text import write_text
from dpone.strategy_intelligence.certification import (
    StrategyCertificationArtifactWriter,
    StrategyCertificationMatrixService,
)
from dpone.strategy_intelligence.certification_bundle import (
    CertificationEvidenceInput,
    StrategyCertificationEvidenceBundleWriter,
)
from dpone.strategy_intelligence.native_transfer_evidence_service import NativeTransferEvidenceBundleService
from dpone.strategy_intelligence.preflight import NativeFastPathPreflightService
from dpone.strategy_intelligence.repair import RepairPlanService, RepairRequest
from dpone.strategy_intelligence.service import StrategyIntelligenceService


def cmd_strategy_advise(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = StrategyIntelligenceService().advise_manifest(
        args.path,
        estimated_rows=args.estimated_rows,
        changed_percent=args.changed_percent,
        delete_percent=args.delete_percent,
        cdc_available=args.cdc_available,
    )
    if args.format == "json":
        write_json(payload)
    elif args.format == "md":
        write_text(_render_md(payload))
    else:
        write_text(_render_text(payload))
    return 0


def register_advise_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("advise", help="Explain and recommend the safest load strategy")
    parser.add_argument("path")
    parser.add_argument("--estimated-rows", type=int)
    parser.add_argument("--changed-percent", type=float)
    parser.add_argument("--delete-percent", type=float)
    parser.add_argument("--cdc-available", action="store_true")
    parser.add_argument("--format", choices=["text", "json", "md"], default="text")
    return parser


def cmd_strategy_preflight(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    result = NativeFastPathPreflightService().check_path(args.source_type, args.sink_type)
    payload = result.to_dict()
    if args.format == "json":
        write_json(payload)
    elif args.format == "md":
        write_text(_render_preflight_md(payload))
    else:
        write_text(_render_preflight_text(payload))
    return 0 if result.ready or not args.fail_on_missing else 1


def register_preflight_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("preflight", help="Check native fast-path local tool availability")
    parser.add_argument("--source-type", required=True)
    parser.add_argument("--sink-type", required=True)
    parser.add_argument("--fail-on-missing", action="store_true")
    parser.add_argument("--format", choices=["text", "json", "md"], default="text")
    return parser


def cmd_strategy_repair_plan(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    plan = RepairPlanService().plan(
        RepairRequest(
            run_id=args.run_id,
            source_type=args.source_type,
            sink_type=args.sink_type,
            strategy_mode=args.strategy,
            failed_stage=args.failed_stage,
            partition_values=tuple(args.partition or ()),
        )
    )
    payload = plan.to_dict()
    if args.format == "json":
        write_json(payload)
    elif args.format == "md":
        write_text(_render_repair_md(payload))
    else:
        write_text(_render_repair_text(payload))
    return 0


def register_repair_plan_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("repair-plan", help="Build a safe repair/resync plan for a failed run")
    parser.add_argument("run_id")
    parser.add_argument("--source-type", required=True)
    parser.add_argument("--sink-type", required=True)
    parser.add_argument("--strategy", required=True)
    parser.add_argument("--failed-stage", default="finalize")
    parser.add_argument("--partition", action="append")
    parser.add_argument("--format", choices=["text", "json", "md"], default="text")
    return parser


def cmd_strategy_certification_artifact(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    matrix = StrategyCertificationMatrixService().build()
    artifact = StrategyCertificationArtifactWriter(base_dir=args.artifact_dir).write(matrix)
    payload = {"json_path": str(artifact.json_path), "markdown_path": str(artifact.markdown_path)}
    if args.format == "json":
        write_json(payload)
    elif args.format == "md":
        write_text(
            f"# dpone strategy certification artifact\n\n- JSON: `{artifact.json_path}`\n- Markdown: `{artifact.markdown_path}`\n"
        )
    else:
        write_text(
            f"dpone strategy certification artifact\n- json_path: {artifact.json_path}\n- markdown_path: {artifact.markdown_path}\n"
        )
    return 0


def register_certification_artifact_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("certification-artifact", help="Write strategy certification matrix artifacts")
    parser.add_argument("--artifact-dir", default="test_artifacts/strategies")
    parser.add_argument("--format", choices=["text", "json", "md"], default="text")
    return parser


def cmd_strategy_certification_bundle(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    artifact = StrategyCertificationEvidenceBundleWriter(args.output_dir).write(
        CertificationEvidenceInput(
            bundle_id=args.bundle_id,
            replay_evidence=tuple(args.replay_evidence or ()),
            matrix_artifacts=tuple(args.matrix_artifact or ()),
            connector_artifacts=tuple(args.connector_artifact or ()),
            benchmark_artifacts=tuple(args.benchmark_artifact or ()),
            native_transfer_evidence=tuple(args.native_transfer_evidence or ()),
            docs_links=tuple(args.docs_link or ()),
        )
    )
    payload = {"json_path": str(artifact.json_path), "markdown_path": str(artifact.markdown_path)}
    if args.format == "json":
        write_json(payload)
    elif args.format == "md":
        write_text(
            "# dpone strategy certification bundle\n\n"
            f"- JSON: `{artifact.json_path}`\n"
            f"- Markdown: `{artifact.markdown_path}`\n"
        )
    else:
        write_text(
            "dpone strategy certification bundle\n"
            f"- json_path: {artifact.json_path}\n"
            f"- markdown_path: {artifact.markdown_path}\n"
        )
    return 0


def register_certification_bundle_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("certification-bundle", help="Aggregate strategy certification evidence artifacts")
    parser.add_argument("--bundle-id", required=True)
    parser.add_argument("--output-dir", default="test_artifacts/strategies/certification_bundle")
    parser.add_argument("--replay-evidence", action="append")
    parser.add_argument("--matrix-artifact", action="append")
    parser.add_argument("--connector-artifact", action="append")
    parser.add_argument("--benchmark-artifact", action="append")
    parser.add_argument("--native-transfer-evidence", action="append")
    parser.add_argument("--docs-link", action="append")
    parser.add_argument("--format", choices=["text", "json", "md"], default="text")
    return parser


def cmd_strategy_native_transfer_evidence(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = NativeTransferEvidenceBundleService().write_from_files(
        run_id=args.run_id,
        plan_json=args.plan_json,
        contract_json=args.contract_json,
        payload_specs=tuple(args.payload or ()),
        output_dir=args.output_dir,
    )
    if args.format == "json":
        write_json(payload)
    elif args.format == "md":
        write_text(_render_native_transfer_evidence_md(payload))
    else:
        write_text(_render_native_transfer_evidence_text(payload))
    return 0


def register_native_transfer_evidence_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "native-transfer-evidence",
        help="Build checksumed native transfer evidence bundle from plan/contract and payload artifacts",
    )
    parser.add_argument("--run-id", required=True)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--plan-json")
    source.add_argument("--contract-json")
    parser.add_argument("--payload", action="append", help="Artifact payload as artifact_name=path")
    parser.add_argument("--output-dir", default="test_artifacts/native-transfer/evidence")
    parser.add_argument("--format", choices=["text", "json", "md"], default="text")
    return parser


def _render_text(payload: dict) -> str:
    decision = payload["decision"]
    lines = [
        "dpone strategy advise",
        f"- strategy: {decision['strategy_mode']}",
        f"- merge_policy: {decision['merge_policy']}",
        f"- native_fast_path: {decision['native_fast_path']}",
        f"- adaptive_batching: {decision['adaptive_batching']['enabled']}",
    ]
    for item in decision.get("reasons", []):
        lines.append(f"- reason: {item['code']} [{item['severity']}]: {item['message']}")
    for item in decision.get("warnings", []):
        lines.append(f"- warning: {item['code']} [{item['severity']}]: {item['message']}")
    return "\n".join(lines) + "\n"


def _render_md(payload: dict) -> str:
    decision = payload["decision"]
    lines = [
        "# dpone strategy advise",
        "",
        f"- strategy: `{decision['strategy_mode']}`",
        f"- merge_policy: `{decision['merge_policy']}`",
        f"- native_fast_path: `{decision['native_fast_path']}`",
        f"- adaptive_batching: `{decision['adaptive_batching']['enabled']}`",
        "",
        "## Reasons",
    ]
    for item in decision.get("reasons", []):
        lines.append(f"- `{item['code']}` ({item['severity']}): {item['message']}")
    return "\n".join(lines) + "\n"


def _render_preflight_text(payload: dict) -> str:
    lines = [f"dpone strategy preflight: {payload['path_id']}", f"- ready: {payload['ready']}"]
    for name, check in payload["tools"].items():
        lines.append(f"- {name}: {'ok' if check['available'] else 'missing'}")
    return "\n".join(lines) + "\n"


def _render_preflight_md(payload: dict) -> str:
    lines = ["# dpone strategy preflight", "", f"- path: `{payload['path_id']}`", f"- ready: `{payload['ready']}`", ""]
    for name, check in payload["tools"].items():
        lines.append(f"- `{name}`: `{'ok' if check['available'] else 'missing'}`")
    return "\n".join(lines) + "\n"


def _render_repair_text(payload: dict) -> str:
    lines = [
        f"dpone strategy repair-plan: {payload['run_id']}",
        f"- safe_to_auto_resume: {payload['safe_to_auto_resume']}",
    ]
    for command in payload["commands"]:
        lines.append(f"- command: {command}")
    return "\n".join(lines) + "\n"


def _render_repair_md(payload: dict) -> str:
    lines = [
        "# dpone strategy repair-plan",
        "",
        f"- run_id: `{payload['run_id']}`",
        f"- safe_to_auto_resume: `{payload['safe_to_auto_resume']}`",
        "",
        "## Commands",
    ]
    for command in payload["commands"]:
        lines.append(f"- `{command}`")
    return "\n".join(lines) + "\n"


def _render_native_transfer_evidence_text(payload: dict) -> str:
    return (
        "dpone strategy native-transfer-evidence\n"
        f"- run_id: {payload['run_id']}\n"
        f"- index_path: {payload['index_path']}\n"
        f"- markdown_path: {payload['markdown_path']}\n"
        f"- artifact_count: {len(payload['artifact_paths'])}\n"
    )


def _render_native_transfer_evidence_md(payload: dict) -> str:
    lines = [
        "# dpone strategy native-transfer-evidence",
        "",
        f"- run_id: `{payload['run_id']}`",
        f"- index_path: `{payload['index_path']}`",
        f"- markdown_path: `{payload['markdown_path']}`",
        f"- artifact_count: `{len(payload['artifact_paths'])}`",
    ]
    return "\n".join(lines) + "\n"
