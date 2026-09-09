from __future__ import annotations

import argparse
import logging

from dpone.commands.output_json import write_json
from dpone.commands.output_text import write_text
from dpone.readiness.managed import ExecutionPlanService, PerformanceAdvisor
from dpone.strategy_intelligence.service import StrategyIntelligenceService


def cmd_perf_advise(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    strategy_payload = StrategyIntelligenceService().advise_manifest(args.path)
    try:
        plan_payload = ExecutionPlanService().plan_manifest(args.path, selector=args.selector)
    except Exception:  # noqa: BLE001 - route decision is advisory in perf output.
        plan_payload = {}
    try:
        recommendations = PerformanceAdvisor().advise_manifest(args.path, selector=args.selector)
    except Exception:  # noqa: BLE001 - perf advice must stay useful for plan-only/auto manifests.
        recommendations = []
    route_decision = plan_payload.get("native_transfer_route_decision")
    route_decision_payload = route_decision if isinstance(route_decision, dict) else {}
    acceleration_payload = _native_acceleration_from_strategy(strategy_payload)
    snapshot_payload = _snapshot_optimization_from_strategy(strategy_payload)
    payload = {
        "recommendations": [item.to_dict() for item in recommendations],
        "strategy_intelligence": strategy_payload,
        "native_transfer_route_decision": route_decision_payload,
        "native_transfer_acceleration": acceleration_payload,
        "native_transfer_snapshot_optimization": snapshot_payload,
    }
    if args.format == "json":
        write_json(payload)
    elif args.format == "md":
        lines = ["# dpone perf advise", ""]
        decision = strategy_payload["decision"]
        lines.append(f"- strategy_intelligence: `{decision['strategy_mode']}` via `{decision['native_fast_path']}`")
        lines.extend(_render_route_decision_md(route_decision_payload))
        lines.extend(_render_acceleration_md(acceleration_payload))
        lines.extend(_render_snapshot_optimization_md(snapshot_payload))
        for item in recommendations:
            lines.append(f"- `{item.code}` ({item.severity}): {item.message} Action: {item.action}")
        write_text("\n".join(lines) + "\n")
    else:
        lines = ["dpone perf advise"]
        decision = strategy_payload["decision"]
        lines.append(f"- strategy_intelligence: {decision['strategy_mode']} via {decision['native_fast_path']}")
        lines.extend(_render_route_decision_text(route_decision_payload))
        lines.extend(_render_acceleration_text(acceleration_payload))
        lines.extend(_render_snapshot_optimization_text(snapshot_payload))
        for item in recommendations:
            lines.append(f"- {item.code} [{item.severity}]: {item.message} -> {item.action}")
        write_text("\n".join(lines) + "\n")
    return 0


def _native_acceleration_from_strategy(strategy_payload: dict) -> dict:
    decision = strategy_payload.get("decision") or {}
    native_transfer = decision.get("native_transfer_plan") or {}
    settings = native_transfer.get("native_ingest_settings") or {}
    bulk_wire = settings.get("bulk_wire") or {}
    acceleration = bulk_wire.get("acceleration")
    return acceleration if isinstance(acceleration, dict) else {}


def _snapshot_optimization_from_strategy(strategy_payload: dict) -> dict:
    decision = strategy_payload.get("decision") or {}
    native_transfer = decision.get("native_transfer_plan") or {}
    settings = native_transfer.get("native_ingest_settings") or {}
    snapshot = settings.get("snapshot_optimization")
    return snapshot if isinstance(snapshot, dict) else {}


def _render_snapshot_optimization_text(snapshot: dict) -> list[str]:
    if not snapshot:
        return []
    lines = [
        (
            "- native_transfer_snapshot_backend: "
            f"{snapshot.get('selected_backend')} "
            f"native_tcp_backend={snapshot.get('native_tcp_backend')} "
            f"compression={snapshot.get('compression')} "
            f"gate={snapshot.get('release_gate')}"
        ),
        (
            "- native_transfer_snapshot_parallelism: "
            f"exports={snapshot.get('max_parallel_exports')} "
            f"loads={snapshot.get('max_parallel_loads')}"
        ),
        (
            "- native_transfer_snapshot_partition_planner: "
            f"{snapshot.get('partition_planner')} confidence={snapshot.get('stats_confidence')}"
        ),
    ]
    lines.extend(_render_source_scan_text(snapshot.get("source_scan") or {}))
    lines.extend(_render_source_export_optimizer_text(snapshot.get("source_export_optimizer") or {}))
    lines.extend(_render_source_materialization_text(snapshot.get("source_materialization") or {}))
    return lines


def _render_snapshot_optimization_md(snapshot: dict) -> list[str]:
    if not snapshot:
        return []
    lines = [
        (
            f"- native_transfer_snapshot_backend: `{snapshot.get('selected_backend')}` "
            f"native_tcp_backend=`{snapshot.get('native_tcp_backend')}` "
            f"compression=`{snapshot.get('compression')}` "
            f"gate=`{snapshot.get('release_gate')}`"
        ),
        (
            "- native_transfer_snapshot_parallelism: "
            f"exports=`{snapshot.get('max_parallel_exports')}` "
            f"loads=`{snapshot.get('max_parallel_loads')}`"
        ),
        (
            "- native_transfer_snapshot_partition_planner: "
            f"`{snapshot.get('partition_planner')}` confidence=`{snapshot.get('stats_confidence')}`"
        ),
    ]
    lines.extend(_render_source_scan_md(snapshot.get("source_scan") or {}))
    lines.extend(_render_source_export_optimizer_md(snapshot.get("source_export_optimizer") or {}))
    lines.extend(_render_source_materialization_md(snapshot.get("source_materialization") or {}))
    return lines


def _render_source_scan_text(source_scan: dict) -> list[str]:
    if not source_scan:
        return []
    chunking = source_scan.get("physical_chunking") or {}
    lines = [
        (
            "- native_transfer_source_scan: "
            f"{source_scan.get('selected_scan')} "
            f"table={source_scan.get('table_kind')} "
            f"chunking={chunking.get('enabled')} "
            f"target_chunk_bytes={chunking.get('target_chunk_bytes')}"
        )
    ]
    lines.extend(f"- native_transfer_source_scan_warning: {item}" for item in source_scan.get("warnings", []) or [])
    lines.extend(f"- native_transfer_source_scan_blocker: {item}" for item in source_scan.get("blockers", []) or [])
    return lines


def _render_source_scan_md(source_scan: dict) -> list[str]:
    if not source_scan:
        return []
    chunking = source_scan.get("physical_chunking") or {}
    lines = [
        (
            "- native_transfer_source_scan: "
            f"`{source_scan.get('selected_scan')}` "
            f"table=`{source_scan.get('table_kind')}` "
            f"chunking=`{chunking.get('enabled')}` "
            f"target_chunk_bytes=`{chunking.get('target_chunk_bytes')}`"
        )
    ]
    warnings = ", ".join(str(item) for item in source_scan.get("warnings", []) or [])
    blockers = ", ".join(str(item) for item in source_scan.get("blockers", []) or [])
    lines.append(f"- native_transfer_source_scan_warnings: `{warnings}`")
    lines.append(f"- native_transfer_source_scan_blockers: `{blockers}`")
    return lines


def _render_source_export_optimizer_text(export_optimizer: dict) -> list[str]:
    if not export_optimizer:
        return []
    lines = [
        (
            "- native_transfer_source_export_optimizer: "
            f"{export_optimizer.get('selected_provider')} "
            f"default={export_optimizer.get('current_default')} "
            f"speedup={export_optimizer.get('measured_speedup_pct')}% "
            f"bottleneck={export_optimizer.get('source_bottleneck')}"
        )
    ]
    rejected = export_optimizer.get("rejected") or {}
    if isinstance(rejected, dict):
        lines.extend(
            f"- native_transfer_source_export_rejected: {provider}={reason}"
            for provider, reason in sorted(rejected.items())
        )
    return lines


def _render_source_export_optimizer_md(export_optimizer: dict) -> list[str]:
    if not export_optimizer:
        return []
    lines = [
        (
            "- native_transfer_source_export_optimizer: "
            f"`{export_optimizer.get('selected_provider')}` "
            f"default=`{export_optimizer.get('current_default')}` "
            f"speedup=`{export_optimizer.get('measured_speedup_pct')}%` "
            f"bottleneck=`{export_optimizer.get('source_bottleneck')}`"
        )
    ]
    rejected = export_optimizer.get("rejected") or {}
    if isinstance(rejected, dict):
        rejected_text = ", ".join(f"{provider}={reason}" for provider, reason in sorted(rejected.items()))
        lines.append(f"- native_transfer_source_export_rejected: `{rejected_text}`")
    return lines


def _render_source_materialization_text(materialization: dict) -> list[str]:
    if not materialization:
        return []
    lines = [
        (
            "- native_transfer_source_materialization: "
            f"{materialization.get('provider')} "
            f"selected={materialization.get('selected')} "
            f"gate={materialization.get('release_gate')} "
            f"work_database={materialization.get('work_database')} "
            f"work_schema={materialization.get('work_schema')} "
            f"cleanup={materialization.get('cleanup_policy')} "
            f"speedup={materialization.get('measured_speedup_pct')}%"
        )
    ]
    lines.extend(
        f"- native_transfer_source_materialization_reason: {item}" for item in materialization.get("reasons", []) or []
    )
    lines.extend(
        f"- native_transfer_source_materialization_warning: {item}"
        for item in materialization.get("warnings", []) or []
    )
    lines.extend(
        f"- native_transfer_source_materialization_blocker: {item}"
        for item in materialization.get("blockers", []) or []
    )
    return lines


def _render_source_materialization_md(materialization: dict) -> list[str]:
    if not materialization:
        return []
    lines = [
        (
            "- native_transfer_source_materialization: "
            f"`{materialization.get('provider')}` "
            f"selected=`{materialization.get('selected')}` "
            f"gate=`{materialization.get('release_gate')}` "
            f"work_database=`{materialization.get('work_database')}` "
            f"work_schema=`{materialization.get('work_schema')}` "
            f"cleanup=`{materialization.get('cleanup_policy')}` "
            f"speedup=`{materialization.get('measured_speedup_pct')}%`"
        )
    ]
    reasons = ", ".join(str(item) for item in materialization.get("reasons", []) or [])
    warnings = ", ".join(str(item) for item in materialization.get("warnings", []) or [])
    blockers = ", ".join(str(item) for item in materialization.get("blockers", []) or [])
    lines.append(f"- native_transfer_source_materialization_reasons: `{reasons}`")
    lines.append(f"- native_transfer_source_materialization_warnings: `{warnings}`")
    lines.append(f"- native_transfer_source_materialization_blockers: `{blockers}`")
    return lines


def _render_acceleration_text(acceleration: dict) -> list[str]:
    if not acceleration:
        return []
    return [
        (
            "- native_transfer_acceleration: "
            f"{acceleration.get('selected_backend')} "
            f"mode={acceleration.get('requested_mode')} "
            f"fallback={_display_value(acceleration.get('fallback_reason'))}"
        )
    ]


def _render_acceleration_md(acceleration: dict) -> list[str]:
    if not acceleration:
        return []
    return [
        (
            f"- native_transfer_acceleration: `{acceleration.get('selected_backend')}` "
            f"mode=`{acceleration.get('requested_mode')}` "
            f"fallback=`{_display_value(acceleration.get('fallback_reason'))}`"
        )
    ]


def _display_value(value: object) -> str:
    return "none" if value is None or value == "" else str(value)


def _render_route_decision_text(decision: dict) -> list[str]:
    if not decision:
        return []
    chain = " -> ".join(str(item) for item in decision.get("fallback_chain", []) or [])
    return [
        f"- native_transfer_route_decision: {decision.get('selected_transport')} gate={decision.get('release_gate')}",
        f"- native_transfer_route_certification: {decision.get('certification_status')}",
        f"- native_transfer_route_fallback_chain: {chain}",
    ]


def _render_route_decision_md(decision: dict) -> list[str]:
    if not decision:
        return []
    chain = " -> ".join(str(item) for item in decision.get("fallback_chain", []) or [])
    return [
        f"- native_transfer_route_decision: `{decision.get('selected_transport')}`",
        f"- native_transfer_route_certification: `{decision.get('certification_status')}`",
        f"- native_transfer_route_fallback_chain: `{chain}`",
    ]


def register_advise_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("advise", help="Recommend native bulk and partitioning optimizations")
    parser.add_argument("path")
    parser.add_argument("--selector")
    parser.add_argument("--format", choices=["text", "json", "md"], default="text")
    return parser
