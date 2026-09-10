from __future__ import annotations

import argparse
import logging

from dpone.commands.output_json import write_json
from dpone.commands.output_text import write_text
from dpone.commands.plan_contract_render import (
    render_evidence_contract_md as _render_evidence_contract_md,
)
from dpone.commands.plan_contract_render import (
    render_evidence_contract_text as _render_evidence_contract_text,
)
from dpone.commands.plan_contract_render import (
    render_external_target_contract_md as _render_external_target_contract_md,
)
from dpone.commands.plan_contract_render import (
    render_external_target_contract_text as _render_external_target_contract_text,
)
from dpone.commands.plan_contract_render import (
    render_replay_contract_md as _render_replay_contract_md,
)
from dpone.commands.plan_contract_render import (
    render_replay_contract_text as _render_replay_contract_text,
)
from dpone.commands.plan_contract_render import (
    render_transport_contract_md as _render_transport_contract_md,
)
from dpone.commands.plan_contract_render import (
    render_transport_contract_text as _render_transport_contract_text,
)
from dpone.commands.plan_mssql_native_render import render_mssql_native
from dpone.commands.plan_native_render import (
    render_columnar_fast_path_md,
    render_columnar_fast_path_text,
    render_native_bulk_wire_md,
    render_native_bulk_wire_text,
    render_snapshot_optimization_md,
    render_snapshot_optimization_text,
)
from dpone.readiness.managed import ExecutionPlanService


def _render_text(payload: dict) -> str:
    lines = [
        "dpone plan",
        f"- process: {payload['process']}",
        f"- source: {payload['source']['type']} {payload['source']['table']}",
        f"- sink: {payload['sink']['type']} {payload['sink']['table']}",
        f"- strategy: {payload['strategy']['mode']}",
        f"- bulk_path: {payload['bulk_path']}",
        f"- staging_first: {payload['staging']['staging_first']}",
        f"- schema_evolution: {payload['schema_evolution']['enabled']}",
        f"- type_inference: {payload['type_inference']['options']['enabled']}",
        f"- physical_design: {payload['physical_design']['options']['enabled']}",
    ]
    merge_policy = payload["strategy"].get("merge_policy")
    if merge_policy:
        lines.append(f"- merge_policy: {merge_policy}")
    schema_evolution = payload.get("schema_evolution") or {}
    if schema_evolution.get("runtime_execution"):
        lines.append(
            "- schema_evolution_execution: "
            f"{schema_evolution.get('runtime_execution')} "
            f"provisioning={schema_evolution.get('provisioning')} "
            f"authority={schema_evolution.get('authority')}"
        )
    lines.extend(render_mssql_native(payload.get("mssql_native") or {}))
    lines.extend(_render_external_target_contract_text(payload.get("physical_design") or {}))
    lines.extend(_render_runtime_storage_text(payload.get("runtime_storage") or {}))
    lines.extend(_render_native_execution_text(payload.get("native_transfer_execution") or {}))
    lines.extend(_render_native_transport_text(payload.get("native_transfer_transport") or {}))
    lines.extend(_render_native_route_decision_text(payload.get("native_transfer_route_decision") or {}))
    lines.extend(render_columnar_fast_path_text(payload.get("columnar_fast_path") or {}))
    lines.extend(render_native_bulk_wire_text(payload.get("native_transfer_bulk_wire") or {}))
    lines.extend(render_snapshot_optimization_text(payload.get("native_transfer_snapshot_optimization") or {}))
    lines.extend(_render_source_impact_text(payload.get("source_impact") or []))
    type_matrix = payload.get("type_matrix") or {}
    if type_matrix:
        lines.append(f"- type_matrix: available={type_matrix.get('available')} profile={type_matrix.get('profile')}")
        lines.append(f"- type_matrix_explain: {type_matrix.get('explain_command')}")
    for warning in payload.get("warnings", []):
        lines.append(f"- warning: {warning}")
    if payload.get("strategy_intelligence"):
        decision = payload["strategy_intelligence"].get("decision", {})
        lines.append(f"- strategy_decision: {decision.get('strategy_mode')}")
        lines.append(f"- native_fast_path: {decision.get('native_fast_path')}")
        native_transfer = decision.get("native_transfer_plan") or {}
        if native_transfer:
            lines.append(f"- native_transfer_export: {native_transfer.get('export_method')}")
            lines.append(f"- native_transfer_ingest: {native_transfer.get('ingest_method')}")
            lines.append(f"- native_transfer_finalizer: {native_transfer.get('finalizer')}")
            partitioning = native_transfer.get("partitioning", {})
            lines.append(f"- native_transfer_partitions: {partitioning.get('strategy')}:{partitioning.get('column')}")
            lines.extend(_render_transport_contract_text(native_transfer.get("transport_contract") or {}))
            lines.extend(_render_replay_contract_text(native_transfer.get("replay_contract") or {}))
            lines.extend(_render_evidence_contract_text(native_transfer.get("evidence_contract") or {}))
    return "\n".join(lines) + "\n"


def _render_md(payload: dict) -> str:
    lines = [
        "# dpone plan",
        "",
        f"- process: `{payload['process']}`",
        f"- source: `{payload['source']['type']} {payload['source']['table']}`",
        f"- sink: `{payload['sink']['type']} {payload['sink']['table']}`",
        f"- strategy: `{payload['strategy']['mode']}`",
        f"- bulk_path: `{payload['bulk_path']}`",
        f"- staging_first: `{payload['staging']['staging_first']}`",
        f"- type_inference: `{payload['type_inference']['options']['enabled']}`",
        f"- physical_design: `{payload['physical_design']['options']['enabled']}`",
        f"- type_matrix: `{(payload.get('type_matrix') or {}).get('profile')}`",
        f"- type_matrix_explain: `{(payload.get('type_matrix') or {}).get('explain_command')}`",
    ]
    merge_policy = payload["strategy"].get("merge_policy")
    if merge_policy:
        lines.append(f"- merge_policy: `{merge_policy}`")
    schema_evolution = payload.get("schema_evolution") or {}
    if schema_evolution.get("runtime_execution"):
        lines.append(
            f"- schema_evolution_execution: `{schema_evolution.get('runtime_execution')}` "
            f"provisioning=`{schema_evolution.get('provisioning')}` "
            f"authority=`{schema_evolution.get('authority')}`"
        )
    lines.extend(render_mssql_native(payload.get("mssql_native") or {}, markdown=True))
    lines.extend(_render_external_target_contract_md(payload.get("physical_design") or {}))
    strategy = payload.get("strategy_intelligence") or {}
    native_transfer = (strategy.get("decision") or {}).get("native_transfer_plan") or {}
    runtime_storage = payload.get("runtime_storage") or {}
    if runtime_storage:
        lines.extend(["", "## Runtime storage", ""])
        lines.extend(_render_runtime_storage_md(runtime_storage))
    native_execution = payload.get("native_transfer_execution") or {}
    if native_execution:
        lines.extend(["", "## Native transfer execution", ""])
        lines.extend(_render_native_execution_md(native_execution))
    native_transport = payload.get("native_transfer_transport") or {}
    if native_transport:
        lines.extend(["", "## Native transfer resolved transport", ""])
        lines.extend(_render_native_transport_md(native_transport))
    route_decision = payload.get("native_transfer_route_decision") or {}
    if route_decision:
        lines.extend(["", "## Native transfer route decision", ""])
        lines.extend(_render_native_route_decision_md(route_decision))
    columnar_fast_path = payload.get("columnar_fast_path") or {}
    if columnar_fast_path:
        lines.extend(["", "## Columnar fast path", ""])
        lines.extend(render_columnar_fast_path_md(columnar_fast_path))
    bulk_wire = payload.get("native_transfer_bulk_wire") or {}
    if bulk_wire:
        lines.extend(["", "## Native transfer bulk wire", ""])
        lines.extend(render_native_bulk_wire_md(bulk_wire))
    snapshot_optimization = payload.get("native_transfer_snapshot_optimization") or {}
    if snapshot_optimization:
        lines.extend(["", "## Native transfer snapshot optimization", ""])
        lines.extend(render_snapshot_optimization_md(snapshot_optimization))
    source_impact = payload.get("source_impact") or []
    if source_impact:
        lines.extend(["", "## Source impact diagnostics", ""])
        lines.extend(_render_source_impact_md(source_impact))
    contract = native_transfer.get("transport_contract") or {}
    if contract:
        lines.extend(["", "## Native transfer transport contract", ""])
        lines.extend(_render_transport_contract_md(contract))
    replay_contract = native_transfer.get("replay_contract") or {}
    if replay_contract:
        lines.extend(["", "## Native transfer replay contract", ""])
        lines.extend(_render_replay_contract_md(replay_contract))
    evidence_contract = native_transfer.get("evidence_contract") or {}
    if evidence_contract:
        lines.extend(["", "## Native transfer evidence contract", ""])
        lines.extend(_render_evidence_contract_md(evidence_contract))
    return "\n".join(lines) + "\n"


def _render_runtime_storage_text(storage: dict) -> list[str]:
    if not storage:
        return []
    return [
        f"- runtime_storage_profile: {storage.get('profile')}",
        f"- runtime_work_dir: {storage.get('work_dir')}",
        f"- runtime_evidence_dir: {storage.get('evidence_dir')}",
        f"- runtime_min_free_bytes: {storage.get('min_free_bytes')}",
        *(_render_transfer_store_text(storage.get("transfer_store") or {})),
    ]


def _render_runtime_storage_md(storage: dict) -> list[str]:
    return [
        f"- profile: `{storage.get('profile')}`",
        f"- work_dir: `{storage.get('work_dir')}`",
        f"- evidence_dir: `{storage.get('evidence_dir')}`",
        f"- checkpoint_dir: `{storage.get('checkpoint_dir')}`",
        f"- debug_dir: `{storage.get('debug_dir')}`",
        f"- min_free_bytes: `{storage.get('min_free_bytes')}`",
        *_render_transfer_store_md(storage.get("transfer_store") or {}),
    ]


def _render_transfer_store_text(store: dict) -> list[str]:
    if not store:
        return []
    return [
        f"- runtime_transfer_store: {store.get('type')} {store.get('uri')}",
        f"- runtime_transfer_store_cleanup: {(store.get('cleanup') or {}).get('temp_objects')}",
    ]


def _render_transfer_store_md(store: dict) -> list[str]:
    if not store:
        return []
    return [
        f"- transfer_store: `{store.get('type')}` `{store.get('uri')}`",
        f"- transfer_store_cleanup: `{(store.get('cleanup') or {}).get('temp_objects')}`",
    ]


def _render_native_execution_text(policy: dict) -> list[str]:
    if not policy:
        return []
    resource = policy.get("resource_policy") or {}
    transport = policy.get("transport") or {}
    return [
        f"- native_transfer_execution: {policy.get('mode')} profile={policy.get('profile')}",
        f"- native_transfer_cleanup: {policy.get('cleanup_policy')}",
        f"- native_transfer_resume: {policy.get('resume_policy')}",
        (
            "- native_transfer_stream_transport: "
            f"{transport.get('mode')} "
            f"prefer_streaming={transport.get('prefer_streaming')} "
            f"fallback_to_file={transport.get('fallback_to_file')}"
        ),
        f"- native_transfer_stream_buffer_bytes: {transport.get('stream_buffer_bytes')}",
        (
            "- native_transfer_resource_policy: "
            f"files={resource.get('max_active_files')} "
            f"active_bytes={resource.get('max_active_bytes')} "
            f"target_file={resource.get('target_file_bytes')} "
            f"max_file={resource.get('max_file_bytes')}"
        ),
    ]


def _render_native_execution_md(policy: dict) -> list[str]:
    resource = policy.get("resource_policy") or {}
    transport = policy.get("transport") or {}
    return [
        f"- mode: `{policy.get('mode')}`",
        f"- profile: `{policy.get('profile')}`",
        f"- cleanup_policy: `{policy.get('cleanup_policy')}`",
        f"- resume_policy: `{policy.get('resume_policy')}`",
        (
            f"- transport: `{transport.get('mode')}` "
            f"prefer_streaming=`{transport.get('prefer_streaming')}` "
            f"fallback_to_file=`{transport.get('fallback_to_file')}`"
        ),
        f"- stream_buffer_bytes: `{transport.get('stream_buffer_bytes')}`",
        f"- stream_checksum: `{transport.get('checksum')}`",
        f"- max_active_files: `{resource.get('max_active_files')}`",
        f"- max_active_bytes: `{resource.get('max_active_bytes')}`",
        f"- target_file_bytes: `{resource.get('target_file_bytes')}`",
        f"- max_file_bytes: `{resource.get('max_file_bytes')}`",
        f"- adaptive_sizing: `{resource.get('adaptive_sizing')}`",
    ]


def _render_native_transport_text(plan: dict) -> list[str]:
    if not plan:
        return []
    eligibility = plan.get("eligibility") or {}
    reasons = ",".join(str(item) for item in eligibility.get("reasons", []) or [])
    return [
        f"- native_transfer_resolved_transport: {plan.get('transport')}",
        f"- native_transfer_stream_eligibility: source={eligibility.get('source')} "
        f"sink={eligibility.get('sink')} codec={eligibility.get('codec')}",
        f"- native_transfer_stream_fallback_reason: {plan.get('fallback_reason')}",
        f"- native_transfer_stream_reasons: {reasons}",
    ]


def _render_native_transport_md(plan: dict) -> list[str]:
    eligibility = plan.get("eligibility") or {}
    return [
        f"- transport: `{plan.get('transport')}`",
        f"- fallback_reason: `{plan.get('fallback_reason')}`",
        f"- source_stream: `{eligibility.get('source')}`",
        f"- sink_stream: `{eligibility.get('sink')}`",
        f"- codec_stream: `{eligibility.get('codec')}`",
        f"- reasons: `{', '.join(str(item) for item in eligibility.get('reasons', []) or [])}`",
    ]


def _render_native_route_decision_text(decision: dict) -> list[str]:
    if not decision:
        return []
    chain = " -> ".join(str(item) for item in decision.get("fallback_chain", []) or [])
    blockers = ",".join(str(item) for item in decision.get("blockers", []) or [])
    warnings = ",".join(str(item) for item in decision.get("warnings", []) or [])
    return [
        (
            "- native_transfer_route_decision: "
            f"{decision.get('selected_transport')} "
            f"certification={decision.get('certification_status')} "
            f"gate={decision.get('release_gate')}"
        ),
        f"- native_transfer_route_certification_mode: {decision.get('certification_mode')}",
        f"- native_transfer_route_fallback_chain: {chain}",
        f"- native_transfer_route_blockers: {blockers}",
        f"- native_transfer_route_warnings: {warnings}",
    ]


def _render_native_route_decision_md(decision: dict) -> list[str]:
    chain = " -> ".join(str(item) for item in decision.get("fallback_chain", []) or [])
    blockers = ", ".join(str(item) for item in decision.get("blockers", []) or [])
    warnings = ", ".join(str(item) for item in decision.get("warnings", []) or [])
    return [
        f"- requested_transport: `{decision.get('requested_transport')}`",
        f"- selected_transport: `{decision.get('selected_transport')}`",
        f"- certification_mode: `{decision.get('certification_mode')}`",
        f"- certification_status: `{decision.get('certification_status')}`",
        f"- release_gate: `{decision.get('release_gate')}`",
        f"- fallback_chain: `{chain}`",
        f"- blockers: `{blockers}`",
        f"- warnings: `{warnings}`",
    ]


def _render_source_impact_text(items: list[dict]) -> list[str]:
    return [f"- source_impact: {item.get('code')} [{item.get('severity')}]" for item in items]


def _render_source_impact_md(items: list[dict]) -> list[str]:
    return [f"- `{item.get('code')}` ({item.get('severity')}): {item.get('action')}" for item in items]


def cmd_plan(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = ExecutionPlanService().plan_manifest(
        args.path,
        selector=args.selector,
        apply_safe_schema=bool(args.apply_safe_schema),
        explain_strategy=bool(args.explain_strategy),
    )
    if args.format == "json":
        write_json(payload)
    elif args.format == "md":
        write_text(_render_md(payload))
    else:
        write_text(_render_text(payload))
    return 0


def register_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("plan", help="Render dry-run execution plan for a manifest process")
    parser.add_argument("path")
    parser.add_argument("--selector")
    parser.add_argument("--apply-safe-schema", action="store_true")
    parser.add_argument("--explain-strategy", action="store_true")
    parser.add_argument("--format", choices=["text", "json", "md"], default="text")
    return parser
