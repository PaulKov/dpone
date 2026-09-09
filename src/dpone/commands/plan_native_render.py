from __future__ import annotations


def render_native_bulk_wire_text(contract: dict) -> list[str]:
    if not contract:
        return []
    lines = [
        (
            "- native_transfer_bulk_wire: "
            f"{contract.get('selected_route')} "
            f"format={contract.get('input_format')} "
            f"source_escaping={contract.get('mssql_source_escaping')}"
        ),
        f"- native_transfer_bulk_wire_schema: {contract.get('schema_hash')}",
    ]
    lines.extend(_render_acceleration_text(contract.get("acceleration") or {}))
    lines.extend(f"- native_transfer_bulk_wire_warning: {item}" for item in contract.get("warnings", []) or [])
    lines.extend(f"- native_transfer_bulk_wire_blocker: {item}" for item in contract.get("blockers", []) or [])
    return lines


def render_native_bulk_wire_md(contract: dict) -> list[str]:
    delimiter = contract.get("delimiter_profile") or {}
    lines = [
        f"- selected_route: `{contract.get('selected_route')}`",
        f"- requested_mode: `{contract.get('requested_mode')}`",
        f"- input_format: `{contract.get('input_format')}`",
        f"- delimiter_profile: `{delimiter.get('name')}`",
        f"- mssql_source_escaping: `{contract.get('mssql_source_escaping')}`",
        f"- schema_hash: `{contract.get('schema_hash')}`",
    ]
    warnings = ", ".join(str(item) for item in contract.get("warnings", []) or [])
    blockers = ", ".join(str(item) for item in contract.get("blockers", []) or [])
    lines.append(f"- warnings: `{warnings}`")
    lines.append(f"- blockers: `{blockers}`")
    lines.extend(_render_acceleration_md(contract.get("acceleration") or {}))
    return lines


def render_snapshot_optimization_text(decision: dict) -> list[str]:
    if not decision:
        return []
    chain = " -> ".join(str(item) for item in decision.get("fallback_chain", []) or [])
    lines = [
        (
            "- native_transfer_snapshot_backend: "
            f"{decision.get('selected_backend')} "
            f"native_tcp_backend={decision.get('native_tcp_backend')} "
            f"compression={decision.get('compression')} "
            f"gate={decision.get('release_gate')}"
        ),
        (
            "- native_transfer_snapshot_parallelism: "
            f"exports={decision.get('max_parallel_exports')} "
            f"loads={decision.get('max_parallel_loads')}"
        ),
        f"- native_transfer_snapshot_fallback_chain: {chain}",
        (
            "- native_transfer_snapshot_partition_planner: "
            f"{decision.get('partition_planner')} confidence={decision.get('stats_confidence')}"
        ),
    ]
    if decision.get("fallback_reason"):
        lines.append(f"- native_transfer_snapshot_fallback_reason: {decision.get('fallback_reason')}")
    lines.extend(_render_source_scan_text(decision.get("source_scan") or {}))
    lines.extend(_render_source_export_optimizer_text(decision.get("source_export_optimizer") or {}))
    lines.extend(f"- native_transfer_snapshot_warning: {item}" for item in decision.get("warnings", []) or [])
    lines.extend(f"- native_transfer_snapshot_blocker: {item}" for item in decision.get("blockers", []) or [])
    lines.extend(f"- native_transfer_snapshot_reason: {item}" for item in decision.get("reasons", []) or [])
    return lines


def render_snapshot_optimization_md(decision: dict) -> list[str]:
    if not decision:
        return []
    chain = " -> ".join(str(item) for item in decision.get("fallback_chain", []) or [])
    warnings = ", ".join(str(item) for item in decision.get("warnings", []) or [])
    blockers = ", ".join(str(item) for item in decision.get("blockers", []) or [])
    reasons = ", ".join(str(item) for item in decision.get("reasons", []) or [])
    lines = [
        f"- requested_backend: `{decision.get('requested_backend')}`",
        f"- selected_backend: `{decision.get('selected_backend')}`",
        f"- native_tcp_backend: `{decision.get('native_tcp_backend')}`",
        f"- compression: `{decision.get('compression')}`",
        f"- packet_size: `{decision.get('packet_size')}`",
        f"- block_rows: `{decision.get('block_rows')}`",
        f"- block_bytes: `{decision.get('block_bytes')}`",
        (
            f"- parallelism: exports=`{decision.get('max_parallel_exports')}` "
            f"loads=`{decision.get('max_parallel_loads')}`"
        ),
        f"- release_gate: `{decision.get('release_gate')}`",
        f"- fallback_chain: `{chain}`",
        f"- partition_planner: `{decision.get('partition_planner')}`",
        f"- stats_confidence: `{decision.get('stats_confidence')}`",
        f"- fallback_reason: `{_display_value(decision.get('fallback_reason'))}`",
        f"- warnings: `{warnings}`",
        f"- blockers: `{blockers}`",
        f"- reasons: `{reasons}`",
    ]
    lines.extend(_render_source_scan_md(decision.get("source_scan") or {}))
    lines.extend(_render_source_export_optimizer_md(decision.get("source_export_optimizer") or {}))
    return lines


def render_columnar_fast_path_text(decision: dict) -> list[str]:
    if not decision:
        return []
    lines = [
        (
            "- columnar_fast_path: "
            f"requested={decision.get('requested_provider')} "
            f"mode={decision.get('requested_mode')} "
            f"selected={decision.get('selected_provider')} "
            f"start_source_io={decision.get('should_start_source_io')}"
        )
    ]
    lines.extend(f"- columnar_fast_path_warning: {item}" for item in decision.get("warnings", []) or [])
    lines.extend(f"- columnar_fast_path_blocker: {item}" for item in decision.get("blockers", []) or [])
    return lines


def render_columnar_fast_path_md(decision: dict) -> list[str]:
    if not decision:
        return []
    warnings = ", ".join(str(item) for item in decision.get("warnings", []) or [])
    blockers = ", ".join(str(item) for item in decision.get("blockers", []) or [])
    return [
        f"- requested_mode: `{decision.get('requested_mode')}`",
        f"- requested_provider: `{decision.get('requested_provider')}`",
        f"- selected_provider: `{decision.get('selected_provider')}`",
        f"- should_start_source_io: `{decision.get('should_start_source_io')}`",
        f"- warnings: `{warnings}`",
        f"- blockers: `{blockers}`",
    ]


def _render_acceleration_text(acceleration: dict) -> list[str]:
    if not acceleration:
        return []
    return [
        (
            "- native_transfer_acceleration: "
            f"{acceleration.get('selected_backend')} "
            f"mode={acceleration.get('requested_mode')} "
            f"fallback={_display_value(acceleration.get('fallback_reason'))}"
        ),
        f"- native_transfer_acceleration_version: {_display_value(acceleration.get('accelerator_version'))}",
    ]


def _render_acceleration_md(acceleration: dict) -> list[str]:
    if not acceleration:
        return []
    return [
        f"- acceleration_backend: `{acceleration.get('selected_backend')}`",
        f"- acceleration_mode: `{acceleration.get('requested_mode')}`",
        f"- acceleration_fallback: `{_display_value(acceleration.get('fallback_reason'))}`",
        f"- acceleration_version: `{_display_value(acceleration.get('accelerator_version'))}`",
    ]


def _display_value(value: object) -> str:
    return "none" if value is None or value == "" else str(value)


def _render_source_scan_text(source_scan: dict) -> list[str]:
    if not source_scan:
        return []
    chunking = source_scan.get("physical_chunking") or {}
    return [
        (
            "- native_transfer_source_scan: "
            f"{source_scan.get('selected_scan')} "
            f"table={source_scan.get('table_kind')} "
            f"chunking={chunking.get('enabled')} "
            f"target_chunk_bytes={chunking.get('target_chunk_bytes')}"
        )
    ]


def _render_source_scan_md(source_scan: dict) -> list[str]:
    if not source_scan:
        return []
    chunking = source_scan.get("physical_chunking") or {}
    return [
        f"- source_scan: `{source_scan.get('selected_scan')}`",
        f"- source_scan_table_kind: `{source_scan.get('table_kind')}`",
        f"- source_scan_chunking: `{chunking.get('enabled')}`",
        f"- source_scan_target_chunk_bytes: `{chunking.get('target_chunk_bytes')}`",
    ]


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
        f"- source_export_provider: `{export_optimizer.get('selected_provider')}`",
        f"- source_export_default: `{export_optimizer.get('current_default')}`",
        f"- source_export_speedup_pct: `{export_optimizer.get('measured_speedup_pct')}`",
        f"- source_export_bottleneck: `{export_optimizer.get('source_bottleneck')}`",
    ]
    rejected = export_optimizer.get("rejected") or {}
    if isinstance(rejected, dict):
        rejected_text = ", ".join(f"{provider}={reason}" for provider, reason in sorted(rejected.items()))
        lines.append(f"- source_export_rejected: `{rejected_text}`")
    return lines


__all__ = [
    "render_columnar_fast_path_md",
    "render_columnar_fast_path_text",
    "render_native_bulk_wire_md",
    "render_native_bulk_wire_text",
    "render_snapshot_optimization_md",
    "render_snapshot_optimization_text",
]
