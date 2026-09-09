"""Runtime source export optimizer integration for MSSQL queryout."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from dpone.runtime.decision_audit import publish_runtime_decision
from dpone.runtime.export_benchmark import ExportProbeRequest, SourceExportBenchmarkService
from dpone.runtime.export_optimizer_models import ExportOptimizerPolicy, parse_export_probes
from dpone.runtime.source_export_optimizer import SourceExportOptimizer
from dpone.runtime.sources.strategies.mssql.mssql_export_benchmark import (
    MssqlBcpProbeRunner,
    MssqlOdbcArrayProbeRunner,
)
from dpone.runtime.storage_policy import RuntimeStoragePolicy


def resolve_mssql_export_provider(
    *,
    connector: Any,
    load_config: Any,
    query: str,
    schema: list[tuple[str, str]],
    current_default: str,
) -> str | None:
    """Return selected source export provider when optimizer is configured."""

    snapshot = _snapshot_mapping(load_config.options)
    if "export_optimizer" not in snapshot and "export_optimizer_probes" not in snapshot:
        return None
    policy = ExportOptimizerPolicy.from_source_options(load_config.options)
    if policy.mode == "off":
        return None
    probes = parse_export_probes(snapshot)
    if probes:
        decision = SourceExportOptimizer().decide(policy, current_default=current_default, probes=probes)
    else:
        work_dir = RuntimeStoragePolicy.from_options(load_config.options).work_dir
        decision = SourceExportBenchmarkService(_runners(connector, policy), SourceExportOptimizer()).benchmark(
            policy,
            current_default=current_default,
            request=ExportProbeRequest(
                source_type="mssql",
                query=query,
                schema=tuple(schema),
                work_dir=work_dir,
                probe_rows=policy.probe_rows,
                max_probe_seconds=policy.max_probe_seconds,
                batch_size=max(1, load_config.batch_size),
            ),
        )
    publish_runtime_decision(
        decision,
        decision_id="source_export.optimizer",
        phase="extract",
        component="mssql_source",
        category="provider_selection",
        fallback_allowed=policy.mode == "auto" and not decision.blockers,
        details={"current_default": current_default, "policy": policy.to_dict()},
    )
    if decision.release_gate == "blocked":
        raise ValueError(", ".join(decision.blockers))
    return decision.selected_provider


def _runners(connector: Any, policy: ExportOptimizerPolicy) -> tuple[Any, ...]:
    packet_sizes = policy.bcp_probe_packets
    return (
        MssqlBcpProbeRunner(
            provider_id="mssql_bcp_native",
            connector=connector,
            file_format="native",
            packet_sizes=packet_sizes,
        ),
        MssqlBcpProbeRunner(
            provider_id="mssql_bcp_character_raw",
            connector=connector,
            file_format="character",
            packet_sizes=packet_sizes,
        ),
        MssqlOdbcArrayProbeRunner(connector=connector, fetch_size=policy.odbc_fetch_size),
    )


def _snapshot_mapping(source_options: Mapping[str, Any] | None) -> Mapping[str, Any]:
    native = (source_options or {}).get("native_transfer")
    if not isinstance(native, Mapping):
        return {}
    snapshot = native.get("snapshot")
    return snapshot if isinstance(snapshot, Mapping) else {}


__all__ = ["resolve_mssql_export_provider"]
